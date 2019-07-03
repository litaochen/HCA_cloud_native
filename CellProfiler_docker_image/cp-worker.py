from __future__ import print_function
import boto3
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time
import watchtower
import string
from boto3.dynamodb.conditions import Key, Attr

import s3worker
import post_run_processor as prp

###################################
# Static info from initialization
###################################
app_config = {}
app_config['S3FS_CREDENTIAL_FILE'] = os.environ['S3FS_CREDENTIAL_FILE']
app_config['IMAGE_DATA_BUCKET_DIR'] = os.environ['IMAGE_DATA_BUCKET_DIR']
app_config['TASK_INPUT_DIR'] = os.environ['TASK_INPUT_DIR']
app_config['TASK_OUTPUT_DIR'] = os.environ['TASK_OUTPUT_DIR']

app_config['QUEUE_URL'] = os.environ['SQS_URL']
app_config['LOG_GROUP_NAME'] = os.environ['CLOUDWATCH_LOG_GROUP_NAME']
app_config['LOG_STREAM_NAME'] = os.environ['CLOUDWATCH_LOG_STREAM_NAME']


######################################
# Task specific info from sqs message
# variables grouped by bucket
######################################
task_config = {}
task_config['run_id'] = ''
task_config['task_id'] = ''
task_config['image_data_bucket'] = ''
task_config['image_data_prefix'] = ''

task_config['cp_pipeline_file_bucket'] = ''
task_config['cp_pipeline_file_key'] = ''

task_config['run_record_bucket'] = ''
task_config['image_list_file_key'] = ''
task_config['task_input_prefix'] = ''
task_config['task_output_prefix'] = ''
task_config['image_list_local_copy'] = ''
task_config['pipeline_file_local_copy'] = ''

#################################
# CLASS TO HANDLE THE SQS QUEUE
#################################


#################################
# AUXILIARY FUNCTIONS
#################################


def monitorAndLog(process, logger):
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
            logger.info(output)


def printandlog(text, logger):
    print(text)
    logger.info(text)

#################################
# RUN CELLPROFILER PROCESS
#################################


# order of actions:
# - get task message from the queue
# - mount image data bucket
# - download image flie list and pipeline file
# - start the work and log outout
# - upload output back to S3
# - unmount image data bucket and clean up files
# - update task status in dynamoDB
# - check if the whole run is done, if yes, run result consolidation

# main work loop
def main():
    queue = JobQueue(app_config['QUEUE_URL'])
    # Main loop. Keep reading messages while they are available in SQS
    while True:
        print("getting next task")
        msg, handle = queue.readMessage()
        if msg is None:
            break
        if msg['task_id'][0] == 'B':
            prepare_for_task(msg)
            print("current task_id: " + task_config['task_id'])
            cp_run_command = build_cp_run_command()
            print('Start the analysis with command: ', cp_run_command)
            os.system(cp_run_command)
            queue.deleteMessage(handle)
            # result = subprocess.check_output(cp_run_command.split(), stderr=subprocess.STDOUT)
            # print(result)

            upload_result_and_clean_up()
        else:
            queue.deleteMessage(handle)
            print("not column B, skipped")

# prepare for task per the message from sqs
# it does the things below:
#   - set up variables per task. other part of the code will use those variables
#   - mount image s3 bucket to IMAGE_DATA_BUCKET_DIR
#   - download image file list and pipeline file


def prepare_for_task(message):
    global task_config
    task_config['run_id'] = message['run_id']
    task_config['task_id'] = message['task_id']
    task_config['image_data_bucket'] = message["image_data"]["s3_bucket"]
    task_config['image_data_prefix'] = message["image_data"]["prefix"]

    task_config['cp_pipeline_file_bucket'] = message['pipeline_file']['s3_bucket']
    task_config['cp_pipeline_file_key'] = message['pipeline_file']['key']

    task_config['run_record_bucket'] = message["run_record_location"]["s3_bucket"]
    task_config['image_list_file_key'] = message['file_list_key']
    task_config['task_input_prefix'] = message['task_input_prefix']
    task_config['task_output_prefix'] = message['task_output_prefix']

    mount_s3_bucket_command = ("s3fs " + task_config['image_data_bucket'] + " " + app_config['IMAGE_DATA_BUCKET_DIR'] +
                               " -o passwd_file=" + app_config['S3FS_CREDENTIAL_FILE'])
    os.system(mount_s3_bucket_command)
    if len(os.listdir(app_config['IMAGE_DATA_BUCKET_DIR'])) == 0:
        print("mount failed")
    else:
        print("mount successfully")

    s3 = boto3.client('s3')
    print("downloading image list...")
    task_config['image_list_local_copy'] = s3worker.download_file(s3, task_config['run_record_bucket'],
                                                                  task_config['image_list_file_key'], app_config['TASK_INPUT_DIR'])

    print("downloading pileline file...")
    task_config['pipeline_file_local_copy'] = s3worker.download_file(s3, task_config['cp_pipeline_file_bucket'],
                                                                     task_config['cp_pipeline_file_key'], app_config['TASK_INPUT_DIR'])


# post processing after CellProfiler finish the analysis
# it does the things below:
#   - upload result to corresponding s3 bucket
#   - clean up temporary files on local disk
#   - unmount image s3 bucket from IMAGE_DATA_BUCKET_DIR
def upload_result_and_clean_up():
    s3 = boto3.client('s3')

    print(os.listdir(app_config['TASK_OUTPUT_DIR']))
    upload_result_command = ("aws s3 mv " + app_config['TASK_OUTPUT_DIR'] +
                             " \"s3://" + task_config['run_record_bucket'] + '/' + task_config['task_output_prefix'] +
                             "\" --recursive")
    print("moving data to S3 with command: \n" + upload_result_command)
    os.system(upload_result_command)
    print(os.listdir(app_config['TASK_OUTPUT_DIR']))

    if len(os.listdir(app_config['TASK_OUTPUT_DIR'])) != 0:
        os.system('rm -r ' + app_config['TASK_OUTPUT_DIR'] + '/*')
    os.system("umount " + app_config['IMAGE_DATA_BUCKET_DIR'])

    # reset task-related config. To be sure does not affect previous run
    for key, value in task_config.iteritems():
        task_config[key] = ''


# construct CellProfiler run command
def build_cp_run_command():
    cmdstem = ""
    with open(task_config['pipeline_file_local_copy']) as openpipe:
        for line in openpipe:
            if 'DateRevision:2' in line:
                print('comes from a CP2 pipeline')
                cmdstem = 'cellprofiler -c -r -b '
                break

    cmdstem = 'cellprofiler -c -r '
    cp_done_file = app_config['TASK_OUTPUT_DIR'] + '/done.txt'

    cp_run_command = (cmdstem + " -p " + "'" + task_config['pipeline_file_local_copy'] + "'" +
                      " --data-file=" + "'" + task_config['image_list_local_copy'] + "'" +
                      " -o " + "'" + app_config['TASK_OUTPUT_DIR'] + "'" +
                      " -d " + "'" + cp_done_file) + "'"
    return cp_run_command
    # logger.info(cmd)

    # subp = subprocess.Popen(
    #     cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # monitorAndLog(subp, logger)


# update task status
def update_task_status(status):
    table = boto3.resource('dynamodb').Table('tasks')

    # update status
    response = table.update_item(
        Key={
            'run_id': task_config['run_id'],
            'task_id': task_config['task_id']
        },
        UpdateExpression="set the_status = :new_status",
        ExpressionAttributeValues={
            ':new_status': 'finished'
        },
        ReturnValues="ALL_NEW"
    )

    print(response)


# update run status
def update_run_status(status):
    table = boto3.resource('dynamodb').Table('tasks')

    # update status
    response = table.update_item(
        Key={
            'run_id': task_config['run_id'],
            'task_id': task_config['task_id']
        },
        UpdateExpression="set the_status = :new_status",
        ExpressionAttributeValues={
            ':new_status': 'finished'
        },
        ReturnValues="ALL_NEW"
    )

    print(response)


# update run status and return to caller
# return values:
#   - (False, "running")            run is still ongoing ,some tasks is in "submitted" status
#   - (True, "Succeed")             run finished with no error, no task is in "submitted" status
#   - (True, "partially failed")    run finished with tasks failed, no task is in "submitted" status
#                                                                   but some tasks are in "failed" status

# if current run is finished, run post_run_processor to consolidate results
def update_run_status():
    task_table = boto3.resource('dynamodb').Table('tasks')
    run_table = boto3.resource('dynamodb').Table('runs')

    # get tasks status
    response = task_table.query(
        KeyConditionExpression=Key('run_id').eq(task_config['run_id'])
    )

    tasks_status =  [ x['the_status'] for x in  response['Items'] ]    

    # get run status
    response = run_table.query(
         KeyConditionExpression=Key('run_id').eq(task_config['run_id'])
    )
    run_status = reponse['Items'][0]


    if 'submitted' in tasks_status and run_status != 'submitted':
        # update run status to "running"
    elif 'failed' in task_status:
        # update run status to failed
        return (True, "failed")
    else:
        # update run status to sucess
        return (True, "success")



class JobQueue():

    def __init__(self, queueURL):
        self.client = boto3.client('sqs')
        self.queueURL = queueURL

    def readMessage(self):
        response = self.client.receive_message(
            QueueUrl=self.queueURL, WaitTimeSeconds=20)
        if 'Messages' in response.keys():
            data = json.loads(response['Messages'][0]['Body'])
            handle = response['Messages'][0]['ReceiptHandle']
            return data, handle
        else:
            return None, None

    def deleteMessage(self, handle):
        self.client.delete_message(
            QueueUrl=self.queueURL, ReceiptHandle=handle)
        return

    def returnMessage(self, handle):
        self.client.change_message_visibility(
            QueueUrl=self.queueURL, ReceiptHandle=handle, VisibilityTimeout=60)
        return


# Entry point

if __name__ == '__main__':
    main()
