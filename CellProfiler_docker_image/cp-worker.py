from __future__ import print_function
import boto3
import glob
import json
import logging
import os
import re
import subprocess, shlex
import sys
import time
import logging
import watchtower
import string
from boto3.dynamodb.conditions import Key, Attr
from collections import Counter

import s3worker
from JobQueue import JobQueue

###################################
# Static info from initialization
###################################
app_config = {}
app_config['S3FS_CREDENTIAL_FILE'] = os.environ['S3FS_CREDENTIAL_FILE']
app_config['IMAGE_DATA_BUCKET_DIR'] = os.environ['IMAGE_DATA_BUCKET_DIR']
app_config['TASK_INPUT_DIR'] = os.environ['TASK_INPUT_DIR']
app_config['TASK_OUTPUT_DIR'] = os.environ['TASK_OUTPUT_DIR']

app_config['task_queue_url'] = os.environ['TASK_QUEUE_URL']
app_config['LOG_GROUP_NAME'] = os.environ['CLOUDWATCH_LOG_GROUP_NAME']
app_config['LOG_STREAM_NAME'] = os.environ['CLOUDWATCH_LOG_STREAM_NAME']

###############################################
# Controlled vocabulary for tasks status
###############################################
task_status = {}
task_status['SCHEDULED'] = 'Scheduled'
task_status['FINISHED'] = 'Finished'
task_status['FAILED'] = 'Failed'

run_status = {}
run_status['SCHEDULED'] = 'Scheduled'
run_status['RUNNING'] = 'Running'
run_status['FINISHED'] = 'Finished'
run_status['FAILED'] = 'Failed'

######################################
# Task specific info from sqs message
# info will be added later
# variables grouped by bucket
######################################
task_config = {}
task_config['file_to_ignore'] = 'Experiment.csv'
logger = None


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
    task_queue = JobQueue(app_config['task_queue_url'])
    # Main loop. Keep reading messages while they are available in SQS
    while True:
        print("getting next task")
        msg, handle = task_queue.readMessage()
        if msg is None:
            break
        if msg['task_id'][0] == 'B':
            prepare_for_task(msg)
            logger.info("current task_id: " + task_config['task_id'])
            cp_run_command = build_cp_run_command()
            logger.info('Start the analysis with command: ' + cp_run_command)
            
            subp = subprocess.Popen(shlex.split(cp_run_command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            monitorAndLog(subp,logger)
            # os.system(cp_run_command)
            # monitorAndLog(process, logger)

            upload_result_and_clean_up()
            update_task_status(task_status['FINISHED'])
            update_run_status()

            # reset task-related config. To be sure does not affect previous run
            for key, value in task_config.iteritems():
                task_config[key] = ''

            task_queue.deleteMessage(handle)

            break
        else:
            task_queue.deleteMessage(handle)
            print("not column B, skipped")


# prepare for task per the message from sqs
# it does the things below:
#   - set up variables per task. other part of the code will use those variables
#   - mount image s3 bucket to IMAGE_DATA_BUCKET_DIR
#   - download image file list and pipeline file


def prepare_for_task(message):
    global task_config, logger
    task_config['user_id'] = message['user_id']
    task_config['submit_date'] = message['submit_date']
    task_config['run_id'] = message['run_id']
    task_config['task_id'] = message['task_id']

    task_config['result_consolidation_queue_url'] = message['result_consolidation_queue_url']

    task_config['run_table'] = message['run_table']
    task_config['task_table'] = message['task_table']
    task_config['image_data_bucket'] = message["image_data"]["s3_bucket"]
    task_config['image_data_prefix'] = message["image_data"]["prefix"]

    task_config['cp_pipeline_file_bucket'] = message['pipeline_file'][
        's3_bucket']
    task_config['cp_pipeline_file_key'] = message['pipeline_file']['key']

    task_config['run_record_bucket'] = message["run_record_location"][
        "s3_bucket"]
    task_config['sub_task_record_prefix'] = message['sub_task_record_prefix']
    task_config['final_output_prefix'] = message['final_output_prefix']
    task_config['image_list_file_key'] = message['file_list_key']
    task_config['task_input_prefix'] = message['task_input_prefix']
    task_config['task_output_prefix'] = message['task_output_prefix']

    logger = get_logger()

    mount_s3_bucket_command = ("s3fs " + task_config['image_data_bucket'] +
                               " " + app_config['IMAGE_DATA_BUCKET_DIR'] +
                               " -o passwd_file=" +
                               app_config['S3FS_CREDENTIAL_FILE'])
    os.system(mount_s3_bucket_command)
    if len(os.listdir(app_config['IMAGE_DATA_BUCKET_DIR'])) == 0:
        logger.info("mount failed")
    else:
        logger.info("mount successfully")

    s3 = boto3.client('s3')
    logger.info("downloading image list...")
    task_config['image_list_local_copy'] = s3worker.download_file(
        s3, task_config['run_record_bucket'],
        task_config['image_list_file_key'], app_config['TASK_INPUT_DIR'])

    logger.info("downloading pileline file...")
    task_config['pipeline_file_local_copy'] = s3worker.download_file(
        s3, task_config['cp_pipeline_file_bucket'],
        task_config['cp_pipeline_file_key'], app_config['TASK_INPUT_DIR'])



# prepare logger
def get_logger():
    watchtower_config = {
        'log_group': app_config['LOG_GROUP_NAME'],
        'stream_name': app_config['LOG_STREAM_NAME'],
        'use_queues': True,
        'create_log_group': False
    }

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - ' + 
        task_config['run_id'] + ' - ' + task_config['task_id'] + ' - %(message)s')

    watchtowerlogger = watchtower.CloudWatchLogHandler(**watchtower_config)
    watchtowerlogger.setFormatter(formatter)
    logger.addHandler(watchtowerlogger)

    return logger


# monitor output from CP process and send to Cloudwatch
def monitorAndLog(process, logger):
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
            logger.info(output)

# post processing after CellProfiler finish the analysis
# it does the things below:
#   - upload result to corresponding s3 bucket
#   - clean up temporary files on local disk
#   - unmount image s3 bucket from IMAGE_DATA_BUCKET_DIR
def upload_result_and_clean_up():
    s3 = boto3.client('s3')

    logger.info(os.listdir(app_config['TASK_OUTPUT_DIR']))
    upload_result_command = ("aws s3 mv " + app_config['TASK_OUTPUT_DIR'] +
                             " \"s3://" + task_config['run_record_bucket'] +
                             '/' + task_config['task_output_prefix'] +
                             "\" --recursive")
    logger.info("moving data to S3 with command: \n" + upload_result_command)
    os.system(upload_result_command)
    logger.info(os.listdir(app_config['TASK_OUTPUT_DIR']))

    if len(os.listdir(app_config['TASK_OUTPUT_DIR'])) != 0:
        os.system('rm -r ' + app_config['TASK_OUTPUT_DIR'] + '/*')
    os.system("umount " + app_config['IMAGE_DATA_BUCKET_DIR'])


# construct CellProfiler run command
def build_cp_run_command():
    cmdstem = ""
    with open(task_config['pipeline_file_local_copy']) as openpipe:
        for line in openpipe:
            if 'DateRevision:2' in line:
                logger.info('comes from a CP2 pipeline')
                cmdstem = 'cellprofiler -c -r -b '
                break

    cmdstem = 'cellprofiler -c -r '
    cp_done_file = app_config['TASK_OUTPUT_DIR'] + '/done.txt'

    cp_run_command = (
        cmdstem + " -p " + "'" + task_config['pipeline_file_local_copy'] +
        "'" + " --data-file=" + "'" + task_config['image_list_local_copy'] +
        "'" + " -o " + "'" + app_config['TASK_OUTPUT_DIR'] + "'" + " -d " +
        "'" + cp_done_file) + "'"
    return cp_run_command
    # logger.info(cmd)

    # subp = subprocess.Popen(
    #     cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # monitorAndLog(subp, logger)


# update task status
def update_task_status(status):
    task_table = boto3.resource('dynamodb').Table(task_config['task_table'])

    # update status
    response = task_table.update_item(
        Key={
            'run_id': task_config['run_id'],
            'task_id': task_config['task_id']
        },
        UpdateExpression="set the_status = :new_status",
        ExpressionAttributeValues={':new_status': status},
        ReturnValues="ALL_NEW")

# update run status
# actions includes:
#   - update run status to "running" if it is till in "scheduled"
#   - if all tasks are done, update runs status to "finished" with conditional writing
#   - if status update succeed, push task to the result consolidataion queue
#   - the conditional writing ensure only one task will be pushed per run
#


def update_run_status():
    run_table = boto3.resource('dynamodb').Table(task_config['run_table'])

    # update run status to running if it is not yet
    try:
        run_table.update_item(
            Key={
                'user_id': task_config['user_id'],
                'submit_date': task_config['submit_date']
            },
            UpdateExpression="set the_status = :new_status",
            ConditionExpression="the_status = :current_status",
            ExpressionAttributeValues={
                ':new_status': run_status['RUNNING'],
                ':current_status': run_status['SCHEDULED']
            },
            ReturnValues="ALL_NEW")
    except:
        pass

    # check if the whole run is done
    # if is_run_finished():
    if True:
        logger.info(
            'All the tasks from this run has been done. prepare for result consolidation.')
        message = build_result_consolidation_message()
        reslut_consolidation_queue = JobQueue(
            task_config['result_consolidation_queue_url'])
        reslut_consolidation_queue.enqueueMessage(message)


# function to build result consolidation task message
def build_result_consolidation_message():
    the_message = {}
    the_message['run_record_bucket'] = task_config['run_record_bucket']
    the_message['sub_task_record_prefix'] = task_config['sub_task_record_prefix']
    the_message['final_output_prefix'] = task_config['final_output_prefix']
    the_message['file_to_ignore'] = task_config['file_to_ignore']

    return the_message


# check if run is finished, disregard if there is error or not
# criteria: if no task is in "scheduled" status, then it is done.
def is_run_finished():
    task_table = boto3.resource('dynamodb').Table(task_config['task_table'])

    # get tasks status
    response = task_table.query(
        KeyConditionExpression=Key('run_id').eq(task_config['run_id']))

    tasks_status = [x['the_status'] for x in response['Items']]

    # show the overall status of the run
    logger.info(dict(Counter(tasks_status)))

    if task_status['SCHEDULED'] in tasks_status:
        return False
    else:
        return True


# Entry point
if __name__ == '__main__':
    main()
