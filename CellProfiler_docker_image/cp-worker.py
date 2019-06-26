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

import s3worker

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
task_config['job_id'] = ''
task_config['task_id'] = ''
task_config['image_data_bucket'] = ''
task_config['image_data_prefix'] = ''

task_config['cp_pipeline_file_bucket'] = ''
task_config['cp_pipeline_file_key'] = ''

task_config['job_record_bucket'] = ''
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

# main work loop
def main():
    queue = JobQueue(app_config['QUEUE_URL'])
    # Main loop. Keep reading messages while they are available in SQS
    while True:
        print("getting next task")
        msg, handle = queue.readMessage()
        if msg is None:
            break
        if msg['task_id'][0] == 'A':
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
            print("not column A, skipped")
       # run CP to process the images
       # upload result to s3 and clean up



        #     result = runCellProfiler(msg)
        #     if result == 'SUCCESS':
        #         print('Batch completed successfully.')
        #         queue.deleteMessage(handle)
        #     else:
        #         print('Returning message to the queue.')
        #         queue.returnMessage(handle)
        # else:
        #     print('No messages in the queue')
        #     break



# prepare for task per the message from sqs
# it does the things below:
#   - set up variables per task. other part of the code will use those variables
#   - mount image s3 bucket to IMAGE_DATA_BUCKET_DIR
#   - download image file list and pipeline file
def prepare_for_task(message):
    global task_config
    task_config['job_id'] = message['job_id']
    task_config['task_id'] = message['task_id']
    task_config['image_data_bucket'] = message['image_data_bucket']
    task_config['image_data_prefix'] = message['image_data_prefix']


    task_config['cp_pipeline_file_bucket'] = message['pipeline_file']['s3_bucket']
    task_config['cp_pipeline_file_key'] = message['pipeline_file']['key']

    task_config['job_record_bucket'] = message['job_record_bucket']
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
    task_config['image_list_local_copy'] = s3worker.download_file(s3, task_config['job_record_bucket'], 
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
                            " \"s3://" +  task_config['job_record_bucket'] + '/' + task_config['task_output_prefix'] +
                              "\" --recursive" )
    print("moving data to S3 with command: \n" + upload_result_command)
    os.system(upload_result_command)
    print(os.listdir(app_config['TASK_OUTPUT_DIR']))

    if len(os.listdir(app_config['TASK_OUTPUT_DIR'])) != 0:
        os.system('rm -r ' + app_config['TASK_OUTPUT_DIR'] + '/*')
    os.system("umount " + app_config['IMAGE_DATA_BUCKET_DIR'])

    #reset task-related config. To be sure does not affect previous run
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

    input_path_to_image = "'" + app_config['IMAGE_DATA_BUCKET_DIR'] + "/" + task_config['image_data_prefix'] + "'"
    cp_run_command = (cmdstem + " -p " + "'" + task_config['pipeline_file_local_copy'] + "'" +
                        " --data-file=" + "'" + task_config['image_list_local_copy'] + "'" + 
                        # " -i " + input_path_to_image + 
                        " -o " + "'" + app_config['TASK_OUTPUT_DIR'] + "'" + 
                        " -d " + "'" + cp_done_file) + "'"
    return cp_run_command    
    # logger.info(cmd)

    # subp = subprocess.Popen(
    #     cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # monitorAndLog(subp, logger)   


def runCellProfiler(message):
    # List the directories in the bucket- this prevents a strange s3fs error
    rootlist = os.listdir(DATA_ROOT)
    for eachSubDir in rootlist:
        subDirName = os.path.join(DATA_ROOT, eachSubDir)
        if os.path.isdir(subDirName):
            trashvar = os.system('ls '+subDirName)

    # Configure the logs
    logger = logging.getLogger(__name__)

    # Prepare paths and parameters

    localOut = LOCAL_OUTPUT + '/%(MetadataID)s' % {'MetadataID': metadataID}
    remoteOut = os.path.join(message['output'], metadataID)
    replaceValues = {'PL': message['pipeline'], 'OUT': localOut, 'FL': message['data_file'],
                     'DATA': DATA_ROOT, 'Metadata': message['Metadata'], 'IN': message['input'],
                     'MetadataID': metadataID}
    # See if this is a message you've already handled, if you've so chosen
    if CHECK_IF_DONE_BOOL.upper() == 'TRUE':
        try:
            s3client = boto3.client('s3')
            bucketlist = s3client.list_objects(
                Bucket=AWS_BUCKET, Prefix=remoteOut+'/')
            objectsizelist = [k['Size'] for k in bucketlist['Contents']]
            if len(objectsizelist) >= int(EXPECTED_NUMBER_FILES):
                if 0 not in objectsizelist:
                    return 'SUCCESS'
        except KeyError:  # Returned if that folder does not exist
            pass

    # Start loggging now that we have a job we care about
    watchtowerlogger = watchtower.CloudWatchLogHandler(
        log_group=LOG_GROUP_NAME, stream_name=metadataID, create_log_group=False)
    logger.addHandler(watchtowerlogger)

    # Build and run CellProfiler command
    cp2 = False
    with open(os.path.join(replaceValues['DATA'], replaceValues['PL']), 'r') as openpipe:
        for line in openpipe:
            if 'DateRevision:2' in line:  # comes from a CP2 pipeline
                cp2 = True
                cmdstem = 'cellprofiler -c -r -b '
    if not cp2:
        cmdstem = 'cellprofiler -c -r '
    cpDone = localOut + '/cp.is.done'
    if message['pipeline'][-3:] != '.h5':
        cmd = cmdstem + \
            '-p %(DATA)s/%(PL)s -i %(DATA)s/%(IN)s -o %(OUT)s -d ' + cpDone
        cmd += ' --data-file=%(DATA)s/%(FL)s -g %(Metadata)s'
    else:
        cmd = cmdstem + '-p %(DATA)s/%(PL)s -o %(OUT)s -d ' + \
            cpDone + ' --data-file=%(DATA)s/%(FL)s -g %(Metadata)s'
    cmd = cmd % replaceValues
    print('Running', cmd)
    logger.info(cmd)

    subp = subprocess.Popen(
        cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    monitorAndLog(subp, logger)

    # Get the outputs and move them to S3
    if os.path.isfile(cpDone):
        if next(open(cpDone)) == 'Complete\n':
            time.sleep(30)
            mvtries = 0
            while mvtries < 3:
                try:
                    printandlog('Move attempt #'+str(mvtries+1), logger)
                    cmd = 'aws s3 mv ' + localOut + ' s3://' + \
                        AWS_BUCKET + '/' + remoteOut + ' --recursive'
                    subp = subprocess.Popen(
                        cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    out, err = subp.communicate()
                    printandlog('== OUT \n'+out, logger)
                    if err == '':
                        break
                    else:
                        printandlog('== ERR \n'+err, logger)
                        mvtries += 1
                except:
                    printandlog('Move failed', logger)
                    printandlog('== ERR \n'+err, logger)
                    time.sleep(30)
                    mvtries += 1
            if mvtries < 3:
                printandlog('SUCCESS', logger)
                logger.removeHandler(watchtowerlogger)
                return 'SUCCESS'
            else:
                printandlog('OUTPUT PROBLEM. Giving up on '+metadataID, logger)
                logger.removeHandler(watchtowerlogger)
                return 'OUTPUT_PROBLEM'
        else:
            printandlog('CP PROBLEM: Done file reports failure', logger)
            logger.removeHandler(watchtowerlogger)
            return 'CP_PROBLEM'
    else:
        printandlog('CP PROBLEM: Done file does not exist.', logger)
        logger.removeHandler(watchtowerlogger)
        return 'CP_PROBLEM'



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

#################################
# MAIN WORKER LOOP
#################################

# def main():
#     queue = JobQueue(QUEUE_URL)
#     # Main loop. Keep reading messages while they are available in SQS
#     while True:
#         msg, handle = queue.readMessage()
#         if msg is not None:
#             result = runCellProfiler(msg)
#             if result == 'SUCCESS':
#                 print('Batch completed successfully.')
#                 queue.deleteMessage(handle)
#             else:
#                 print('Returning message to the queue.')
#                 queue.returnMessage(handle)
#         else:
#             print('No messages in the queue')
#             break

#################################
# MODULE ENTRY POINT
#################################


# def mount_image_data_bucket():

if __name__ == '__main__':
    main()




# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO)
#     print('Worker started')
#     main()
#     print('Worker finished')
