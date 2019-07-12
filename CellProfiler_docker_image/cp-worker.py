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

class CpWorker():
    def __init__(self):
        self.app_config = {}
        self.app_config
        self.app_config = {}
        self.app_config['S3FS_CREDENTIAL_FILE'] = os.environ['S3FS_CREDENTIAL_FILE']
        self.app_config['IMAGE_DATA_BUCKET_DIR'] = os.environ['IMAGE_DATA_BUCKET_DIR']
        self.app_config['TASK_INPUT_DIR'] = os.environ['TASK_INPUT_DIR']
        self.app_config['TASK_OUTPUT_DIR'] = os.environ['TASK_OUTPUT_DIR']

        self.app_config['task_queue_url'] = os.environ['TASK_QUEUE_URL']
        self.app_config['LOG_GROUP_NAME'] = os.environ['CLOUDWATCH_LOG_GROUP_NAME']
        self.app_config['LOG_STREAM_NAME'] = os.environ['CLOUDWATCH_LOG_STREAM_NAME']

        self.task_config = {}
        self.task_config['file_to_ignore'] = 'Experiment.csv'

        self.task_status = {}
        self.task_status['SCHEDULED'] = 'Scheduled'
        self.task_status['FINISHED'] = 'Finished'
        self.task_status['FAILED'] = 'Failed'

        self.run_status = {}
        self.run_status['SCHEDULED'] = 'Scheduled'
        self.run_status['RUNNING'] = 'Running'
        self.run_status['FINISHED'] = 'Finished'
        self.run_status['FAILED'] = 'Failed'

        self.logger = self.get_logger()
        self.task_queue = JobQueue(self.app_config['task_queue_url'])

        self.task_counter = 0


    #################################
    # RUN CELLPROFILER PROCESS
    #################################
    # main work loop
    # order of actions:
    # - get task message from the queue
    # - mount image data bucket
    # - download image flie list and pipeline file
    # - start the work and log outout
    # - upload output back to S3
    # - unmount image data bucket and clean up files
    # - update task status in dynamoDB
    # - check if the whole run is done, if yes, run result consolidation

    def run(self):
        while True:
            print("getting next task")
            msg, handle = self.task_queue.readMessage()
            if msg is None:
                time.sleep(300)
                continue

            self.task_counter += 1
            self.prepare_for_task(msg)
            self.logger.info("current task_id: " + self.task_config['task_id'])
            cp_run_command = self.build_cp_run_command()
            self.logger.info('Start the analysis with command: ' + cp_run_command)
            
            subp = subprocess.Popen(shlex.split(cp_run_command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.monitorAndLog(subp, self.logger)

            self.upload_result_and_clean_up()
            self.update_task_status(self.task_status['FINISHED'])
            self.update_run_status()

            # reset task-related config. To be sure does not affect previous run
            for key, value in self.task_config.iteritems():
                self.task_config[key] = ''

            self.task_queue.deleteMessage(handle)

            # for test only
            if self.task_counter >= 2:
                break


# prepare for task per the message from sqs
# it does the things below:
#   - set up variables per task. other part of the code will use those variables
#   - mount image s3 bucket to IMAGE_DATA_BUCKET_DIR
#   - download image file list and pipeline file

    def prepare_for_task(self, message):
        self.task_config['user_id'] = message['user_id']
        self.task_config['submit_date'] = message['submit_date']
        self.task_config['run_id'] = message['run_id']
        self.task_config['task_id'] = message['task_id']

        self.task_config['result_consolidation_queue_url'] = message['result_consolidation_queue_url']

        self.task_config['run_table'] = message['run_table']
        self.task_config['task_table'] = message['task_table']
        self.task_config['image_data_bucket'] = message["image_data"]["s3_bucket"]
        self.task_config['image_data_prefix'] = message["image_data"]["prefix"]

        self.task_config['cp_pipeline_file_bucket'] = message['pipeline_file'][
            's3_bucket']
        self.task_config['cp_pipeline_file_key'] = message['pipeline_file']['key']

        self.task_config['run_record_bucket'] = message["run_record_location"][
            "s3_bucket"]
        self.task_config['sub_task_record_prefix'] = message['sub_task_record_prefix']
        self.task_config['final_output_prefix'] = message['final_output_prefix']
        self.task_config['image_list_file_key'] = message['file_list_key']
        self.task_config['task_input_prefix'] = message['task_input_prefix']
        self.task_config['task_output_prefix'] = message['task_output_prefix']

        self.set_handler_formatter()

        mount_s3_bucket_command = ("s3fs " + self.task_config['image_data_bucket'] +
                                " " + self.app_config['IMAGE_DATA_BUCKET_DIR'] +
                                " -o passwd_file=" +
                                self.app_config['S3FS_CREDENTIAL_FILE'])
        os.system(mount_s3_bucket_command)
        if len(os.listdir(self.app_config['IMAGE_DATA_BUCKET_DIR'])) == 0:
            self.logger.info("mount failed")
        else:
            self.logger.info("mount successfully")

        s3 = boto3.client('s3')
        self.logger.info("downloading image list...")
        self.task_config['image_list_local_copy'] = s3worker.download_file(
            s3, self.task_config['run_record_bucket'],
            self.task_config['image_list_file_key'], self.app_config['TASK_INPUT_DIR'])

        self.logger.info("downloading pileline file...")
        self.task_config['pipeline_file_local_copy'] = s3worker.download_file(
            s3, self.task_config['cp_pipeline_file_bucket'],
            self.task_config['cp_pipeline_file_key'], self.app_config['TASK_INPUT_DIR'])

    # prepare logger
    # the formatter is not set yet, will set the formatter by task
    def get_logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        watchtower_config = {
            'log_group': self.app_config['LOG_GROUP_NAME'],
            'stream_name': self.app_config['LOG_STREAM_NAME'],
            'use_queues': True,
            'create_log_group': False
        }

        watchtowerlogger = watchtower.CloudWatchLogHandler(**watchtower_config)
        logger.addHandler(watchtowerlogger)
        return logger

    # set log handler formatter by task
    def set_handler_formatter(self):
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - ' + 
            self.task_config['run_id'] + ' - ' + self.task_config['task_id'] + ' - %(message)s')

        self.logger.handlers[0].setFormatter(formatter)



    # monitor output from CP process and send to Cloudwatch
    def monitorAndLog(self, process, logger):
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
    def upload_result_and_clean_up(self):
        s3 = boto3.client('s3')

        self.logger.info(os.listdir(self.app_config['TASK_OUTPUT_DIR']))
        upload_result_command = ("aws s3 mv " + self.app_config['TASK_OUTPUT_DIR'] +
                                " \"s3://" + self.task_config['run_record_bucket'] +
                                '/' + self.task_config['task_output_prefix'] +
                                "\" --recursive")
        self.logger.info("moving data to S3 with command: \n" + upload_result_command)
        os.system(upload_result_command)
        self.logger.info(os.listdir(self.app_config['TASK_OUTPUT_DIR']))

        if len(os.listdir(self.app_config['TASK_OUTPUT_DIR'])) != 0:
            os.system('rm -r ' + self.app_config['TASK_OUTPUT_DIR'] + '/*')
        os.system("umount " + self.app_config['IMAGE_DATA_BUCKET_DIR'])


    # construct CellProfiler run command
    def build_cp_run_command(self):
        cmdstem = ""
        with open(self.task_config['pipeline_file_local_copy']) as openpipe:
            for line in openpipe:
                if 'DateRevision:2' in line:
                    self.logger.info('comes from a CP2 pipeline')
                    cmdstem = 'cellprofiler -c -r -b '
                    break

        cmdstem = 'cellprofiler -c -r '
        cp_done_file = self.app_config['TASK_OUTPUT_DIR'] + '/done.txt'

        cp_run_command = (
            cmdstem + " -p " + "'" + self.task_config['pipeline_file_local_copy'] +
            "'" + " --data-file=" + "'" + self.task_config['image_list_local_copy'] +
            "'" + " -o " + "'" + self.app_config['TASK_OUTPUT_DIR'] + "'" + " -d " +
            "'" + cp_done_file) + "'"
        return cp_run_command


    # update task status
    def update_task_status(self, status):
        task_table = boto3.resource('dynamodb').Table(self.task_config['task_table'])

        # update status
        response = task_table.update_item(
            Key={
                'run_id': self.task_config['run_id'],
                'task_id': self.task_config['task_id']
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
    def update_run_status(self):
        run_table = boto3.resource('dynamodb').Table(self.task_config['run_table'])

        # update run status to running if it is not yet
        try:
            run_table.update_item(
                Key={
                    'user_id': self.task_config['user_id'],
                    'submit_date': self.task_config['submit_date']
                },
                UpdateExpression="set the_status = :new_status",
                ConditionExpression="the_status = :current_status",
                ExpressionAttributeValues={
                    ':new_status': self.run_status['RUNNING'],
                    ':current_status': self.run_status['SCHEDULED']
                },
                ReturnValues="ALL_NEW")
        except:
            pass

        # check if the whole run is done
        # if is_run_finished():
        if True:
            self.logger.info(
                'All the tasks from this run has been done. prepare for result consolidation.')
            message = self.build_result_consolidation_message()
            reslut_consolidation_queue = JobQueue(
                self.task_config['result_consolidation_queue_url'])
            reslut_consolidation_queue.enqueueMessage(message)
            self.logger.info("run result consolidation task pushed to the queue")


    # function to build result consolidation task message
    def build_result_consolidation_message(self):
        the_message = {}
        the_message['run_id'] = self.task_config['run_id']
        the_message['run_record_bucket'] = self.task_config['run_record_bucket']
        the_message['sub_task_record_prefix'] = self.task_config['sub_task_record_prefix']
        the_message['final_output_prefix'] = self.task_config['final_output_prefix']
        the_message['file_to_ignore'] = self.task_config['file_to_ignore']

        return the_message


    # check if run is finished, disregard if there is error or not
    # criteria: if no task is in "scheduled" status, then it is done.
    def is_run_finished(self):
        task_table = boto3.resource('dynamodb').Table(self.task_config['task_table'])

        # get tasks status
        response = task_table.query(
            KeyConditionExpression=Key('run_id').eq(self.task_config['run_id']))

        tasks_status = [x['the_status'] for x in response['Items']]

        # show the overall status of the run
        logger.info(dict(Counter(tasks_status)))

        if self.task_status['SCHEDULED'] in tasks_status:
            return False
        else:
            return True


# Entry point
if __name__ == '__main__':
    CpWorker().run()
