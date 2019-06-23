import boto3
import json
import os
import sys
import xml.etree.ElementTree as ET
import csv

import s3worker


# parse and submit job request
# args:
#   - job_id: the unique job id for current job request
#   - input: a dict describes the input dir in s3 bucket, pipeline file
#   - metadata_file_extension: the extention of  the metadata file: "xdce", no dot.
#   - job_record_dir: a dict describes the output dir in s3 bucket
#                  certain dir structure will be created to hold output from each job


def submit_job(job_id, input, metadata_file_extention, job_record_dir):
    s3 = boto3.client('s3')

    the_bucket = input['image_data']['s3_bucket']
    the_prefix = input['image_data']['prefix']

    # check if output_path has "/" at the end, if not, add one
    if job_record_dir[-1:] != '/':
        the_valid_job_record_dir = job_record_dir + '/'
    else:
        the_valid_job_record_dir = job_record_dir

    input_file_prefix = the_valid_job_record_dir + job_id + "/input/"

# find the metadata file first
    metadata_files = s3worker.find_by_extension(
        s3, the_bucket, the_prefix, metadata_file_extention)
    try:
        assert len(metadata_files) == 1
    except AssertionError as e:
        e.args += ("Found " + str(len(metadata_files)) +
                   " metadata file(s) with extension: ", 'xdce')
        raise e

    metadata_file = metadata_files[0]
#  download the metadata file to /tmp (will be more efficient when the file is big)
# /tmp is guaranteed to be available during the execution of your Lambda function
    local_copy = s3worker.download_file(
        s3, the_bucket, metadata_file, '/tmp')

    rows = parse_metadata_file(local_copy)

# save individual image file list by well and add to queue
    create_tasks(s3, the_bucket, job_id, rows, input_file_prefix)

    count_tasks_in_queue()


# parse the metadata and return a list of dictionaries ready to save as csv
# args:
#   - path_to_xml_doc: the path to the xdce file
#   - returns: a list of dictionaries ready to save as csv
def parse_metadata_file(path_metadata_file):
    tree = ET.parse(path_metadata_file)
    root = tree.getroot()
    groups = {}

    # extract the information we are interested in
    for image in root.iter('Image'):
        filename = image.attrib['filename']
        the_well = image.find('Well')
        well = the_well.get('label')
        row = the_well.find('Row').get('number')
        column = the_well.find('Column').get('number')
        field = image.find('Identifier').get('field_index')
        color = image.find('EmissionFilter').get('name')
        URL_title = "URL_" + color
        group_id = well + "@" + field

        # update the group list
        if not group_id in groups:
            groups[group_id] = {
                "Row_Number": row,
                "Column_Number": column,
                "Well_Location": well,
                "Field_Index": field,
                URL_title: "file:" + path_metadata_file + filename
            }
        else:
            groups[group_id][URL_title] = "file:" + \
                path_metadata_file + filename

    rows = []
    for key, val in groups.items():
        rows.append(val)
    return rows


# write rows to csv file and submit to sqs, each file contains groups from the same well
# args:
#   -s3_client: the s3 client used to interact with S3 bucket
#   -the_bucket: the S3 bucket to save the file list
#   -job_id: the job id. Will be written in the message to worker.
#   - rows: a list of dict, each dict is one row of the image list,
#         contains images from the same well, same field
#   - input_file_prefix: the path to save the csv files as task input
#   - each task will have individual directory for the image list and output
#   - a post-processing lambda will combine the output to sigle result file later

# todo: use boto3 to save the file to S3 (local -> S3)
def create_tasks(s3_client, the_bucket, the_job_id, rows, input_file_prefix):
    # the input_file_prefix should have "/" at the end
    # if not, add it
    if input_file_prefix[-1:] != '/':
        the_valid_input_file_prefix = input_file_prefix + '/'
    else:
        the_valid_input_file_prefix = input_file_prefix

    current_well = ""
    rows_for_task = []

    # add a dummy row to flush the last well
    rows.append({"Well_Location": "dummy"})
    for row in rows:
        if row['Well_Location'] != current_well:
            # one well is done. save to local file under "/tmp" and upload to S3
            if current_well != "":
                local_file = '/tmp/' + the_job_id + '-' + current_well + '.csv'
                file_list_key = the_valid_input_file_prefix + \
                    current_well + '/' + current_well + ".csv"

                save_to_csv(rows_for_task, local_file)
                s3worker.upload_file(s3_client, local_file,
                                     the_bucket, file_list_key)
                os.remove(local_file)
                rows_for_task = []

                # enqueue the task
                message = json.dumps({
                    "job_id": the_job_id,
                    "task_id": current_well,
                    "file_list": {
                        "bucket": the_bucket,
                        "file_list_key": file_list_key
                    },
                    "pipeline_file": "path_to_pipeline_file",
                    "output_dir": input_file_prefix
                })
                enqueue_task(message)

            current_well = row['Well_Location']
        rows_for_task.append(row)


# helper function to save dict to csv file
#   args:
#       - output_path: specify the path to save the file
#       - filename: the csv file name
#       - rows: the rows to be written to the file

def save_to_csv(rows, filename):
    print("writing data to: " + filename)
    with open(filename, 'w') as f:
        w = csv.DictWriter(f, rows[0].keys())
        w.writeheader()
        w.writerows(rows)


# submit message to aws sqs
def enqueue_task(message):
    client = boto3.client('sqs')

    # send a message
    print("sending the message..")
    response = client.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
    )

    print("message sent")


# check number of messages in the queue
def count_tasks_in_queue():
    client = boto3.client('sqs')
    print("getting number of messages in the queue")
    # get queue attributes
    response = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible'
        ]
    )
    print(response)


# test the procrdure:
#   - read metadata from input dir ".xdce file"
#   - write file list to to the output dir
#   - submit taks to sqs
#   - check number of tasks in sqs

# constants for dev
# will be converted into arguments later
queue_url = 'https://sqs.us-east-1.amazonaws.com/263120685370/HCA-tasks'
input = {
    'image_data': {
        's3_bucket': 'hca-cloud-native',
        'prefix': 'example_data/'
    },
    'pipeline_file': {
        's3_bucket': 'hca-cloud-native',
        'key': 'pipeline_files/test.cppipe'
    }
}

# the location to store the analysis result
# sub-dir structure will be created to save result from each taks
output_directory = {
    's3_bucket': 'hca-cloud-native',
    'prefix': 'analysis_result/'
}

# the job id. It is part of the output directory structure
job_id = "20190621_ABC123"


submit_job(job_id, input, 'xdce', 'analysis_result/')
