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
#   - job_record_dir: a dict describes the dir to store run related into in s3 bucket
#                  certain dir structure will be created to hold output from each job

# example job_request:
# job_request = {
#     "sqs_url": "https://sqs.us-east-1.amazonaws.com/263120685370/HCA-tasks",
#     "image_data": {
#         "s3_bucket": "hca-cloud-native",
#         "prefix": "example_data/"
#     },
#     "pipeline_file": {
#         "s3_bucket": "hca-cloud-native",
#         "key": "pipeline_files/test.cppipe"
#     },
#     "job_record_dir": {
#           "s3_bucket": "hca-cloud-native",
#           "prefix": "analysis_result/"
#       }
# }

def submit_job(job_id, job_request, metadata_file_extention):
    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")

    the_bucket = job_request["image_data"]["s3_bucket"]
    the_prefix = job_request["image_data"]["prefix"]

    # check if output_path has "/" at the end, if not, add one
    if job_request["job_record_dir"]["prefix"][-1:] != "/":
        the_valid_job_record_dir = job_request["job_record_dir"]["prefix"] + "/"
    else:
        the_valid_job_record_dir = job_request["job_record_dir"]["prefix"]

    input_file_prefix = the_valid_job_record_dir + job_id + "/input/"
    output_file_prefix = the_valid_job_record_dir + job_id + "/output/"

    # find the metadata file first
    metadata_files = s3worker.find_by_extension(
        s3, the_bucket, the_prefix, metadata_file_extention)
    try:
        assert len(metadata_files) == 1
    except AssertionError as e:
        e.args += ("Found " + str(len(metadata_files)) +
                   " metadata file(s) with extension: ", "xdce")
        raise e

    metadata_file = metadata_files[0]
    #  download the metadata file to /tmp (will be more efficient when the file is big)
    # /tmp is guaranteed to be available during the execution of your Lambda function
    local_copy = s3worker.download_file(
        s3, the_bucket, metadata_file, "/tmp")

    rows = parse_metadata_file(local_copy, job_request["image_data"]["prefix"])

    # build the template of the task
    # the missing pieces will be added by "create_task" function
    task_template = {
        "job_id": job_id,
        "task_id": "TO_BE_ADDED",
        "image_data_bucket": job_request["image_data"]["s3_bucket"],
        "image_data_prefix": job_request["image_data"]["prefix"],
        "pipeline_file": job_request["pipeline_file"],
        "job_record_bucket": job_request["job_record_dir"]["s3_bucket"],
        "task_input_dir": input_file_prefix,
        "task_output_dir": output_file_prefix,
        "file_list_key": "TO_BE_ADDED"
    }

    # save individual image file list by well and add to queue
    create_tasks(s3, task_template, rows, sqs, job_request["sqs_url"])

    count_tasks_in_queue()


# parse the metadata and return a list of dictionaries ready to save as file list csv
# args:
#   - metadata_file: the path to the local copy of the metadat file
#   - image_prefix: the prefix to the images in S3 bucket.
#                   The prefix will be added to the image path
#                   The worker will mount S3 bucket as local storage and access images through the prefix
#                   The bucket info exists in the task message, not here.
#   - returns: a list of dictionaries ready to save as csv
def parse_metadata_file(metadata_file, image_prefix):
    # the location where the images will appear in CP worker
    # need to define here since CP requires the full path to the image in the file list. 
    IMAGE_DATA_BUCKET_DIR = '/home/ubuntu/image_data_bucket/'

    # check if image_prefix ends with "/". if not, add it
    if image_prefix[-1:] != "/":
        valid_image_prefix = image_prefix + "/"
    else:
        valid_image_prefix = image_prefix

    tree = ET.parse(metadata_file)
    root = tree.getroot()
    groups = {}

    # extract the information we are interested in
    for image in root.iter("Image"):
        filename = image.attrib["filename"]
        the_well = image.find("Well")
        well = the_well.get("label")
        row = the_well.find("Row").get("number")
        column = the_well.find("Column").get("number")
        field = image.find("Identifier").get("field_index")
        color = image.find("EmissionFilter").get("name")
        URL_title = "URL_" + color
        group_id = well + "@" + field

        # update the group list
        if not group_id in groups:
            groups[group_id] = {
                "Row_Number": row,
                "Column_Number": column,
                "Well_Location": well,
                "Field_Index": field,
                URL_title: "file:" + IMAGE_DATA_BUCKET_DIR + valid_image_prefix + filename
            }
        else:
            groups[group_id][URL_title] = "file:" + IMAGE_DATA_BUCKET_DIR + valid_image_prefix + filename

    rows = []
    for key, val in groups.items():
        rows.append(val)
    return rows


# write rows to csv file, submit to S3 and sqs, each file contains groups from the same well
# args:
#   -s3_client: the s3 client used to interact with S3 bucket
#   -the_bucket: the S3 bucket to save the file list
#   -task_template: the message template to enqueue to sqs
#   - rows: a list of dict, each dict is one row of the image list,
#         contains images from the same well, same field
#   - each task will have individual directory for the image list and output
#   - a post-processing lambda will combine the output to sigle result file later

# task template:
    # task_template = {
    #     "job_id": job_id,
    #     "task_id": "TO_BE_ADDED",
    #     "job_record_bucket": job_request["job_record_dir"]["s3_bucket"],
    #     "task_input_dir": input_file_prefix,
    #     "task_output_dir": output_file_prefix,
    #     "file_list_key": "TO_BE_ADDED",
    #     "pipeline_file": job_request["pipeline_file"]
    # }

def create_tasks(s3_client, task_template, rows,
                 sqs_client, QueueUrl):
    # the input_file_prefix should have "/" at the end
    # if not, add it
    if task_template["task_input_dir"][-1:] != "/":
        the_valid_input_file_prefix = task_template["task_input_dir"] + "/"
    else:
        the_valid_input_file_prefix = task_template["task_input_dir"]

    current_well = ""
    rows_for_task = []

    # add a dummy row to flush the last well
    rows.append({"Well_Location": "dummy"})
    for row in rows:
        if row["Well_Location"] != current_well:
            # one well is done. save to local file under "/tmp" and upload to S3
            if current_well != "":
                local_file = "/tmp/" + \
                    task_template["job_id"] + "-" + current_well + ".csv"
                file_list_key = the_valid_input_file_prefix + \
                    current_well + "/" + current_well + ".csv"

                save_to_csv(rows_for_task, local_file)
                s3worker.upload_file(s3_client, local_file,
                                     task_template["job_record_bucket"], file_list_key)
                os.remove(local_file)
                rows_for_task = []

                # compplete and enqueue the task
                the_task = task_template.copy()
                the_task["task_id"] = current_well
                the_task["file_list_key"] = file_list_key

                message = json.dumps(the_task)
                enqueue_task(sqs_client, message)

            current_well = row["Well_Location"]
        rows_for_task.append(row)


# helper function to save dict to csv file
#   args:
#       - output_path: specify the path to save the file
#       - filename: the csv file name
#       - rows: the rows to be written to the file

def save_to_csv(rows, filename):
    print("writing data to: " + filename)
    with open(filename, "w") as f:
        w = csv.DictWriter(f, rows[0].keys())
        w.writeheader()
        w.writerows(rows)


# submit message to aws sqs
# args:
#   - sqs_client: the sqs client to interact with sqs
#   - QueueUrl: the queue url
#   - message: the message to enqueue
def enqueue_task(sqs_client, message):
    # send a message
    print("sending the message..")
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
    )

    print("message sent")


# check number of messages in the queue
def count_tasks_in_queue():
    client = boto3.client("sqs")
    print("getting number of messages in the queue")
    # get queue attributes
    response = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible"
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
queue_url = "https://sqs.us-east-1.amazonaws.com/263120685370/HCA-tasks"
job_record_dir = {
    "s3_bucket": "hca-cloud-native",
    "prefix": "analysis_result/"
}

job_request = {
    "sqs_url": queue_url,
    "image_data": {
        "s3_bucket": "hca-cloud-native",
        "prefix": "example_data/"
    },
    "pipeline_file": {
        "s3_bucket": "hca-cloud-native",
        "key": "pipeline_files/test.cppipe"
    },
    "job_record_dir": job_record_dir
}

# the location to store the analysis result
# sub-dir structure will be created to save result from each taks


# the job id. It is part of the output directory structure
job_id = "20190621_ABC123"


submit_job(job_id, job_request, "xdce")
