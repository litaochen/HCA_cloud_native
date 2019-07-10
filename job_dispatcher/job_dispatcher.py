import boto3
import json
import os
import sys
import xml.etree.ElementTree as ET
import csv
import time

sys.path.append("..")  # Adds higher directory to python modules path.
from libs import s3worker
from libs.JobQueue import JobQueue


# parse and submit run request
# args:
#   - run_id: the unique run id for current run request
#   - input: a dict describes the input dir in s3 bucket, pipeline file
#   - metadata_file_extension: the extention of  the metadata file: "xdce", no dot.
#   - run_record_location: a dict describes the dir to store run related into in s3 bucket
#                  certain dir structure will be created to hold output from each run

# example run_request:
# run_request = {
#     "user_id": user_id,
#     "submit_date": timestamp,
#     "run_id": run_id,
#     "run_description": run_description,
#     "sqs_url": queue_url,
#     "image_data": {
#         "s3_bucket": "hca-cloud-native",
#         "prefix": "example_data/"
#     },
#     "pipeline_file": {
#         "s3_bucket": "hca-cloud-native",
#         "key": "pipeline_files/test.cppipe"
#     },
#     "run_record_location": run_record_location
# }

def submit_run(run_request):
    # add run record into run table
    run_table = boto3.resource('dynamodb').Table('runs')
    try:
        run_table.put_item(Item = run_request)
    except Exception as e:
        print(e)


    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")

    the_bucket = run_request["image_data"]["s3_bucket"]
    the_prefix = run_request["image_data"]["prefix"]
    metadata_file_extention = run_request["image_data"]['metadata_file_extention']

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

    rows = parse_metadata_file(local_copy, run_request["image_data"]["prefix"])

    # build the template of the task
    task_template = build_task_template(run_request)

    # save individual image file list by well and add to queue
    create_tasks(s3, task_template, rows, sqs, run_request["task_queue_url"])

    count_tasks_in_queue(run_request['task_queue_url'])

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
            groups[group_id][URL_title] = "file:" + \
                IMAGE_DATA_BUCKET_DIR + valid_image_prefix + filename

    rows = []
    for key, val in groups.items():
        rows.append(val)
    return rows


# build task template based on run request
# args:
#   - run_request: the run request
def build_task_template(run_request):
    task_template = run_request.copy()

    # check if output_path has "/" at the end, if not, add one
    if task_template["run_record_location"]["prefix"][-1:] != "/":
        the_valid_run_record_location = task_template["run_record_location"]["prefix"] + "/"
    else:
        the_valid_run_record_location = task_template["run_record_location"]["prefix"]

    sub_task_record_prefix = the_valid_run_record_location + task_template['run_id'] + "/sub_tasks/"
    final_output_prefix = the_valid_run_record_location + task_template['run_id'] + "/consolidated_results/"

    # add additional info to the task template
    # the missing pieces will be added by "create_task" function
    task_template["task_id"] = "TO_BE_ADDED"
    task_template["the_status"] = "submitted"
    task_template["sub_task_record_prefix"] = sub_task_record_prefix
    task_template["final_output_prefix"] = final_output_prefix
    task_template["task_input_prefix"] = "TO_BE_ADDED"
    task_template["task_output_prefix"] = "TO_BE_ADDED"
    task_template["file_list_key"] = "TO_BE_ADDED"

    return task_template

    # task_template = {

    #     "image_data_bucket": run_request["image_data"]["s3_bucket"],
    #     "image_data_prefix": run_request["image_data"]["prefix"],
    #     "pipeline_file": run_request["pipeline_file"],
    #     "run_record_bucket": run_request["run_record_location"]["s3_bucket"],

    # }


# write rows to csv file, submit to S3 and sqs, each file contains groups from the same well
# args:
#   -s3_client: the s3 client used to interact with S3 bucket
#   -the_bucket: the S3 bucket to save the file list
#   -task_template: the message template to enqueue to sqs
#   - rows: a list of dict, each dict is one row of the image list,
#         contains images from the same well, same field
#   - each task will have individual directory for the image list and output
#   - a post-processing lambda will combine the output to sigle result file later

def create_tasks(s3_client, task_template, rows,
                 sqs_client, QueueUrl):
    current_well = ""
    rows_for_task = []

    task_queue = JobQueue(QueueUrl)

    # add a dummy row to flush the last well
    rows.append({"Well_Location": "dummy"})
    for row in rows:
        if row["Well_Location"] != current_well:
            # one well is done. save to local file under "/tmp" and upload to S3
            if current_well != "":
                the_task = task_template.copy()
                the_task["task_id"] = current_well
                the_task["task_input_prefix"] = the_task["sub_task_record_prefix"] + \
                    current_well + "/input/"
                the_task["task_output_prefix"] = the_task["sub_task_record_prefix"] + \
                    current_well + "/output/"
                the_task["file_list_key"] = the_task["task_input_prefix"] + \
                    current_well + ".csv"

                local_file = "/tmp/" + \
                    task_template["run_id"] + "-" + current_well + ".csv"

                save_to_csv(rows_for_task, local_file)
                s3worker.upload_file(s3_client, local_file,
                                     task_template["run_record_location"]["s3_bucket"], the_task["file_list_key"])
                os.remove(local_file)
                rows_for_task = []

                # add task into task table
                task_table = boto3.resource('dynamodb').Table(task_template['task_table'])
                try:
                    task_table.put_item(Item = the_task)
                except Exception as e:
                    print(e)

                # enqueue the task
                task_queue.enqueueMessage(the_task)
                # message = json.dumps(the_task)
                # enqueue_task(sqs_client, message)

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


# check number of messages in the queue
def count_tasks_in_queue(queue_url):
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
task_queue_url = "https://sqs.us-east-1.amazonaws.com/263120685370/HCA-tasks"
reslut_consolidataion_queue_url = "https://sqs.us-east-1.amazonaws.com/263120685370/HCA-result-consolidation"
user_id = "litao"
timestamp = str(int(round(time.time() * 1000)))
run_id = user_id + '_' + timestamp
run_name = 'test run on Incell data'
run_description = 'this is a test run to check the app functionalities, good luck'
run_table = 'runs'
task_table = 'tasks'

run_record_location = {
    "s3_bucket": "hca-cloud-native",
    "prefix": "run_history/"
}

run_request = {
    "user_id": user_id,
    "submit_date": timestamp,
    "run_id": run_id,
    "run_description": run_description,
    "task_queue_url": task_queue_url,
    "reslut_consolidataion_queue_url": reslut_consolidataion_queue_url,
    "run_table": run_table,
    "task_table": task_table,
    "image_data": {
        "s3_bucket": "hca-cloud-native",
        "prefix": "example_data/",
        "metadata_file_extention": "xdce"
    },
    "pipeline_file": {
        "s3_bucket": "hca-cloud-native",
        "key": "pipeline_files/test.cppipe"
    },
    "run_record_location": run_record_location
}

# the location to store the analysis result
# sub-dir structure will be created to save result from each taks

submit_run(run_request)
