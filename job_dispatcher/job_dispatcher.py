import csv
import xml.etree.ElementTree as ET
import sys
import os
import json
import boto3

# constants
queue_url = 'https://sqs.us-east-1.amazonaws.com/263120685370/HCA-tasks'
path_to_file = '/Users/litaochen/OneDrive/Projects/HCA_cloud_native/example_data/'


# parse the metadata and return a list of dictionaries ready to save as csv
# args:
#   - path_to_xml_doc: the path to the xdce file
#   - returns: a list of dictionaries ready to save as csv


def parse_metadata_file(path_to_xml_doc):
    tree = ET.parse(path_to_xml_doc)
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
                URL_title: "file:" + path_to_file + filename
            }
        else:
            groups[group_id][URL_title] = "file:" + path_to_file + filename

    rows = []
    for key, val in groups.items():
        rows.append(val)
    return rows


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


# write rows to csv file and submit to sqs, each file contains groups from the same well
# args:
#   - rows: a list of dict, each dict is one row of the image list,
#         contains images from the same well, same field
#   - output_path: the path to save the csv files
def create_tasks(rows, output_path):
    print("splitting job into tasks:")
    current_well = ""
    rows_for_task = []

    # add a dummy row to flush the last well
    rows.append({"Well_Location": "dummy"})
    for row in rows:
        if row['Well_Location'] != current_well:
            if current_well != "":
                file_list = output_path + '/' + current_well + ".csv"
                save_to_csv(rows_for_task, file_list)
                rows_for_task = []

                # enqueue the task
                message = json.dumps({
                    "job_id": "ABC123",
                    "task_id": current_well,
                    "file_list": file_list,
                    "pipeline_file": "path_to_pipeline_file",
                    "output_dir": output_path
                })
                enqueue_task(message)

            current_well = row['Well_Location']
        rows_for_task.append(row)


def submit_job(xml_doc, output_path):
    rows = parse_metadata_file(xml_doc)
    create_tasks(rows, output_path)

    count_tasks_in_queue()


# entry point of the script
if __name__ == '__main__':
    # only accept one file
    if len(sys.argv) != 3:
        print("wrong number of arguments.")
        print("usage: python metadata_parser.py xdce_file output_dir")
        exit(1)

    # check if file exists
    if not os.path.isfile(sys.argv[1]):
        print("file does not exist: " + sys.argv[1])
        exit(1)

    # check if output dir exists
    if not os.path.isdir(sys.argv[2]):
        print("output directory does not exist: " + sys.argv[2])
        exit(1)

    # set common variables
    xml_doc = sys.argv[1]
    output_path = sys.argv[2]

    submit_job(xml_doc, output_path)
