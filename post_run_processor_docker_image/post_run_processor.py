# the post run processor is used to consolidate run result from each individual tasks
# and put the result into the final output dir of each job
import os
import sys
import boto3
import pandas as pd

import s3worker
from JobQueue import JobQueue


######################
# App static variables
######################
app_config = {}
app_config['job_queue'] = os.environ['RESULT_CONSOLIDATION_QUEUE_URL']
app_config['run_log_file_keyword'] = 'Experiment.csv'


# main work loop
def main():
    print(app_config)
    task_queue = JobQueue(app_config['job_queue'])
    while True:
        message, handle = task_queue.readMessage()
        if message is None:
            print("nothing in the task list")
            break

        print("got one task:")
        print(message)

        s3_client = boto3.client('s3')
        result_file_groups = get_result_files(
            s3_client, message['run_record_bucket'], message['sub_task_record_prefix'], app_config['run_log_file_keyword'])
        consolidate_and_upload(s3_client, result_file_groups,
                               message['run_record_bucket'], message['final_output_prefix'])
        task_queue.returnMessage(handle)
        break


# get a list of output files from each individual tasks
# args:
#   - s3: the boto3 s3 client to interact with S3
#   - the_bucket:   the bucket where the run result is stored
#   - the_prefix:   the prefix to the run result. Best practice is to point to "sub_tasks" dir
#   - run_log_fname_keyword: the keyword to identify the run log. Will be excluded from result files
def get_result_files(s3_client, the_bucket, the_prefix, run_log_fname_keyword):
    content = s3worker.get_content_in_dir(
        s3_client, the_bucket, the_prefix, True)['files']
    # filter out the input files, done file and run log, only keep the output files
    content = list(filter(lambda x: 'output' in x
                          and x[-4:] == '.csv'
                          and run_log_fname_keyword not in x, content))

    # group result files by their name, same file name indicates the same output file from each task
    result_file_groups = {}
    for f in content:
        filename = f.split('/')[-1]
        if filename not in result_file_groups:
            result_file_groups[filename] = [f]
        else:
            result_file_groups[filename].append(f)

    return result_file_groups


# Download, consolidate the result files and save back to run result directory in S3
# note "/tmp" is guarenteed to be available to AWS Lambda function
# args:
#   - s3: the boto3 s3 client to interact with S3
#   - result_file_groups: a groups of files, key is  the file name per output,
#                                            value is a list of the key to the file in S3
#   - the_bucket:   the bucket where the run result is stored
#   - consolidated_data_prefix:   the prefix to where the consolicated data file should be saved
def consolidate_and_upload(s3, result_file_groups, the_bucket, consolidated_data_prefix):
    for key, val in result_file_groups.items():
        dfs = []
        for f in result_file_groups[key]:
            local_copy = s3worker.download_file(s3, the_bucket, f, '/tmp')
            dfs.append(pd.read_csv(local_copy))
        combined_result = pd.concat(dfs, ignore_index=True)
        combined_file_name = 'combined_' + key
        combined_local_copy = '/tmp/' + combined_file_name
        combined_result.to_csv(combined_local_copy,
                               index=False, encoding='utf-8-sig')
        print("uploading consolidated result file to s3 bucket. file: " +
              combined_file_name)
        s3worker.upload_file(s3, combined_local_copy, the_bucket,
                             consolidated_data_prefix + combined_file_name)
        print("uploading done")
        os.system("rm /tmp/*")
        print("directory \"/tmp\" cleared!")


if __name__ == '__main__':
    main()
