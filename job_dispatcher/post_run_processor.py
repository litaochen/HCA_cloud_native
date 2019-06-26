# the post run processor is used to consolidate run result from each individual tasks
# and put the result into the final output dir of each job

import boto3
import s3worker
import pandas as pd
s3 = boto3.client('s3')


run_log_file_name_keyword = 'Experiment.csv'
the_bucket = 'hca-cloud-native'
the_prefix = 'run_history/20190626_ABC123/sub_tasks'

# get list of files in the run
content = s3worker.get_content_in_dir(s3, the_bucket, the_prefix, True)['files']
# filter out the input files, done file and run log, only keep the output files
content = list(filter(lambda x: 'output' in x \
                        and x[-4:] == '.csv' \
                        and run_log_file_name_keyword not in x, content))

# group result files by their name, same file name indicates the same output file from each task
result_file_groups = {}
for f in content:
    filename = f.split('/')[-1]
    if filename not in result_file_groups:
        result_file_groups[filename] = [f]
    else:
        result_file_groups[filename].append(f)


# combine the files
# downlaod to local disk "/tmp" for further consolidation
# note "/tmp" is guarenteed to be available to Lambda function in AWS
for key, val in result_file_groups.iteritems():
    dfs = []
    for f in result_file_groups[key]:
        print(key)
        print(f)
        local_copy = s3worker.download_file(s3, the_bucket, f, '/tmp')
        print("local copy is at: " + local_copy)
        dfs.append(pd.read_csv(local_copy))
    combined_result = pd.concat(dfs, ignore_index=True)
    combined_result.to_csv('/tmp/' + 'combined' + key, index=False, encoding='utf-8-sig')

