# this file contains some useful functions to operate on S3 bucket
# resuable for other part of our app

import boto3

# get list if directories / flies in current "directory"
# args:
#   - s3_client: a boto3 s3 client to work on S3 bucket
#   - the_bucket: the bucket to work on
#   - the_prefix: the prefix to the target directory. Must have "/" at the end
#   - num_items: number of item to retrieve, by default no limit.

def get_content_in_dir(s3_client, the_bucket, the_prefix, max_items = None):
    # check if prefix has "/" at the end
    try:
         assert the_prefix[-1:] == '/'
    except AssertionError as e:
        e.args += ("the prefix should end with", '/')
        raise e
   
    kwargs = {'Bucket': the_bucket, 
               'Prefix': the_prefix, 
               'Delimiter': '/',
               'StartAfter': the_prefix
              }

    if not max_items is None:
        kwargs['MaxKeys'] = max_items

    # get dir or file list
    content = {'dirs': [], 'files': []}
    while True:
        response = s3_client.list_objects_v2(**kwargs)

        if 'CommonPrefixes' in response.keys():
            for item in response['CommonPrefixes']:
                content['dirs'].append(item['Prefix'])

        if 'Contents' in response.keys():
            for item in response['Contents']:
                content['files'].append(item['Key'])
       
        try:
            kwargs['ContinuationToken'] = response['NextContinuationToken']
        except KeyError:
            break
    
    return content


# check if certain file type exists in current dir
# mainly used to check if the metadata file "xdce" exists.
# if find the file, return a list of the keys of the files
# args:
#   - s3_client: a boto3 s3 client to work on S3 bucket
#   - the_bucket: the bucket to work on
#   - the_prefix: the prefix to the target directory. Must have "/" at the end
def find_by_extension(s3_client, the_bucket, the_prefix, file_extension):
    # check if prefix has "/" at the end
    try:
         assert the_prefix[-1:] == '/'
    except AssertionError as e:
        e.args += ("the prefix should end with '/' ")
        raise e

    content = get_content_in_dir(s3_client, the_bucket, the_prefix)

    target_files = []

    for item in content['files']:
        if item.split('.')[-1] == file_extension:
            target_files.append(item)
    
    return target_files


# quick test
bucket = 'hca-cloud-native'
prefix = 'example_data/'

s3 = boto3.client('s3')

content = get_content_in_dir(s3, bucket, prefix)
print(content['dirs'])
print(len(content['files']))

metadata_files = find_by_extension(s3, bucket, prefix, 'xdce')
print(metadata_files)