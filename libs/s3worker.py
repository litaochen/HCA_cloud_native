# this file contains some useful functions to operate on S3 bucket
# resuable for other part of our app

import boto3

# get list if directories / flies in current "directory"
# args:
#   - s3_client: a boto3 s3 client to work on S3 bucket
#   - the_bucket: the bucket to work on
#   - the_prefix: the prefix to the target directory. Must have "/" at the end
#   - num_items: number of item to retrieve, by default no limit.


def get_content_in_dir(s3_client, the_bucket, the_prefix, recursive=False, max_items=None):
    # check if prefix has "/" at the end, if not, add one
    if the_prefix[-1:] != '/':
        the_valid_prefix = the_prefix + '/'
    else:
        the_valid_prefix = the_prefix

    kwargs = {'Bucket': the_bucket,
              'Prefix': the_valid_prefix,
              'StartAfter': the_valid_prefix
              }

    if not recursive:
        kwargs['Delimiter'] = '/'

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
    # check if prefix has "/" at the end, if not, add one
    if the_prefix[-1:] != '/':
        the_valid_prefix = the_prefix + '/'
    else:
        the_valid_prefix = the_prefix

    content = get_content_in_dir(s3_client, the_bucket, the_valid_prefix)

    target_files = []

    for item in content['files']:
        if item.split('.')[-1] == file_extension:
            target_files.append(item)

    return target_files


# download file from s3 to local storge
# mainly used to download input files for CellProfiler to use, file-list, pipeline file
#   args:
#   - s3_client: a boto3 s3 client to work on S3 bucket
#   - the_bucket: the s3 bucket where the file is stored
#   - the_key:   the key of the file to download
#   - destination:  the location to save the file. For Lambda, it is /tmp
#   - it will return the path to the file on local disk

def download_file(s3_client, the_bucket, the_key, destination):
    filename = the_key.split('/')[-1]
    saved_as = destination + '/' + filename

    with open(saved_as, 'wb') as data:
        s3_client.download_fileobj(the_bucket, the_key, data)

    return saved_as


# upload file from s3 to local storge
#   args:
#   - s3_client: a boto3 s3 client to work on S3 bucket
#   - the_bucket: the s3 bucket where the file is stored
#   - the_key:   the key of the file, decide where the file goes in the bucket
#   - it will return the path to the file on local disk

def upload_file(s3_client, path_to_file, the_bucket, the_key):
    try:
        with open(path_to_file, 'rb') as data:
            s3_client.upload_fileobj(data, the_bucket, the_key)
    except IOError as e:
        e.args += ("Can't read file: ", path_to_file)
        raise e


# # quick test
# bucket = 'hca-cloud-native'
# prefix = 'example_data/'
# key_of_file = 'example_data/example.xdce'
# test_dir = 'test/'

# s3 = boto3.client('s3')

# # list dir content
# content = get_content_in_dir(s3, bucket, prefix)
# print(content['dirs'])
# print(len(content['files']))

# # get metadata
# metadata_files = find_by_extension(s3, bucket, prefix, 'xdce')
# print(metadata_files)

# # download file test
# local_copy_location = download_file(s3, bucket, key_of_file, '/tmp')
# print("location of the local copy: " + local_copy_location)


# # upload a file test
# upload_file(s3, '/tmp/example.xdce', bucket, test_dir + "test_upload.xdce")
