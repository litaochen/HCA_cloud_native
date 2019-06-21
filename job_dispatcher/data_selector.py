# functions used to allow user to select dataset to analyz
# will be refactord to Lambda functions later

import boto3

client = boto3.client("s3")


# get list if directories / flies in current "directory"
def get_content_in_dir(the_bucket, the_prefix):
    # notice we only returns the first 100 objects. Enough for our purpose
    response = client.list_objects_v2(
        Bucket=the_bucket,
        Delimiter='/',
        Prefix=the_prefix,
        MaxKeys=100,
        StartAfter=the_prefix
    )

    # get dir or file list
    if 'CommonPrefixes' in response.keys():
        return [item['Prefix'] for item in response['CommonPrefixes']]
    elif 'Contents' in response.keys():
        return [file['Key'] for file in response['Contents']]
    else:
        raise KeyError("oops, for the bucket: " + the_bucket +
                       ", this directory does not exist: " + the_prefix)


# check if certain file type exists in current dir
# mainly used to check if the metadata file "xdce" exists.
# if find the file, return the key of the file
def find_by_extension(bucket, the_prefix, file_extension):
    kwargs = {'Bucket': bucket, 'Prefix': the_prefix, 'Delimiter': '/'}

    while True:
        response = client.list_objects_v2(**kwargs)
        if not 'Contents' in response.keys():
            raise KeyError("this directory does not exist: " + the_prefix)

        for obj in response['Contents']:
            if obj['Key'].split['.'][-1] == file_extension:
                return obj['Key']

        try:
            kwargs['ContinuationToken'] = response['NextContinuationToken']
        except KeyError:
            return None


# test
try:
    # test list file / dir function
    print(get_content_in_dir("hca-cloud-native", "example_data/"))

    # print(find_by_extension("hca-cloud-native", "example_data/", "xdce"))
except KeyError as error:
    print(error)
