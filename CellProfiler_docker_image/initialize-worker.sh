#!/bin/bash

# constants
# number of seconds to wait to start next CP worker process
SECONDS_TO_START=30

# arguments are passed into the container as environment variables
echo "App: $APP_NAME"
echo "Region: $AWS_REGION"
echo 'SQS URL: $SQS_URL'
echo 'Cloudwatch log group: $CLOUDWATCH_LOG_GROUP_NAME'
echo 'Cloudwatch log stream: $CLOUDWATCH_LOG_STREAM_NAME'
echo 'Number of cores assigned: $NUM_CORES'

# arguments created here and will be passed to CP worker
export IMAGE_DATA_BUCKET='/home/ubuntu/image_data_bucket'
export TASK_INPUT_DIR='/home/ubuntu/task_input'
export TASK_OUTPUT_DIR='/home/ubuntu/task_output'


# 1. CONFIGURE AWS CLI
echo "configuring AWS CLI..."
aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
aws configure set default.region $AWS_REGION

echo "Trying to tag instance for cost analysis..."
MY_INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id -m 2 -s)
if [ $? -eq 0 ]; then
    echo "Instance ID $MY_INSTANCE_ID"
    aws ec2 create-tags --resources $MY_INSTANCE_ID --tags Key=Name,Value=${APP_NAME}-Worker
else
    echo "Can't retrieve INSTANCE_ID, instance not tagged."
fi 

# 2. PREPARE DIRECTORIES FOR ANALYSIS AND S3 MOUNTING
#       image_data_bucket:  the bucket that stores the images to process
#       task_input_bucket:  the bucket to store task input data, like file-list and pipeline file
#       task_output:       the place to hold analysis run output. Will be 

echo "Preparing directories for S3 bucket mounting..."
echo $AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY > /home/ubuntu/aws_credentials
chmod 600 /home/ubuntu/aws_credentials
mkdir -p "$IMAGE_DATA_BUCKET"
# mount by command: s3fs the-s3-bucket $IMAGE_DATA_BUCKET -o passwd_file=/home/ubuntu/aws_credentials
# unmount by command: umount $IMAGE_DATA_BUCKET

echo "setting up task input and output directories..."
mkdir -p "$TASK_INPUT_DIR"
mkdir -p "$TASK_INPUT_DIR"

# 3. RUN CP WORKERS. 
echo "All set! Starting CP worker...."
for((k=0; k<$NUM_CORES; k++)); do
    python ./cp-worker.py |& tee $k.out &
    sleep $SECONDS_TO_START
done
wait