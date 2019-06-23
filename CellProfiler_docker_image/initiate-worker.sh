#!/bin/bash

# arguments are passed into the container as environment variables
echo "App: $APP_NAME"
echo "Region: $AWS_REGION"
echo "Queue: $SQS_URL"
echo "Number of cores: $NUM_CORES"

# 1. CONFIGURE AWS CLI
aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
aws configure set default.region $AWS_REGION
MY_INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id -m 2 -s)
if [ $? -ne 0 ]; then
    echo "Instance ID $MY_INSTANCE_ID"
    aws ec2 create-tags --resources $MY_INSTANCE_ID --tags Key=Name,Value=${APP_NAME}-Worker
fi 

# 2. PREPARE DIRECTORIES FOR ANALYSIS AND S3 MOUNTING
#       image_data_bucket:  the bucket that stores the images to process
#       task_input_bucket:  the bucket to store task input data, like file-list and pipeline file
#       task_output:       the place to hold analysis run output. Will be 

echo $AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY > /home/ubuntu/aws_credentials
chmod 600 /home/ubuntu/credentials
mkdir -p /home/ubuntu/image_data_bucket
# mount by command: s3fs the-s3-bucket /home/ubuntu/image_data_bucket -o passwd_file=/home/ubuntu/aws_credentials

mkdir -p /home/ubuntu/task_input
mkdir -p /home/ubuntu/task_output

# 3. SET UP ALARMS
# aws cloudwatch put-metric-alarm --alarm-name ${APP_NAME}_${MY_INSTANCE_ID} --alarm-actions arn:aws:swf:${AWS_REGION}:${OWNER_ID}:action/actions/AWS_EC2.InstanceId.Terminate/1.0 --statistic Maximum --period 60 --threshold 1 --comparison-operator LessThanThreshold --metric-name CPUUtilization --namespace AWS/EC2 --evaluation-periods 15 --dimensions "Name=InstanceId,Value=${MY_INSTANCE_ID}" 

# 4. RUN CP WORKERS
for((k=0; k<$NUM_CORES; k++)); do
    python cp-worker.py |& tee $k.out &
    sleep $SECONDS_TO_START
done
wait