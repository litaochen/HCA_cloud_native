#!/bin/bash

# constants
# number of seconds to wait to start next CP worker process
SECONDS_TO_START=30

# arguments are passed into the container as environment variables
echo "App: $APP_NAME"
echo "Region: $AWS_REGION"
echo "result consolidation queue url: $RESULT_CONSOLIDATION_QUEUE_URL"
echo "Cloudwatch log group: $CLOUDWATCH_LOG_GROUP_NAME"
echo "Cloudwatch log stream: $CLOUDWATCH_LOG_STREAM_NAME"
echo "Number of cores assigned: $NUM_CORES"


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


# 3. RUN CP WORKERS. 
echo "All set! Starting CP worker...."
bash

# for((k=0; k<$NUM_CORES; k++)); do
#     python ./cp-worker.py |& tee $k.out &
#     sleep $SECONDS_TO_START
# done
# wait