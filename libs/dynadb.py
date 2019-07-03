# a lib for working with AWS DynamoDB

import boto3
from boto3.dynamodb.conditions import Key, Attr
import time
millis = str(int(round(time.time() * 1000)))
print(millis)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('tasks')


# get status
response = table.query(
    KeyConditionExpression=Key('run_id').eq('litao_1562118658410')
)

tasks_status =  [ x['status'] for x in  response['Items'] ]

# print(tasks_status)


# update status
response = table.update_item(
    Key={
        'run_id': 'litao_1562118658410',
        'task_id': 'H - 1'
    },
    UpdateExpression="set the_status = :new_status",
    ExpressionAttributeValues={
        ':new_status': 'finished'
    },
    ReturnValues="ALL_NEW"
)

print(response)