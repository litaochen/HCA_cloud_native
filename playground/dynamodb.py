import boto3
from boto3.dynamodb.conditions import Key, Attr



run_table = boto3.resource('dynamodb').Table('runs')

# update run status to running if it is not yet
response = run_table.update_item(
    Key={
        'user_id': 'litao',
        'submit_date': '1562790206606'
    },
    UpdateExpression="set the_status = :new_status",
    ConditionExpression='the_status = :current_status',
    ExpressionAttributeValues={
        ':new_status': 'Scheduled',
        ':current_status': 'Running'
    },
    ReturnValues="ALL_NEW")
print(response)