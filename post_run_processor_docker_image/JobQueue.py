# a class to handle job queue operation

import json
import boto3

class JobQueue():
    def __init__(self, queueURL):
        self.client = boto3.client('sqs')
        self.queueURL = queueURL

    def readMessage(self):
        response = self.client.receive_message(QueueUrl=self.queueURL,
                                               WaitTimeSeconds=20)
        if 'Messages' in response.keys():
            data = json.loads(response['Messages'][0]['Body'])
            handle = response['Messages'][0]['ReceiptHandle']
            return data, handle
        else:
            return None, None

    def deleteMessage(self, handle):
        self.client.delete_message(QueueUrl=self.queueURL,
                                   ReceiptHandle=handle)
        return

    def returnMessage(self, handle):
        self.client.change_message_visibility(QueueUrl=self.queueURL,
                                              ReceiptHandle=handle,
                                              VisibilityTimeout=60)
        return
    
    def enqueueMessage(self, the_message_in_json):
        message = json.dumps(the_message_in_json)
        print("sending the message..")
        response = self.client.send_message(
            QueueUrl=self.queueURL,
            MessageBody=message,
        )

        print("message sent")
