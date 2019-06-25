# test sqs
from __future__ import print_function
import boto3
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time
import watchtower
import string

class JobQueue():

    def __init__(self, queueURL):
        self.client = boto3.client('sqs')
        self.queueURL = queueURL

    def readMessage(self):
        response = self.client.receive_message(
            QueueUrl=self.queueURL, WaitTimeSeconds=20)
        if 'Messages' in response.keys():
            data = json.loads(response['Messages'][0]['Body'])
            handle = response['Messages'][0]['ReceiptHandle']
            return data, handle
        else:
            return None, None

    def deleteMessage(self, handle):
        self.client.delete_message(
            QueueUrl=self.queueURL, ReceiptHandle=handle)
        return

    def returnMessage(self, handle):
        self.client.change_message_visibility(
            QueueUrl=self.queueURL, ReceiptHandle=handle, VisibilityTimeout=60)
        return


QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/263120685370/HCA-tasks'

queue = JobQueue(QUEUE_URL)
msg, handle = queue.readMessage()
if msg is not None:
    print(msg)