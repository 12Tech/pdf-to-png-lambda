import io
import os
import sys
import json
import logging
import subprocess

import boto3
from environs import Env
env = Env()
env.read_env()

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def convert(event, context):
    """ """

    data = extract_data_from_s3_event(event)

    object_key = data.get('key')
    bucket_name = data.get('bucket_name')

    object_name, memory_object = retrieve_fileobj_from_s3(bucket_name, object_key)

    tmp_file = os.path.join('tmp', object_key)
    with io.open(tmp_file, 'w+') as object_at_tmp:
        object_at_tmp.write(memory_object.read())

    cmd = "/opt/bin/convert"
    returned_value = subprocess.call(cmd, shell=True)  # returns the exit code in unix
    print('returned value:', returned_value)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def extract_data_from_event(event: dict) -> dict:
    """Extract data from triggered event"""

    sns_data = extract_data_from_sns_event(event)
    s3_event = json.loads(sns_data.get('Message'))
    s3_data = extract_data_from_s3_event(s3_event)

    return s3_data

def extract_data_from_sns_event(event: dict) -> dict:
    """Extract key from sns topic"""

    record_zero = event.get('Records')[0]
    subject = record_zero.get('Sns').get('Subject')
    message = record_zero.get('Sns').get('Message')


    sns_event_data = {
        "Subject": subject,
        "Message": message
    }

    return sns_event_data

def extract_data_from_s3_event(event: dict) -> dict:
    """Extract key from published s3 event"""

    record_zero = event.get('Records')[0]
    s3_region = record_zero.get('awsRegion')
    s3_bucket_name = record_zero.get('s3').get('bucket').get('name')
    s3_key = record_zero.get('s3').get('object').get('key')

    s3_event_data = {
        "region": s3_region,
        "bucket_name": s3_bucket_name,
        "key": s3_key
    }

    return s3_event_data

def retrieve_fileobj_from_s3(bucket_name: str, object_key: str) -> (str, io.BytesIO):
    """Retrieve a fileobj from s3 bucket and store it to a memory stream

    :param bucket_name: S3 Bucket name where the object is hosted
    :type bucket_name: str
    :param object_key: Object at the Bucket
    :type object_key: str
    :return: A tuple containing the object key plus the object content
    :rtype: tuple
    """

    s3_object = s3.Object(bucket_name, object_key)
    memory_object = io.BytesIO()
    s3_object.download_fileobj(memory_object)

    return (object_key, memory_object)