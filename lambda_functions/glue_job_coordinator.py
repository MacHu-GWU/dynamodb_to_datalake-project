# -*- coding: utf-8 -*-

import os
import boto3

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")

DYNAMODB_EXPORT_TO_S3_POST_PROCESS_WORKER_FUNCTION_NAME = os.environ[
    "DYNAMODB_EXPORT_TO_S3_POST_PROCESS_WORKER_FUNCTION_NAME"
]

def lambda_handler(event, context):
    pass