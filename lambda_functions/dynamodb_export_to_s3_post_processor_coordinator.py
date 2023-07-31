# -*- coding: utf-8 -*-

"""
This lambda function will be triggered by the creation of DynamoDB export to s3
manifest file. It parses the manifest file (data file list), group every 100 files
into a list and send it to the post processor worker lambda function.
"""

import typing as T
import json
import os
import boto3

s3_client = boto3.client("s3")
lbd_client = boto3.client("lambda")

DYNAMODB_EXPORT_TO_S3_POST_PROCESS_WORKER_FUNCTION_NAME = os.environ[
    "DYNAMODB_EXPORT_TO_S3_POST_PROCESS_WORKER_FUNCTION_NAME"
]


def grouper_list(iterable: T.Iterable, n: int) -> T.Iterable[list]:
    """Evenly divide list into fixed-length piece, no filled value if chunk
    size smaller than fixed-length.

    Example::

        >>> list(grouper_list(range(10), n=3)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    chunk = list()
    counter = 0
    for item in iterable:
        counter += 1
        chunk.append(item)
        if counter == n:
            yield chunk
            chunk = list()
            counter = 0
    if len(chunk) > 0:
        yield chunk


def lambda_handler(event, context):
    """
    :param event: triggered by s3 put event, example:
        s3://111122223333-us-east-1-data/projects/dynamodb_to_datalake/dynamodb_export/AWSDynamoDB/01690735825858-1a2b3c4d/manifest-files.json
    """
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    parts = key.split("/")
    if (parts[-1] != "manifest-files.json") or (parts[-3] != "AWSDynamoDB"):
        raise ValueError(f"s3://{bucket}/{key} is not a dynamodb export manifest file!")
    parts[-4] = "dynamodb_export_processed"
    parts.pop()
    parts.append("data")
    dynamodb_export_processed_prefix = "/".join(parts)

    res = s3_client.get_object(Bucket=bucket, Key=key)
    lines = res["Body"].read().splitlines()

    all_key_list = [json.loads(line)["dataFileS3Key"] for line in lines]

    for ith, key_list in enumerate(
        grouper_list(all_key_list, 100),
        start=1,
    ):
        lbd_client.invoke(
            FunctionName=DYNAMODB_EXPORT_TO_S3_POST_PROCESS_WORKER_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(
                {
                    "ith": ith,
                    "bucket": bucket,
                    "key_list": key_list,
                    "dynamodb_export_processed_prefix": dynamodb_export_processed_prefix,
                }
            ),
        )
