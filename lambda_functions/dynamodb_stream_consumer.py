# -*- coding: utf-8 -*-

"""
This is the DynamoDB stream consumer lambda function source code.

This script reads data from DynamoDB Stream, transform it, and save it to S3.

- batch size: 100
- batch window: 10 seconds
"""

import typing as T
import os
import json
import uuid
from datetime import datetime

import boto3

s3_client = boto3.client("s3")
sts_client = boto3.client("sts")
aws_account_id = sts_client.get_caller_identity()["Account"]
aws_region = os.environ["AWS_REGION"]

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_PREFIX"]  # processed dynamodb stream data s3 folder
if S3_PREFIX.endswith("/"):
    S3_PREFIX = S3_PREFIX[:-1]


def lambda_handler(event, context):
    records = event["Records"]
    print(f"received {len(records)} records")
    # print(records[:3]) # for debug only

    groups: T.Dict[str, T.List[dict]] = dict()  # partition data by update_at
    for record in records:
        if record["eventName"] == "REMOVE":  # ignore delete event
            continue

        # parse dynamodb stream record
        dynamodb_data = record["dynamodb"]
        account = dynamodb_data["Keys"]["account"]["S"]
        create_at = dynamodb_data["Keys"]["create_at"]["S"]
        update_at = dynamodb_data["NewImage"]["update_at"]["S"]
        entity = dynamodb_data["NewImage"]["entity"]["S"]
        amount = int(dynamodb_data["NewImage"]["amount"]["N"])
        is_credit = int(dynamodb_data["NewImage"]["is_credit"]["N"])
        note = dynamodb_data["NewImage"]["note"]["S"]

        data = dict(
            account=account,
            create_at=create_at,
            update_at=update_at,
            entity=entity,
            amount=amount,
            is_credit=is_credit,
            note=note,
        )

        # partition data by update_at, this field indicate when this record is updated
        update_at_datetime = datetime.strptime(update_at, "%Y-%m-%dT%H:%M:%S.%f%z")
        year = str(update_at_datetime.year).zfill(4)
        month = str(update_at_datetime.month).zfill(2)
        day = str(update_at_datetime.day).zfill(2)
        hour = str(update_at_datetime.hour).zfill(2)
        minute = str(update_at_datetime.minute).zfill(2)
        partition = f"{year}-{month}-{day}-{hour}-{minute}"
        try:
            groups[partition].append(data)
        except KeyError:
            groups[partition] = [data]

    # write cdc data to s3 by partition
    for partition, data_list in groups.items():
        year, month, day, hour, minute = partition.split("-")
        bucket = S3_BUCKET
        key = (
            f"{S3_PREFIX}"
            f"/year={year}/month={month}/day={day}/hour={hour}/minute={minute}"
            f"/{uuid.uuid4().hex}.json"
        )
        lines = [json.dumps(data) for data in data_list]
        print(f"write {len(data_list)} records to s3://{bucket}/{key}")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body="\n".join(lines),
            ContentType="application/json",
        )
