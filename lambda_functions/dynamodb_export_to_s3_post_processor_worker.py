# -*- coding: utf-8 -*-

"""
This lambda function will be invoked by the dynamodb export to s3 post processor
coordinator lambda function.
"""

import json
import gzip

import boto3

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """
    :param event: example, {"ith": ..., "bucket": ..., "key_list": [...]}
    """
    ith = event["ith"]
    bucket = event["bucket"]
    key_list = event["key_list"]
    dynamodb_export_processed_prefix = event["dynamodb_export_processed_prefix"]

    lines = list()
    for key in key_list:
        # read data
        res = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
        items = [
            json.loads(line)
            for line in (
                gzip.decompress(res["Body"].read()).decode("utf-8").splitlines()
            )
        ]
        # process
        for item in items:
            account = item["Item"]["account"]["S"]
            create_at = item["Item"]["create_at"]["S"]
            update_at = item["Item"]["update_at"]["S"]
            entity = item["Item"]["entity"]["S"]
            amount = int(item["Item"]["amount"]["N"])
            is_credit = int(item["Item"]["is_credit"]["N"])
            note = item["Item"]["note"]["S"]

            row = dict(
                account=account,
                create_at=create_at,
                update_at=update_at,
                entity=entity,
                amount=amount,
                is_credit=is_credit,
                note=note,
            )
            line = json.dumps(row)
            lines.append(line)

    # write data
    key = f"{dynamodb_export_processed_prefix}/{str(ith).zfill(6)}.json.gz"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=gzip.compress("\n".join(lines).encode("utf-8")),
        ContentType="application/x-gzip",
    )
