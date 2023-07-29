# -*- coding: utf-8 -*-

"""
This script continuously ingest data into DynamoDB table
"""

import typing as T
import csv
import time
import random
from datetime import datetime, timezone

import pandas as pd
import pynamodb_mate as pm
from boto_session_manager import BotoSesManager
from s3pathlib import S3Path, context
from rich import print as rprint

# ------------------------------------------------------------------------------
aws_profile = "awshsh_app_dev_us_east_1"
# ------------------------------------------------------------------------------

bsm = BotoSesManager(profile_name=aws_profile)
aws_account_id = bsm.aws_account_id
aws_region = bsm.aws_region
context.attach_boto_session(bsm.boto_ses)


class Transaction(pm.Model):
    """
    Dynamodb table data model
    """

    class Meta:
        table_name = "transaction"
        region = aws_region
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    account = pm.UnicodeAttribute(hash_key=True)
    create_at = pm.UTCDateTimeAttribute(range_key=True)
    update_at = pm.UTCDateTimeAttribute()
    entity = pm.UnicodeAttribute()
    amount = pm.NumberAttribute()
    is_credit = pm.NumberAttribute()  # 0 or 1
    note = pm.UnicodeAttribute(null=True)


with bsm.awscli():  # connect to AWS
    pm.Connection()

Transaction.create_table(wait=True)

_data = list()
for transaction in Transaction.scan():
    account = transaction.attribute_values["account"]
    create_at_datetime = transaction.attribute_values["create_at"]
    update_at_datetime = transaction.attribute_values["update_at"]
    entity = transaction.attribute_values["entity"]
    amount = transaction.attribute_values["amount"]
    is_credit = transaction.attribute_values["is_credit"]
    note = transaction.attribute_values["note"]

    create_at = create_at_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    update_at = update_at_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    create_year = create_at_datetime.year
    create_month = create_at_datetime.month
    create_day = create_at_datetime.day
    create_hour = create_at_datetime.hour
    create_minute = create_at_datetime.minute

    row = dict(
        id=f"account:{account},create_at:{create_at}",
        account=account,
        create_at=create_at,
        create_year=create_year,
        create_month=create_month,
        create_day=create_day,
        create_hour=create_hour,
        create_minute=create_minute,
        update_at=update_at,
        entity=entity,
        amount=amount,
        is_credit=is_credit,
        note=note,
    )
    # if create_at != update_at:
    #     rprint(row)
    _data.append(row)

df1 = pd.DataFrame(_data)
df1 = df1.sort_values("id")
df1_records = df1.to_dict(orient="records")
# rprint(df1_records[:3])
print(f"df1.shape = {df1.shape}")

# ---
s3dir_athena_result = S3Path(f"s3://{bsm.aws_account_id}-{bsm.aws_region}-data/athena/results/").to_dir()
response = bsm.athena_client.start_query_execution(
    QueryString="SELECT * FROM mydatabase.transaction",
    QueryExecutionContext=dict(
        Catalog="AwsDataCatalog",
        Database="mydatabase",
    ),
    ResultConfiguration=dict(
        OutputLocation=s3dir_athena_result.uri,
    )
)
exec_id = response["QueryExecutionId"]
time.sleep(3)
s3path_athena_result = s3dir_athena_result.joinpath(f"{exec_id}.csv")

with s3path_athena_result.open("r") as f:
    df2 = pd.read_csv(f)
    df2 = df2.drop(
        [
            "_hoodie_commit_time",
            "_hoodie_commit_seqno",
            "_hoodie_record_key",
            "_hoodie_partition_path",
            "_hoodie_file_name",
        ],
        axis=1,
    )
    df2 = df2.sort_values("id")
    df2_records = df2.to_dict(orient="records")
    # rprint(df2_records[:3])
    print(f"df2.shape = {df2.shape}")


for record1, record2 in zip(df1_records, df2_records):
    if record1 != record2:
        if len(record1) != len(record2):
            print("record1 and record2 has different number of fields")
        if record2["entity"] == record2["note"]:
            continue
        print("-" * 80)
        rprint(record1)
        rprint(record2)
        for key, value1 in record1.items():
            value2 = record2[key]
            if value1 != value2:
                print(f"{key}: {value1} != {value2}")

