# -*- coding: utf-8 -*-

import time
import pynamodb_mate as pm

from .config import DYNAMODB_TABLE
from .boto_ses import bsm


class Transaction(pm.Model):
    """
    Dynamodb table data model
    """

    class Meta:
        table_name = DYNAMODB_TABLE
        region = bsm.aws_region
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    account = pm.UnicodeAttribute(hash_key=True)
    create_at = pm.UTCDateTimeAttribute(range_key=True)
    update_at = pm.UTCDateTimeAttribute()
    entity = pm.UnicodeAttribute()
    amount = pm.NumberAttribute()
    is_credit = pm.NumberAttribute()  # 0 or 1
    note = pm.UnicodeAttribute(null=True)


DYNAMODB_TABLE_ARN = (
    f"arn:aws:dynamodb:{bsm.aws_region}:{bsm.aws_account_id}:table/{DYNAMODB_TABLE}"
)


def create_dynamodb_table():
    print(f"create DynamoDB table {DYNAMODB_TABLE!r}")
    with bsm.awscli():
        Transaction.create_table(wait=True)

    # ref: update_continuous_backups
    print("enable point in time recovery")
    bsm.dynamodb_client.update_continuous_backups(
        TableName=DYNAMODB_TABLE,
        PointInTimeRecoverySpecification=dict(
            PointInTimeRecoveryEnabled=True,
        ),
    )
    time.sleep(5)

    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_table.html
    print("enable dynamodb stream")
    bsm.dynamodb_client.update_table(
        TableName=DYNAMODB_TABLE,
        StreamSpecification=dict(
            StreamEnabled=True,
            StreamViewType="NEW_AND_OLD_IMAGES",
        ),
    )

    console_url = (
        f"https://{bsm.aws_region}.console.aws.amazon.com"
        f"/dynamodbv2/home?region={bsm.aws_region}#table?name={DYNAMODB_TABLE}"
    )
    print(f"preview at: {console_url}")


def delete_dynamodb_table():
    print(f"delete DynamoDB table {DYNAMODB_TABLE!r}")
    with bsm.awscli():
        Transaction.delete_table()
