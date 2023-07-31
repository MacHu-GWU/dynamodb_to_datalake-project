# -*- coding: utf-8 -*-

import pynamodb_mate as pm

from .config_init import config
from .boto_ses import bsm


class Transaction(pm.Model):
    """
    Dynamodb table data model
    """

    class Meta:
        table_name = config.dynamodb_table
        region = config.aws_region
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    account = pm.UnicodeAttribute(hash_key=True)
    create_at = pm.UTCDateTimeAttribute(range_key=True)
    update_at = pm.UTCDateTimeAttribute()
    entity = pm.UnicodeAttribute()
    amount = pm.NumberAttribute()
    is_credit = pm.NumberAttribute()  # 0 or 1
    note = pm.UnicodeAttribute(null=True)


def get_dynamodb_table_console_url(
    aws_region: str,
    table: str,
) -> str:
    return (
        f"https://{aws_region}.console.aws.amazon.com"
        f"/dynamodbv2/home?region={aws_region}#table?name={table}"
    )


def create_dynamodb_table():
    with bsm.awscli():
        Transaction.create_table(wait=True)


def enable_point_in_time_recovery():
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_continuous_backups.html
    bsm.dynamodb_client.update_continuous_backups(
        TableName=config.dynamodb_table,
        PointInTimeRecoverySpecification=dict(
            PointInTimeRecoveryEnabled=True,
        ),
    )


def enable_dynamodb_stream():
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_table.html
    try:
        bsm.dynamodb_client.update_table(
            TableName=config.dynamodb_table,
            StreamSpecification=dict(
                StreamEnabled=True,
                StreamViewType="NEW_AND_OLD_IMAGES",
            ),
        )
    except Exception as e:
        if "Table already has an enabled stream" in str(e):
            pass
        else:
            raise e


def delete_dynamodb_table():
    with bsm.awscli():
        try:
            Transaction.delete_table()
        except Exception as e:
            if "not found" in str(e).lower():
                pass
            else:
                raise NotImplementedError
