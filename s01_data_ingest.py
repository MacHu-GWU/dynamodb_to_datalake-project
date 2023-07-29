# -*- coding: utf-8 -*-

"""
This script continuously ingest data into DynamoDB table
"""

import time
import random
from datetime import datetime, timezone

from faker import Faker
from ordered_set import OrderedSet
import pynamodb_mate as pm
from rich import print as rprint

from s00_lib import bsm, database, table, s3dir_athena_result

fake = Faker()


def get_utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)


class Transaction(pm.Model):
    """
    Dynamodb table data model
    """

    class Meta:
        table_name = table
        region = bsm.aws_region
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    account = pm.UnicodeAttribute(hash_key=True)
    create_at = pm.UTCDateTimeAttribute(range_key=True)
    update_at = pm.UTCDateTimeAttribute()
    entity = pm.UnicodeAttribute()
    amount = pm.NumberAttribute()
    is_credit = pm.NumberAttribute()  # 0 or 1
    note = pm.UnicodeAttribute(null=True)


digits = "0123456789"


def rnd_digits(n: int) -> str:
    return "".join([random.choice(digits) for _ in range(n)])


def random_account() -> str:
    return "-".join(
        [
            rnd_digits(3),
            rnd_digits(3),
            rnd_digits(4),
        ]
    )


# try to read all account from Hudi table via Athena
try:
    bsm.glue_client.get_table(
        CatalogId=bsm.aws_account_id,
        DatabaseName=database,
        Name=table,
    )
    response = bsm.athena_client.start_query_execution(
        QueryString=f"SELECT DISTINCT account FROM {database}.{table}",
        QueryExecutionContext=dict(
            Catalog="AwsDataCatalog",
            Database=database,
        ),
        ResultConfiguration=dict(
            OutputLocation=s3dir_athena_result.uri,
        ),
    )
    exec_id = response["QueryExecutionId"]
    time.sleep(3)  # wait 3 seconds for the query to finish
    s3path_athena_result = s3dir_athena_result.joinpath(f"{exec_id}.csv")
    _accounts = [
        line[1:-1] for line in s3path_athena_result.read_text().splitlines()[1:]
    ]
    account_set = OrderedSet(_accounts)
except Exception as e:
    # if table not found, start with an empty set
    if "Entity Not Found" in str(e):
        account_set = OrderedSet()  # in memory cache of all account
    else:
        print(e)
        raise NotImplementedError


def new_transaction():
    """
    Simulate an event that create a new transaction.
    """
    account = random_account()
    account_set.add(account)
    now = get_utc_now()
    transaction = Transaction(
        account=account,
        create_at=now,
        update_at=now,
        entity=fake.company(),
        amount=random.randint(1, 1000),
        is_credit=random.randint(0, 1),
        note=fake.sentence(),
    )
    # print(f"create new transaction: {transaction.attribute_values}")
    transaction.save()


def update_transaction_note():
    """
    Simulate an event that update an existing transaction.
    """
    account = random.choice(account_set)  # choose an existing account
    # get all related transaction
    transaction_list = list(
        Transaction.query(
            hash_key=account,
            scan_index_forward=False,
            limit=3,
        )
    )
    transaction = random.choice(transaction_list)  # randomly choose one to update
    now = get_utc_now()
    transaction.update(
        actions=[
            Transaction.update_at.set(now),
            Transaction.note.set(fake.sentence()),
        ]
    )


def run_data_faker():
    # create 1 initial transaction first
    new_transaction()

    ith = 0
    while 1:
        ith += 1
        time.sleep(
            0.001 * random.randint(5, 15)
        )  # create a new event every 0.01 seconds
        if random.randint(1, 100) <= 90:
            new_transaction()
        else:
            update_transaction_note()
        print(f"finished ith: {ith}")


if __name__ == "__main__":
    with bsm.awscli():  # connect to AWS
        pm.Connection()

        # Transaction.delete_all()
        run_data_faker()
