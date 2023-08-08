# -*- coding: utf-8 -*-

"""
This script continuously ingest data into DynamoDB table
"""

import json
import time
import random
from datetime import datetime, timezone

from faker import Faker
from ordered_set import OrderedSet
import pynamodb_mate as pm
from rich import print as rprint

from .config_init import config
from .boto_ses import bsm
from .dynamodb_table import Transaction
from .athena import run_athena_query

fake = Faker()


def get_utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)


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


def get_existing_account_set() -> OrderedSet[str]:
    # try to read all account from Hudi table via Athena
    try:
        bsm.glue_client.get_table(
            CatalogId=bsm.aws_account_id,
            DatabaseName=config.glue_database,
            Name=config.glue_table,
        )
        table_exists = True
    except Exception as e:
        # if table not found, start with an empty set
        if "Entity Not Found" in str(e):
            table_exists = False
        else:
            print(e)
            raise NotImplementedError

    if table_exists:
        df = run_athena_query(
            database=config.glue_database,
            sql=f"SELECT DISTINCT account FROM {config.glue_database}.{config.glue_table}",
        )
        account_set = OrderedSet(df[Transaction.account.attr_name])
    else:
        account_set = OrderedSet()  # in memory cache of all account
    return account_set


def new_transaction(
    account_set: OrderedSet[str],
):
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
    transaction.save()
    return transaction


def update_transaction_note(
    account_set: OrderedSet[str],
):
    """
    Simulate an event that update an existing transaction.
    """
    account = random.choice(account_set)  # choose an existing account

    # get 3 most recent related transaction
    transaction_list = list(
        Transaction.query(
            hash_key=account,
            scan_index_forward=False,
            limit=3,
        )
    )

    # randomly choose one to update
    transaction = random.choice(transaction_list)

    now = get_utc_now()
    note = fake.sentence()
    transaction.update_at = now
    transaction.note = note

    transaction.update(
        actions=[
            Transaction.update_at.set(now),
            Transaction.note.set(note),
        ]
    )

    return transaction


def run_data_faker(
    sleep_millisecond: int = 10,
    verbose: bool = True,
):
    with bsm.awscli():  # connect to AWS
        pm.Connection()

        account_set = get_existing_account_set()

        # create 1 initial transaction first
        # so that it's safe to call update_transaction_note()
        new_transaction(account_set)

        sleep_lower = int(sleep_millisecond * 0.5)
        sleep_upper = int(sleep_millisecond * 1.5)

        ith = 0
        while 1:
            ith += 1
            time.sleep(0.001 * random.randint(sleep_lower, sleep_upper))
            # 70% = insert, 30% = update
            if random.randint(1, 100) <= 70:
                operation = "insert"
                transaction = new_transaction(account_set)
            else:
                operation = "update"
                transaction = update_transaction_note(account_set)
            if verbose:
                # format the data for logging
                data = transaction.hudify()
                del data["id"]
                print(f"Simulate {ith} ith event: {operation} {data}")
