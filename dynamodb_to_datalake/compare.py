# -*- coding: utf-8 -*-

import typing as T
import random

import polars as pl
from rich import print as rprint
from datetime import datetime, timezone

from .config_init import config
from .boto_ses import bsm
from .dynamodb_table import Transaction
from .athena import run_athena_query


def hudify_transaction(transaction: Transaction) -> dict:
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
    return row


def read_from_dynamodb() -> T.List[T.Dict[str, T.Any]]:
    rows = list()
    for transaction in Transaction.scan(
        # limit=10,
    ):
        row = hudify_transaction(transaction)
        rows.append(row)

    df = pl.DataFrame(rows)
    df = df.sort("id")
    records = df.to_dicts()
    # rprint(records[:3])
    print(f"dynamodb table shape: {df.shape}")
    return records


def read_from_hudi() -> T.List[T.Dict[str, T.Any]]:
    df = run_athena_query(
        database=config.glue_database,
        sql=f"SELECT * FROM {config.glue_database}.{config.glue_table}",
    )
    df = df.drop(
        [
            "_hoodie_commit_time",
            "_hoodie_commit_seqno",
            "_hoodie_record_key",
            "_hoodie_partition_path",
            "_hoodie_file_name",
        ]
    )

    df = df.sort("id")
    records = df.to_dicts()
    # rprint(records[:3])
    print(f"hudi table shape: {df.shape}")
    return records


def compare():
    """
    Compare the data in dynamodb and hudi, see if they are exactly the same.
    """
    records1 = read_from_dynamodb()
    records2 = read_from_hudi()
    print(f"records in dynamodb: {len(records2)}")
    print(f"records in hudi: {len(records1)}")
    records1_id_set = {record["id"] for record in records1}
    records2_id_set = {record["id"] for record in records2}
    delta_id_set = records1_id_set.difference(records2_id_set)
    delta_id_list = list(delta_id_set)
    rprint(delta_id_list[:10])
    is_same = True
    for record1, record2 in zip(records1, records2):
        if record1 != record2:
    #         print("-" * 80)
    #         rprint(record1)
    #         rprint(record2)
            is_same = False
    #         for key, value1 in record1.items():
    #             value2 = record2[key]
    #             if value1 != value2:
    #                 print(f"{key}: {value1} != {value2}")
    if is_same is True:
        print("The data in dynamodb and hudi are exactly the same.")
    else:
        print("The data in dynamodb and hudi are not the same.")


def investigate():
    records2 = read_from_hudi()

    for _ in range(10):
        record2 = random.choice(records2)
        account = record2["account"]
        create_at = record2["create_at"]
        create_at_datetime = datetime.strptime(create_at, "%Y-%m-%dT%H:%M:%S.%f%z")
        transaction = Transaction.get(account, create_at_datetime)
        record1 = hudify_transaction(transaction)
        if record1 != record2:
            print("-" * 80)
            rprint(record1)
            rprint(record2)
            for key, value1 in record1.items():
                value2 = record2[key]
                if value1 != value2:
                    print(f"{key}: {value1} != {value2}")