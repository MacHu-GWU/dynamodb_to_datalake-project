# -*- coding: utf-8 -*-

import typing as T
import textwrap

import polars as pl
from rich import print as rprint

from .config_init import config
from .dynamodb_table import Transaction
from .athena import run_athena_query


T_RECORDS = T.List[T.Dict[str, T.Any]]


def read_from_dynamodb_table() -> T_RECORDS:
    rows = list()
    for transaction in Transaction.scan(
        # limit=10,
    ):
        rows.append(transaction.hudify())
    df = pl.DataFrame(rows)
    df = df.sort("id")
    records = df.to_dicts()
    return records


def read_from_hudi_table() -> T_RECORDS:
    df = run_athena_query(
        database=config.glue_database,
        sql=textwrap.dedent(f"""
        SELECT
            id,
            {Transaction.account.attr_name},
            {Transaction.create_at.attr_name},
            {Transaction.update_at.attr_name},
            {Transaction.entity.attr_name},
            {Transaction.amount.attr_name},
            {Transaction.is_credit.attr_name},
            {Transaction.note.attr_name}
        FROM {config.glue_database}.{config.glue_table} ORDER BY id
        """),
        verbose=False,
    )
    rows = df.to_dicts()
    return rows


def compare():
    """
    Compare the data in dynamodb and hudi, see if they are exactly the same.
    """
    dynamodb_rows = read_from_dynamodb_table()
    hudi_rows = read_from_hudi_table()
    n_dynamodb_rows = len(dynamodb_rows)
    n_hudi_rows = len(hudi_rows)
    print(f"n_dynamodb_rows: {n_dynamodb_rows}")
    print(f"n_hudi_rows: {n_hudi_rows}")
    if n_dynamodb_rows != n_hudi_rows:
        raise ValueError("n_dynamodb_rows != n_hudi_rows")
    else:
        print("NICE! n_dynamodb_rows == n_hudi_rows")

    # print first 10 rows that are different
    is_same = True
    diff_count = 0
    for dynamodb_row, hudi_row in zip(dynamodb_rows, hudi_rows):
        if dynamodb_row != hudi_row:
            is_same = False
            diff_count += 1
            if diff_count <= 10:
                print(f"dynamodb_row: {dynamodb_row}")
                print(f"hudi_row: {hudi_row}")

    if is_same is True:
        print("NICE! The data in dynamodb and hudi are exactly the same.")
    else:
        print("OPS! The data in dynamodb and hudi are not the same.")
