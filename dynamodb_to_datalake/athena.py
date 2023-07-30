# -*- coding: utf-8 -*-

"""
Athena related functions.
"""

import polars as pl

from .config_init import config
from .boto_ses import bsm
from .s3paths import s3dir_athena_result
from .paths import path_query_result
from .waiter import Waiter


def run_athena_query(
    database: str,
    sql: str,
) -> pl.DataFrame:
    """
    Run athena query and get the result as a polars.DataFrame.
    """
    print(f"run_athena_query:")
    print(sql)
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/start_query_execution.html
    response = bsm.athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext=dict(
            Catalog="AwsDataCatalog",
            Database=database,
        ),
        ResultConfiguration=dict(
            OutputLocation=s3dir_athena_result.uri,
        ),
    )
    exec_id = response["QueryExecutionId"]

    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/get_query_execution.html
    for _ in Waiter(
        delays=1,
        timeout=10,
    ):
        response = bsm.athena_client.get_query_execution(
            QueryExecutionId=exec_id,
        )
        status = response["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "CANCELLED"]:
            raise RuntimeError(f"status = {status}")
        else:
            pass

    print("")

    s3path_athena_result = s3dir_athena_result.joinpath(f"{exec_id}.csv")
    with s3path_athena_result.open("rb") as f:
        df = pl.read_csv(f.read())
    return df


def preview_hudi_table(
    limit: int = 10,
):
    """
    Preview the Dynamodb equavilent Hudi table via Athena.
    """
    print(f"preview hudi table '{config.glue_database}.{config.glue_table}'")
    df = run_athena_query(
        database=config.glue_database,
        sql=f"SELECT * FROM {config.glue_table} LIMIT {limit}",
    )
    df.write_csv(str(path_query_result), has_header=True)
    print(f"preview data: file://{path_query_result}")

    df = run_athena_query(
        database=config.glue_database,
        sql=f"SELECT COUNT(*) as n_rows FROM {config.glue_table}",
    )
    n_rows = df.to_dicts()[0]["n_rows"]
    print(f"n_rows = {n_rows}")
