# -*- coding: utf-8 -*-

from . import s3paths
from .config_init import config
from .dynamodb_table import get_dynamodb_table_console_url
from .lambda_function import get_lambda_function_console_url
from .glue_catalog import get_glue_database_console_url
from .glue_job import get_glue_job_console_url

def show_info():
    print("------ S3 info")
    print(f"s3dir_artifacts: {s3paths.s3dir_artifacts.console_url}")
    print(f"s3dir_data: {s3paths.s3dir_data.console_url}")
    print(f"s3dir_glue_artifacts: {s3paths.s3dir_glue_artifacts.console_url}")

    print(f"s3dir_dynamodb_stream: {s3paths.s3dir_dynamodb_stream.console_url}")
    print(f"s3dir_dynamodb_export: {s3paths.s3dir_dynamodb_export.console_url}")
    print(f"s3dir_dynamodb_export_processed: {s3paths.s3dir_dynamodb_export_processed.console_url}")
    print(f"s3dir_database: {s3paths.s3dir_database.console_url}")
    print(f"s3dir_table: {s3paths.s3dir_table.console_url}")

    print("------ DynamoDB")
    url = get_dynamodb_table_console_url(
        aws_region=config.aws_region,
        table=config.dynamodb_table,
    )
    print(f"dynamodb table {config.dynamodb_table!r}: {url}")

    print("------ Lambda Functions")
    url = get_lambda_function_console_url(
        aws_region=config.aws_region,
        function_name=config.lambda_function_name_dynamodb_stream_consumer,
    )
    print(f"lambda function {config.lambda_function_name_dynamodb_stream_consumer!r}: {url}")

    print("------ Glue Catalog")
    url = get_glue_database_console_url(
        aws_region=config.aws_region,
        database=config.glue_database,
    )
    print(f"glue database {config.glue_database!r}: {url}")

    print("------ Glue Jobs")
    url = get_glue_job_console_url(
        aws_region=config.aws_region,
        job_name=config.glue_job_name_initial_load,
    )
    print(f"glue job {config.glue_job_name_initial_load!r}: {url}")

    url = get_glue_job_console_url(
        aws_region=config.aws_region,
        job_name=config.glue_job_name_incremental,
    )
    print(f"glue job {config.glue_job_name_incremental!r}: {url}")

