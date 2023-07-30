# -*- coding: utf-8 -*-

import time

from s3pathlib import S3Path

from .conifg_init import config
from .boto_ses import bsm
from .s3_bucket import create_bucket


from . import dynamodb_table
from . import glue_catalog
from . import glue_job
from . import lambda_function
from . import paths
from . import s3paths


def create_infrastructure():
    print("=== Create AWS Resources")
    print("--- Create S3 buckets")
    for bucket in [
        config.s3_bucket_artifacts,
        config.s3_bucket_data,
        config.s3_bucket_glue_assets,
    ]:
        print(f"Create {bucket!r}")
        create_bucket(
            s3_client=bsm.s3_client,
            aws_region=config.aws_region,
            bucket=bucket,
        )
        print(f"  done, preview at: {S3Path(bucket).console_url}")

    print("--- Create DynamoDB table")
    print(f"Create DynamoDB table {config.dynamodb_table!r}")
    console_url = dynamodb_table.get_dynamodb_table_console_url(
        aws_region=bsm.aws_region,
        table=config.dynamodb_table,
    )
    print(f"Preview at: {console_url}")
    dynamodb_table.create_dynamodb_table()

    # print("Enable point in time recovery")
    # dynamodb_table.enable_point_in_time_recovery()
    # time.sleep(10)
    #
    # print("Enable dynamodb stream")
    # dynamodb_table.enable_dynamodb_stream()

    print("  done!")

    print("--- Create Glue Database")
    print(f"Create Glue database {config.glue_database!r}")
    console_url = glue_catalog.get_glue_database_console_url(
        aws_region=bsm.aws_region,
        database=config.glue_database,
    )
    print(f"Preview at: {console_url}")
    glue_catalog.create_glue_database(
        glue_client=bsm.glue_client,
        database=config.glue_database,
    )
    print("  done!")


def cleanup():
    # clean up s3
    print("=== Clean up S3 folder and files")
    print(f"clean up {s3paths.s3dir_artifacts.uri}")
    print(f"  preview at: {s3paths.s3dir_artifacts.console_url}")
    s3paths.s3dir_artifacts.delete()

    print(f"clean up {s3paths.s3dir_data.uri}")
    print(f"  preview at: {s3paths.s3dir_data.console_url}")
    s3paths.s3dir_data.delete()

    print("=== Clean up Glue Catalog table")
    print(f"clean up glue catalog table {config.glue_database}.{config.glue_table}")
    console_url = glue_catalog.get_glue_database_console_url(
        aws_region=bsm.aws_region,
        database=config.glue_database,
    )
    print(f"  preview at: {console_url}")
    glue_catalog.delete_glue_table_if_exists(
        glue_client=bsm.glue_client,
        database=config.glue_database,
        table=config.glue_table,
    )

    print("=== Clean up DynamoDB table")
    print(f"clean up DynamoDB table {config.dynamodb_table!r}")
    console_url = dynamodb_table.get_dynamodb_table_console_url(
        aws_region=bsm.aws_region,
        table=config.dynamodb_table,
    )
    print(f"  preview at: {console_url}")
    dynamodb_table.delete_dynamodb_table()

    print("=== Clean up lambda function")
    print(f"clean up Lambda function {config.lambda_function_name_dynamodb_stream_consumer!r}")
    lambda_function.delete_function_if_exists(
        lambda_client=bsm.lambda_client,
        function_name=config.lambda_function_name_dynamodb_stream_consumer,
    )

    print("=== Clean up glue job")
    print(f"clean up Glue job {config.glue_job_name_initial_load!r}")
    # console_url = (
    #     f"https://{bsm.aws_region}.console.aws.amazon.com"
    #     f"/glue/home?region={bsm.aws_region}#/v2/data-catalog/databases"
    # )
    # print(f"clean up glue catalog, preview at: {console_url}")
    # try:
    #     bsm.glue_client.delete_table(
    #         CatalogId=bsm.aws_account_id,
    #         DatabaseName=DATABASE,
    #         Name=TABLE,
    #     )
    # except Exception as e:
    #     # database not found, no need to delete table
    #     if f"database {DATABASE} not found" in str(e).lower():
    #         return
    #     # table not found, no need to delete table
    #     elif f"table {TABLE} not found" in str(e).lower():
    #         return
    #     else:
    #         raise NotImplementedError
    #
    # # clean up dynamodb table
    # delete_dynamodb_table()
