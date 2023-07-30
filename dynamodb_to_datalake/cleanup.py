# -*- coding: utf-8 -*-

from .boto_ses import bsm
from .config import DATABASE, TABLE
from .s3paths import s3dir_artifacts, s3dir_data
from .dynamodb_table import delete_dynamodb_table
from .glue_job import delete_glue_job_if_exists


def cleanup():
    # clean up s3
    print(f"clean up {s3dir_artifacts.uri}, preview at: {s3dir_artifacts.console_url}")
    s3dir_artifacts.delete()
    print(f"clean up {s3dir_data.uri}, preview at: {s3dir_data.console_url}")
    s3dir_data.delete()

    # clean up glue catalog table
    console_url = (
        f"https://{bsm.aws_region}.console.aws.amazon.com"
        f"/glue/home?region={bsm.aws_region}#/v2/data-catalog/databases"
    )
    print(f"clean up glue catalog, preview at: {console_url}")
    try:
        bsm.glue_client.delete_table(
            CatalogId=bsm.aws_account_id,
            DatabaseName=DATABASE,
            Name=TABLE,
        )
    except Exception as e:
        # database not found, no need to delete table
        if f"database {DATABASE} not found" in str(e).lower():
            return
        # table not found, no need to delete table
        elif f"table {TABLE} not found" in str(e).lower():
            return
        else:
            raise NotImplementedError

    # clean up dynamodb table
    delete_dynamodb_table()
