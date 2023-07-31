# -*- coding: utf-8 -*-

"""
Dynamodb export related functions.
"""

import typing as T
import json
import gzip
import dataclasses
from datetime import datetime

from .vendor.aws_dynamodb_export_to_s3 import Export
from .config_init import config
from .boto_ses import bsm
from .s3paths import (
    s3dir_dynamodb_export,
    s3dir_dynamodb_export_processed,
    s3path_dynamodb_export_tracker,
)


def export_dynamodb_to_s3():
    now = datetime.utcnow()
    print(
        f"export dynamodb {config.dynamodb_table} at point-in-time {now} to s3 {s3dir_dynamodb_export.uri}"
    )
    export = Export.export_table_to_point_in_time(
        dynamodb_client=bsm.dynamodb_client,
        table_arn=config.dynamodb_table_arn,
        s3_bucket=s3dir_dynamodb_export.bucket,
        s3_prefix=s3dir_dynamodb_export.key,
    )
    print("it may takes a few minutes to complete")
    print(f"export_arn = {export.arn}")

    # also dump the latest export ARN to s3 tracker file, so glue job can
    # read from it and figure out where to read the initial load
    s3path_dynamodb_export_tracker.write_text(
        json.dumps({"export_arn": export.arn}),
        content_type="application/json",
    )


def preprocess_dynamodb_export_data():
    """
    The Dynamodb export data format is not friendly to AWS Glue job,
    we need to preprocess it before we can use it in initial load Glue job.
    """
    print("preprocess dynamodb export data")
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_exports.html
    res = bsm.dynamodb_client.list_exports(
        TableArn=config.dynamodb_table_arn,
        MaxResults=25,
    )
    export_summaries = res.get("ExportSummaries", [])
    if len(export_summaries) == 0:
        raise ValueError(
            "There's no export yet, you may need to run export_dynamodb_to_s3() first"
        )

    # get the latest export
    export_arn = export_summaries[0]["ExportArn"]
    export_status = export_summaries[0]["ExportStatus"]

    if export_status != "COMPLETED":
        raise ValueError(
            f"dynamodb export {export_arn} is not completed yet, "
            f"status is still {export_status!r}"
        )

    export_id = export_arn.split("/")[-1]
    s3dir_dynamodb_export_data = s3dir_dynamodb_export.joinpath(
        "AWSDynamoDB",
        export_id,
        "data",
    ).to_dir()

    print(f"preprocess dynamodb export data in: {s3dir_dynamodb_export_data.uri}")
    print(f"preview s3dir_exports: {s3dir_dynamodb_export_data.console_url}")
    print(f"processed data will be saved in: {s3dir_dynamodb_export_processed.uri}")
    print(f"preview s3dir_processed: {s3dir_dynamodb_export_processed.console_url}")

    accounts = set()
    for s3path in s3dir_dynamodb_export_data.iter_objects():
        # read data
        items = [
            json.loads(line)
            for line in gzip.decompress(s3path.read_bytes())
            .decode("utf-8")
            .splitlines()
        ]
        # process data
        lines = list()
        for item in items:
            account = item["Item"]["account"]["S"]
            create_at = item["Item"]["create_at"]["S"]
            update_at = item["Item"]["update_at"]["S"]
            entity = item["Item"]["entity"]["S"]
            amount = int(item["Item"]["amount"]["N"])
            is_credit = int(item["Item"]["is_credit"]["N"])
            note = item["Item"]["note"]["S"]

            row = dict(
                account=account,
                create_at=create_at,
                update_at=update_at,
                entity=entity,
                amount=amount,
                is_credit=is_credit,
                note=note,
            )
            line = json.dumps(row)
            lines.append(line)
            accounts.add(account)

        # write data
        s3path_processed = s3dir_dynamodb_export_processed.joinpath(s3path.fname)
        s3path_processed.write_text(
            "\n".join(lines),
            content_type="application/json",
        )

    print(f"number of unique accounts: {len(accounts)}")


@dataclasses.dataclass
class DynamoDBExport:
    pass


def get_last_dynamodb_export() -> T.Optional[DynamoDBExport]:
    res = bsm.dynamodb_client.list_exports(
        TableArn=config.dynamodb_table_arn,
        MaxResults=25,
    )
    export_summaries = res.get("ExportSummaries", [])
    if len(export_summaries) == 0:
        raise ValueError(
            "There's no dynamodb export yet, "
            "you may need to run export dynamodb to s3 first."
        )
    print(export_summaries)
    export_arn = export_summaries[0]["ExportArn"]
    export_status = export_summaries[0]["ExportStatus"]

    if export_status != "COMPLETED":
        raise ValueError(
            f"dynamodb export {export_arn} is not completed yet, "
            f"status is still {export_status!r}"
        )

    # export_id = export_arn.split("/")[-1]
    # s3dir_dynamodb_export_data = s3dir_dynamodb_export.joinpath(
    #     "AWSDynamoDB",
    #     export_id,
    #     "data",
    # )
