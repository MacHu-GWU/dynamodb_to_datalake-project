# -*- coding: utf-8 -*-

"""
Dynamodb export related functions.
"""

import typing as T
import json
import gzip
import dataclasses
from datetime import datetime

from .config import DYNAMODB_TABLE
from .boto_ses import bsm
from .dynamodb_table import DYNAMODB_TABLE_ARN
from .s3paths import s3dir_dynamodb_export, s3dir_dynamodb_export_processed


def export_dynamodb_to_s3():
    now = datetime.utcnow()
    print(
        f"export dynamodb {DYNAMODB_TABLE} at point-in-time {now} to s3 {s3dir_dynamodb_export.uri}"
    )
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/export_table_to_point_in_time.html
    response = bsm.dynamodb_client.export_table_to_point_in_time(
        TableArn=DYNAMODB_TABLE_ARN,
        ExportTime=now,
        S3Bucket=s3dir_dynamodb_export.bucket,
        S3Prefix=s3dir_dynamodb_export.key,
        ExportFormat="DYNAMODB_JSON",
    )
    export_arn = response["ExportDescription"]["ExportArn"]
    print("it may takes a few minutes to complete")
    print(f"export_arn = {export_arn}")


def preprocess_dynamodb_export_data():
    """
    The Dynamodb export data format is not friendly to AWS Glue job,
    we need to preprocess it before we can use it in initial load Glue job.
    """
    print("preprocess dynamodb export data")
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_exports.html
    res = bsm.dynamodb_client.list_exports(
        TableArn=DYNAMODB_TABLE_ARN,
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

            create_at_datetime = datetime.strptime(create_at, "%Y-%m-%dT%H:%M:%S.%f%z")
            create_year = str(create_at_datetime.year).zfill(4)
            create_month = str(create_at_datetime.month).zfill(2)
            create_day = str(create_at_datetime.day).zfill(2)
            create_hour = str(create_at_datetime.hour).zfill(2)
            create_minute = str(create_at_datetime.minute).zfill(2)

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
        TableArn=DYNAMODB_TABLE_ARN,
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
