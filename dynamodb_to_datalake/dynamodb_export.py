# -*- coding: utf-8 -*-

"""
Dynamodb export related functions.
"""

import json
from datetime import datetime

from .vendor.aws_dynamodb_export_to_s3 import Export
from .config_init import config
from .boto_ses import bsm
from .s3paths import (
    s3dir_dynamodb_export,
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
