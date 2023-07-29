# -*- coding: utf-8 -*-

import typing as T
import json
from datetime import datetime, timedelta, timezone

from pathlib_mate import Path

from .config import (
    APP_NAME,
    GLUE_ROLE_NAME,
    DATABASE,
    TABLE,
    DYNAMODB_INITIAL_LOAD_EXPORT_ARN,
)
from .boto_ses import bsm
from .s3paths import (
    s3dir_glue_artifacts,
    s3dir_dynamodb_export_processed,
    s3dir_dynamodb_stream,
    s3dir_table,
    s3path_incremental_glue_job_input,
    s3path_incremental_glue_job_tracker,
)
from .paths import (
    path_glue_script_initial_load,
    path_glue_script_incremental,
)
from .dynamodb_export import get_last_dynamodb_export

glue_role_arn = f"arn:aws:iam::{bsm.aws_account_id}:role/{GLUE_ROLE_NAME}"
glue_job_name_initial_load = f"{APP_NAME}_initial_load"
glue_job_name_incremental = f"{APP_NAME}_incremental"


def delete_glue_job_if_exists(job_name: str):
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/delete_job.html
    bsm.glue_client.delete_job(
        JobName=job_name,
    )


def create_glue_job(
    job_name: str,
    job_script: Path,
    additional_params: T.Optional[T.Dict[str, str]] = None,
):
    # ensure glue job is deleted first
    delete_glue_job_if_exists(job_name)

    # upload glue job script to s3
    s3path_artifact = s3dir_glue_artifacts.joinpath(job_script.basename)
    s3path_artifact.write_text(
        job_script.read_text(),
        content_type="text/plain",
    )
    console_url = (
        f"https://{bsm.aws_region}.console.aws.amazon.com/gluestudio"
        f"/home?region={bsm.aws_region}#/editor/job/{job_name}/script"
    )
    print(f"create glue job {job_name!r} from {s3path_artifact.uri}")
    print(f"preview etl script at: {s3path_artifact.console_url}")
    print(f"preview glue job at: {console_url}")

    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/create_job.html
    if additional_params is None:
        additional_params = {}
    # necessary job parameters to use hudi
    default_arguments = {
        "--datalake-formats": "hudi",
        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false",
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": f"s3://aws-glue-assets-{bsm.aws_account_id}-{bsm.aws_region}/sparkHistoryLogs/",
        "--enable-job-insights": "false",
        "--enable-glue-datacatalog": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--job-bookmark-option": "job-bookmark-disable",
        "--job-language": "python",
        "--TempDir": f"s3://aws-glue-assets-{bsm.aws_account_id}-{bsm.aws_region}/temporary/",
    }
    default_arguments.update(additional_params)
    bsm.glue_client.create_job(
        Name=job_name,
        LogUri="string",
        Role=glue_role_arn,
        ExecutionProperty={"MaxConcurrentRuns": 1},
        Command={
            "Name": "glueetl",
            "ScriptLocation": s3path_artifact.uri,
        },
        DefaultArguments=default_arguments,
        MaxRetries=0,
        GlueVersion="4.0",
        WorkerType="G.1X",
        NumberOfWorkers=2,
        Timeout=60,
    )


def create_initial_load_glue_job():
    create_glue_job(
        job_name=glue_job_name_initial_load,
        job_script=path_glue_script_initial_load,
        additional_params={
            "--S3URI_DYNAMODB_EXPORT_PROCESSED": s3dir_dynamodb_export_processed.uri,
            "--S3URI_TABLE": s3dir_table.uri,
            "--DATABASE_NAME": DATABASE,
            "--TABLE_NAME": TABLE,
        },
    )


def create_incremental_glue_job():
    create_glue_job(
        job_name=glue_job_name_incremental,
        job_script=path_glue_script_incremental,
        additional_params={
            "--S3URI_INCREMENTAL_GLUE_JOB_INPUT": s3path_incremental_glue_job_input.uri,
            "--S3URI_INCREMENTAL_GLUE_JOB_TRACKER": s3path_incremental_glue_job_tracker.uri,
            "--S3URI_TABLE": s3dir_table.uri,
            "--DATABASE_NAME": DATABASE,
            "--TABLE_NAME": TABLE,
        },
    )


def run_initial_load_glue_job():
    bsm.glue_client.start_job_run(
        JobName=glue_job_name_initial_load,
    )


def run_incremental_glue_job():
    print("run incremental glue job")
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_export.html
    response = bsm.dynamodb_client.describe_export(
        ExportArn=DYNAMODB_INITIAL_LOAD_EXPORT_ARN,
    )
    export_time = response["ExportDescription"]["ExportTime"].astimezone(timezone.utc)
    epoch_start_time: datetime = export_time - timedelta(minutes=2)

    print(f"preview s3path_incremental_glue_job_tracker: {s3path_incremental_glue_job_tracker.console_url}")
    if s3path_incremental_glue_job_tracker.exists() is False:
        s3path_incremental_glue_job_tracker.write_text(
            epoch_start_time.strftime("%Y-%m-%d-%H-%M"),
            content_type="text/plain",
        )

    update_at = s3path_incremental_glue_job_tracker.read_text()
    start_at = update_at
    now = datetime.utcnow()
    end_at = (now - timedelta(minutes=2)).strftime("%Y-%m-%d-%H-%M")
    if end_at <= start_at:
        print("no sufficient incremental data to process, do nothing.")
        return

    # update_at=2023-07-29-05-40
    todo_s3path_list = list()
    print(f"process incremental data {start_at!r} < X <= {end_at!r}")
    for s3dir in s3dir_dynamodb_stream.iterdir():
        if start_at < s3dir.basename.split("=", 1)[1] <= end_at:
            for s3path in s3dir.iter_objects():
                todo_s3path_list.append(s3path)

    input_data = {
        "s3uri_list": [
            s3path.uri
            for s3path in todo_s3path_list
        ]
    }
    s3path_incremental_glue_job_input.write_text(
        json.dumps(input_data),
        content_type="application/json",
    )
    print(f"preview s3path_incremental_glue_job_input: {s3path_incremental_glue_job_input.console_url}")
    bsm.glue_client.start_job_run(
        JobName=glue_job_name_incremental,
    )

    s3path_incremental_glue_job_tracker.write_text(
        end_at,
        content_type="text/plain",
    )
