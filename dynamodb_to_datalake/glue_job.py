# -*- coding: utf-8 -*-

import typing as T
import json
import dataclasses
from datetime import datetime, timedelta, timezone

from pathlib_mate import Path

from .config_init import config
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


def get_glue_job_console_url(
    aws_region: str,
    job_name: str,
) -> str:
    return (
        f"https://{aws_region}.console.aws.amazon.com/gluestudio"
        f"/home?region={aws_region}#/editor/job/{job_name}/script"
    )


def get_glue_job(
    glue_client,
    job_name: str,
) -> T.Optional[dict]:
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/batch_get_jobs.html
    res = glue_client.batch_get_jobs(
        JobNames=[job_name],
    )
    if job_name in res.get("JobsNotFound", []):
        return None
    else:
        return res["Jobs"][0]


def delete_glue_job(
    glue_client,
    job_name: str,
):
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/delete_job.html
    glue_client.delete_job(
        JobName=job_name,
    )


def delete_glue_job_if_exists(
    glue_client,
    job_name: str,
):
    job_detail = get_glue_job(glue_client, job_name)
    if job_detail is not None:
        delete_glue_job(glue_client, job_name)


def create_hudi_glue_job(
    glue_client,
    job_name: str,
    job_script: Path,
    glue_role_arn: str,
    additional_params: T.Optional[T.Dict[str, str]] = None,
):
    # ensure glue job is deleted first
    delete_glue_job_if_exists(glue_client, job_name)

    # upload glue job script to s3
    s3path_artifact = s3dir_glue_artifacts.joinpath(job_script.basename)
    s3path_artifact.write_text(
        job_script.read_text(),
        content_type="text/plain",
    )
    # console_url = (
    #     f"https://{bsm.aws_region}.console.aws.amazon.com/gluestudio"
    #     f"/home?region={bsm.aws_region}#/editor/job/{job_name}/script"
    # )
    # print(f"create glue job {job_name!r} from {s3path_artifact.uri}")
    # print(f"preview etl script at: {s3path_artifact.console_url}")
    # print(f"preview glue job at: {console_url}")

    # create glue job
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/create_job.html
    if additional_params is None:
        additional_params = {}

    # necessary job parameters to use hudi
    default_arguments = {
        "--datalake-formats": "hudi",
        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false",
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--enable-job-insights": "false",
        "--enable-glue-datacatalog": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--job-bookmark-option": "job-bookmark-disable",
        "--job-language": "python",
        "--spark-event-logs-path": f"s3://{config.s3_bucket_glue_assets}/sparkHistoryLogs/",
        "--TempDir": f"s3://{config.s3_bucket_glue_assets}/temporary/",
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
    create_hudi_glue_job(
        glue_client=bsm.glue_client,
        job_name=config.glue_job_name_initial_load,
        job_script=path_glue_script_initial_load,
        glue_role_arn=config.glue_role_arn,
        additional_params={
            "--S3URI_DYNAMODB_EXPORT_PROCESSED": s3dir_dynamodb_export_processed.uri,
            "--S3URI_TABLE": s3dir_table.uri,
            "--DATABASE_NAME": config.glue_database,
            "--TABLE_NAME": config.glue_table,
        },
    )


def create_incremental_glue_job():
    create_hudi_glue_job(
        glue_client=bsm.glue_client,
        job_name=config.glue_job_name_incremental,
        job_script=path_glue_script_incremental,
        glue_role_arn=config.glue_role_arn,
        additional_params={
            "--S3URI_INCREMENTAL_GLUE_JOB_INPUT": s3path_incremental_glue_job_input.uri,
            "--S3URI_INCREMENTAL_GLUE_JOB_TRACKER": s3path_incremental_glue_job_tracker.uri,
            "--S3URI_TABLE": s3dir_table.uri,
            "--DATABASE_NAME": config.glue_database,
            "--TABLE_NAME": config.glue_table,
        },
    )


def run_initial_load_glue_job():
    bsm.glue_client.start_job_run(
        JobName=config.glue_job_name_initial_load,
    )



# @dataclasses.dataclass
# class Tracker:
#     @classmethod
#     def read(cls):
#         s3path_incremental_glue_job_tracker.exists() is False:
#         s3path_incremental_glue_job_tracker.write_text(
#             epoch_start_time.strftime("%Y-%m-%d-%H-%M"),
#             content_type="text/plain",
#         )


# def run_incremental_glue_job():
#     # ensure there is no running incremental glue job
#     print("run incremental glue job")
#     paginator = bsm.glue_client.get_paginator("get_job_runs")
#     response_iterator = paginator.paginate(
#         JobName=glue_job_name_incremental,
#         PaginationConfig={
#             "MaxItems": 10,
#             "PageSize": 50,
#         }
#     )
#     job_runs = list()
#     for response in response_iterator:
#         job_runs.extend(response.get("JobRuns", []))
#
#     if len(job_runs) > 0:
#         state = job_runs[0]["JobRunState"]
#         if state in ["STOPPED", "STOPPED", "FAILED", "TIMEOUT", "ERROR"]:
#             pass
#         elif state in ["STARTING", "RUNNING", "STOPPING", "WAITING"]:
#             print("there is a running incremental glue job, do nothing")
#             return
#
#     # figure out the incremental data start time if it is the first time to run
#     # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_export.html
#     response = bsm.dynamodb_client.describe_export(
#         ExportArn=DYNAMODB_INITIAL_LOAD_EXPORT_ARN,
#     )
#     export_time = response["ExportDescription"]["ExportTime"].astimezone(timezone.utc)
#     epoch_start_time: datetime = export_time - timedelta(minutes=2)
#
#     print(f"preview s3path_incremental_glue_job_tracker: {s3path_incremental_glue_job_tracker.console_url}")
#     if s3path_incremental_glue_job_tracker.exists() is False:
#         s3path_incremental_glue_job_tracker.write_text(
#             epoch_start_time.strftime("%Y-%m-%d-%H-%M"),
#             content_type="text/plain",
#         )
#
#     update_at = s3path_incremental_glue_job_tracker.read_text()
#     start_at = update_at
#     now = datetime.utcnow()
#     end_at = (now - timedelta(minutes=2)).strftime("%Y-%m-%d-%H-%M")
#     if end_at <= start_at:
#         print("no sufficient incremental data to process, do nothing.")
#         return
#
#     # update_at=2023-07-29-05-40
#     todo_s3path_list = list()
#     print(f"process incremental data {start_at!r} < X <= {end_at!r}")
#     for s3dir in s3dir_dynamodb_stream.iterdir():
#         if start_at < s3dir.basename.split("=", 1)[1] <= end_at:
#             for s3path in s3dir.iter_objects():
#                 todo_s3path_list.append(s3path)
#
#     if len(todo_s3path_list) == 0:
#         print("no incremental data to process, do nothing.")
#         return
#
#     input_data = {
#         "s3uri_list": [
#             s3path.uri
#             for s3path in todo_s3path_list
#         ]
#     }
#     s3path_incremental_glue_job_input.write_text(
#         json.dumps(input_data),
#         content_type="application/json",
#     )
#     print(f"preview s3path_incremental_glue_job_input: {s3path_incremental_glue_job_input.console_url}")
#     bsm.glue_client.start_job_run(
#         JobName=glue_job_name_incremental,
#     )
#
#     s3path_incremental_glue_job_tracker.write_text(
#         end_at,
#         content_type="text/plain",
#     )
