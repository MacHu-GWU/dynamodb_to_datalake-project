# -*- coding: utf-8 -*-

import typing as T
from pathlib_mate import Path

from .config_init import config
from .boto_ses import bsm
from .s3paths import (
    s3dir_glue_artifacts,
    s3dir_dynamodb_export_processed,
    s3dir_dynamodb_stream,
    s3dir_table,
    s3dir_incremental_glue_job_input,
    s3path_incremental_glue_job_tracker,
)
from .paths import (
    path_glue_script_initial_load,
    path_glue_script_incremental,
)
from .incremental_load_orchestration import CDCTracker


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
    print(f"create glue job {job_name!r} from {s3path_artifact.uri}")
    print(f"preview etl script at: {s3path_artifact.console_url}")
    console_url = get_glue_job_console_url(
        aws_region=config.aws_region, job_name=job_name
    )
    print(f"preview glue job at: {console_url}")

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
        "--CODE_ETAG": s3path_artifact.etag,
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
            "--S3URI_INCREMENTAL_GLUE_JOB_TRACKER": s3path_incremental_glue_job_tracker.uri,
            "--S3URI_TABLE": s3dir_table.uri,
            "--DATABASE_NAME": config.glue_database,
            "--TABLE_NAME": config.glue_table,
        },
    )


def run_initial_load_glue_job():
    print("run initial load glue job")
    bsm.glue_client.start_job_run(
        JobName=config.glue_job_name_initial_load,
    )


def run_incremental_glue_job(epoch_processed_partition: str):
    tracker = CDCTracker.read(
        bsm=bsm,
        s3path_tracker=s3path_incremental_glue_job_tracker,
        s3dir_glue_job_input=s3dir_incremental_glue_job_input,
        s3dir_dynamodb_stream=s3dir_dynamodb_stream,
        glue_job_name=config.glue_job_name_incremental,
        epoch_processed_partition=epoch_processed_partition,
    )
    tracker.run_glue_job(bsm=bsm)
