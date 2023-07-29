# -*- coding: utf-8 -*-

import typing as T
import json
import gzip
from datetime import datetime

import pynamodb_mate as pm
from pathlib_mate import Path
from boto_session_manager import BotoSesManager
from s3pathlib import S3Path, context
from aws_lambda_layer.api import publish_source_artifacts

from rich import print as rprint

# ------------------------------------------------------------------------------
# Demo Project Configuration comes here
# ------------------------------------------------------------------------------
# AWS profile
aws_profile = "awshsh_app_dev_us_east_1"
# app name, common prefix for all resources
app_name = "dynamodb_to_datalake"
# dynamodb table name
dynamodb_table = "transaction"
# glue catalog
database = "dynamodb_to_datalake"
table = "transaction"


# ------------------------------------------------------------------------------
# Prepare variables
# ------------------------------------------------------------------------------
bsm = BotoSesManager(profile_name=aws_profile)
aws_account_id = bsm.aws_account_id
aws_region = bsm.aws_region
context.attach_boto_session(bsm.boto_ses)

# s3 path
artifacts_bucket = f"{bsm.aws_account_id}-{bsm.aws_region}-artifacts"
data_bucket = f"{bsm.aws_account_id}-{bsm.aws_region}-data"

s3dir_artifacts = S3Path(f"s3://{artifacts_bucket}/projects/{app_name}/").to_dir()
s3dir_lambda_artifacts = s3dir_artifacts.joinpath("lambda").to_dir()
s3dir_glue_artifacts = s3dir_artifacts.joinpath("glue").to_dir()

s3dir_data = S3Path(f"s3://{data_bucket}/projects/{app_name}/").to_dir()
s3dir_database = s3dir_data.joinpath("databases", database).to_dir()
s3dir_table = s3dir_database.joinpath("tables", table).to_dir()
s3dir_athena_result = s3dir_data.joinpath("athena", "results").to_dir()

s3dir_dynamodb_stream = s3dir_data.joinpath("dynamodb_stream").to_dir()
s3dir_dynamodb_export = s3dir_data.joinpath("dynamodb_export").to_dir()
s3dir_dynamodb_export_processed = s3dir_data.joinpath(
    "dynamodb_export_processed"
).to_dir()

# local path
dir_here = Path.dir_here(__file__)
dir_lbd_funcs = dir_here.joinpath("lambda_functions")
dir_glue_jobs = dir_here.joinpath("glue_jobs")
dir_build_lambda = dir_here.joinpath("build", "lambda")
dir_build_glue = dir_here.joinpath("build", "glue")

# iam role
lambda_role = f"arn:aws:iam::{bsm.aws_account_id}:role/all-services-admin-role"
glue_role = f"arn:aws:iam::{bsm.aws_account_id}:role/all-services-admin-role"

# dynamodb
dynamodb_table_arn = (
    f"arn:aws:dynamodb:{bsm.aws_region}:{bsm.aws_account_id}:table/{table}"
)


def create_database(delete_table: bool = True):
    try:
        # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_database.html
        bsm.glue_client.get_database(
            CatalogId=bsm.aws_account_id,
            Name=database,
        )
    except Exception as e:
        if "not found" in str(e).lower():
            # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/create_database.html
            bsm.glue_client.create_database(
                CatalogId=bsm.aws_account_id,
                DatabaseInput=dict(
                    Name=database,
                ),
            )
        else:
            raise NotImplementedError

    if delete_table:
        try:
            # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_table.html
            bsm.glue_client.get_table(
                CatalogId=bsm.aws_account_id,
                DatabaseName=database,
                Name=table,
            )
            # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/delete_table.html
            bsm.glue_client.delete_table(
                CatalogId=bsm.aws_account_id,
                DatabaseName=database,
                Name=table,
            )
        except Exception as e:
            if "not found" in str(e).lower():
                pass
            else:
                raise NotImplementedError


class Transaction(pm.Model):
    """
    Dynamodb table data model
    """

    class Meta:
        table_name = dynamodb_table
        region = bsm.aws_region
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    account = pm.UnicodeAttribute(hash_key=True)
    create_at = pm.UTCDateTimeAttribute(range_key=True)
    update_at = pm.UTCDateTimeAttribute()
    entity = pm.UnicodeAttribute()
    amount = pm.NumberAttribute()
    is_credit = pm.NumberAttribute()  # 0 or 1
    note = pm.UnicodeAttribute(null=True)


def create_dynamodb_table():
    with bsm.awscli():
        pm.Connection()
        Transaction.create_table(wait=True)

    # ref: update_continuous_backups
    bsm.dynamodb_client.update_continuous_backups(
        TableName=dynamodb_table,
        PointInTimeRecoverySpecification=dict(
            PointInTimeRecoveryEnabled=True,
        ),
    )

    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_table.html
    bsm.dynamodb_client.update_table(
        TableName=dynamodb_table,
        StreamSpecification=dict(
            StreamEnabled=True,
            StreamViewType="NEW_AND_OLD_IMAGES",
        ),
    )


def create_dynamodb_stream_consumer_lambda_function():
    function_name = f"{app_name}_dynamodb_stream_consumer"
    print(f"create Dynamodb stream consumer lambda function: {function_name!r}")
    # delete first
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/get_function.html
    try:
        bsm.lambda_client.get_function(
            FunctionName=function_name,
        )
        bsm.lambda_client.delete_function(
            FunctionName=function_name,
        )
    except Exception as e:
        if "Function not found" in str(e):
            pass
        else:
            raise NotImplementedError

    # then create
    path_py = dir_lbd_funcs.joinpath("dynamodb_stream_consumer.py")
    source_artifacts_deployment = publish_source_artifacts(
        bsm=bsm,
        path_setup_py_or_pyproject_toml=dir_here,
        package_name=f"{app_name}_dynamodb_stream_consumer",
        path_lambda_function=path_py,
        version="0.1.1",
        dir_build=dir_build_lambda,
        s3dir_lambda=s3dir_lambda_artifacts,
        use_pathlib=True,
        verbose=True,
    )
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/create_function.html
    bsm.lambda_client.create_function(
        FunctionName=f"{app_name}_dynamodb_stream_consumer",
        Runtime="python3.10",
        Role=lambda_role,
        Handler=f"{path_py.fname}.lambda_handler",
        Code=dict(
            S3Bucket=source_artifacts_deployment.s3path_source_zip.bucket,
            S3Key=source_artifacts_deployment.s3path_source_zip.key,
        ),
        Timeout=3,
        MemorySize=256,
        Environment={
            "Variables": {
                "S3_BUCKET": s3dir_dynamodb_stream.bucket,
                "S3_PREFIX": s3dir_dynamodb_stream.key,
            },
        },
    )
    print(
        "don't forget to go to aws console and manually configure dynamodb stream trigger"
    )
    print("batch size = 100")
    print("buffer seconds = 10 seconds")


def export_dynamodb_to_s3():
    now = datetime.utcnow()
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/export_table_to_point_in_time.html
    response = bsm.dynamodb_client.export_table_to_point_in_time(
        TableArn=dynamodb_table_arn,
        ExportTime=now,
        S3Bucket=s3dir_dynamodb_export.bucket,
        S3Prefix=s3dir_dynamodb_export.key,
        ExportFormat="DYNAMODB_JSON",
    )
    export_arn = response["ExportDescription"]["ExportArn"]
    print(
        f"export dynamodb {table} at point-in-time {now} to s3 {s3dir_dynamodb_export.uri}"
    )
    print("it may takes a few minutes to complete")
    print(f"export_arn = {export_arn}")


def preprocess_dynamodb_export_data():
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_exports.html
    res = bsm.dynamodb_client.list_exports(
        TableArn=dynamodb_table_arn,
        MaxResults=25,
    )
    export_summaries = res.get("ExportSummaries", [])
    if len(export_summaries) == 0:
        raise ValueError(
            "There's no export yet, you may need to run export_dynamodb_to_s3() first"
        )

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
    )

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


def _delete_glue_job_if_exists(job_name: str):
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/delete_job.html
    bsm.glue_client.delete_job(
        JobName=job_name,
    )


def _create_glue_job(
    job_name: str,
    job_script: Path,
    additional_params: T.Optional[T.Dict[str, str]] = None,
):
    _delete_glue_job_if_exists(job_name)
    s3path_artifact = s3dir_glue_artifacts.joinpath(job_script.basename)
    s3path_artifact.write_text(
        job_script.read_text(),
        content_type="text/plain",
    )
    console_url = (
        f"https://{aws_region}.console.aws.amazon.com/gluestudio/home?region={aws_region}#"
        f"/editor/job/{job_name}/script"
    )
    print(f"create glue job {job_name!r} from {s3path_artifact.uri}")
    print(f"preview etl script at: {s3path_artifact.console_url}")
    print(f"preview glue job at: {console_url}")
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/create_job.html

    if additional_params is None:
        additional_params = {}
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
        Role=glue_role,
        ExecutionProperty={"MaxConcurrentRuns": 1},
        Command={
            "Name": "glueetl",
            "ScriptLocation": s3path_artifact.uri,
        },
        DefaultArguments=default_arguments,
        MaxRetries=1,
        GlueVersion="4.0",
        WorkerType="G.1X",
        NumberOfWorkers=2,
        Timeout=60,
    )


def create_initial_load_glue_job():
    _create_glue_job(
        job_name=f"{app_name}_initial_load",
        job_script=dir_glue_jobs.joinpath("initial_load.py"),
        additional_params={
            "--S3URI_DYNAMODB_EXPORT_PROCESSED": s3dir_dynamodb_export_processed.uri,
            "--S3URI_TABLE": s3dir_table.uri,
            "--DATABASE_NAME": database,
            "--TABLE_NAME": table,
        },
    )


def run_initial_load_glue_job():
    pass
