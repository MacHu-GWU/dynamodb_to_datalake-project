# -*- coding: utf-8 -*-

from aws_lambda_layer.api import publish_source_artifacts

from .config import APP_NAME, LAMBDA_ROLE_NAME
from .boto_ses import bsm
from .paths import (
    dir_project_root,
    dir_build_lambda,
    path_lbd_func_dynamodb_stream_consumer,
)
from .s3paths import (
    s3dir_lambda_artifacts,
    s3dir_dynamodb_stream,
)

lambda_role_arn = f"arn:aws:iam::{bsm.aws_account_id}:role/{LAMBDA_ROLE_NAME}"


def create_dynamodb_stream_consumer_lambda_function():
    function_name = f"{APP_NAME}_dynamodb_stream_consumer"
    print(f"create Dynamodb stream consumer lambda function: {function_name!r}")
    console_url = (
        f"https://{bsm.aws_region}.console.aws.amazon.com"
        f"/lambda/home?region={bsm.aws_region}#/functions"
        f"/{function_name}?tab=code"
    )
    print(f"preview at: {console_url}")
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
    source_artifacts_deployment = publish_source_artifacts(
        bsm=bsm,
        path_setup_py_or_pyproject_toml=dir_project_root,
        package_name=function_name,
        path_lambda_function=path_lbd_func_dynamodb_stream_consumer,
        version="0.1.1",
        dir_build=dir_build_lambda,
        s3dir_lambda=s3dir_lambda_artifacts,
        use_pathlib=True,
        verbose=True,
    )
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/create_function.html
    bsm.lambda_client.create_function(
        FunctionName=function_name,
        Runtime="python3.10",
        Role=lambda_role_arn,
        Handler=f"{path_lbd_func_dynamodb_stream_consumer.fname}.lambda_handler",
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
