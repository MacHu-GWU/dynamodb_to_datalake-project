# -*- coding: utf-8 -*-

"""
Lambda function related functions.
"""

import typing as T
from aws_lambda_layer.api import publish_source_artifacts

from .config_init import config
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


def get_lambda_function_console_url(
    aws_region: str,
    function_name: str,
) -> str:
    return (
        f"https://{aws_region}.console.aws.amazon.com"
        f"/lambda/home?region={aws_region}#/functions"
        f"/{function_name}?tab=code"
    )


def get_lambda_function(
    lambda_client,
    function_name: str,
) -> T.Optional[dict]:
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/get_function.html
    try:
        response = lambda_client.get_function(
            FunctionName=function_name,
        )
        return response["Configuration"]
    except Exception as e:
        if "not found" in str(e).lower():
            return None
        else:
            raise NotImplementedError


def delete_function(
    lambda_client,
    function_name: str,
):
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/delete_function.html
    lambda_client.delete_function(
        FunctionName=function_name,
    )


def delete_function_if_exists(
    lambda_client,
    function_name: str,
):
    if get_lambda_function(lambda_client, function_name) is not None:
        delete_function(lambda_client, function_name)


def create_dynamodb_stream_consumer_lambda_function():
    function_name = config.lambda_function_name_dynamodb_stream_consumer
    print(f"create Dynamodb stream consumer lambda function: {function_name!r}")
    console_url = get_lambda_function_console_url(
        aws_region=config.aws_region,
        function_name=function_name,
    )
    print(f"preview at: {console_url}")
    delete_function_if_exists(
        lambda_client=bsm.lambda_client,
        function_name=function_name,
    )

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
        Role=config.lambda_role_arn,
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
                "CODE_ETAG": source_artifacts_deployment.s3path_source_zip.etag,
            },
        },
    )
    print(
        "don't forget to go to aws console and manually configure dynamodb stream trigger"
    )
    print("batch size = 100")
    print("buffer seconds = 10 seconds")
