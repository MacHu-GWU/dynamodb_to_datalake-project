# -*- coding: utf-8 -*-

from .config_init import config
from .s3paths import s3dir_artifacts, s3dir_data
from .cdk_deploy import cdk_destroy, get_cloudformation_stack_console_url


def cleanup():
    print("--- Clean up s3")
    print(f"clean up {s3dir_artifacts.uri}, preview at: {s3dir_artifacts.console_url}")
    s3dir_artifacts.delete()
    print(f"clean up {s3dir_data.uri}, preview at: {s3dir_data.console_url}")
    s3dir_data.delete()

    print("--- Clean up CloudFormation")
    url = get_cloudformation_stack_console_url(
        aws_region=config.aws_region,
        stack_name=config.cloudformation_stack_name,
    )
    print(
        f"clean up cloudformation stack {config.cloudformation_stack_name!r}, preview at: {url}"
    )
    cdk_destroy()
