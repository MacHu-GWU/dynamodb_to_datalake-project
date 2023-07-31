# -*- coding: utf-8 -*-

import subprocess

from .config_init import config
from .paths import dir_project_root


def get_cloudformation_stack_console_url(
    aws_region: str,
    stack_name: str,
):
    return (
        f"https://{aws_region}.console.aws.amazon.com"
        f"/cloudformation/home?region={aws_region}#"
        f"/stacks?filteringText={stack_name}&filteringStatus=active&viewNested=true"
    )


def cdk_deploy():
    print(
        f"🚀 You are deploying stack to AWS Account {config.aws_account_id}, "
        f"Region = {config.aws_region}."
    )
    console_url = get_cloudformation_stack_console_url(
        aws_region=config.aws_region,
        stack_name=config.cloudformation_stack_name,
    )
    print(f"preview cloudformation stack at: {console_url}")
    with dir_project_root.temp_cwd():
        args = [
            "cdk",
            "deploy",
            "--require-approval",
            "never",
            "--profile",
            config.aws_profile,
        ]
        subprocess.run(args, check=True)


def cdk_destroy():
    print(
        f"🔥 You are destroying stack from AWS Account {config.aws_account_id}, "
        f"Region = {config.aws_region}."
    )
    console_url = get_cloudformation_stack_console_url(
        aws_region=config.aws_region,
        stack_name=config.cloudformation_stack_name,
    )
    print(f"preview cloudformation stack at: {console_url}")
    with dir_project_root.temp_cwd():
        args = [
            "cdk",
            "destroy",
            "--profile",
            config.aws_profile,
        ]
        subprocess.run(args, check=True)
