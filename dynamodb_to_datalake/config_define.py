# -*- coding: utf-8 -*-

"""
Project level configuration.
"""

import dataclasses
from boto_session_manager import BotoSesManager

from .compat import cached_property


@dataclasses.dataclass
class Config:
    """
    :param app_name: app name, common prefix for all resources
    :param aws_profile: AWS cli profile for this project
    :param dynamodb_table: dynamodb table name
    :param glue_database: glue catalog database name
    :param glue_table: glue catalog table name
    :param lambda_role_name: AWS Lambda Function IAM role name (name only)
    :param glue_role_name: AWS Glue Job IAM role name (name only)
    """

    app_name: str
    aws_profile: str
    dynamodb_table: str
    glue_database: str
    glue_table: str
    lambda_role_name: str
    glue_role_name: str

    @cached_property
    def bsm(self) -> BotoSesManager:
        return BotoSesManager(profile_name=self.aws_profile)

    @cached_property
    def aws_account_id(self) -> str:
        return self.bsm.aws_account_id

    @cached_property
    def aws_region(self) -> str:
        return self.bsm.aws_region

    @property
    def dynamodb_table_arn(self) -> str:
        return f"arn:aws:dynamodb:{self.aws_region}:{self.aws_account_id}:table/{self.dynamodb_table}"

    @property
    def lambda_role_arn(self) -> str:
        return f"arn:aws:iam::{self.aws_account_id}:role/{self.lambda_role_name}"

    @property
    def glue_role_arn(self) -> str:
        return f"arn:aws:iam::{self.aws_account_id}:role/{self.glue_role_name}"

    @property
    def stack_name(self) -> str:
        return self.app_name.replace("_", "-")

    @property
    def lambda_function_name_dynamodb_stream_consumer(self) -> str:
        return f"{self.app_name}_dynamodb_stream_consumer"

    @property
    def lambda_function_name_dynamodb_export_to_s3_post_process_coordinator(self) -> str:
        return f"{self.app_name}_export_post_process_coordinator"

    @property
    def lambda_function_name_dynamodb_export_to_s3_post_process_worker(self) -> str:
        return f"{self.app_name}_export_post_process_worker"

    @property
    def lambda_function_name_glue_job_coordinator(self) -> str:
        return f"{self.app_name}_glue_job_coordinator"

    @property
    def glue_job_name_initial_load(self) -> str:
        return f"{self.app_name}_initial_load"

    @property
    def glue_job_name_incremental(self) -> str:
        return f"{self.app_name}_incremental"

    @property
    def s3_bucket_artifacts(self) -> str:
        return f"{self.aws_account_id}-{self.aws_region}-artifacts"

    @property
    def s3_bucket_data(self) -> str:
        return f"{self.aws_account_id}-{self.aws_region}-data"

    @property
    def s3_bucket_glue_assets(self) -> str:
        return f"aws-glue-assets-{self.aws_account_id}-{self.aws_region}"
