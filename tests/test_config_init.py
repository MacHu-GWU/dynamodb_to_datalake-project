# -*- coding: utf-8 -*-

from dynamodb_to_datalake.config_init import config


def test():
    _ = config
    _ = config.app_name
    _ = config.aws_profile
    _ = config.bsm
    _ = config.aws_account_id
    _ = config.aws_region
    _ = config.app_name_slug
    _ = config.app_name_snake
    _ = config.dynamodb_table
    _ = config.dynamodb_table_arn
    _ = config.lambda_role_name
    _ = config.glue_role_name
    _ = config.lambda_role_arn
    _ = config.glue_role_arn
    _ = config.cloudformation_stack_name
    _ = config.glue_database
    _ = config.glue_table
    _ = config.lambda_function_name_dynamodb_stream_consumer
    _ = config.lambda_function_name_dynamodb_export_to_s3_post_process_coordinator
    _ = config.lambda_function_name_dynamodb_export_to_s3_post_process_worker
    _ = config.glue_job_name_initial_load
    _ = config.glue_job_name_incremental
    _ = config.s3_bucket_artifacts
    _ = config.s3_bucket_data
    _ = config.s3_bucket_glue_assets


if __name__ == "__main__":
    from dynamodb_to_datalake.tests.helper import run_cov_test

    run_cov_test(__file__, "dynamodb_to_datalake.config_init")
