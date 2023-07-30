# -*- coding: utf-8 -*-

from .config_define import Config


# last dynamodb stream partition update_at=2023-07-29-05-40
# DYNAMODB_INITIAL_LOAD_EXPORT_ARN = "arn:aws:dynamodb:us-east-1:807388292768:table/transaction/export/01690609366022-e7facaf3"

config = Config(
    app_name="dynamodb_to_datalake",
    aws_profile="awshsh_app_dev_us_east_1",
    dynamodb_table="transaction",
    glue_database="dynamodb_to_datalake",
    glue_table="transaction",
    lambda_role_name="dynamodb_to_datalake_lambda_role",
    glue_role_name="dynamodb_to_datalake_glue_role",
)
