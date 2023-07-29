# -*- coding: utf-8 -*-

# AWS cli profile for this project
AWS_PROFILE = "awshsh_app_dev_us_east_1"

# app name, common prefix for all resources
APP_NAME = "dynamodb_to_datalake"

# dynamodb table name
DYNAMODB_TABLE = "transaction"

# glue catalog database name
DATABASE = "dynamodb_to_datalake"

# glue catalog table name
TABLE = "transaction"

# AWS Lambda Function IAM role name (name only)
LAMBDA_ROLE_NAME = "all-services-admin-role"

# AWS Glue Job IAM role name (name only)
GLUE_ROLE_NAME = "all-services-admin-role"

#
# last dynamodb stream partition update_at=2023-07-29-05-40
DYNAMODB_INITIAL_LOAD_EXPORT_ARN = "arn:aws:dynamodb:us-east-1:807388292768:table/transaction/export/01690609366022-e7facaf3"
