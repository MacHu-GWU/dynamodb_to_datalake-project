# -*- coding: utf-8 -*-

"""
S3 file paths enumeration.
"""

from s3pathlib import S3Path

from .config_init import config


# s3 folder to store deployment artifacts
s3dir_artifacts = S3Path(
    f"s3://{config.s3_bucket_artifacts}/projects/{config.app_name}/"
).to_dir()
# s3 folder to store lambda deployment artifacts
s3dir_lambda_artifacts = s3dir_artifacts.joinpath("lambda").to_dir()
# s3 folder to store glue deployment artifacts
s3dir_glue_artifacts = s3dir_artifacts.joinpath("glue").to_dir()

# s3 folder to store data
s3dir_data = S3Path(
    f"s3://{config.s3_bucket_data}/projects/{config.app_name}/"
).to_dir()
# glue catalog database s3 location
s3dir_database = s3dir_data.joinpath("databases", config.glue_database).to_dir()
# glue catalog table s3 location
s3dir_table = s3dir_database.joinpath("tables", config.glue_table).to_dir()
# s3 folder to store Athena query results
s3dir_athena_result = s3dir_data.joinpath("athena", "results").to_dir()

# s3 folder to store dynamodb stream CDC data
s3dir_dynamodb_stream = s3dir_data.joinpath("dynamodb_stream").to_dir()
# s3 folder to store dynamodb export to s3 raw data
s3dir_dynamodb_export = s3dir_data.joinpath("dynamodb_export").to_dir()
# s3 folder to store dynamodb export to s3 processed data
s3dir_dynamodb_export_processed = s3dir_data.joinpath(
    "dynamodb_export_processed"
).to_dir()
# s3 path to store latest dynamodb export tracker
s3path_dynamodb_export_tracker = s3dir_data.joinpath("dynamodb_export_tracker.json")

# s3 directory to store incremental glue job input parameter
s3dir_incremental_glue_job_input = s3dir_data.joinpath(
    "glue_jobs",
    "incremental_glue_job_input",
).to_dir()

# s3 path to store incremental glue job progress tracker
s3path_incremental_glue_job_tracker = s3dir_data.joinpath(
    "glue_jobs",
    "incremental_glue_job_tracker.json",
)
