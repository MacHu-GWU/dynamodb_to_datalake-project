# -*- coding: utf-8 -*-

# from s00_lib import (
#     create_database,
#     create_dynamodb_table,
#     create_dynamodb_stream_consumer_lambda_function,
#     export_dynamodb_to_s3,
#     preprocess_dynamodb_export_data,
#     create_initial_load_glue_job,
# )

# create_database()
# create_dynamodb_table()
# create_dynamodb_stream_consumer_lambda_function()
# export_dynamodb_to_s3()
# preprocess_dynamodb_export_data()
# create_initial_load_glue_job()

# from dynamodb_to_datalake.api import (
#     cleanup,
#     create_dynamodb_table,
#     delete_dynamodb_table,
#     create_dynamodb_stream_consumer_lambda_function,
#     export_dynamodb_to_s3,
#     preprocess_dynamodb_export_data,
#     preview_hudi_table,
#     create_initial_load_glue_job,
#     create_incremental_glue_job,
#     run_initial_load_glue_job,
#     run_incremental_glue_job,
#     compare,
# )

# cleanup()
# create_dynamodb_table()
# create_dynamodb_stream_consumer_lambda_function()
# export_dynamodb_to_s3()
# preprocess_dynamodb_export_data()

# create_initial_load_glue_job()
# run_initial_load_glue_job()
# preview_hudi_table() # 4515 rows

# create_incremental_glue_job()
# run_incremental_glue_job()
# compare()

# delete_dynamodb_table()
# cleanup()


from dynamodb_to_datalake.show_info import show_info
from dynamodb_to_datalake.cdk_deploy import cdk_deploy
from dynamodb_to_datalake.dynamodb_export import export_dynamodb_to_s3
from dynamodb_to_datalake.dynamodb_export import preprocess_dynamodb_export_data
from dynamodb_to_datalake.glue_job import run_initial_load_glue_job, run_incremental_glue_job
from dynamodb_to_datalake.athena import preview_hudi_table
from dynamodb_to_datalake.compare import compare, investigate

# show_info()
# cdk_deploy()
# export_dynamodb_to_s3()
# run_initial_load_glue_job()
# preview_hudi_table()
# run_incremental_glue_job(epoch_processed_partition="2023-07-30-21-31")
# preview_hudi_table()
# compare() # dynamodb table shape: (58318, 13), hudi table shape: (52596, 13)
investigate()

# preprocess_dynamodb_export_data()

# create_infrastructure()
# cleanup()
