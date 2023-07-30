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
# from dynamodb_to_datalake.runbook import (
#     create_infrastructure,
#     cleanup,
# )

show_info()
cdk_deploy()
# export_dynamodb_to_s3()

# create_infrastructure()
# cleanup()
