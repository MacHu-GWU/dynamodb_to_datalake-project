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

from dynamodb_to_datalake.api import (
    cleanup,
    create_dynamodb_table,
    delete_dynamodb_table,
    create_dynamodb_stream_consumer_lambda_function,
    export_dynamodb_to_s3,
    preprocess_dynamodb_export_data,
    preview_hudi_table,
    create_initial_load_glue_job,
    create_incremental_glue_job,
    run_initial_load_glue_job,
    run_incremental_glue_job,
    compare,
)

# cleanup()
# create_dynamodb_table()
# create_dynamodb_stream_consumer_lambda_function()
# export_dynamodb_to_s3()
# preprocess_dynamodb_export_data()

# create_initial_load_glue_job()
# run_initial_load_glue_job()
# preview_hudi_table() # 4515 rows

# create_incremental_glue_job()
run_incremental_glue_job()
# compare()

# delete_dynamodb_table()

