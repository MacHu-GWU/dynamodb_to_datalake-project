# -*- coding: utf-8 -*-

from dynamodb_to_datalake.api import (
    show_info,
    cdk_deploy,
    cdk_destroy,
    export_dynamodb_to_s3,
    run_initial_load_glue_job,
    run_incremental_glue_job,
    run_athena_query,
    preview_hudi_table,
    compare,
    investigate,
    cleanup,
)

show_info()
cdk_deploy()
# export_dynamodb_to_s3()
# run_initial_load_glue_job()
# preview_hudi_table()
# run_incremental_glue_job(epoch_processed_partition="2023-07-30-21-31")
# preview_hudi_table()
# compare() # dynamodb table shape: (58318, 13), hudi table shape: (52596, 13)
# investigate()
# cleanup()
