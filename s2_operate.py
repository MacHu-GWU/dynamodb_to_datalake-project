# -*- coding: utf-8 -*-

from dynamodb_to_datalake.api import (
    show_info,
    cdk_deploy_1_iam_role,
    cdk_deploy_2_everything,
    cdk_destroy,
    export_dynamodb_to_s3,
    run_initial_load_glue_job,
    run_incremental_glue_job,
    run_athena_query,
    preview_hudi_table,
    compare,
    cleanup,
)

# show_info()
# cdk_deploy_1_iam_role()
# cdk_deploy_2_everything()
# export_dynamodb_to_s3()
# run_initial_load_glue_job()
# preview_hudi_table()
# preview_hudi_table()
compare() # dynamodb table shape: (58318, 13), hudi table shape: (52596, 13)
# cleanup()
