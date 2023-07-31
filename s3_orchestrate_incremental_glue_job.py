# -*- coding: utf-8 -*-

import time
from dynamodb_to_datalake.glue_job import run_incremental_glue_job

while 1:
    run_incremental_glue_job(epoch_processed_partition="2023-07-30-21-31")
    print("waiting 60 seconds ...")
    time.sleep(60)
