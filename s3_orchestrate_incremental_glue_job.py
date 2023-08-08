# -*- coding: utf-8 -*-

import time
from dynamodb_to_datalake.glue_job import run_incremental_glue_job

while 1:
    run_incremental_glue_job(epoch_processed_partition="year=2023/month=08/day=01/hour=00/minute=00")
    print("waiting 60 seconds ...")
    time.sleep(60)
