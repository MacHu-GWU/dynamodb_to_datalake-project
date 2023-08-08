# -*- coding: utf-8 -*-

from dynamodb_to_datalake.data_faker import run_data_faker

run_data_faker(
    sleep_millisecond=10,
    # verbose=False,
)
