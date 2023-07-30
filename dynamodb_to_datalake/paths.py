# -*- coding: utf-8 -*-

"""
local file paths enumeration.
"""

from pathlib_mate import Path

# git repo root directory
dir_project_root = Path.dir_here(__file__).parent

# lambda function source code
dir_lbd_funcs = dir_project_root.joinpath("lambda_functions")
path_lbd_func_dynamodb_stream_consumer = dir_lbd_funcs.joinpath("dynamodb_stream_consumer.py")

# glue job source code
dir_glue_jobs = dir_project_root.joinpath("glue_jobs")
path_glue_script_initial_load = dir_glue_jobs.joinpath("initial_load.py")
path_glue_script_incremental = dir_glue_jobs.joinpath("incremental.py")

# lambda function deployment package build directory
dir_build_lambda = dir_project_root.joinpath("build", "lambda")

# temp athena query result csv file
path_query_result = dir_project_root.joinpath("query_result.csv")
