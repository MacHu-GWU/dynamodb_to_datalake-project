# -*- coding: utf-8 -*-

"""
local file paths enumeration.
"""

from pathlib_mate import Path

# git repo root directory
dir_project_root = Path.dir_here(__file__).parent

# virtualenv
dir_venv = dir_project_root / ".venv"
dir_venv_bin = dir_venv / "bin"

# virtualenv executable paths
bin_pytest = dir_venv_bin / "pytest"

# test related
dir_htmlcov = dir_project_root / "htmlcov"
path_cov_index_html = dir_htmlcov / "index.html"
dir_unit_test = dir_project_root / "tests"

# config management
dir_config = dir_project_root.joinpath("config")
# project level config
path_config_json = dir_config.joinpath("config.json")
# cdk stack resources activation config, defines which resources to activate
config = dir_config.joinpath("cdk_stack_resource_activation_config.json")

# lambda function source code
dir_lbd_funcs = dir_project_root.joinpath("lambda_functions")
path_lbd_func_dynamodb_stream_consumer = dir_lbd_funcs.joinpath("dynamodb_stream_consumer.py")
path_lbd_func_dynamodb_export_to_s3_post_processor_coordinator = dir_lbd_funcs.joinpath("dynamodb_export_to_s3_post_processor_coordinator.py")
path_lbd_func_dynamodb_export_to_s3_post_processor_worker = dir_lbd_funcs.joinpath("dynamodb_export_to_s3_post_processor_worker.py")

# glue job source code
dir_glue_jobs = dir_project_root.joinpath("glue_jobs")
path_glue_script_initial_load = dir_glue_jobs.joinpath("initial_load.py")
path_glue_script_incremental = dir_glue_jobs.joinpath("incremental.py")

# lambda function deployment package build directory
dir_build_lambda = dir_project_root.joinpath("build", "lambda")

# temp athena query result csv file
path_query_result = dir_project_root.joinpath("query_result.csv")
