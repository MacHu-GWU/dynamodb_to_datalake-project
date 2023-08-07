# -*- coding: utf-8 -*-

from datetime import datetime, timezone
from rds_to_datalake.incremental_load_orchestration import (
    datetime_to_s3_key,
    filename_to_datetime,
    PerTableTodo,
    GlueJobInput,
)


class TestGlueJobInput:
    def test(self):
        glue_job_input = GlueJobInput(
            todo_list=[
                PerTableTodo(
                    table="accounts",
                    start_after=datetime(2021, 1, 1, tzinfo=timezone.utc).isoformat(),
                    end_until=datetime(2021, 1, 2, tzinfo=timezone.utc).isoformat(),
                    s3uri_list=[
                        "s3://bucket/file1",
                        "s3://bucket/file2",
                    ],
                )
            ]
        )
        glue_job_input1 = GlueJobInput.from_dict(glue_job_input.to_dict())
        assert glue_job_input == glue_job_input1


def test_datetime_to_s3_key():
    dt = datetime(2023, 1, 7, 8, 30, 15, 123000, tzinfo=timezone.utc)
    assert datetime_to_s3_key(dt) == "2023/01/07/08/20230107-083015123"


def test_filename_to_datetime():
    filename = "20230107-083015123"
    dt = filename_to_datetime(filename)
    assert dt == datetime(2023, 1, 7, 8, 30, 15, 123000, tzinfo=timezone.utc)


def test_iso_format_seder():
    dt = datetime(2023, 1, 7, 8, 30, 15, 123000, tzinfo=timezone.utc)
    time_str = dt.isoformat()
    dt1 = datetime.fromisoformat(time_str)
    assert dt1.tzinfo == timezone.utc


if __name__ == "__main__":
    from rds_to_datalake.tests.helper import run_cov_test

    run_cov_test(__file__, "rds_to_datalake.incremental_load_orchestration")
