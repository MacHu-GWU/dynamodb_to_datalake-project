# -*- coding: utf-8 -*-

"""
Incremental load orchestration logics.

[CN]

这个模块实现了每隔一段时间将最新的 Incremental Data Load 到 Hudi Table 中的 Cron Job 逻辑.
"""

import typing as T
import json
import enum
import dataclasses
from datetime import datetime, timedelta
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager


class JobRunStateEnum(enum.Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    WAITING = "WAITING"


PARTITION_DATETIME_FORMAT = "year=%Y/month=%m/day=%d/hour=%H/minute=%M"


@dataclasses.dataclass
class CDCTracker:
    """
    Cron Job 的编排逻辑实现.

    :param s3path_tracker: where you store the cdc tracker data.
    :param s3dir_glue_job_input: where you store the glue job input parameters
        the folder structure looks like ``${reverse_sequence_id}-${sequence_id}``,
        the sequence id starts from 1, 2, ...::

        ...
        ${s3dir_glue_job_input}/999999997-000000003.json
        ${s3dir_glue_job_input}/999999998-000000002.json
        ${s3dir_glue_job_input}/999999999-000000001.json

    :param s3dir_dynamodb_stream: where you store the processed dynamodb stream data
        the data is partitioned on minutes level with the::

        ${s3dir_dynamodb_stream}/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/
        ${s3dir_dynamodb_stream}/year=2023/month=01/day=01/hour=00/minute=01/
        ${s3dir_dynamodb_stream}/year=2023/month=01/day=01/hour=00/minute=02/
        ${s3dir_dynamodb_stream}/year=2023/month=01/day=01/hour=00/minute=03/
        ${s3dir_dynamodb_stream}/.../
    :param glue_job_name: the incremental glue job name.
    :param epoch_processed_partition: where the incremental data from.

    :param last_glue_job_run_id: the last glue job run id
    :param last_glue_job_run_sequence_id: the last glue job run sequence id
    :param last_processed_partition: the last processed partition
    :param next_processed_partition: the next processed partition if the last
        glue job run succeed.
    :param ready_to_run_next_glue_job: whether the next glue job is ready to run.
        basically if the last glue job is not succeeded, failed, stopped, then
        it is NOT ready.
    """

    # static attributes
    s3path_tracker: S3Path = dataclasses.field()
    s3dir_glue_job_input: S3Path = dataclasses.field()
    s3dir_dynamodb_stream: S3Path = dataclasses.field()
    glue_job_name: str = dataclasses.field()
    epoch_processed_partition: str = dataclasses.field()

    # dynamic attributes
    last_glue_job_run_id: T.Optional[str] = dataclasses.field(default=None)
    last_glue_job_run_sequence_id: T.Optional[int] = dataclasses.field(default=None)
    last_processed_partition: T.Optional[str] = dataclasses.field(default=None)
    next_processed_partition: T.Optional[str] = dataclasses.field(default=None)
    ready_to_run_next_glue_job: T.Optional[bool] = dataclasses.field(default=None)

    @classmethod
    def read(
        cls,
        bsm: BotoSesManager,
        s3path_tracker: S3Path,
        s3dir_glue_job_input: S3Path,
        s3dir_dynamodb_stream: S3Path,
        glue_job_name: str,
        epoch_processed_partition: str,
    ):
        """
        Read the tracker data from s3. If not exists, create a new one with
        initial value.
        """
        # set initial value if tracker not exists
        if s3path_tracker.exists(bsm=bsm) is False:
            tracker = cls(
                s3path_tracker=s3path_tracker,
                s3dir_glue_job_input=s3dir_glue_job_input,
                s3dir_dynamodb_stream=s3dir_dynamodb_stream,
                glue_job_name=glue_job_name,
                epoch_processed_partition=epoch_processed_partition,
                last_glue_job_run_id=None,
                last_glue_job_run_sequence_id=0,
                last_processed_partition=epoch_processed_partition,
                ready_to_run_next_glue_job=True,
            )
            tracker.write(bsm=bsm)
            return tracker
        # read from s3 if tracker exists
        else:
            data = json.loads(s3path_tracker.read_text(bsm=bsm))
            return cls(
                s3path_tracker=s3path_tracker,
                s3dir_glue_job_input=s3dir_glue_job_input,
                s3dir_dynamodb_stream=s3dir_dynamodb_stream,
                glue_job_name=glue_job_name,
                epoch_processed_partition=epoch_processed_partition,
                last_glue_job_run_id=data["last_glue_job_run_id"],
                last_glue_job_run_sequence_id=data["last_glue_job_run_sequence_id"],
                last_processed_partition=data["last_processed_partition"],
                next_processed_partition=data["next_processed_partition"],
                ready_to_run_next_glue_job=data["ready_to_run_next_glue_job"],
            )

    def write(
        self,
        bsm: BotoSesManager,
    ):
        """
        Write the tracker data to s3.
        """
        self.s3path_tracker.write_text(
            json.dumps(
                {
                    "last_glue_job_run_id": self.last_glue_job_run_id,
                    "last_glue_job_run_sequence_id": self.last_glue_job_run_sequence_id,
                    "last_processed_partition": self.last_processed_partition,
                    "next_processed_partition": self.next_processed_partition,
                    "ready_to_run_next_glue_job": self.ready_to_run_next_glue_job,
                },
                indent=4,
            ),
            content_type="application/json",
            bsm=bsm,
        )

    @property
    def last_processed_datetime(self) -> datetime:
        return datetime.strptime(
            self.last_processed_partition,
            PARTITION_DATETIME_FORMAT,
        )

    @property
    def s3path_glue_job_input(self) -> S3Path:
        """
        Find the s3path of the glue job input file if we run a new glue job.

        If last glue job run sequence id is 3, then the file name will be
        ``999999997-000000003.json``. This naming convention can return latest
        glue job input parameter file first when we list the s3 directory.
        """
        filename = (
            f"{str(1000000000 - self.last_glue_job_run_sequence_id).zfill(9)}"
            f"-{str(self.last_glue_job_run_sequence_id).zfill(9)}.json"
        )
        return self.s3dir_glue_job_input.joinpath(filename)

    def _run_glue_job(self, bsm: BotoSesManager):
        # prepare the glue job parameters
        print("prepare the glue job parameters.")
        # let's say if the last processed partition is 2023-01-01-00-00
        # then we only process incremental files >= 2023-01-01-00-01
        start_after_partition = (
            self.last_processed_datetime + timedelta(minutes=1)
        ).strftime(PARTITION_DATETIME_FORMAT)
        start_after_key = (
            self.s3dir_dynamodb_stream.joinpath(start_after_partition).to_dir().key
        )

        # let's say if the utc now is 2023-01-01 00:10:30.123456
        # then we only process incremental files < 2023-01-01-00-09
        # because the data at 2023-01-01-00-09 may still on-the-fly
        # in this case, the next processed datatime should be 2023-01-01-00-08
        next_processed_datetime1 = datetime.utcnow() - timedelta(minutes=2)
        next_processed_datetime2 = self.last_processed_datetime + timedelta(minutes=60)
        next_processed_datetime = min(
            next_processed_datetime1, next_processed_datetime2
        )
        end_before_partition = (
            next_processed_datetime + timedelta(minutes=1)
        ).strftime(PARTITION_DATETIME_FORMAT)
        end_before_key = (
            self.s3dir_dynamodb_stream.joinpath(end_before_partition).to_dir().key
        )

        s3uri_list = list()
        for s3path in self.s3dir_dynamodb_stream.iter_objects(
            start_after=start_after_key,
        ):
            if s3path.key < end_before_key:
                s3uri_list.append(s3path.uri)

        if len(s3uri_list) == 0:
            print(
                f"there is no new data between ({start_after_partition}, {end_before_partition}) to process, do nothing"
            )
            return False

        self.s3path_glue_job_input.write_text(
            json.dumps(
                {
                    "start_after_partition": start_after_partition,
                    "end_before_partition": end_before_partition,
                    "s3uri_list": s3uri_list,
                },
                indent=4,
            ),
            content_type="application/json",
        )
        self.next_processed_partition = next_processed_datetime.strftime(
            PARTITION_DATETIME_FORMAT
        )

        # start job run
        # Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/start_job_run.html
        print("start job run.")
        try:
            res = bsm.glue_client.start_job_run(
                JobName=self.glue_job_name,
                Arguments={
                    "--S3URI_INCREMENTAL_GLUE_JOB_INPUT": self.s3path_glue_job_input.uri,
                },
            )
            job_run_id = res["JobRunId"]
            print(f"job run id = {job_run_id}")
            self.last_glue_job_run_id = job_run_id
            self.last_glue_job_run_sequence_id += 1
            self.ready_to_run_next_glue_job = False
            self.write(bsm=bsm)
            return True
        except Exception as e:
            if "concurrent runs exceeded" in str(e).lower():
                return False
            else:
                raise NotImplementedError

    def run_glue_job(self, bsm: BotoSesManager) -> bool:
        """
        Try to run glue job.

        :return: a boolean flag to indicate if it runs the glue job,
        """
        print("try to run incremental glue job.")
        if self.ready_to_run_next_glue_job:
            return self._run_glue_job(bsm=bsm)
        else:
            if self.last_glue_job_run_id is None:
                raise ValueError

            # get job run status
            # Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_job_run.html
            print("there is a running incremental glue job, check the status.")
            res = bsm.glue_client.get_job_run(
                JobName=self.glue_job_name,
                RunId=self.last_glue_job_run_id,
            )
            state = res["JobRun"]["JobRunState"]

            # if finished (succeeded or failed), update the tracker and run another job
            if state in [
                JobRunStateEnum.STOPPED.value,
                JobRunStateEnum.SUCCEEDED.value,
                JobRunStateEnum.FAILED.value,
                JobRunStateEnum.TIMEOUT.value,
                JobRunStateEnum.ERROR.value,
            ]:
                self.last_processed_partition = self.next_processed_partition
                self.next_processed_partition = None
                self.ready_to_run_next_glue_job = True
                print(
                    f"previous glue job finished, "
                    f"status = {state!r}, run another one."
                )
                return self._run_glue_job(bsm=bsm)
            else:
                print(
                    f"there is a running incremental glue job, "
                    f"status = {state!r}, do nothing."
                )
                return False
