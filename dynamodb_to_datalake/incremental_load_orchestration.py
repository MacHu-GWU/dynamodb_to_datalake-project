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
YYYY_MM_DD_HH_MM_FORMAT = "%Y-%m-%d %H:%M"
# max_incremental_interval = 300  # seconds
max_incremental_interval = 3600 * 24 * 365  # seconds
max_incremental_files = 100  # n files


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

    def get_glue_job_input_s3path(self, sequence_id) -> S3Path:
        """
        Find the s3path of the glue job input file where the glue job can
        read the input data from.

        If the glue job run sequence id is 3, then the file name will be
        ``999999997-000000003.json``. This naming convention can return the
        latest glue job input parameter file first when we list the s3 directory.
        """
        filename = (
            f"{str(1000000000 - sequence_id).zfill(9)}"
            f"-{str(sequence_id).zfill(9)}.json"
        )
        return self.s3dir_glue_job_input.joinpath(filename)

    @property
    def last_glue_job_input_s3path(self) -> S3Path:
        return self.get_glue_job_input_s3path(
            sequence_id=self.last_glue_job_run_sequence_id,
        )

    @property
    def next_glue_job_input_s3path(self) -> S3Path:
        return self.get_glue_job_input_s3path(
            sequence_id=self.last_glue_job_run_sequence_id + 1,
        )

    def run_glue_job(self, bsm: BotoSesManager):
        print("prepare the glue job parameters.")
        # let's say if the last processed partition is 2023-01-01-00-00
        # then we only process incremental files >= 2023-01-01-00-01
        start_after_datetime = self.last_processed_datetime + timedelta(minutes=1)
        start_after_partition = start_after_datetime.strftime(PARTITION_DATETIME_FORMAT)
        start_after_key = (
            self.s3dir_dynamodb_stream.joinpath(start_after_partition).to_dir().key
        )

        # let's say if the utc now is 2023-01-01 00:10:30.123456
        # then we only process incremental files < 2023-01-01-00-09
        # because the data at 2023-01-01-00-09 may still on-the-fly
        # in this case, the next processed datatime should be 2023-01-01-00-08
        next_processed_datetime1 = datetime.utcnow() - timedelta(minutes=2)
        next_processed_datetime2 = self.last_processed_datetime + timedelta(
            seconds=max_incremental_interval
        )
        next_processed_datetime = min(
            next_processed_datetime1, next_processed_datetime2
        )
        next_processed_partition = next_processed_datetime.strftime(PARTITION_DATETIME_FORMAT)
        end_before_datetime = next_processed_datetime + timedelta(minutes=1)
        end_before_partition = end_before_datetime.strftime(PARTITION_DATETIME_FORMAT)
        end_before_key = (
            self.s3dir_dynamodb_stream.joinpath(end_before_partition).to_dir().key
        )

        s3uri_list = list()
        for s3path in self.s3dir_dynamodb_stream.iter_objects(
            start_after=start_after_key,
        ):
            if s3path.key < end_before_key:
                s3uri_list.append(s3path.uri)

        s3uri_list = s3uri_list[:max_incremental_files]

        if len(s3uri_list) == 0:
            print(
                f"there is no new data between "
                f"({start_after_datetime.strftime(YYYY_MM_DD_HH_MM_FORMAT)}, "
                f"{end_before_datetime.strftime(YYYY_MM_DD_HH_MM_FORMAT)}) "
                f"to process, do nothing"
            )
            self.last_processed_partition = next_processed_partition
            self.next_processed_partition = None
            self.ready_to_run_next_glue_job = True
            self.write(bsm=bsm)
            return False

        s3path_glue_job_input = self.next_glue_job_input_s3path
        print(f"write glue job input data to s3: {s3path_glue_job_input.uri}")
        s3path_glue_job_input.write_text(
            json.dumps(
                {
                    "start_after_partition": start_after_partition,
                    "end_before_partition": end_before_partition,
                    "s3uri_list": s3uri_list,
                },
                indent=4,
            ),
            content_type="application/json",
            bsm=bsm,
        )
        self.next_processed_partition = next_processed_partition

        # start glue job run
        # Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/start_job_run.html
        print("start glue job run.")
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
            console_url = (
                f"https://{bsm.aws_region}.console.aws.amazon.com/gluestudio"
                f"/home?region={bsm.aws_region}#/editor/job/{self.glue_job_name}/script"
            )
            print(f"preview job run at: {console_url}")
            return True
        except Exception as e:
            if "concurrent runs exceeded" in str(e).lower():
                return False
            else:
                raise NotImplementedError(
                    f"didn't implement the error handling logic for exception: {e!r}"
                )

    def try_to_run_glue_job(self, bsm: BotoSesManager) -> bool:
        """
        Check the status of the last glue job run, if it is finished, then
        update the tracker and run a new glue job.

        :return: a boolean flag to indicate if it runs the glue job,
        """
        print(f"try to run incremental glue job {self.glue_job_name!r}")
        if self.ready_to_run_next_glue_job:
            return self.run_glue_job(bsm=bsm)
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
                return self.run_glue_job(bsm=bsm)
            else:
                print(
                    f"there is a running incremental glue job, "
                    f"status = {state!r}, do nothing."
                )
                return False
