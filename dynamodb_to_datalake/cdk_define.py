# -*- coding: utf-8 -*-

# ------------------------------------------------------------------------------
# Import dependencies
# ------------------------------------------------------------------------------
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3_notifications
import aws_cdk.aws_iam as iam
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_lambda_event_sources as lambda_event_sources
import aws_cdk.aws_glue as glue

from constructs import Construct

from aws_lambda_layer.api import publish_source_artifacts

from .config_define import Config
from .config_init import config
from .boto_ses import bsm
from . import paths
from . import s3paths
from .s3_bucket import is_bucket_exists
from .dynamodb_table import Transaction


class Stack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        config: Config,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.config = config
        self.declare_s3_bucket()
        self.declare_iam_role()
        self.declare_dynamodb_table()
        self.declare_glue_catalog()
        self.declare_glue_job()
        self.declare_lambda_function()

    def declare_s3_bucket(self):
        if is_bucket_exists(bsm.s3_client, config.s3_bucket_artifacts) is False:
            self.s3_bucket_artifacts = s3.Bucket(
                self,
                f"S3BucketArtifacts",
                bucket_name=config.s3_bucket_artifacts,
            )
        else:
            self.s3_bucket_artifacts = s3.Bucket.from_bucket_name(
                self,
                f"S3BucketArtifacts",
                bucket_name=config.s3_bucket_artifacts,
            )

        if is_bucket_exists(bsm.s3_client, config.s3_bucket_data) is False:
            self.s3_bucket_data = s3.Bucket(
                self,
                f"S3BucketData",
                bucket_name=config.s3_bucket_data,
            )
        else:
            self.s3_bucket_data = s3.Bucket.from_bucket_name(
                self,
                f"S3BucketData",
                bucket_name=config.s3_bucket_data,
            )

        for bucket, description in [
            (config.s3_bucket_glue_assets, "GlueAssets"),
        ]:
            if is_bucket_exists(bsm.s3_client, bucket) is False:
                bucket = s3.Bucket(
                    self,
                    f"S3Bucket{description}",
                    bucket_name=bucket,
                )

    def declare_iam_role(self):
        self.lambda_role = iam.Role(
            self,
            f"LambdaRole",
            role_name=self.config.lambda_role_name,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

        self.glue_role = iam.Role(
            self,
            f"GlueRole",
            role_name=self.config.glue_role_name,
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

    def declare_dynamodb_table(self):
        self.dynamodb_table_transaction = dynamodb.Table(
            self,
            "DynamodbTableTransaction",
            table_name=self.config.dynamodb_table,
            partition_key=dynamodb.Attribute(
                name=Transaction.account.attr_name, type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name=Transaction.create_at.attr_name,
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

    def declare_glue_catalog(self):
        self.glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.config.glue_database,
            ),
        )

    def declare_glue_job(self):
        default_arguments = {
            "--datalake-formats": "hudi",
            "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false",
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--enable-job-insights": "false",
            "--enable-glue-datacatalog": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python",
            "--spark-event-logs-path": f"s3://{self.config.s3_bucket_glue_assets}/sparkHistoryLogs/",
            "--TempDir": f"s3://{self.config.s3_bucket_glue_assets}/temporary/",
        }

        s3path_artifact = s3paths.s3dir_glue_artifacts.joinpath(
            paths.path_glue_script_initial_load.basename
        )
        s3path_artifact.write_text(
            paths.path_glue_script_initial_load.read_text(),
            content_type="text/plain",
        )
        self.glue_job_initial_load = glue.CfnJob(
            self,
            "GlueJobInitialLoad",
            name=self.config.glue_job_name_initial_load,
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=s3path_artifact.uri,
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            max_retries=0,
            timeout=60,
            default_arguments={
                **default_arguments,
                "--S3URI_DYNAMODB_EXPORT_PROCESSED": s3paths.s3dir_dynamodb_export_processed.uri,
                "--S3URI_DYNAMODB_EXPORT_TRACKER": s3paths.s3path_dynamodb_export_tracker.uri,
                "--S3URI_TABLE": s3paths.s3dir_table.uri,
                "--DATABASE_NAME": self.config.glue_database,
                "--TABLE_NAME": self.config.glue_table,
                "--CODE_ETAG": s3path_artifact.etag,
            },
        )

        s3path_artifact = s3paths.s3dir_glue_artifacts.joinpath(
            paths.path_glue_script_incremental.basename
        )
        s3path_artifact.write_text(
            paths.path_glue_script_incremental.read_text(),
            content_type="text/plain",
        )
        self.glue_job_incremental = glue.CfnJob(
            self,
            "GlueJobIncremental",
            name=self.config.glue_job_name_incremental,
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=s3path_artifact.uri,
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            max_retries=0,
            timeout=60,
            default_arguments={
                **default_arguments,
                "--S3URI_INCREMENTAL_GLUE_JOB_TRACKER": s3paths.s3path_incremental_glue_job_tracker.uri,
                "--S3URI_TABLE": s3paths.s3dir_table.uri,
                "--DATABASE_NAME": self.config.glue_database,
                "--TABLE_NAME": self.config.glue_table,
                "--CODE_ETAG": s3path_artifact.etag,
            },
        )

    def declare_lambda_function(self):
        # --- dynamodb_stream_consumer
        source_artifacts_deployment = publish_source_artifacts(
            bsm=bsm,
            path_setup_py_or_pyproject_toml=paths.dir_project_root,
            package_name=self.config.app_name,
            path_lambda_function=paths.path_lbd_func_dynamodb_stream_consumer,
            version="0.1.1",
            dir_build=paths.dir_build_lambda,
            s3dir_lambda=s3paths.s3dir_lambda_artifacts.joinpath(
                "dynamodb_stream_consumer"
            ).to_dir(),
            use_pathlib=True,
            verbose=True,
        )

        self.lambda_function_dynamodb_stream_consumer = lambda_.Function(
            self,
            "LambdaFunctionDynamoDBStreamConsumer",
            function_name=self.config.lambda_function_name_dynamodb_stream_consumer,
            runtime=lambda_.Runtime.PYTHON_3_10,
            role=self.lambda_role,
            timeout=cdk.Duration.seconds(3),
            memory_size=256,
            handler=f"{paths.path_lbd_func_dynamodb_stream_consumer.fname}.lambda_handler",
            code=lambda_.Code.from_bucket(
                bucket=self.s3_bucket_artifacts,
                key=source_artifacts_deployment.s3path_source_zip.key,
            ),
            environment={
                "S3_BUCKET": s3paths.s3dir_dynamodb_stream.bucket,
                "S3_PREFIX": s3paths.s3dir_dynamodb_stream.key,
                "CODE_ETAG": source_artifacts_deployment.s3path_source_zip.etag,
            },
        )

        self.lambda_function_dynamodb_stream_consumer.add_event_source(
            lambda_event_sources.DynamoEventSource(
                self.dynamodb_table_transaction,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=cdk.Duration.seconds(10),
            )
        )

        # --- dynamodb_export_to_s3_post_processor_coordinator
        source_artifacts_deployment = publish_source_artifacts(
            bsm=bsm,
            path_setup_py_or_pyproject_toml=paths.dir_project_root,
            package_name=self.config.app_name,
            path_lambda_function=paths.path_lbd_func_dynamodb_export_to_s3_post_processor_coordinator,
            version="0.1.1",
            dir_build=paths.dir_build_lambda,
            s3dir_lambda=s3paths.s3dir_lambda_artifacts.joinpath(
                "dynamodb_export_to_s3_post_processor_coordinator"
            ).to_dir(),
            use_pathlib=True,
            verbose=True,
        )

        self.lambda_function_dynamodb_export_to_s3_post_processor_coordinator = lambda_.Function(
            self,
            "LambdaFunctionDynamoDBExportToS3PostProcessorCoordinator",
            function_name=self.config.lambda_function_name_dynamodb_export_to_s3_post_process_coordinator,
            runtime=lambda_.Runtime.PYTHON_3_10,
            role=self.lambda_role,
            timeout=cdk.Duration.seconds(120),
            memory_size=256,
            handler=f"{paths.path_lbd_func_dynamodb_export_to_s3_post_processor_coordinator.fname}.lambda_handler",
            code=lambda_.Code.from_bucket(
                bucket=self.s3_bucket_artifacts,
                key=source_artifacts_deployment.s3path_source_zip.key,
            ),
            environment={
                "DYNAMODB_EXPORT_TO_S3_POST_PROCESS_WORKER_FUNCTION_NAME": config.lambda_function_name_dynamodb_export_to_s3_post_process_worker,
                "CODE_ETAG": source_artifacts_deployment.s3path_source_zip.etag,
            },
        )

        self.s3_bucket_data.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(
                self.lambda_function_dynamodb_export_to_s3_post_processor_coordinator,
            ),
            s3.NotificationKeyFilter(
                prefix=s3paths.s3dir_dynamodb_export.key,
                suffix="manifest-files.json",
            ),
        )

        # --- dynamodb_export_to_s3_post_processor_worker
        source_artifacts_deployment = publish_source_artifacts(
            bsm=bsm,
            path_setup_py_or_pyproject_toml=paths.dir_project_root,
            package_name=self.config.app_name,
            path_lambda_function=paths.path_lbd_func_dynamodb_export_to_s3_post_processor_worker,
            version="0.1.1",
            dir_build=paths.dir_build_lambda,
            s3dir_lambda=s3paths.s3dir_lambda_artifacts.joinpath(
                "dynamodb_export_to_s3_post_processor_worker"
            ).to_dir(),
            use_pathlib=True,
            verbose=True,
        )

        self.lambda_function_dynamodb_export_to_s3_post_processor_worker = lambda_.Function(
            self,
            "LambdaFunctionDynamoDBExportToS3PostProcessorWorker",
            function_name=self.config.lambda_function_name_dynamodb_export_to_s3_post_process_worker,
            runtime=lambda_.Runtime.PYTHON_3_10,
            role=self.lambda_role,
            timeout=cdk.Duration.seconds(900),
            memory_size=1024,
            handler=f"{paths.path_lbd_func_dynamodb_export_to_s3_post_processor_worker.fname}.lambda_handler",
            code=lambda_.Code.from_bucket(
                bucket=self.s3_bucket_artifacts,
                key=source_artifacts_deployment.s3path_source_zip.key,
            ),
            environment={
                "CODE_ETAG": source_artifacts_deployment.s3path_source_zip.etag,
            },
        )


def app_synth():
    app = cdk.App()

    stack = Stack(
        app,
        construct_id=f"DynamoDBtoDataLakeStack",
        stack_name=config.cloudformation_stack_name,
        config=config,
    )

    app.synth()
