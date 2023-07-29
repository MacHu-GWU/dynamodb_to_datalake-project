# -*- coding: utf-8 -*-

import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# create spark session
conf = (
    SparkConf()
    .setAppName("MyApp")
    .setAll(
        [
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            ("spark.sql.hive.convertMetastoreParquet", "false"),
        ]
    )
)
spark_ses = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark_ctx = spark_ses.sparkContext
glue_ctx = GlueContext(spark_ctx)

# resolve job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3URI_DYNAMODB_EXPORT_PROCESSED",
        "S3URI_TABLE",
        "DATABASE_NAME",
        "TABLE_NAME",
    ]
)
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

S3URI_DYNAMODB_EXPORT_PROCESSED = args["S3URI_DYNAMODB_EXPORT_PROCESSED"]
S3URI_TABLE = args["S3URI_TABLE"]
DATABASE_NAME = args["DATABASE_NAME"]
TABLE_NAME = args["TABLE_NAME"]

import boto3

boto_ses = boto3.session.Session()
sts_client = boto_ses.client("sts")
aws_account_id = sts_client.get_caller_identity()["Account"]
aws_region = boto_ses.region_name

print(f"aws_account_id = {aws_account_id}")
print(f"aws_region = {aws_region}")

pdf_initial = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [
            S3URI_DYNAMODB_EXPORT_PROCESSED,
        ],
        "recurse": True,
    },
    format="json",
    format_options={"multiline": True},
).toDF()
pdf_initial.printSchema()


def show_df(pdf, n: int = 3):
    pdf.show(n, vertical=True, truncate=False)

# show_df(pdf_initial)
# pdf_initial.count()

database = DATABASE_NAME
table = TABLE_NAME

additional_options = {
    "hoodie.table.name": table,
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "update_at",
    "hoodie.datasource.write.partitionpath.field": "create_year,create_month,create_day,create_hour,create_minute",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": database,
    "hoodie.datasource.hive_sync.table": table,
    "hoodie.datasource.hive_sync.partition_fields": "create_year,create_month,create_day,create_hour,create_minute",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "path": S3URI_TABLE,
}

(
    pdf_initial.write.format("hudi")
    .options(**additional_options)
    .mode("overwrite")
    .save()
)

job.commit()
