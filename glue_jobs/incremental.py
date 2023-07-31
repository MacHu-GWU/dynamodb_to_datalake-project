# -*- coding: utf-8 -*-

import sys
import json

import boto3

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.context import SparkContext

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# ------------------------------------------------------------------------------
# create spark session
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# resolve job parameters
# ------------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3URI_INCREMENTAL_GLUE_JOB_INPUT",
        "S3URI_INCREMENTAL_GLUE_JOB_TRACKER",
        "S3URI_TABLE",
        "DATABASE_NAME",
        "TABLE_NAME",
    ]
)
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

S3URI_INCREMENTAL_GLUE_JOB_INPUT = args["S3URI_INCREMENTAL_GLUE_JOB_INPUT"]
S3URI_INCREMENTAL_GLUE_JOB_TRACKER = args["S3URI_INCREMENTAL_GLUE_JOB_TRACKER"]
S3URI_TABLE = args["S3URI_TABLE"]
DATABASE_NAME = args["DATABASE_NAME"]
TABLE_NAME = args["TABLE_NAME"]

# ------------------------------------------------------------------------------
# create boto3 session
# ------------------------------------------------------------------------------
boto_ses = boto3.session.Session()
sts_client = boto_ses.client("sts")
aws_account_id = sts_client.get_caller_identity()["Account"]
aws_region = boto_ses.region_name

print(f"aws_account_id = {aws_account_id}")
print(f"aws_region = {aws_region}")

# ------------------------------------------------------------------------------
# parse input data from s3
# ------------------------------------------------------------------------------
s3_client = boto3.client("s3")
parts = S3URI_INCREMENTAL_GLUE_JOB_INPUT.split("/", 3)
bucket = parts[2]
key = parts[3]
response = s3_client.get_object(
    Bucket=bucket,
    Key=key,
)
input_data = json.loads(response["Body"].read().decode("utf-8"))
s3uri_list = input_data["s3uri_list"]

pdf_incremental = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": s3uri_list,
    },
    format="json",
    format_options={"multiline": True},
).toDF()
pdf_incremental.printSchema()


def show_df(pdf, n: int = 3):
    pdf.show(n, vertical=True, truncate=False)

# show_df(pdf_incremental)
# pdf_incremental.count()

# ------------------------------------------------------------------------------
# generate create_year, create_month, ..., create_minute columns
# ------------------------------------------------------------------------------
pdf_incremental_1 = pdf_incremental.withColumn(
    "id",
    F.concat(
        F.lit("account:"),
        pdf_incremental.account,
        F.lit(",create_at:"),
        pdf_incremental.create_at,
    )
).withColumn(
    "create_year",
    F.substring(pdf_incremental.create_at, 1, 4),
).withColumn(
    "create_month",
    F.substring(pdf_incremental.create_at, 6, 2),
).withColumn(
    "create_day",
    F.substring(pdf_incremental.create_at, 9, 2),
).withColumn(
    "create_hour",
    F.substring(pdf_incremental.create_at, 12, 2),
).withColumn(
    "create_minute",
    F.substring(pdf_incremental.create_at, 15, 2),
)
# show_df(pdf_incremental_1)

# add row_number column
pdf_incremental_2 = (
    pdf_incremental_1.withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy("id").orderBy(F.col("update_at").desc())
        )
    )
)
# show_df(pdf_incremental_2)

# pdf_tmp = pdf_incremental_2.select(
#     pdf_incremental_2.id,
#     pdf_incremental_2.update_at,
#     pdf_incremental_2.row_number,
# ).sort(pdf_incremental_2.id.desc())
# pdf_tmp.show(3, truncate=False)

# generate the final dataframe
pdf_incremental_3 = (
    pdf_incremental_2
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
# show_df(pdf_incremental_3)

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
    pdf_incremental_3.write.format("hudi")
    .options(**additional_options)
    .mode("append")
    .save()
)

job.commit()
