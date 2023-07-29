import sys
import boto3

from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark_ses = glue_ctx.spark_session

aws_region = "us-east-1"
boto_ses = boto3.session.Session(region_name=aws_region)
aws_account_id = boto_ses.client("sts").get_caller_identity()["Account"]
bucket = f"{aws_account_id}-{aws_region}-data"

s3_client = boto_ses.client("s3")

tracker_key = "projects/dynamodb_to_datalake/tracker.txt"
try:
    response = s3_client.get_object(Bucket=bucket, key=tracker_key)
    last_processed_key = response["Body"].read().decode("utf-8")
except Exception as e:
    last_processed_key = "projects/dynamodb_to_datalake/dynamodb_stream/2023-01-01-00-00-00-000000.json"

s3uri_list = list()
key_list = list()
paginator = s3_client.get_paginator("list_objects_v2")
for response in paginator.paginate(
    Bucket=bucket,
    Prefix="projects/dynamodb_to_datalake/dynamodb_stream/",
    PaginationConfig=dict(
        PageSize=1000,
    )
):
    for content in response["Contents"]:
        key = content["Key"]
        if key > last_processed_key:
            s3uri_list.append("s3://{}/{}".format(bucket, key))
            key_list.append(key)

if len(s3uri_list):
    for s3uri in s3uri_list:
        print(s3uri)
    # read the data
    gdf = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": s3uri_list,
        },
        format="json",
        format_options={
            "multiline": True,
        },
    )
    pdf = gdf.toDF()
    # pdf.show(10)

    # process the data
    window_spec = Window.partitionBy(["id"]).orderBy("rating_updated_at")
    pdf_with_row_number = pdf.withColumn(
        "row_number",
        row_number().over(window_spec)
    )
    pdf_final = pdf_with_row_number.select(
        "id", "title", "rating", "rating_updated_at",
    ).where(pdf_with_row_number.row_number == 1).withColumn(
        "year",
        lit(2023),
    )

    pdf_final = spark_ses.createDataFrame(
        [
            (1, "title1", 95, "2023-07-25T03:19:00.123456", 2023),
            (2, "title2", 44, "2023-07-25T03:20:00.123456", 2023),
            (3, "title3", 66, "2023-07-25T03:21:00.123456", 2023),
        ],
        ("id", "title", "rating", "rating_updated_at", "year"),
    )
    pdf_final.show(100)

    additional_options = {
        "hoodie.table.name": "movie",
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "rating_updated_at",
        "hoodie.datasource.write.partitionpath.field": "",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "mydatabase",
        "hoodie.datasource.hive_sync.table": "movie",
        # "hoodie.datasource.hive_sync.partition_fields": "year",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "path": f"s3://{bucket}/projects/hudi-poc/databases/mydatabase/movie"
    }
    (
        pdf_final.write.format("hudi")
        .options(**additional_options)
        .mode("overwrite")
        .save()
    )

    # new_last_processed_key = max(key_list)
    # s3_client.put_object(
    #     Bucket=bucket,
    #     Key=tracker_key,
    #     Body=new_last_processed_key,
    # )
else:
    pass

job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)
job.commit()
