from dynamodb_to_datalake.boto_ses import bsm
from dynamodb_to_datalake.s3paths import s3dir_dynamodb_stream
from rich import print as rprint
# ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/batch_get_jobs.html
# res = bsm.glue_client.batch_get_jobs(
#     JobNames=["dynamodb_to_datalake_incremental",],
# )
# rprint(res)

# res = bsm.lambda_client.get_function(
#     FunctionName="helladfkljasdlf",
# )
# rprint(res)
#
# "https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_database.html"
for s3path in s3dir_dynamodb_stream.iter_objects(
    start_after="projects/dynamodb_to_datalake/dynamodb_stream/update_at=2023-07-30-21-27/",
):
    if s3path.key < "projects/dynamodb_to_datalake/dynamodb_stream/update_at=2023-07-30-21-31/":
        print(s3path.uri)

