import json
from dynamodb_to_datalake.boto_ses import bsm
from dynamodb_to_datalake.s3paths import s3dir_dynamodb_stream, s3dir_incremental_glue_job_input
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
# for s3path in s3dir_dynamodb_stream.iter_objects(
#     start_after="projects/dynamodb_to_datalake/dynamodb_stream/update_at=2023-07-30-21-27/",
# ):
#     if s3path.key < "projects/dynamodb_to_datalake/dynamodb_stream/update_at=2023-07-30-21-31/":
#         print(s3path.uri)

"2023-07-30-21-31"

s3uri_list_dynamodb_stream = list()
for s3path in (
    s3dir_dynamodb_stream.iter_objects(
        start_after=s3dir_dynamodb_stream.joinpath(f"update_at=2023-07-30-21-32/").key,
    )
):
    s3uri_list_dynamodb_stream.append(s3path.uri)

s3uri_list_glue_job_incremental_input = list()
for s3path_input in s3dir_incremental_glue_job_input.iter_objects():
    s3uri_list = json.loads(s3path_input.read_text())["s3uri_list"]
    s3uri_list_glue_job_incremental_input.extend(s3uri_list)

s3uri_list_dynamodb_stream.sort()
s3uri_list_glue_job_incremental_input.sort()

print(len(s3uri_list_dynamodb_stream))
print(len(s3uri_list_glue_job_incremental_input))

for s3uri_1, s3uri_2 in zip(s3uri_list_dynamodb_stream, s3uri_list_glue_job_incremental_input):
    if s3uri_1 != s3uri_2:
        print(s3uri_1, s3uri_2)
