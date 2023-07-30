from dynamodb_to_datalake.boto_ses import bsm
from rich import print as rprint
# ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/batch_get_jobs.html
# res = bsm.glue_client.batch_get_jobs(
#     JobNames=["dynamodb_to_datalake_incremental",],
# )
# rprint(res)

res = bsm.lambda_client.get_function(
    FunctionName="helladfkljasdlf",
)
rprint(res)

"https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_database.html"