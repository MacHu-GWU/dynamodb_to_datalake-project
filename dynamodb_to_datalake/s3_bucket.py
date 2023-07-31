# -*- coding: utf-8 -*-


def is_bucket_exists(
    s3_client,
    bucket: str,
) -> bool:
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except Exception as e:
        if "not found" in str(e).lower():
            return False
        else:
            raise NotImplementedError


def create_bucket(
    s3_client,
    aws_region,
    bucket: str,
):
    if is_bucket_exists(s3_client, bucket) is True:
        return
    kwargs = dict(
        Bucket=bucket,
    )
    if aws_region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = dict(LocationConstraint=aws_region)
    s3_client.create_bucket(**kwargs)


