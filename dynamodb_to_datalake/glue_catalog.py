# -*- coding: utf-8 -*-

import typing as T


def get_glue_database(
    glue_client,
    database: str,
) -> T.Optional[dict]:
    """
    Get glue database details. Return None if not found.
    """
    try:
        # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_database.html
        res = glue_client.get_database(
            Name=database,
        )
        return res["Database"]
    except Exception as e:
        if "not found" in str(e).lower():
            return None
        else:
            raise NotImplementedError


def get_glue_table(
    glue_client,
    database: str,
    table: str,
) -> T.Optional[dict]:
    """
    Get glue table details. Return None if not found.
    """
    try:
        # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_table.html
        res = glue_client.get_table(
            DatabaseName=database,
            Name=table,
        )
        return res["Table"]
    except Exception as e:
        if "not found" in str(e).lower():
            return None
        else:
            raise NotImplementedError


def delete_glue_table(
    glue_client,
    database: str,
    table: str,
):
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/delete_table.html
    glue_client.delete_table(
        DatabaseName=database,
        Name=table,
    )


def delete_glue_table_if_exists(
    glue_client,
    database: str,
    table: str,
):
    if get_glue_table(glue_client, database, table) is not None:
        delete_glue_table(glue_client, database, table)


def delete_glue_database(
    glue_client,
    database: str,
):
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/delete_database.html
    glue_client.delete_database(
        Name=database,
    )


def delete_glue_database_if_exists(
    glue_client,
    database: str,
):
    if get_glue_database(glue_client, database) is not None:
        delete_glue_database(glue_client, database)


def create_glue_database(
    glue_client,
    database: str,
):
    if get_glue_database(glue_client, database) is None:
        # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/create_database.html
        glue_client.create_database(
            DatabaseInput=dict(
                Name=database,
            ),
        )


def get_glue_database_console_url(
    aws_region: str,
    database: str,
) -> str:
    return (
        f"https://{aws_region}.console.aws.amazon.com"
        f"/glue/home?region={aws_region}#/v2/data-catalog/databases/view/{database}"
    )
