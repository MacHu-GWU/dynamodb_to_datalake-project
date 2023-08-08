# -*- coding: utf-8 -*-


def is_role_exists(
    iam_client,
    role_name: str,
) -> bool:
    try:
        iam_client.get_role(RoleName=role_name)
        return True
    except Exception as e:
        if "not found" in str(e).lower():
            return False
        else:
            raise NotImplementedError


def get_iam_role_console_url(aws_region: str, role_name: str) -> str:
    return (
        f"https://{aws_region}.console.aws.amazon.com/iamv2"
        f"/home?region={aws_region}#/roles/details"
        f"/{role_name}?section=permissions"
    )
