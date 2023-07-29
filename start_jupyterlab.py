# -*- coding: utf-8 -*-


"""
$ JUPYTER_WORKSPACE_LOCATION=/local_path_to_workspace/jupyter_workspace/
$ docker run -it -v ~/.aws:/home/glue_user/.aws -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/
-e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh
"""

import os
import subprocess
from pathlib import Path
from boto_session_manager import BotoSesManager

aws_profile = "awshsh_app_dev_us_east_1"

bsm = BotoSesManager(profile_name=aws_profile)
aws_region = bsm.aws_region
res = bsm.sts_client.get_session_token()
aws_access_key_id = res["Credentials"]["AccessKeyId"]
aws_secret_access_key = res["Credentials"]["SecretAccessKey"]
aws_session_token = res["Credentials"]["SessionToken"]

dir_home = Path.home()
dir_here = Path(__file__).absolute().parent
dir_project_root = dir_here
image = "amazon/aws-glue-libs:glue_libs_4.0.0_image_01-arm64"
path_jupyter_start_sh = Path("/home/glue_user/jupyter/jupyter_start.sh")

args = ["docker", "run", "--rm"]
# mount .aws folder to container so that the container has AWS permission
args.extend(["-v", f"{dir_home}/.aws:/home/glue_user/.aws"])
# mount project root folder to container
args.extend(["-v", f"{dir_project_root}/:/home/glue_user/workspace/jupyter_workspace/"])
args.extend(["-e", f"AWS_PROFILE={aws_profile}"])
args.extend(["-e", "DISABLE_SSL=true"])
args.extend(
    ["-p", "4040:4040", "-p", "18080:18080", "-p", "8998:8998", "-p", "8888:8888"]
)
args.extend(["-e", f"AWS_REGION={aws_region}"])
args.extend(["-e", f"AWS_ACCESS_KEY_ID={aws_access_key_id}"])
args.extend(["-e", f"AWS_SECRET_ACCESS_KEY={aws_secret_access_key}"])
args.extend(["-e", f"AWS_SESSION_TOKEN={aws_session_token}"])
args.extend(["--name", "glue_jupyter_lab"])
args.extend([image, str(path_jupyter_start_sh)])
subprocess.run(args)
