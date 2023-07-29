# -*- coding: utf-8 -*-

from boto_session_manager import BotoSesManager
from s3pathlib import context

from .config import AWS_PROFILE

bsm = BotoSesManager(profile_name=AWS_PROFILE)
context.attach_boto_session(bsm.boto_ses)
