# -*- coding: utf-8 -*-

from s3pathlib import context

from .config_init import config

bsm = config.bsm
context.attach_boto_session(bsm.boto_ses)
