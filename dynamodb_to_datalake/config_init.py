# -*- coding: utf-8 -*-

import json
from .paths import path_config_json
from .config_define import Config

config = Config(
    **json.loads(path_config_json.read_text())
)
