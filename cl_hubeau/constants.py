# -*- coding: utf-8 -*-
"""
Module's constants
"""

import platformdirs

APP_NAME = "cl-hubeau"
DIR_CACHE = platformdirs.user_cache_dir(APP_NAME, ensure_exists=True)
CACHE_NAME = "clhubeau_http_cache.sqlite"
