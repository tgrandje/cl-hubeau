# -*- coding: utf-8 -*-

from dotenv import load_dotenv
from importlib_metadata import version

from .config import _config

load_dotenv(override=True)
__version__ = version(__package__)
