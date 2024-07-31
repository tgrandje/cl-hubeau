#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Package configuration
"""

from datetime import timedelta

_config = {
    "DEFAULT_EXPIRE_AFTER": timedelta(days=30),
    "DEFAULT_EXPIRE_AFTER_REALTIME": timedelta(minutes=15),
    "SIZE": 1000,  # Default size for each API's result
    "RATE_LIMITER": 10,  # queries per second
    "TQDM_LEAVE": None,  # keep tqdm progressbar after completion
    "THREADS": 10,  # Max number of threads to permform one request to an endpoint
}
