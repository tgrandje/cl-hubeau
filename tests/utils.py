# -*- coding: utf-8 -*-

import functools
import logging


def silence_api_version_warning(func):

    def filter_api_version(record):
        return not record.msg == "api_version not found among API response"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        logger = logging.getLogger("cl_hubeau.session.session")
        logger.propagate = False
        logger.addFilter(filter_api_version)

        try:
            return func(*args, **kwargs)
        finally:
            logger.propagate = True
            logger.removeFilter(filter_api_version)

    return wrapper
