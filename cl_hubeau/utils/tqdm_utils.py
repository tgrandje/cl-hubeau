#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 16:25:43 2025

@author: thomasgrandjean
"""

from tqdm_loggable.utils import is_interactive_session, is_stdout_only_session


def tqdm_custom(*args, **kwargs):
    """
    small patch to adapt tqdm_loggable behaviour for spyder
    """

    if is_interactive_session() or is_stdout_only_session():
        from tqdm import tqdm
    else:
        from tqdm_loggable.tqdm_logging import tqdm_logging as tqdm
    return tqdm(*args, **kwargs)
