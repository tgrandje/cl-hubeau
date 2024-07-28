# -*- coding: utf-8 -*-

from importlib_metadata import version

from .piezometry import PiezometrySession


__version__ = version(__package__)

__all__ = [
    "PiezometrySession",
]
