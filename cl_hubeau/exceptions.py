#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Custom exceptions
"""
from typing import Union, Any


class UnexpectedValueError(ValueError):

    def __init__(
        self,
        arg_name: str,
        arg_value: Any,
        allowed_values: Union[list, set, tuple],
        *args,
        **kwargs,
    ):
        self.arg_name = arg_name
        self.arg_value = arg_value
        self.allowed_values = allowed_values
        message = (
            f"{arg_name} must be among {allowed_values}, "
            f"found {arg_name}='{arg_value}' instead"
        )
        super().__init__(message, *args, **kwargs)


class UnexpectedArguments(ValueError):

    def __init__(self, unexpected_kwargs: dict, doc_url: str, *args, **kwargs):
        self.unexpected_kwargs = unexpected_kwargs
        self.doc_url = doc_url
        message = (
            f"found unexpected arguments {unexpected_kwargs}, "
            f"please have a look at the documentation on {doc_url}"
        )
        super().__init__(message, *args, **kwargs)
