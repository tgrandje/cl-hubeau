#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Get list of cities' and departements' codes
"""

import os

from pynsee import get_area_list
from pynsee.utils.init_conn import init_conn


def init_pynsee_connection():
    """
    Initiate an INSEE API connection with tokens and proxies.
    """
    keys = ["insee_key", "insee_secret", "http_proxy", "https_proxy"]
    kwargs = {x: os.environ[x] for x in keys if x in os.environ}
    kwargs.update(
        {x: os.environ[x.upper()] for x in keys if x.upper() in os.environ}
    )
    init_conn(**kwargs)


def get_cities():
    init_pynsee_connection()
    cities = get_area_list("communes", "*", silent=True)
    return cities["CODE"].unique().tolist()


def get_regions():
    try:
        init_pynsee_connection()
        regs = get_area_list("regions", "*", silent=True)
        return regs["CODE"].unique().tolist()
    except Exception:
        return [
            "01",
            "02",
            "03",
            "04",
            "06",
            "11",
            "21",
            "22",
            "23",
            "24",
            "25",
            "26",
            "27",
            "28",
            "31",
            "32",
            "41",
            "42",
            "43",
            "44",
            "52",
            "53",
            "54",
            "72",
            "73",
            "74",
            "75",
            "76",
            "82",
            "83",
            "84",
            "91",
            "93",
            "94",
        ]


def get_departements():
    try:
        init_pynsee_connection()
        deps = get_area_list("departements", "*", silent=True)
        return deps["CODE"].unique().tolist()
    except Exception:
        # pynsee not working, return a simple constant
        return [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "25",
            "26",
            "27",
            "28",
            "29",
            "2A",
            "2B",
            "30",
            "31",
            "32",
            "33",
            "34",
            "35",
            "36",
            "37",
            "38",
            "39",
            "40",
            "41",
            "42",
            "43",
            "44",
            "45",
            "46",
            "47",
            "48",
            "49",
            "50",
            "51",
            "52",
            "53",
            "54",
            "55",
            "56",
            "57",
            "58",
            "59",
            "60",
            "61",
            "62",
            "63",
            "64",
            "65",
            "66",
            "67",
            "68",
            "69",
            "70",
            "71",
            "72",
            "73",
            "74",
            "75",
            "76",
            "77",
            "78",
            "79",
            "80",
            "81",
            "82",
            "83",
            "84",
            "85",
            "86",
            "87",
            "88",
            "89",
            "90",
            "91",
            "92",
            "93",
            "94",
            "95",
            "971",
            "972",
            "973",
            "974",
            "976",
        ]


if __name__ == "__main__":
    regs = get_regions()
