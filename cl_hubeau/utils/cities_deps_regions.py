#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Get list of cities' and departements' codes
"""

from datetime import date
from itertools import chain
import logging
import os
from typing import Union

import diskcache
import geopandas as gpd
from french_cities.ultramarine_pseudo_cog import _get_ultramarines_cities
from french_cities import set_vintage
import pandas as pd
from pynsee import get_area_list, get_geo_list, get_geodata
from pynsee.utils.init_connection import init_conn

from cl_hubeau.constants import DIR_CACHE, DISKCACHE
from cl_hubeau.config import _config


cache = diskcache.Cache(os.path.join(DIR_CACHE, DISKCACHE))


def silence_sirene_logs(func):
    """
    decorator deactivating critical/error/warning log entries from pynsee on
    missing SIRENE credentials:  this is intended behaviour in cl-hubeau
    context
    """

    def filter_no_credential_or_no_results(record):
        return (
            not record.msg.startswith(
                "INSEE API credentials have not been found"
            )
            and not record.msg.startswith(
                "Invalid credentials, the following APIs returned error codes"
            )
            and not record.msg.startswith(
                "Remember to subscribe to SIRENE API"
            )
            and not (
                record.msg.startswith("Existing environment variable %s")
                and (
                    "http_proxy" in record.args or "https_proxy" in record.args
                )
            )
        )

    def wrapper(*args, **kwargs):
        # Note: deactivate pynsee log to substitute by a more accurate
        pynsee_logs = "_get_credentials", "requests_session", "init_connection"
        for log in pynsee_logs:
            pynsee_log = logging.getLogger(f"pynsee.utils.{log}")
            pynsee_log.addFilter(filter_no_credential_or_no_results)
        try:
            return func(*args, **kwargs)
        except Exception:
            raise
        finally:
            for log in pynsee_logs:
                pynsee_log = logging.getLogger(f"pynsee.utils.{log}")
                pynsee_log.removeFilter(filter_no_credential_or_no_results)

    return wrapper


@silence_sirene_logs
def init_pynsee_connection():
    """
    Initiate an INSEE API connection with tokens and proxies.
    """
    keys = ["http_proxy", "https_proxy"]
    kwargs = {x: os.environ[x] for x in keys if x in os.environ}
    kwargs.update(
        {x: os.environ[x.upper()] for x in keys if x.upper() in os.environ}
    )
    init_conn(**kwargs)


@cache.memoize(
    tag="cities", expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds()
)
@silence_sirene_logs
def _get_pynsee_arealist_cities(
    code_departement: tuple = None,
    code_region: tuple = None,
) -> pd.DataFrame:
    """
    Retrieve french cities for all times (including ultramarines)

    Parameters
    ----------
    code_departement : Union[str, list, tuple, set], optional
        If set, will restrict cities codes to the desired departements
    code_region : Union[str, list, tuple, set], optional
        If set, will restrict cities codes to the desired code_region

    Returns
    -------
    pd.DataFrame
        DataFrame of cities
    """

    init_pynsee_connection()

    cities = pd.concat(
        [
            get_area_list("communes", "*", silent=True),
            _get_ultramarines_cities(
                date.today().strftime("%Y-01-01"),
                threads=_config["THREADS"],
            ),
        ]
    ).drop_duplicates("CODE")

    if code_departement or code_region:

        restored, *_ = cache.get("cities_rich", tag="cities")
        if restored is not None:
            cities = restored
        else:
            cities["CODE_INIT"] = cities["CODE"]
            cities = set_vintage(cities, date.today().year, "CODE")

            kwargs = {"silent": True}
            dep_reg = pd.concat(
                [
                    get_geo_list("communesAssociees", **kwargs),
                    get_geo_list("communes", **kwargs),
                    get_geo_list("communesDeleguees", **kwargs),
                ]
            )[["CODE", "CODE_REG", "CODE_DEP"]]

            cities = cities.merge(dep_reg, on="CODE", how="left")

            cache.set(
                "cities_rich",
                cities,
                tag="cities",
                expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds(),
            )

        if code_departement:
            cities = cities[cities.CODE_DEP.isin(set(code_departement))]
        if code_region:
            cities = cities[cities.CODE_REG.isin(set(code_region))]

    return cities


@silence_sirene_logs
def _get_pynsee_geolist_cities():
    """
    Retrieve the french regions with their dep & region at current date.
    Note: this is only used in an inner function to link to the postcodes
    dataset which is not historised, so this should be ok
    """
    init_pynsee_connection()
    cities = get_geo_list("communes", silent=True)
    return cities


@silence_sirene_logs
def _get_pynsee_arealist_regions():
    "Retrieve the french regions for all times (to also get areas < 2016)"
    init_pynsee_connection()
    regs = get_area_list("regions", "*", silent=True)
    return regs


@silence_sirene_logs
def _get_pynsee_geodata_latest(
    type_geo="commune", crs: int = 2154
) -> gpd.GeoDataFrame:
    """
    Simple wrapper to retrieve latest geodataframes from the geoplateform using
    pynsee. This initiates insee connection and silence SIRENE logs.

    Parameters
    ----------
    type_geo : str, optional
        Either "commune", "région", "département". The default is "commune".
    crs : int, optional
        Which CRS use to project the GeoDataFrame. The default is 2154.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        GeoDataFrame generated by pynsee
    """
    init_pynsee_connection()
    if type_geo[-1] == "s":
        type_geo = type_geo[:-1]

    gdf = get_geodata(f"ADMINEXPRESS-COG-CARTO.LATEST:{type_geo}", crs=crs)
    return gdf


@silence_sirene_logs
def _get_pynsee_geolist_departements():
    """
    Retrieve french departements with their current regions.

    Note: no departement's code has changed in recent years, and regions are
    only used as a convenience group, it is not expected that users might
    query the departements' from their previous's regions code: this seems
    safe to query only on the current date.
    """
    init_pynsee_connection()
    deps = get_geo_list("departements", silent=True)
    return deps


@silence_sirene_logs
def _get_pynsee_arealist_departements():
    "Retrieve french departements for all times"
    init_pynsee_connection()
    deps = get_area_list("departements", "*", silent=True)
    return deps


def get_cities(
    code_departement: Union[str, list, tuple, set] = None,
    code_region: Union[str, list, tuple, set] = None,
):
    """
    Retrieve all unique cities' codes in the whole timeline.

    This allows to query datasets using Region codes even if you don't
    know the dataset's vintage.

    Parameters
    ----------
    code_departement : Union[str, list, tuple, set], optional
        If set, will restrict cities codes to the desired departements
    code_region : Union[str, list, tuple, set], optional
        If set, will restrict cities codes to the desired code_region

    Returns
    -------
    list
        Codes of cities (past or present)

    Examples
    -------
    >>> get_cities()
    ['01001', '01002', '01003', ..., '97615', '97616', '97617']
    """

    # Convert args to tuple to allow caching
    kwargs = {"code_departement": code_departement, "code_region": code_region}
    for key, val in kwargs.items():
        if val:
            if isinstance(val, str):
                val = val.split(",")
            kwargs[key] = tuple(val)

    cities = _get_pynsee_arealist_cities(**kwargs)
    return cities["CODE"].unique().tolist()


def get_regions() -> list:
    """
    Retrieve all unique regions' codes in the whole timeline.

    This allows to query datasets using Region codes even if you don't
    know the dataset's vintage.

    Returns
    -------
    list
        Codes of departements

    Examples
    -------
    >>> get_regions()
    ['01', '02', '03',...,  '91', '93', '94']
    """
    regs = _get_pynsee_arealist_regions()
    return regs["CODE"].unique().tolist()


def get_departements_from_regions(reg: Union[str, list, set, tuple]) -> list:
    """
    Retrieve current departements' codes for one or more regions.

    Parameters
    ----------
    reg : Union[str, list, set, tuple]
        Code(s) of desired regions.

    Returns
    -------
    list
        List of departements

    Examples
    -------
    >>> get_departements_from_regions("32")
    ['02', '59', '60', '62', '80']

    >>> get_departements_from_regions(["32", "01"])
    ['02', '59', '60', '62', '80', '971']
    """
    if isinstance(reg, str):
        reg = reg.split(",")
    deps = _get_pynsee_geolist_departements()
    deps = deps.groupby("CODE_REG")["CODE"].agg(list)
    if isinstance(reg, str):
        reg = [reg]
    deps = list(chain(*deps.loc[reg].values))
    return deps


def get_departements() -> list:
    """
    Retrieve all unique departements' codes in the whole timeline. (For
    instance, including 20 for Corse).

    This allows to query datasets using Departement codes even if you don't
    know the dataset's vintage.

    Returns
    -------
    list
        Codes of departements

    Examples
    -------
    >>> get_departements()
    ['01', '02', '03',..., '973', '974', '976']
    """
    deps = _get_pynsee_arealist_departements()
    return deps["CODE"].unique().tolist()


# if __name__ == "__main__":
#     bb = _get_mesh()
