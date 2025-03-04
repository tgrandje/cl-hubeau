#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenienence module retrieving SAGE's cities to run targeted queries
"""
import os

import diskcache
import geopandas as gpd
from pynsee.geodata import get_geodata
from requests import Session

from cl_hubeau.utils.cities_deps_regions import silence_sirene_logs
from cl_hubeau.constants import DIR_CACHE, DISKCACHE
from cl_hubeau.config import _config

cache = diskcache.Cache(os.path.join(DIR_CACHE, DISKCACHE))


@cache.memoize(
    tag="SAGE_cities", expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds()
)
@silence_sirene_logs
def cities_for_sage() -> dict:
    """
    Retrieve a dictionnary of SAGE's cities composition. Each value is a list
    of cities codes, each key is a SAGE official code.

    Returns
    -------
    dict
        SAGE's cities composition

    Examples
    -------
    >>> d = cities_for_sage()
    >>> print(d)

    {
     'SAGE01001': ['62318', '62496', '62127', '62903', '62273', '62102', '62251', '62201', '62354', '62887', '62264', '62402', '62604', '62603', '62845', '62235', '62446', '62460', '62648', '62483', ...],
      ... ,
      'SAGE10004': ['97405', '97416', '97417', '97412', '97404', '97414', '97422', '97403', '97401', '97413', '97419', '97406', '97424', '97423', '97410', '97421', '97415', '97408']
    }


    """

    url = "https://services.sandre.eaufrance.fr/geo/zpl"
    payload = {
        "REQUEST": "getFeature",
        "service": "WFS",
        "VERSION": "2.0.0",
        "TYPENAMES": "sa:Sage",
        "OUTPUTFORMAT": "geojson",
    }

    session = Session()
    session.proxies.update(
        {
            "https": os.environ.get("https_proxy", None),
            "http": os.environ.get("http_proxy", None),
        }
    )

    r = session.get(url, params=payload)
    sages = gpd.read_file(r.content)
    sages = sages.loc[:, ["CodeNatZone", "NomZone", "geometry"]]

    com = get_geodata("ADMINEXPRESS-COG-CARTO.LATEST:commune", crs=sages.crs)

    sages = sages.sjoin(com)
    sages = sages.groupby("CodeNatZone")["insee_com"].agg(list).to_dict()
    return sages


# @cache.memoize(
#     tag="DCE", expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds()
# )
def basins_sub_basins() -> dict:

    session = Session()
    session.proxies.update(
        {
            "https": os.environ.get("https_proxy", None),
            "http": os.environ.get("http_proxy", None),
        }
    )

    url = "https://services.sandre.eaufrance.fr/geo/mdo"
    params = {
        "request": "GetFeature",
        "service": "WFS",
        "version": "2.0.0",
        "outputFormat": "application/json; subtype=geojson",
        "typeNames": "SsBassinDCEAdmin",
    }

    r = session.get(url, params=params)
    subbasins = gpd.read_file(r.content)

    params.update({"typeNames": "BassinDCE"})
    r = session.get(url, params=params)
    basins = gpd.read_file(r.content)

    df = subbasins.merge(
        basins.drop(["geometry", "gid"], axis=1),
        on="CdEuBassinDCE",
        how="outer",
    )
    print(df)
    # -> test with a merge on deps!

    # # https://services.sandre.eaufrance.fr/geo/mdo?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=SousBassinDCE_Communes&OUTPUTFORMAT=CSV
    dep = get_geodata("ADMINEXPRESS-COG-CARTO.LATEST:departement", crs=df.crs)

    df = df.sjoin(dep)
    df.groupby()
    return df


if __name__ == "__main__":
    df = basins_sub_basins()
