#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenienence module retrieving SAGE's cities to run targeted queries
"""
import os
from typing import Union

import diskcache
import geopandas as gpd
from pynsee.geodata import get_geodata
from requests import Session

from cl_hubeau.utils.cities_deps_regions import silence_sirene_logs
from cl_hubeau.constants import DIR_CACHE, DISKCACHE
from cl_hubeau.config import _config

cache = diskcache.Cache(os.path.join(DIR_CACHE, DISKCACHE))


@cache.memoize(
    tag="SAGE", expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds()
)
def _inner_sages() -> gpd.GeoDataFrame:
    """
    Retrieve a GeoDataFrame of SAGEs. This should only be used to
    construct an ad hoc mesh grid of bboxes. This is cached but should only
    be called through get_sage which can filter that dataframe.

    Returns
    -------
    gpd.GeoDataFrame:
        SAGE GeoDataFrame. Only 2 columns: ["geometry", "CodeNatZone"]

    Examples
    -------
    >>> gdf = _inner_sages()
    >>> gdf.head()
                                                geometry CdEuSsBassinDCEAdmin  \
    0  POLYGON ((3.09897 49.68437, 3.10018 49.68509, ...             FRA_ESCA
    1  POLYGON ((5.50417 48.55017, 5.50454 48.55137, ...            FRB1_MEUS

      CdBassinDCE
    0           A
    1          B1


    """

    session = Session()
    session.proxies.update(
        {
            "https": os.environ.get("https_proxy", None),
            "http": os.environ.get("http_proxy", None),
        }
    )

    url = "https://services.sandre.eaufrance.fr/geo/zpl"
    params = {
        "REQUEST": "getFeature",
        "service": "WFS",
        "VERSION": "2.0.0",
        "TYPENAMES": "sa:Sage",
        "OUTPUTFORMAT": "geojson",
    }

    r = session.get(url, params=params)
    df = gpd.read_file(r.content)

    keep = ["geometry", "CodeNatZone"]
    df = df.loc[:, keep]

    return df


def _get_sage(
    sage: Union[str, None, list, tuple, set] = None, crs: int = 4326
):
    """
    Get GeoDataFrame of SAGE(s). This should only be used to generate a
    mesh of selected bounding boxes when hub'eau's internal dataset misses some
    datasets.


    Parameters
    ----------
    sage : Union[str, None, list, tuple, set]
        Desired SAGE code. Default is None.
    crs : int, optional
        Desired projection. The default is 4326.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        GeoDataFrame of selected subbasins

    Examples
    -------
    >>> get_sage("SAGE06022")
                                                geometry CodeNatZone
    0  POLYGON ((5.74855 43.71708, 5.74807 43.71784, ...   SAGE06022

    """
    gdf = _inner_sages()
    if sage:
        if isinstance(sage, str):
            sage = [sage]
        gdf = gdf.query(f"CodeNatZone.isin({sage})")
    return gdf


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

    # TODO : deprecate function when all APIs are covered by data consolidation
    # with hydrological datasets

    sages = _inner_sages()

    com = get_geodata("ADMINEXPRESS-COG-CARTO.LATEST:commune", crs=sages.crs)

    sages = sages.sjoin(com)
    sages = sages.groupby("CodeNatZone")["code_insee"].agg(list).to_dict()
    return sages


@cache.memoize(
    tag="DCE", expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds()
)
def _inner_basins_sub_basins() -> gpd.GeoDataFrame:
    """
    Retrieve a GeoDataFrame of basins/subbasins. This should only be used to
    construct an ad hoc mesh grid of bboxes. This is cached but should only
    be called through _get_dce_subbasins which can filter that dataframe

    Returns
    -------
    gpd.GeoDataFrame:
        Subbasins GeoDataFrame. Only 3 columns: ["geometry", "CdBassinDCE"
        "CdEuSsBassinDCEAdmin"]

    Examples
    -------
    >>> gdf = _inner_basins_sub_basins()
    >>> gdf.head()
                                                geometry CdEuSsBassinDCEAdmin  \
    0  POLYGON ((3.09897 49.68437, 3.10018 49.68509, ...             FRA_ESCA
    1  POLYGON ((5.50417 48.55017, 5.50454 48.55137, ...            FRB1_MEUS

      CdBassinDCE
    0           A
    1          B1


    """

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

    keep = [
        "geometry",
        "CdEuSsBassinDCEAdmin",
        "NomSsBassinDCEAdmin",
        "CdBassinDCE",
        "NomBassinDCE",
    ]
    df = df.loc[:, keep]

    return df


def _get_dce_subbasins(
    subbasin: Union[str, None, list, tuple, set] = None,
    basin: Union[str, None, list, tuple, set] = None,
    crs: int = 4326,
) -> gpd.GeoDataFrame:
    """
    Get GeoDataFrame of subbasin/basin. This should only be used to generate a
    mesh of selected bounding boxes when hub'eau's internal dataset misses some
    datasets.


    Parameters
    ----------
    subbasin : Union[str, None, list, tuple, set], optional
        Desired european subbasin code. The default is None.
    basin : Union[str, None, list, tuple, set], optional
        Desired basin code. The default is None.
    crs : int, optional
        Desired projection. The default is 4326.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        GeoDataFrame of selected subbasins

    Examples
    -------
    >>> get_dce_subbasins(subbasin="FRA_ESCA")
                                                geometry CdEuSsBassinDCEAdmin  \
    0  POLYGON ((3.09897 49.68437, 3.10018 49.68509, ...             FRA_ESCA

      CdBassinDCE
    0           A

    >>> get_dce_subbasins(basin="G")
                                                 geometry CdEuSsBassinDCEAdmin  \
    22  POLYGON ((2.4291 45.88538, 2.42914 45.88537, 2...             FRG_LMOY
    23  MULTIPOLYGON (((-1.28457 46.17318, -1.28444 46...             FRG_LACV
    24  POLYGON ((4.18938 44.77437, 4.18953 44.77422, ...              FRG_ALA
    25  POLYGON ((0.86215 47.6767, 0.86092 47.67711, 0...              FRG_MSL
    26  MULTIPOLYGON (((-3.25169 47.31108, -3.25169 47...             FRG_VICO
    27  MULTIPOLYGON (((2.26949 45.96998, 2.26929 45.9...             FRG_VICR

       CdBassinDCE
    22           G
    23           G
    24           G
    25           G
    26           G
    27           G

    """
    gdf = _inner_basins_sub_basins()
    if subbasin:
        if isinstance(subbasin, str):
            subbasin = [subbasin]
        gdf = gdf.query(f"CdEuSsBassinDCEAdmin.isin({subbasin})")
    if basin:
        if isinstance(basin, str):
            basin = [basin]
        gdf = gdf.query(f"CdBassinDCE.isin({basin})")
    return gdf.to_crs(crs)
