#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 14:39:07 2025

Creates a mesh of square bounding boxes from the geometry for usual territorial
objects (region, basin, etc.).

"""

import logging
import os
from typing import Union

import diskcache
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon

from cl_hubeau.config import _config
from cl_hubeau.constants import DIR_CACHE, DISKCACHE
from cl_hubeau.utils.cities_deps_regions import _get_pynsee_geodata_latest
from cl_hubeau.utils.hydro_perimeters_queries import (
    _get_dce_subbasins,
    _get_sage,
)

logger = logging.getLogger(__name__)
cache = diskcache.Cache(os.path.join(DIR_CACHE, DISKCACHE))


@cache.memoize(
    tag="bbox_mesh_grid",
    expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds(),
)
def _set_grid(
    crs: int = 4326,
    buffer: int = 10_000,
    side: int = 0.5,
) -> list:

    gdf = _get_pynsee_geodata_latest("commune", crs=2154)
    geom = gpd.GeoSeries([gdf.union_all()], crs=2154).simplify(buffer / 10)
    geom = geom.buffer(buffer).to_crs(4326)

    xmin, ymin, xmax, ymax = geom.total_bounds
    cols = list(np.arange(xmin, xmax + side, side))
    rows = list(np.arange(ymin, ymax + side, side))
    polygons = []
    for x in cols[:-1]:
        for y in rows[:-1]:
            polygons.append(
                Polygon(
                    [
                        (x, y),
                        (x + side, y),
                        (x + side, y + side),
                        (x, y + side),
                    ]
                )
            )
    grid = gpd.GeoDataFrame({"geometry": polygons}, crs=4326)
    return grid


@cache.memoize(
    tag="bbox_mesh",
    expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds(),
)
def _get_mesh(
    code_region: Union[str, list, tuple, set, None] = None,
    code_departement: Union[str, list, tuple, set, None] = None,
    code_commune: Union[str, list, tuple, set, None] = None,
    code_bassin: Union[str, list, tuple, set, None] = None,
    code_sous_bassin: Union[str, list, tuple, set, None] = None,
    code_sage: Union[str, list, tuple, set, None] = None,
    crs: int = 4326,
    buffer: int = 10_000,
    side: int = 0.5,
    **kwargs,
) -> list:
    """
    Creates a mesh englobing the desired area and return bounding boxes as a
    list of lists. This can be used when hub'eau allows querying with a bbox
    object and can bypass missing fields.

    Note that bounding boxes will overlap if CRS is not 4326. In that case,
    mesh might not be consisted of squares.

    Parameters
    ----------
    code_region : Union[str, list, tuple, set, None], optional
        Desired region (if any). The default is None.
    code_departement : Union[str, list, tuple, set, None], optional
        Desired departement (if any). The default is None.
    code_commune : Union[str, list, tuple, set, None], optional
        Desired cities (if any). The default is None.
    code_bassin : Union[str, list, tuple, set, None], optional
        Desired basin (if any). The default is None.
    code_sous_bassin : Union[str, list, tuple, set, None], optional
        Desired subbasin (if any). The default is None.
    code_sage : Union[str, list, tuple, set, None], optional
        Desired sage (if any). The default is None.
    crs : int, optional
        Desired projection for bounding boxes. The default is 4326.
    buffer : int, optional
        Buffer set around the geometry before constructing the mesh, to be
        certain to capture all data on geographical borders.
    side : int, optional
        The grid's width/height (expressed in degrees). The default is .5.
    kwargs :
        ignored

    Returns
    -------
    list
        List of bounding boxes

    Examples
    -------
    >>> _get_mesh()
    [
        [-61.80983862, -21.38963076, 99938.19016138, 99978.61036924],
        ...
        [-61.80983862, -21.38963076, 99938.19016138, 99978.61036924]
    ]

    >>> _get_mesh(code_region="11")

    >>> _get_mesh(code_bassin="G")

    >>> _get_mesh(code_sage="SAGE06022")
    """

    # Note : use this instead of **kwargs to set specific arguments on function
    # signature:
    d = {}
    for x in (
        "code_region",
        "code_departement",
        "code_commune",
        "code_bassin",
        "code_sous_bassin",
        "code_sage",
    ):
        try:
            d[x] = locals()[x]
        except KeyError:
            continue
        else:
            if isinstance(d[x], str):
                d[x] = d[x].split(",")

    # 1. create a fixed grid based on commune geodataset
    grid = _set_grid(crs, buffer, side)

    # 2. select squares from the fixed grid according to kwargs
    if code_region or code_departement or code_commune:
        gdf = _get_pynsee_geodata_latest("commune", crs=4326)
    elif code_bassin or code_sous_bassin:
        gdf = _get_dce_subbasins(crs=4326)
    elif code_sage:
        gdf = _get_sage()

    if code_region:
        gdf = gdf.query(
            f"code_insee_de_la_region.isin({d['code_region']})"
        ).copy()
    if code_departement:
        gdf = gdf.query(
            f"code_insee_du_departement.isin({d['code_departement']})"
        ).copy()
    if code_commune:
        gdf = gdf.query(f"code_insee.isin({d['code_commune']})").copy()
    if code_bassin:
        gdf = gdf.query(f"CdBassinDCE.isin({d['code_bassin']})").copy()
    if code_sous_bassin:
        gdf = gdf.query(
            f"CdEuSsBassinDCEAdmin.isin({d['code_sous_bassin']})"
        ).copy()
    if code_sage:
        gdf = gdf.query(f"CodeNatZone.isin({d['code_sage']})").copy()

    try:
        grid = grid.sjoin(
            gpd.GeoSeries([gdf.union_all()], crs=4326).to_frame(),
            how="inner",
            predicate="intersects",
        )
    except UnboundLocalError:
        # keep full mesh for whole territory
        pass

    grid = grid.to_crs(crs)

    return grid.geometry.bounds.values.tolist()
