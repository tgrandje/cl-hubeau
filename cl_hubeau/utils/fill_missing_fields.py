#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 24 10:32:57 2025

@author: thomasgrandjean
"""

import geopandas as gpd
from cl_hubeau.utils import (
    _get_pynsee_geodata_latest,
    _get_pynsee_geolist_cities,
)

from cl_hubeau.utils.hydro_perimeters_queries import _get_dce_subbasins


def _fill_missing_cog(
    gdf: gpd.GeoDataFrame,
    code_commune: str = "code_commune",
    code_departement: str = "code_departement",
    code_region: str = "code_region",
    libelle_commune: str = "libelle_commune",
    libelle_departement: str = "libelle_departement",
    libelle_region: str = "libelle_region",
) -> gpd.GeoDataFrame:
    """
    Fill missing region, departement & cities elements (codes & labels) using
    the closest geometry in an buffer of 10km.

    Note: this is approximative, particularly on international borders but will
    avoid missing stations when looping over (for instance) code_regions.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame of results with missing official geographic data.
    code_commune : str, optional
        The city's code field to fill. The default is "code_commune".
    code_departement : str, optional
        The department's code field to fill. The default is "code_departement".
    code_region : str, optional
        The region's code field to fill. The default is "code_region".
    libelle_commune : str, optional
        The city's name field to fill. The default is "libelle_commune".
    libelle_departement : str, optional
        The department's name field to fill. The default is
        "libelle_departement".
    libelle_region : str, optional
        The region's name field to fill. The default is "libelle_region".

    Returns
    -------
    gdf : gpd.GeoDataFrame
        Filled results.

    """

    if gdf[gdf[code_region].isnull()].empty:
        return gdf

    # missing region_code, departement_code, commune_code, let's fill those
    # with closest geometry, up to 10km
    crs = gdf.crs
    gdf = gdf.to_crs(2154)

    coms = _get_pynsee_geodata_latest("commune", crs=2154)
    cities_labels = _get_pynsee_geolist_cities()

    # first pass, perform an exact spatial join
    missing = gdf[
        (gdf[code_commune].isnull())
        | (gdf[code_departement].isnull())
        | (gdf[code_region].isnull())
    ].index
    missing = (
        gdf.loc[missing].sjoin(coms, how="left").drop("index_right", axis=1)
    )
    # get labels
    missing = missing.merge(
        cities_labels[["TITLE", "TITLE_DEP", "TITLE_REG", "CODE"]],
        how="left",
        left_on="code_insee",
        right_on="CODE",
    ).drop("CODE", axis=1)

    coalesce = {
        code_commune: "code_insee",
        code_departement: "code_insee_du_departement",
        code_region: "code_insee_de_la_region",
        libelle_commune: "TITLE",
        libelle_departement: "TITLE_DEP",
        libelle_region: "TITLE_REG",
    }
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    # second pass, perform a spatial join to nearest, up to 10km
    missing = gdf[
        (gdf[code_commune].isnull())
        | (gdf[code_departement].isnull())
        | (gdf[code_region].isnull())
    ].index
    missing = (
        gdf.loc[missing]
        .sjoin_nearest(
            coms,
            how="left",
            max_distance=10_000,
        )
        .drop("index_right", axis=1)
    )
    # get labels
    missing = missing.merge(
        cities_labels[["TITLE", "TITLE_DEP", "TITLE_REG", "CODE"]],
        how="left",
        left_on="code_commune",
        right_on="CODE",
    ).drop("CODE", axis=1)
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    gdf = gdf.to_crs(crs)
    return gdf


def _fill_missing_basin_subbasin(
    gdf: gpd.GeoDataFrame,
    code_sous_bassin: str = "code_sous_bassin",
    libelle_sous_bassin: str = "libelle_sous_bassin",
    code_bassin: str = "code_bassin",
    libelle_bassin: str = "libelle_bassin",
) -> gpd.GeoDataFrame:
    """
    Fill missing basin & subbasins elements (codes & labels) using
    the closest geometry in an buffer of 10km.

    Note: this is approximative, particularly on international borders but will
    avoid missing stations when looping over (for instance) basins.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame of results with missing (sub)basins data.
    code_sous_bassin : str, optional
        The sub-watershed's code field to fill. The default is
        "code_sous_bassin".
    libelle_sous_bassin : str, optional
        The sub-watershed's name field to fill. The default is
        "libelle_sous_bassin".
    code_bassin : str, optional
        The watershed's code field to fill. The default is "code_bassin".
    libelle_bassin : str, optional
        The watershed's name field to fill. The default is "libelle_bassin".

    Returns
    -------
    gdf : gpd.GeoDataFrame
        Filled results.

    """

    if gdf[gdf[code_bassin].isnull()].empty:
        return gdf

    # missing code_bassin, code_sous_bassin, etc., let's fill those with
    # closest geometry, up to 10km
    crs = gdf.crs
    gdf = gdf.to_crs(2154)

    # spatial join to nearest, up to 10km
    basins = _get_dce_subbasins(crs=2154)

    missing = gdf[
        (gdf[code_sous_bassin].isnull()) | (gdf[code_bassin].isnull())
    ].index

    missing = (
        gdf.loc[missing].sjoin(basins, how="left").drop("index_right", axis=1)
    )

    coalesce = {
        code_sous_bassin: "CdEuSsBassinDCEAdmin",
        libelle_sous_bassin: "NomSsBassinDCEAdmin",
        code_bassin: "CdBassinDCE",
        libelle_bassin: "NomBassinDCE",
    }
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    missing = gdf[
        (gdf[code_sous_bassin].isnull()) | (gdf[code_bassin].isnull())
    ].index
    missing = (
        gdf.loc[missing]
        .sjoin_nearest(
            basins,
            how="left",
            max_distance=10_000,
        )
        .drop("index_right", axis=1)
    )
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    gdf = gdf.to_crs(crs)
    return gdf
