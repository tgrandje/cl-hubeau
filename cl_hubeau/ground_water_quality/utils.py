#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for ground water quality consumption
"""

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.ground_water_quality import GroundWaterQualitySession
from cl_hubeau import _config
from cl_hubeau.utils import get_departements


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to PiezometrySession.get_stations (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_departement`
        as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of piezometers

    """

    with GroundWaterQualitySession() as session:

        deps = get_departements()
        results = [
            session.get_stations(
                num_departement=dep, format="geojson", **kwargs
            )
            for dep in tqdm(
                deps,
                desc="querying dep/dep",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = gpd.pd.concat(results, ignore_index=True)
    try:
        results["code_bss"]
        results = results.drop_duplicates("code_bss")
    except KeyError:
        pass
    return results


def get_analyses(bss_ids: list, **kwargs) -> pd.DataFrame:
    """
    Retrieve analyses from multiple piezometers.

    Use an inner loop for multiple piezometers to avoid reaching 20k results
    threshold from hub'eau API.

    Parameters
    ----------
    bss_ids : list
        List of bss_id codes for qualitometers
    **kwargs :
        kwargs passed to GroundWaterQualitySession.get_analyses (hence mostly
        intended for hub'eau API's arguments). Do not use `bss_id` as they
        are set by the current function.

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    with GroundWaterQualitySession() as session:
        results = [
            session.get_analyses(bss_id=code, **kwargs)
            for code in tqdm(
                bss_ids,
                desc="querying qualito/qualito",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


if __name__ == "__main__":
    gdf = get_all_stations()
    df = get_analyses(gdf["bss_id"].head(20))
