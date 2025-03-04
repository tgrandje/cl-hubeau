#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for piezometry consumption
"""

from typing import Union

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.hydrobiology.hydrobiology_scraper import HydrobiologySession
from cl_hubeau import _config
from cl_hubeau.utils import (
    get_departements,
    get_regions,
    get_departements_from_regions,
)

# get_indexes
# get_taxa


def get_all_stations(**kwargs) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Retrieve all stations from France.

    If no location is specified (neither geographic nor hydrographic locator)
    a loop will be introduced on each region.

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrobiologySession.get_stations (intended for hub'eau
        API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of stations. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    areas = {
        "code_region",
        "code_departement",
        "code_commune",
        "code_bassin",
        "code_sous_bassin",
        "code_masse_eau",
        "code_cours_eau",
        "code_station_hydrobio",
    }
    if not any(kwargs.get(x) for x in areas):
        # no specific location -> let's loop over regions to avoid reaching
        # the 20k threshold
        code_region = get_regions()
    else:
        code_region = kwargs.pop("code_region")
        if isinstance(code_region, str):
            code_region = code_region.split(",")

    format_ = kwargs.get("format", "geojson")

    with HydrobiologySession() as session:
        if code_region:
            results = [
                session.get_stations(code_region=reg, format=format_, **kwargs)
                for reg in tqdm(
                    code_region,
                    desc="querying reg/reg",
                    leave=_config["TQDM_LEAVE"],
                    position=tqdm._get_free_pos(),
                )
            ]
        else:
            results = session.get_stations(format=format_, **kwargs)

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = gpd.pd.concat(results, ignore_index=True)
    try:
        results["code_bss"]
        results = results.drop_duplicates("code_station_hydrobio")
    except KeyError:
        pass
    # TODO : check that you get 20560 results (be wary of side effects about
    # stations on regional borders)
    return results


def get_all_indexes(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all indexes from France.

    If no location is specified (neither geographic nor hydrographic locator)
    a loop will be introduced on each departement.

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrobiologySession.get_indexes (intended for hub'eau
        API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of stations. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    areas = {
        "code_region",
        "code_departement",
        "code_commune",
        "code_bassin",
        "code_sous_bassin",
        "code_masse_eau",
        "code_cours_eau",
        "code_station_hydrobio",
    }
    if not any(kwargs.get(x) for x in areas):
        # no specific location -> let's loop over departements to avoid
        # reaching the 20k threshold
        code_departement = get_departements()
    elif kwargs.get("code_region"):
        code_departement = get_departements_from_regions(
            kwargs.pop("code_region")
        )
        code_departement  # TODO !
    elif kwargs.get(""):
        # TODO : query on FRG_ALA gets 410519 results!
        pass
    else:
        code_region = kwargs.pop("code_region")
        if isinstance(code_region, str):
            code_region = code_region.split(",")

    format_ = kwargs.get("format", "geojson")

    with HydrobiologySession() as session:
        if code_region:
            results = [
                session.get_stations(code_region=reg, format=format_, **kwargs)
                for reg in tqdm(
                    code_region,
                    desc="querying reg/reg",
                    leave=_config["TQDM_LEAVE"],
                    position=tqdm._get_free_pos(),
                )
            ]
        else:
            results = session.get_stations(format=format_, **kwargs)

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = gpd.pd.concat(results, ignore_index=True)
    try:
        results["code_bss"]
        results = results.drop_duplicates("code_station_hydrobio")
    except KeyError:
        pass
    # TODO : check that you get 20560 results (be wary of side effects about
    # stations on regional borders)
    return results


def get_all_taxa(**kwargs) -> gpd.GeoDataFrame:
    # TODO
    pass


# def get_chronicles(codes_bss: list, **kwargs) -> pd.DataFrame:
#     """
#     Retrieve chronicles from multiple piezometers.

#     Use an inner loop for multiple piezometers to avoid reaching 20k results
#     threshold from hub'eau API.

#     Parameters
#     ----------
#     codes_bss : list
#         List of code_bss codes for piezometers
#     **kwargs :
#         kwargs passed to PiezometrySession.get_chronicles (hence mostly
#         intended for hub'eau API's arguments). Do not use `code_bss` as they
#         are set by the current function.

#     Returns
#     -------
#     results : pd.dataFrame
#         DataFrame of results

#     """

#     with PiezometrySession() as session:
#         results = [
#             session.get_chronicles(code_bss=code, **kwargs)
#             for code in tqdm(
#                 codes_bss,
#                 desc="querying piezo/piezo",
#                 leave=_config["TQDM_LEAVE"],
#                 position=tqdm._get_free_pos(),
#             )
#         ]
#     results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
#     results = pd.concat(results, ignore_index=True)
#     return results


# # def get_realtime_chronicles(
# #     codes_bss: list = None, bss_ids: list = None, **kwargs
# # ) -> pd.DataFrame:
# #     """
# #     Retrieve realtimes chronicles from multiple piezometers.
# #     Uses a reduced timeout for cache expiration.

# #     Note that `codes_bss` and `bss_ids` are mutually exclusive!

# #     Parameters
# #     ----------
# #     codes_bss : list, optional
# #         List of code_bss codes for piezometers. The default is None.
# #     bss_ids : list, optional
# #         List of bss_id codes for piezometers. The default is None.
# #     **kwargs :
# #         kwargs passed to PiezometrySession.get_realtime_chronicles (hence
# #         mostly intended for hub'eau API's arguments). Do not use `code_bss` as
# #         they are set by the current function.

# #     Returns
# #     -------
# #     results : pd.dataFrame
# #         DataFrame of results

# #     """

# #     if codes_bss and bss_ids:
# #         raise ValueError(
# #             "only one argument allowed among codes_bss and bss_ids"
# #         )
# #     if not codes_bss and not bss_ids:
# #         raise ValueError(
# #             "exactly one argument must be set among codes_bss and bss_ids"
# #         )

# #     code_names = "code_bss" if codes_bss else "bss_id"
# #     codes = codes_bss if codes_bss else bss_ids

# #     with PiezometrySession(
# #         expire_after=_config["DEFAULT_EXPIRE_AFTER_REALTIME"]
# #     ) as session:
# #         results = [
# #             session.get_realtime_chronicles(**{code_names: code}, **kwargs)
# #             for code in tqdm(
# #                 codes,
# #                 desc="querying piezo/piezo",
# #                 leave=_config["TQDM_LEAVE"],
# #                 position=tqdm._get_free_pos(),
# #             )
# #         ]
# #     results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
# #     results = pd.concat(results, ignore_index=True)
# #     return results


if __name__ == "__main__":
    df = get_all_stations()
