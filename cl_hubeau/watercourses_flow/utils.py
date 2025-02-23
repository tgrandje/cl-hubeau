import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from datetime import date

from cl_hubeau.watercourses_flow.watercourses_flow_scraper import (
    WatercoursesFlowSession,
)
from cl_hubeau import _config
from cl_hubeau.utils import get_departements, prepare_kwargs_loops


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WatercoursesFlowSession.get_stations (hence mostly
        intended for hub'eau API's arguments). Do not use `format` or
        `code_departement` as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of stations

    """

    with WatercoursesFlowSession() as session:

        deps = get_departements()
        results = [
            session.get_stations(
                code_departement=dep, format="geojson", **kwargs
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
        results["code_station"]
        results = results.drop_duplicates("code_station")
    except KeyError:
        pass
    return results


def get_all_observations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all observsations from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WatercoursesFlowSession.get_observations (hence mostly
        intended for hub'eau API's arguments). Do not use `format` or
        `code_departement` as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of observations
    """

    # Set a loop for yearly querying as dataset are big
    start_auto_determination = False
    if "date_observation_min" not in kwargs:
        start_auto_determination = True
        kwargs["date_observation_min"] = "1960-01-01"
    if "date_observation_max" not in kwargs:
        kwargs["date_observation_max"] = date.today().strftime("%Y-%m-%d")

    desc = "querying 4months/4months" + (
        " & dep/dep" if "code_departement" in kwargs else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_observation_min",
        "date_observation_max",
        kwargs,
        start_auto_determination,
    )

    with WatercoursesFlowSession() as session:

        results = [
            session.get_observations(
                format="geojson",
                **kwargs,
                **kw_loop,
            )
            for kw_loop in tqdm(
                kwargs_loop,
                desc=desc,
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    results = results.drop_duplicates()
    return results


def get_all_campaigns(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all campaigns from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WatercoursesFlowSession.get_campaigns (hence mostly
        intended for hub'eau API's arguments). Do not use `code_departement`
        as this is set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of campaigns
    """

    with WatercoursesFlowSession() as session:
        try:
            results = session.get_campaigns(**kwargs)
        except ValueError:
            # If request is too big
            deps = get_departements()
            results = [
                session.get_campaigns(code_departement=dep, **kwargs)
                for dep in tqdm(
                    deps,
                    desc="querying dep/dep",
                    leave=_config["TQDM_LEAVE"],
                    position=tqdm._get_free_pos(),
                )
            ]
            results = [
                x.dropna(axis=1, how="all") for x in results if not x.empty
            ]
            results = gpd.pd.concat(results, ignore_index=True)
        return results
