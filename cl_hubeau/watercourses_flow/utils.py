import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from datetime import date

from cl_hubeau.watercourses_flow.watercourses_flow_scraper import (
    WatercoursesFlowSession,
)
from cl_hubeau import _config
from cl_hubeau.utils import (
    get_departements,
    get_departements_from_regions,
    prepare_kwargs_loops,
)


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WatercoursesFlowSession.get_stations (hence mostly
        intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of stations

    """

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    elif any(x in kwargs for x in ("code_commune", "code_station")):
        deps = [""]
    else:
        deps = get_departements()

    with WatercoursesFlowSession() as session:

        kwargs["format"] = kwargs.get("format", "geojson")

        results = [
            session.get_stations(code_departement=dep, **kwargs)
            for dep in tqdm(
                deps,
                desc="querying dep/dep",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()
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
        intended for hub'eau API's arguments).

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

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    elif any(x in kwargs for x in ("code_commune", "code_station")):
        deps = [""]
    else:
        deps = get_departements()
    kwargs["code_departement"] = deps

    desc = "querying 6months/6months" + (" & dep/dep" if deps != [""] else "")

    kwargs_loop = prepare_kwargs_loops(
        "date_observation_min",
        "date_observation_max",
        kwargs,
        start_auto_determination,
    )

    kwargs["format"] = kwargs.get("format", "geojson")

    with WatercoursesFlowSession() as session:

        results = [
            session.get_observations(
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
    if not results:
        return pd.DataFrame()
    results = pd.concat(results, ignore_index=True)
    results = results.drop_duplicates()
    return results


def get_all_campaigns(**kwargs) -> pd.DataFrame:
    """
    Retrieve all campaigns from France.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use a code_region argument to query the results on a given region

    Parameters
    ----------
    **kwargs :
        kwargs passed to WatercoursesFlowSession.get_campaigns (hence mostly
        intended for hub'eau API's arguments).

    Returns
    -------
    results : pd.DataFrame
        DataFrame of campaigns
    """

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    else:
        deps = get_departements()
    kwargs["code_departement"] = deps

    with WatercoursesFlowSession() as session:
        results = [
            session.get_campaigns(**kwargs)
            for dep in tqdm(
                deps,
                desc="querying dep/dep",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
        results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
        if not results:
            return pd.DataFrame()
        results = gpd.pd.concat(results, ignore_index=True)
    return results
