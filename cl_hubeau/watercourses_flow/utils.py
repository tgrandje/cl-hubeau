import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.watercourses_flow.watercourses_flow_scraper import (
    WatercoursesFlowSession,
)
from cl_hubeau import _config
from cl_hubeau.utils import get_departements


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WatercoursesFlowSession.get_stations (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_departement`
        as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of stations

    """

    with WatercoursesFlowSession() as session:

        deps = get_departements()
        results = [
            session.get_stations(code_departement=dep, format="geojson", **kwargs)
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


# if __name__ == "__main__":
#     print(get_all_stations())
