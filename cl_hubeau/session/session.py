# -*- coding: utf-8 -*-

from copy import deepcopy
from datetime import datetime
import logging
import os

import geopandas as gpd
import pandas as pd
import pebble
from requests_cache import CachedSession
from tqdm import tqdm

from cl_hubeau.constants import DIR_CACHE, CACHE_NAME, DEFAULT_EXPIRE_AFTER


class BaseHubeauSession(CachedSession):

    THREADS = 1
    BASE_URL = "https://hubeau.eaufrance.fr/api"
    CACHE_NAME = os.path.join(DIR_CACHE, CACHE_NAME)
    SIZE = 1000

    def __init__(
        self,
        expire_after: int = DEFAULT_EXPIRE_AFTER,
        **kwargs,
    ):
        super().__init__(
            cache_name=self.CACHE_NAME, expire_after=expire_after, **kwargs
        )

    @staticmethod
    def list_to_str_param(x: list, max_autorized_values: int = None) -> str:
        """
        Join array of arguments to an accepted string format

        Parameters
        ----------
        x : list
            List of arguments
        max_autorized_values : int, optional
            Maximum authorized values in the list

        Returns
        -------
        str
            Concatenated arguments

        """
        if any(isinstance(x, y) for y in (list, tuple, set)):
            if max_autorized_values and len(x) > max_autorized_values:
                msg = (
                    f"Should not have more than {max_autorized_values}, "
                    f"found {len(x)} instead"
                )
                raise ValueError(msg)
            return ",".join(x)
        elif isinstance(x, str):
            return x
        else:
            ValueError(f"unexpected format for {x}")

    @staticmethod
    def ensure_date_format_is_ok(date_str: str) -> None:
        """
        Raise an exception if date is not in an accepted format

        Parameters
        ----------
        date_str : str
            Date argument (as a string)

        Raises
        ------
        ValueError
            In the date's format is forbidden

        Returns
        -------
        None

        """
        try:
            datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                "hubeau date should respect yyyy-MM-dd format"
            ) from exc

    def request(
        self,
        method: str,
        url: str,
        *args,
        **kwargs,
    ):
        r = super().request(
            method,
            url,
            *args,
            **kwargs,
        )
        if not r.ok:
            error = r.json()
            raise ValueError(
                f"Connection error on {method=} {url=} with {kwargs=}, "
                f"got {error}"
            )
        return r

    def get_result(self, method, url, params):

        copy_params = deepcopy(params)
        copy_params["size"] = 1
        copy_params["page"] = 1
        js = self.request(method=method, url=url, params=copy_params).json()

        logging.debug(js)

        count_rows = js["count"]
        logging.info(f"{count_rows} exepcted results")
        count_pages = count_rows // self.SIZE + (
            0 if count_rows % self.SIZE == 0 else 1
        )

        params["size"] = self.SIZE
        iterables = [deepcopy(params) for x in range(count_pages)]
        for x in range(count_pages):
            iterables[x].update({"page": x + 1})

        threads = min(self.THREADS, count_pages)

        def func(params):
            return self.request("GET", url=url, params=params)

        results = []
        key = (
            "data"
            if not ("format" in params and params["format"] == "geojson")
            else "features"
        )
        with tqdm(desc="querying", total=count_pages) as pbar:
            if threads > 1:
                with pebble.ThreadPool(threads) as pool:
                    future = pool.map(func, iterables)
                    iterator = future.result()
                    while True:
                        try:
                            result = next(iterator)
                            result = result.json()[key]
                            results += result
                        except StopIteration:
                            break

                        pbar.update()
            else:
                for x in iterables:
                    result = func(params=x)
                    result = result.json()[key]
                    results += result
                    pbar.update()

        if "format" in params and params["format"] == "geojson":
            results = gpd.GeoDataFrame.from_features(results, crs=4326)
        else:
            results = pd.DataFrame(results)
        if not len(results) == count_rows:
            msg = (
                "results do not match expected results - "
                f"expected {count_rows}, got {len(results)} instead"
            )
            raise ValueError(msg)
        return results
