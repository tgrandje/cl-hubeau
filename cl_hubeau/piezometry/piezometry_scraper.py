# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 14:03:41 2024

low level class to collect data from the piezometry API from hub'eau
"""

import logging
from typing import List

import pandas as pd

from cl_hubeau.session import BaseHubeauSession


class PiezometrySession(BaseHubeauSession):

    def get_stations(
        self,
        bbox: List[float] = None,
        bss_id: List[str] = None,
        code_bdlisa: List[str] = None,
        code_bss: List[str] = None,
        code_commune: List[str] = None,
        code_departement: List[str] = None,
        codes_masse_eau_edl: List[str] = None,
        date_recherche: str = None,
        fields: List[str] = None,
        format: str = "json",
        nb_mesures_piezo_min: int = None,
        srid: int = None,
    ):
        """
        Endpoint /v1/niveaux_nappes/stations
        """

        params = {}
        if date_recherche:
            self.ensure_date_format_is_ok(date_recherche)
            params["date_recherche"] = date_recherche
        if format and format not in ("json", "geojson"):
            raise ValueError(
                "format must be amont ('json', 'geojson'), "
                f"found {format=} instead"
            )
        if format:
            params["format"] = format

        if nb_mesures_piezo_min:
            params["nb_mesures_piezo_min"] = nb_mesures_piezo_min
        if srid:
            params["srid"] = srid

        if bbox:
            params["bbox"] = self.list_to_str_param(bbox, 4)
        if bss_id:
            params["bss_id"] = self.list_to_str_param(bss_id, 200)
        if code_bdlisa:
            params["code_bdlisa"] = self.list_to_str_param(code_bdlisa, 200)
        if code_bss:
            params["code_bss"] = self.list_to_str_param(code_bss, 200)
        if code_commune:
            params["code_commune"] = self.list_to_str_param(code_commune, 200)
        if code_departement:
            params["code_departement"] = self.list_to_str_param(
                code_departement, 100
            )
        if codes_masse_eau_edl:
            params["codes_masse_eau_edl"] = self.list_to_str_param(
                codes_masse_eau_edl, 200
            )
        if fields:
            params["fields"] = self.list_to_str_param(fields)

        method = "GET"
        url = self.BASE_URL + "/v1/niveaux_nappes/stations"

        df = self.get_result(method, url, params=params)
        try:
            df = df.drop_duplicates("bss_id")
        except KeyError:
            pass

        return df

    def get_chronicles(
        self,
        code_bss: List[str] = "07548X0009/F",
        date_debut_mesure: str = None,
        date_fin_mesure: str = None,
        fields: str = None,
        sort: str = None,
    ):
        """
        Endpoint /v1/niveaux_nappes/chroniques
        """
        if not code_bss:
            # reset to default hubeau value, which is set even when code_bss is
            # missing
            code_bss = "07548X0009/F"
            msg = f"code_bss is missing, will be set to {code_bss=} by hubeau"
            logging.warning(msg)
        else:
            code_bss = self.list_to_str_param(code_bss)
        params = {"code_bss": code_bss}

        if date_debut_mesure:
            self.ensure_date_format_is_ok(date_debut_mesure)
            params["date_debut_mesure"] = date_debut_mesure
        if date_fin_mesure:
            self.ensure_date_format_is_ok(date_fin_mesure)
            params["date_fin_mesure"] = date_fin_mesure
        if sort and sort not in ("asc", "desc"):
            raise ValueError(
                f"sort must be amont ('asc', 'desc'), found {sort=} instead"
            )
        if sort:
            params["sort"] = sort
        if fields:
            params["fields"] = self.list_to_str_param(fields)
        method = "GET"
        url = self.BASE_URL + "/v1/niveaux_nappes/chroniques"

        df = self.get_result(method, url, params=params)
        df = df.drop_duplicates(keep="first")

        try:
            df["timestamp_mesure"] = pd.to_datetime(
                df["timestamp_mesure"] * 1e6
            )
        except KeyError:
            pass
        return df

    def get_chronicles_real_time(
        self,
        bbox: List[float] = None,
        bss_id: List[str] = None,
        code_bss: List[str] = "07548X0009/F",
        date_debut_mesure: str = None,
        date_fin_mesure: str = None,
        fields: str = None,
        niveau_ngf_max: float = None,
        niveau_ngf_min: float = None,
        profondeur_max: float = None,
        profondeur_min: float = None,
        sort: str = None,
    ):
        """
        Endpoint /v1/niveaux_nappes/chroniques_tr
        """
        params = {}
        if bbox:
            params["bbox"] = self.list_to_str_param(bbox, 4)
        if bss_id:
            params["bss_id"] = self.list_to_str_param(bss_id, 200)
        if not code_bss:
            # reset to default hubeau value, which is set even when code_bss is
            # missing
            code_bss = "07548X0009/F"
            msg = f"code_bss is missing, will be set to {code_bss=} by hubeau"
            logging.warning(msg)
        else:
            code_bss = self.list_to_str_param(code_bss)
        params["code_bss"] = code_bss

        if date_debut_mesure:
            self.ensure_date_format_is_ok(date_debut_mesure)
            params["date_debut_mesure"] = date_debut_mesure
        if date_fin_mesure:
            self.ensure_date_format_is_ok(date_fin_mesure)
            params["date_fin_mesure"] = date_fin_mesure
        if sort and sort not in ("asc", "desc"):
            raise ValueError(
                f"sort must be amont ('asc', 'desc'), found {sort=} instead"
            )
        if sort:
            params["sort"] = sort
        if fields:
            params["fields"] = self.list_to_str_param(fields)
        if niveau_ngf_max:
            params["niveau_ngf_max"] = niveau_ngf_max
        if niveau_ngf_min:
            params["niveau_ngf_min"] = niveau_ngf_min
        if profondeur_max:
            params["profondeur_max"] = profondeur_max
        if profondeur_min:
            params["profondeur_min"] = profondeur_min
        if sort and sort not in ("asc", "desc"):
            raise ValueError(
                f"sort must be amont ('asc', 'desc'), found {sort=} instead"
            )
        if sort:
            params["sort"] = sort

        method = "GET"
        url = self.BASE_URL + "/v1/niveaux_nappes/chroniques_tr"

        df = self.get_result(method, url, params=params, force_refresh=True)
        df = df.drop_duplicates(keep="first")

        try:
            df["timestamp_mesure"] = pd.to_datetime(
                df["timestamp_mesure"] * 1e6
            )
        except KeyError:
            pass
        return df


# if __name__ == "__main__":
#     import logging

#     logging.basicConfig(level=logging.WARNING)
#     with PiezometrySession() as session:
#         # df = session.get_chronicles(code_bss="07548X0009/F")
#         df = session.get_chronicles_real_time()
