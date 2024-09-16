# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 10:57:00 2024

low level class to collect data from the watercourses-flow API from hub'eau
"""

import pandas as pd

from cl_hubeau.session import BaseHubeauSession


class WatercoursesFlowSession(BaseHubeauSession):
    """
    Base session class to handle the watercourses-flow API
    """

    def __init__(self, *args, **kwargs):

        super().__init__(version="1.0.0", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 1000

    def get_stations(self, **kwargs):
        """
        Lister les stations
        Endpoint /v1/ecoulement/stations

        Doc: https://hubeau.eaufrance.fr/page/api-ecoulement
        """

        params = {}

        try:
            variable = kwargs.pop("format")
            if variable not in ("json", "geojson"):
                raise ValueError(
                    "format must be among ('json', 'geojson'), "
                    f"found format='{variable}' instead"
                )
            params["format"] = variable
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(kwargs.pop("bbox"), None, 4)
        except KeyError:
            pass

        for arg in (
            "code_station",
            "libelle_station",
            "code_departement",
            "libelle_departement",
            "code_commune",
            "libelle_commune",
            "code_region",
            "libelle_region",
            "code_bassin",
            "libelle_bassin",
            "code_cours_eau",
            "libelle_cours_eau",
            "fields",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        for arg in ("distance", "latitude", "longitude"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["sort"] = kwargs.pop("sort")
        except KeyError:
            pass

        try:
            params["Accept"] = kwargs.pop("Accept")
        except KeyError:
            pass

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-ecoulement"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/ecoulement/stations"
        df = self.get_result(method, url, params=params)

        return df

    def get_observations(self, **kwargs):
        """
        Lister les observations
        Endpoint /v1/ecoulement/observations

        Doc: https://hubeau.eaufrance.fr/page/api-ecoulement
        """

        params = {}

        try:
            variable = kwargs.pop("format")
            if variable not in ("json", "geojson"):
                raise ValueError(
                    "format must be among ('json', 'geojson'), "
                    f"found format='{variable}' instead"
                )
            params["format"] = variable
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(kwargs.pop("bbox"), None, 4)
        except KeyError:
            pass

        for arg in "date_observation_min", "date_observation_max":
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_station",
            "libelle_station",
            "code_departement",
            "libelle_departement",
            "code_commune",
            "libelle_commune",
            "code_region",
            "libelle_region",
            "code_bassin",
            "libelle_bassin",
            "code_cours_eau",
            "libelle_cours_eau",
            "code_campagne",
            "code_reseau",
            "libelle_reseau",
            "code_ecoulement",
            "libelle_ecoulement",
            "fields",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        for arg in ("distance", "latitude", "longitude"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["sort"] = kwargs.pop("sort")
        except KeyError:
            pass

        try:
            params["Accept"] = kwargs.pop("Accept")
        except KeyError:
            pass

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-ecoulement"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/ecoulement/observations"
        df = self.get_result(method, url, params=params)

        return df

    def get_campagnes(self, **kwargs):
        """
        Lister les campagnes
        Endpoint /v1/ecoulement/campagnes

        Doc: https://hubeau.eaufrance.fr/page/api-ecoulement
        """

        params = {}

        for arg in "date_campagne_min", "date_campagne_max":
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_campagne",
            "code_type_campagne",
            "libelle_type_campagne",
            "code_reseau",
            "libelle_reseau",
            "code_departement",
            "libelle_departement",
            "fields",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        try:
            params["sort"] = kwargs.pop("sort")
        except KeyError:
            pass

        try:
            params["Accept"] = kwargs.pop("Accept")
        except KeyError:
            pass

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-ecoulement"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/ecoulement/campagnes"
        df = self.get_result(method, url, params=params)

        return df


# if __name__ == "__main__":
#     with WatercoursesFlowSession() as session:
#         df = session.get_stations(code_departement="59", format="geojson")
#         # df = session.get_campagnes(code_campagne=[12])

#         print(type(df))
