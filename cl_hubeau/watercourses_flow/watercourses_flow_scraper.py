#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
low level class to collect data from the watercourses-flow API from hub'eau
"""

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class WatercoursesFlowSession(BaseHubeauSession):
    """
    Base session class to handle the watercourses-flow API
    """

    DOC_URL = "https://hubeau.eaufrance.fr/page/api-ecoulement"

    def __init__(self, *args, **kwargs):

        super().__init__(version="1.1.0", *args, **kwargs)

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
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
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
            "code_cours_eau",
            "libelle_cours_eau",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_bassin",
            "libelle_bassin",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 15)
            except KeyError:
                continue

        try:
            fields = kwargs.pop("fields")
            params["fields"] = self.list_to_str_param(fields)
        except KeyError:
            pass

        for arg in ("distance", "latitude", "longitude"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

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
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
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
            "code_cours_eau",
            "libelle_cours_eau",
            "code_campagne",
            "code_reseau",
            "libelle_reseau",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_bassin",
            "libelle_bassin",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 15)
            except KeyError:
                continue

        for arg in (
            "code_ecoulement",
            "libelle_ecoulement",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 5)
            except KeyError:
                continue

        try:
            fields = kwargs.pop("fields")
            params["fields"] = self.list_to_str_param(fields)
        except KeyError:
            pass

        for arg in ("distance", "latitude", "longitude"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/ecoulement/observations"
        df = self.get_result(
            method,
            url,
            time_start="date_observation_min",
            time_end="date_observation_max",
            params=params,
        )

        return df

    def get_campaigns(self, **kwargs):
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

        try:
            code_campagne = kwargs.pop("code_campagne")
            params["code_campagne"] = self.list_to_str_param(code_campagne, 20)
        except KeyError:
            pass

        for arg in (
            "code_reseau",
            "libelle_reseau",
            "code_departement",
            "libelle_departement",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        try:
            params["code_type_campagne"] = (
                self._ensure_val_among_authorized_values(
                    "code_type_campagne", kwargs, {"1", "2"}, str
                )
            )
        except KeyError:
            pass

        try:
            params["libelle_type_campagne"] = (
                self._ensure_val_among_authorized_values(
                    "libelle_type_campagne",
                    kwargs,
                    {"Usuelle", "Complémentaire"},
                    lambda x: x.capitalize(),
                )
            )
        except KeyError:
            pass

        try:
            fields = kwargs.pop("fields")
            params["fields"] = self.list_to_str_param(fields)
        except KeyError:
            pass

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/ecoulement/campagnes"
        df = self.get_result(
            method,
            url,
            time_start="date_campagne_min",
            time_end="date_campagne_max",
            params=params,
        )

        return df


# if __name__ == "__main__":
#     with WatercoursesFlowSession() as session:
#         # df = session.get_stations(code_departement="59", format="geojson")
#         # df = session.get_campaigns(code_campagne=[12])
#         df = session.get_observations(code_station="F6640008")

#         print(df)
