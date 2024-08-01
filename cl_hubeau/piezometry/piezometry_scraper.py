# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 14:03:41 2024

low level class to collect data from the piezometry API from hub'eau
"""

import logging

import pandas as pd

from cl_hubeau.session import BaseHubeauSession


class PiezometrySession(BaseHubeauSession):
    """
    Base session class to handle the piezometry API
    """

    def __init__(self, *args, **kwargs):
        super().__init__(version="1.4.1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 5000

    def get_stations(self, **kwargs):
        """
        lister les stations de mesure
        Endpoint /v1/niveaux_nappes/stations

        Ce service permet de rechercher les stations de mesure des niveaux des
        nappes d'eau (stations piézométriques).
        Source de données : banque nationale d'Accès aux Données sur les Eaux
        Souterraines (ADES) http://www.ades.eaufrance.fr/"
        La taille de page par défaut : 5000, taille max de la page : 20000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.

        Doc: https://hubeau.eaufrance.fr/page/api-piezometrie
        """

        params = {}

        for arg in "date_recherche":
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue
        try:
            variable = kwargs.pop("format")
            if variable not in ("json", "geojson"):
                raise ValueError(
                    "format must be among ('json', 'geojson'), "
                    f"found {format=} instead"
                )
            params["format"] = variable
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in ("nb_mesures_piezo_min", "srid"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        for arg in (
            "bss_id",
            "code_bdlisa",
            "code_bss",
            "code_commune",
            "codes_masse_eau_edl",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        try:
            variable = kwargs.pop("code_departement")
            params["code_departement"] = self.list_to_str_param(variable, 200)
        except KeyError:
            pass

        method = "GET"
        url = self.BASE_URL + "/v1/niveaux_nappes/stations"

        df = self.get_result(method, url, params=params)

        return df

    def get_chronicles(self, **kwargs):
        """
        Lister les chroniques piézométriques
        Endpoint /v1/niveaux_nappes/chroniques

        Ce service permet de lister les niveaux des nappes d'eau (chroniques
        piézométriques) d'une station de mesure des eaux souterraines.
        Source de données : banque nationale d'Accès aux Données sur les Eaux
        Souterraines (ADES) http://www.ades.eaufrance.fr/"

        La taille de page par défaut : 5000, taille max de la page : 20000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        """

        params = {}

        try:
            params["code_bss"] = self.list_to_str_param(
                kwargs.pop("code_bss"), 200
            )
        except KeyError:
            # reset to default hubeau value, which is set even when code_bss is
            # missing
            code_bss = "07548X0009/F"
            msg = f"code_bss is missing, will be set to {code_bss=} by hubeau"
            logging.warning(msg)
            params["code_bss"] = code_bss

        for arg in "date_debut_mesure", "date_fin_mesure":
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        try:
            variable = kwargs.pop("sort")
            if variable not in ("asc", "desc"):
                raise ValueError(
                    "sort must be among ('asc', 'desc'), "
                    f"found sort='{variable}' instead"
                )
            params["sort"] = variable
        except KeyError:
            pass

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        method = "GET"
        url = self.BASE_URL + "/v1/niveaux_nappes/chroniques"

        df = self.get_result(method, url, params=params)

        try:
            df["timestamp_mesure"] = pd.to_datetime(
                df["timestamp_mesure"] * 1e6
            )
        except KeyError:
            pass

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-piezometrie"
            )

        return df

    def get_realtime_chronicles(self, **kwargs):
        """
        Lister les chroniques piézométriques en temps réel
        Endpoint /v1/niveaux_nappes/chroniques_tr

        Ce service permet de lister les niveaux des nappes d'eau (chroniques
        piézométriques) d'une station de mesure des eaux souterraines en temps
        réél.
        Ce service diffuse des données brutes. Les données corrigées sont
        diffusées par l’API « chroniques »
        Source de données : BRGM http://www.brgm.fr/"
        La taille de page par défaut : 5000, taille max de la page : 20000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.

        Doc: https://hubeau.eaufrance.fr/page/api-piezometrie
        """
        params = {}

        try:
            params["code_bss"] = self.list_to_str_param(
                kwargs.pop("code_bss"), 200
            )
        except KeyError:
            # reset to default hubeau value, which is set even when code_bss is
            # missing
            code_bss = "07548X0009/F"
            msg = f"code_bss is missing, will be set to {code_bss=} by hubeau"
            logging.warning(msg)
            params["code_bss"] = code_bss

        try:
            params["bss_id"] = self.list_to_str_param(
                kwargs.pop("bss_id"), 200
            )
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in "date_debut_mesure", "date_fin_mesure":
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "niveau_ngf_max",
            "niveau_ngf_min",
            "profondeur_max",
            "profondeur_min",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            variable = kwargs.pop("sort")
            if variable not in ("asc", "desc"):
                raise ValueError(
                    "sort must be among ('asc', 'desc'), "
                    f"found sort='{variable}' instead"
                )
            params["sort"] = variable
        except KeyError:
            pass

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        method = "GET"
        url = self.BASE_URL + "/v1/niveaux_nappes/chroniques_tr"

        df = self.get_result(
            method,
            url,
            params=params,
        )

        try:
            df["timestamp_mesure"] = pd.to_datetime(
                df["timestamp_mesure"] * 1e6
            )
        except KeyError:
            pass

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-piezometrie"
            )

        return df


# if __name__ == "__main__":
#     import logging

#     logging.basicConfig(level=logging.WARNING)
#     with PiezometrySession() as session:
#         df = session.get_chronicles(code_bss="07548X0009/F")
#         # df = session.get_realtime_chronicles()
