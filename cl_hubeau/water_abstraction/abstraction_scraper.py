# -*- coding: utf-8 -*-
"""
Created on 2024-11-06

low level class to collect data from the water abstraction API from hub'eau
"""

import pandas as pd

from cl_hubeau.session import BaseHubeauSession


class AbstractionSession(BaseHubeauSession):
    """
    Base session class to handle the water abstraction API
    """

    def __init__(self, *args, **kwargs):

        super().__init__(version="v1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau's doc
        self.size = 1000

    def get_ouvrages(self, **kwargs):
        """
        Lister les ouvrages de prélèvement
        Endpoint /v1/prelevements/referentiel/ouvrages

        Ce service permet d'interroger les ouvrages de prélèvements en eau.
        Un ouvrage est composé de un ou plusieurs points de prélèvement proches et de mêmes types.
        Les données de volumes annuels prélevés sont rattachées aux ouvrages et non aux points de prélèvement

        Si la valeur du paramètre size n'est pas renseignée, la taille de page
        par défaut : 1000, taille max de la page : 10000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        Trie par défaut : annee asc

        Doc: https://hubeau.eaufrance.fr/page/api-prelevements-eau
        """

        params = {}
        for arg in ("date_exploitation",):
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
                    f"found format='{variable}' instead"
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

        for arg in (
            "code_bdlisa",
            "code_commune_insee",
            "code_departement",
            "code_entite_hydro_cours_eau",
            "code_entite_hydro_plan_eau",
            "code_mer_ocean",
            "code_ouvrage",
            "code_type_milieu",
            "codes_points_prelevements",
            "fields",
            "libelle_departement",
            "nom_commune",
            "nom_ouvrage",
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
            variable = kwargs.pop("sort")
            if variable not in ("asc", "desc"):
                raise ValueError(
                    "sort must be among ('asc', 'desc'), "
                    f"found sort='{variable}' instead"
                )
            params["sort"] = variable
        except KeyError:
            pass

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-prelevements-eau"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/prelevements/referentiel/ouvrages"
        df = self.get_result(method, url, params=params)

        return df

    def get_points_prelevement(self, **kwargs):
        """
        Lister les points de prélèvement
        Endpoint /v1/prelevements/referentiel/points_prelevement

        Ce service permet d'interroger les points de prélèvements en eau.
        Un point de prélèvement fait partie d'un ouvrage.
        Un point de prélèvement peut constituer un ouvrage à lui seul, ou bien être accompagné d'autres points de prélèvement.

        Si la valeur du paramètre size n'est pas renseignée, la taille de page
        par défaut : 1000, taille max de la page : 10000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        Trie par défaut : code_point_prelevement asc

        Doc: https://hubeau.eaufrance.fr/page/api-prelevements-eau
        """

        params = {}
        for arg in ("date_exploitation", ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_bdlisa",
            "code_bss_point_eau",
            "code_commune_insee",
            "code_departement",
            "code_entite_hydro_cours_eau",
            "code_entite_hydro_plan_eau",
            "code_mer_ocean",
            "code_nature",
            "code_ouvrage",
            "code_point_prelevement",
            "code_type_milieu",
            "code_zone_hydro",
            "fields",
            "libelle_departement",
            "nappe_accompagnement",
            "nom_commune",
            "nom_point_prelevement",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
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

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-hydrometrie"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/prelevements/referentiel/points_prelevement"
        df = self.get_result(method, url, params=params)

        return df

    def get_chroniques(self, **kwargs):
        """
        Lister les chroniques de volumes annuels
        Endpoint /v1/prelevements/chroniques

        Ce service permet d’interroger les chroniques de volumes annuels d’eau prélevés par ouvrages.

        Doc: https://hubeau.eaufrance.fr/page/api-prelevements-eau
        """

        params = {}

        for arg in ("annee",
                    "code_commune_insee",
                    "code_departement",
                    "code_mode_obtention_volume",
                    "code_ouvrage",
                    "code_qualification_volume",
                    "code_statut_instruction",
                    "code_statut_volume",
                    "code_usage",
                    "fields",
                    "libelle_departement",
                    "producteur_donnee",
                    ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in (
            "distance",
            "latitude",
            "longitude",
            "prelevement_ecrasant",
            "volume_max",
            "volume_min",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

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
            variable = kwargs.pop("sort")
            if variable not in ("asc", "desc"):
                raise ValueError(
                    "sort must be among ('asc', 'desc'), "
                    f"found sort='{variable}' instead"
                )
            params["sort"] = variable
        except KeyError:
            pass

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-prelevements-eau"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/prelevements/chroniques"

        df = self.get_result(method, url, params=params)

        return df

# if __name__ == "__main__":
#     import logging

#     logging.basicConfig(level=logging.WARNING)
#     with AbstractionSession() as session:
#         gdf = session.get_ouvrages(code_departement="31", format="geojson")
#         df = session.get_points_prelevement(nom_commune="Custines")
#         pass

#         df = session.get_chroniques(
#             code_ouvrage="OPR0000000076",
#             code_qualification_volume="1",
#             fields='code_ouvrage,annee,volume'
#         )
#         df = df.pivot_table(
#             index="annee", columns="code_ouvrage", values="volume"
#         ).plot()
#     import matplotlib.pyplot as plt
#     plt.show()
#     pass
