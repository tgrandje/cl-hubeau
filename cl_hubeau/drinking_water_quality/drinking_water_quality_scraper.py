# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 14:03:41 2024

low level class to collect data from the drinking water quality API from
hub'eau
"""
import pandas as pd
from cl_hubeau.session import BaseHubeauSession


class DrinkingWaterQualitySession(BaseHubeauSession):
    """
    Base session class to handle the drinking water quality API
    """

    def __init__(self, *args, **kwargs):

        super().__init__(version="v1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 5000

    def get_cities_networks(self, **kwargs):
        """
        Liens entre UDI (Unités de distribution, ou réseaux) et communes.
        Endpoint /v1/qualite_eau_potable/communes_udi

        Liens entre UDI (Unités de distribution, ou réseaux) et communes.

        Doc: https://hubeau.eaufrance.fr/page/api-qualite-eau-potable
        """

        params = {}

        try:
            variable = kwargs.pop("sort")
            if variable not in ("asc", "desc"):
                raise ValueError(
                    "format must be among ('asc', 'sort'), "
                    f"found sort='{variable}' instead"
                )
            params["sort"] = variable
        except KeyError:
            params["sort"] = "asc"

        try:
            years = kwargs.pop("annee")
            if any(isinstance(years, x) for x in (list, tuple, set)):
                years = [str(x) for x in years]
            else:
                years = [str(years)]
            params["annee"] = self.list_to_str_param(years, 10)
        except KeyError:
            pass

        for arg in (
            "code_commune",
            "code_reseau",
            "fields",
            "nom_commune",
            "nom_reseau",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-qualite-eau-potable"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/qualite_eau_potable/communes_udi"
        df = self.get_result(method, url, params=params)

        return df

    def get_control_results(self, **kwargs):
        """
        Lister les analyses d'une ou plusieurs UDI
        Endpoint /v1/qualite_eau_potable/resultats_dis


        Prélèvements, résultats d'analyses et conclusions sanitaires issus du
        contrôle sanitaire de l'eau distribuée commune par commune.

        Doc: https://hubeau.eaufrance.fr/page/api-qualite-eau-potable
        """

        params = {}

        try:
            variable = kwargs.pop("sort")
            if variable not in ("asc", "desc"):
                raise ValueError(
                    "format must be among ('asc', 'sort'), "
                    f"found sort='{variable}' instead"
                )
            params["sort"] = variable
        except KeyError:
            params["sort"] = "asc"

        for arg in ("borne_inf_resultat", "borne_sup_resultat"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        for arg in "date_max_prelevement", "date_min_prelevement":
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_commune",
            "code_departement",
            "code_parametre",
            "code_parametre_cas",
            "code_parametre_se",
            "code_reseau",
            "nom_commune",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        for arg in (
            "code_lieu_analyse",
            "code_prelevement",
            "libelle_parametre",
            "libelle_parametre_maj",
            "nom_distributeur",
            "nom_moa",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        for arg in (
            "conformite_limites_bact_prelevement",
            "conformite_limites_pc_prelevement",
            "conformite_references_bact_prelevement",
            "conformite_references_pc_prelevement",
        ):
            try:
                variable = kwargs.pop(arg)
                if variable not in ("C", "D, S"):
                    raise ValueError(
                        f"{arg} must be among ('C', 'D', 'S'), "
                        f"found {arg}='{variable}' instead"
                    )
                params[arg] = variable
            except KeyError:
                continue

        if kwargs:
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-qualite-eau-potable"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/qualite_eau_potable/resultats_dis"
        df = self.get_result(method, url, params=params)

        try:
            df["date_prelevement"] = pd.to_datetime(df["date_prelevement"])
        except KeyError:
            pass

        return df


# if __name__ == "__main__":
#     import logging

#     # logging.basicConfig(level=logging.WARNING)
#     with HydrometrySession() as session:
#         gdf = session.get_sites(code_departement="02", format="geojson")
#         # df = session.get_observations(code_entite="K437311001")

#         df = session.get_realtime_observations(
#             code_entite="K437311001",
#             grandeur_hydro="Q",
#             # date_debut_obs="2010-01-01",
#         )
#         df.pivot_table(
#             index="date_obs", columns="grandeur_hydro", values="resultat_obs"
#         ).plot()
