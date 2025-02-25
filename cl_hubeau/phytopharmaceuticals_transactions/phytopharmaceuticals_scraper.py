# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 14:03:41 2024

low level class to collect data from the phytopharmaceuticals' transactions API
from hub'eau
"""

import pandas as pd

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class PhytopharmaceuticalsSession(BaseHubeauSession):
    """
    Base session class to handle the Phyto API
    """

    DOC_URL = "https://hubeau.eaufrance.fr/page/api-vente-achat-phytos"

    def __init__(self, *args, **kwargs):

        super().__init__(version="v1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 1000

    def active_substances_sold(self, **kwargs):
        """
        Lister les ventes de substances actives
        Endpoint /v1/vente_achat_phyto/ventes/substances

        Ventes de substances actives. Les données comprennent l'année, le lieu,
        le n° d'AMM, le libellé, le code CAS de la substance, la quantité
        vendue, …

        Doc: https://hubeau.eaufrance.fr/page/api-vente-achat-phytos
        """

        params = {}

        try:
            field = "type_territoire"
            allowed = {"Département", "Région", "National"}
            params[field] = self._ensure_val_among_authorized_values(
                field, kwargs, allowed, lambda x: x.capitalize()
            )
        except KeyError:
            pass

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            params["sort"] = "desc"

        try:
            variable = kwargs.pop("classification_mention")
            params["classification_mention"] = self.list_to_str_param(
                variable, 2
            )
        except KeyError:
            pass

        try:
            variable = kwargs.pop("classification")
            params["classification"] = self.list_to_str_param(variable, 9)
        except KeyError:
            pass

        for arg in [
            "libelle_substance",
            "code_cas",
            "code_substance",
            "fonction",
            "code_territoire",
            "libelle_territoire",
            "amm",
        ]:
            try:
                params[arg] = self.list_to_str_param(kwargs.pop(arg), 200)
            except KeyError:
                pass

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        for arg in (
            "annee_min",
            "annee_max",
            "quantite_min",
            "quantite_max",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/vente_achat_phyto/ventes/substances"

        df = self.get_result(method, url, params=params)

        return df

    def phytopharmaceutical_products_sold(self, **kwargs):
        "https://hubeau.eaufrance.fr/api/v1/vente_achat_phyto/ventes/produits"
        """
        Lister les ventes de substances actives
        Endpoint /v1/vente_achat_phyto/ventes/produits

        Ventes de produits phytopharmaceutiques. Les données comprennent
        l'année, le lieu, le n° AMM, la quantité de produit vendue, …

        Doc: https://hubeau.eaufrance.fr/page/api-vente-achat-phytos
        """

        params = {}

        try:
            field = "type_territoire"
            allowed = {"Département", "Région", "National"}
            params[field] = self._ensure_val_among_authorized_values(
                field, kwargs, allowed, lambda x: x.capitalize()
            )
        except KeyError:
            pass

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            params["sort"] = "desc"

        try:
            variable = kwargs.pop("classification_mention")
            params["classification_mention"] = self.list_to_str_param(
                variable, 2
            )
        except KeyError:
            pass

        try:
            params["eaj"] = self._ensure_val_among_authorized_values(
                "eaj", kwargs, {"Oui", "Non"}, lambda x: x.capitalize()
            )
            # Note: do not consider eaj = ["Oui", "Non"], even if this is
            # authorized by the API as the result is the same as not setting
            # the parameter
        except KeyError:
            pass

        try:
            params["unite"] = self._ensure_val_among_authorized_values(
                "unite", kwargs, {"l", "kg"}, lambda x: x.lower()
            )
            # Note: do not consider unite = ["l", "kg"], even if this is
            # authorized by the API as the result is the same as not setting
            # the parameter
        except KeyError:
            pass

        except KeyError:
            pass

        for arg in [
            "code_territoire",
            "libelle_territoire",
            "amm",
        ]:
            try:
                params[arg] = self.list_to_str_param(kwargs.pop(arg), 200)
            except KeyError:
                pass

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        for arg in (
            "annee_min",
            "annee_max",
            "quantite_min",
            "quantite_max",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/vente_achat_phyto/ventes/produits"

        df = self.get_result(method, url, params=params)

        return df

    def active_substances_bought(self, **kwargs):
        "https://hubeau.eaufrance.fr/api/v1/vente_achat_phyto/achats/substance"
        pass

    def phytopharmaceutical_products_bought(self, **kwargs):
        "https://hubeau.eaufrance.fr/api/v1/vente_achat_phyto/achats/produits"
        pass


if __name__ == "__main__":
    import logging
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_style("darkgrid")

    # logging.basicConfig(level=logging.WARNING)
    with PhytopharmaceuticalsSession() as session:

        # =====================================================================
        #         Exemple 1: sold substances
        # =====================================================================
        # df = pd.concat(
        #     [
        #         session.active_substances_sold(
        #             annee_min=2010,
        #             annee_max=2015,
        #             code_territoire=["32"],
        #             type_territoire="Région",
        #         ),
        #         session.active_substances_sold(
        #             annee_min=2016,
        #             annee_max=2020,
        #             code_territoire=["32"],
        #             type_territoire="Région",
        #         ),
        #         session.active_substances_sold(
        #             annee_min=2021,
        #             code_territoire=["32"],
        #             type_territoire="Région",
        #         ),
        #     ]
        # )

        # ax = (
        #     df.pivot_table(
        #         "quantite",
        #         index=["libelle_territoire", "fonction"],
        #         columns=["annee"],
        #     )
        #     .loc["HAUTS-DE-FRANCE"]
        #     .T.plot(kind="bar", stacked=True)
        # )
        # handles, labels = ax.get_legend_handles_labels()
        # lgd = ax.legend(
        #     handles, labels, loc="upper center", bbox_to_anchor=(0.5, -0.2)
        # )
        # plt.show()
        #
        # =====================================================================
        #         Exemple 2: sold products
        # =====================================================================
        # df = pd.concat(
        #     [
        #         session.phytopharmaceutical_products_sold(
        #             annee_min=2010,
        #             annee_max=2015,
        #             code_territoire=["32"],
        #             type_territoire="Région",
        #             eaj="Oui",
        #             unite="l",
        #         ),
        #         session.phytopharmaceutical_products_sold(
        #             annee_min=2016,
        #             annee_max=2020,
        #             code_territoire=["32"],
        #             type_territoire="Région",
        #             eaj="Oui",
        #             unite="l",
        #         ),
        #         session.phytopharmaceutical_products_sold(
        #             annee_min=2021,
        #             code_territoire=["32"],
        #             type_territoire="Région",
        #             eaj="Oui",
        #             unite="l",
        #         ),
        #     ]
        # )

        # ax = (
        #     df.pivot_table(
        #         "quantite",
        #         index=["libelle_territoire"],
        #         columns=["annee"],
        #     )
        #     .loc["HAUTS-DE-FRANCE"]
        #     .T.plot(kind="bar", stacked=True)
        # )
        # handles, labels = ax.get_legend_handles_labels()
        # lgd = ax.legend(
        #     handles, labels, loc="upper center", bbox_to_anchor=(0.5, -0.2)
        # )
        # plt.show()

        # gdf = session.get_sites(
        #     code_departement=["02", "59"], format="geojson"
        # )
        # df = session.get_observations(code_entite="K437311001")

        # df = session.get_realtime_observations(
        #     code_entite="K437311001",
        #     grandeur_hydro="Q",
        #     # date_debut_obs="2010-01-01",
        # )
        # df.pivot_table(
        #     index="date_obs", columns="grandeur_hydro", values="resultat_obs"
        # ).plot()
