# -*- coding: utf-8 -*-
"""
low level class to collect data from the services indiators API from hub'eau
"""

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class WaterServicesSession(BaseHubeauSession):
    """
    Base session class to handle the services WaterServices API
    """

    DOC_URL = "https://hubeau.eaufrance.fr/page/api-indicateurs-services"

    def __init__(self, *args, **kwargs):
        super().__init__(version="0.3.1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau indicator's doc
        self.size = 5000

    def get_communes(self, **kwargs):
        """
        Lister les services par commune
        Endpoint /v0/indicateurs_services/communes

        Appel des indicateurs de performance des services publics d'eau et
        d'assainissement par commune (un minimum, maximum et moyenne
        sont calculés quand il existe plusieurs services publics
        sur une même commune)

        Source de données : Observatoire des services publics de l'eau
        et de l'assainissement : prix de l'eau et performance des services
        Site web : http://services.eaufrance.fr/

        Si la valeur du paramètre size n'est pas renseignée, la taille
        de page par défaut : 2000, taille max de la page : 10000.
        La profondeur d'accès aux résultats est : 20000, calcul de
        la profondeur = numéro de la page * nombre maximum de
        résultats dans une page.

        Doc: https://hubeau.eaufrance.fr/page/api-indicateurs-services
        """

        params = {}

        try:
            params["annee"] = kwargs.pop("annee")
        except KeyError:
            pass

        try:
            params["code_commune"] = self.list_to_str_param(
                kwargs.pop("code_commune"),
                200
            )
        except KeyError:
            pass

        try:
            params["code_departement"] = kwargs.pop("code_departement")
        except KeyError:
            pass

        try:
            params["detail_service"] = self._ensure_val_among_authorized_values(
                "detail_service", kwargs, {"true", "false"}
            )
        except KeyError:
            pass

        try:
            params["fields"] = self.list_to_str_param(
                kwargs.pop("fields")
            )
        except KeyError:
            pass

        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            pass

        try:
            params["type_service"] = self._ensure_val_among_authorized_values(
                "type_service", kwargs, {"AEP", "AC", "ANC"}
            )
        except KeyError:
            pass

        if kwargs :
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v0/indicateurs_services/communes"

        df = self.get_result(method, url, params=params)
        return df

    def get_indicators(self, **kwargs):
        """
        Lister les services par indicateur
        Endpoint /v0/indicateurs_services/indicateurs

        Appel par indicateur. Retourne la liste des valeurs de performance par service public d'eau et d'assainissement par service.

        Source de données : Observatoire des services publics de l'eau et de l'assainissement : prix de l'eau et performance des services.
        Site web :
        http://services.eaufrance.fr/

        Si la valeur du paramètre size n'est pas renseignée, la taille de page par défaut : 2000, taille max de la page : 5000.
        La profondeur d'accès aux résultats est : 20000, calcul de la profondeur = numéro de la page * nombre maximum de résultats dans une page.

        Doc: https://hubeau.eaufrance.fr/page/api-indicateurs-services
        """
        params = {}

        try:
            params["annee"] = kwargs.pop("annee")
        except KeyError:
            pass

        try:
            params["code_indicateur"] = kwargs.pop("code_indicateur")
        except KeyError:
            pass

        try:
            params["fields"] = self.list_to_str_param(
                kwargs.pop("fields")
            )
        except KeyError:
            pass

        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json"}
            )
        except KeyError:
            pass

        if kwargs :
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v0/indicateurs_services/indicateurs"

        df = self.get_result(method, url, params=params)

        return df

    def get_services(self, **kwargs) :
        """
        Lister les services par service
        Endpoint : /v0/indicateurs_services/services

        Appel par service. Retourne la liste des indicateurs de performance par service public d'eau et d'assainissement pour une commune donnée.

        Source de données : Observatoire des services publics de l'eau et de l'assainissement : prix de l'eau et performance des services
        Site web :
        http://services.eaufrance.fr/

        Si la valeur du paramètre size n'est pas renseignée, la taille de page par défaut : 5000, taille max de la page : 20000.
        La profondeur d'accès aux résultats est : 20000, calcul de la profondeur = numéro de la page * nombre maximum de résultats dans une page.
        """

        params = {}

        try:
            params["annee"] = kwargs.pop("annee")
        except KeyError:
            pass

        try:
            params["code_commune"] = self.list_to_str_param(
                kwargs.pop("code_commune"),
                200
            )
        except KeyError:
            pass

        try:
            params["code_departement"] = kwargs.pop("code_departement")
        except KeyError:
            pass

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json"}
            )
        except KeyError:
            pass

        try:
            params["type_service"] = self._ensure_val_among_authorized_values(
                "type_service", kwargs, {"AEP", "AC", "ANC"}
            )
        except KeyError:
            pass

        if kwargs :
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v0/indicateurs_services/services"

        df = self.get_result(method, url, params=params)

        return df
