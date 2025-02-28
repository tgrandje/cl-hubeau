---
layout: default
title: API Ecoulement des cours d'eau
language: fr
handle: /watercourses_flow
nav_order: 7

---
# API Ecoulement des cours d'eau

[https://hubeau.eaufrance.fr/page/api-ecoulement](https://hubeau.eaufrance.fr/page/api-ecoulement)

`cl-hubeau` définit :

* des fonctions de haut niveau implémentant des boucles basiques ;
* des fonctions de bas niveau qui implémentent directement les différents points d'entrée de l'API.

{: .warning }
Lors de l'utilisation des fonctions de bas niveau, l'utilisateur est responsable
de la consommation de l'API. En particulier, il s'agit d'être vigilant quant au seuil
de 20 000 résultats récupérables d'une seule requête.
Par ailleurs, la gestion du cache par les fonctions de bas niveau est de la responsabilité
de l'utilisateur, notamment pour l'accès aux données de temps réel (expiration par défaut
fixée à 30 jours).

Dans les deux cas, les fonctions implémentées sont conçues pour boucler sur les résultats de la
requête : les arguments optionnels `size` et `page` ou `cursor` ne doivent pas être fournis
au client python.

## Fonctions de haut niveau

### Récupération de la totalité des stations

Cette fonction permet de récupérer les stations de la France entière.

```python
from cl_hubeau import watercourses_flow
gdf = watercourses_flow.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "stations" de l'API, à l'exception de :
* `format` (fixé par défaut au format geojson pour retourner un geodataframe)
* `code_departement` (utilisé pour boucler sur les données nationales)

Par exemple :
```python
from cl_hubeau import watercourses_flow
gdf = watercourses_flow.get_all_stations(code_cours_eau="D0110600")
```

### Récupération de la totalité des campagnes

Cette fonction permet de récupérer les observations de la France entière.

```python
from cl_hubeau import watercourses_flow
gdf = watercourses_flow.get_all_campaigns()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "campagnes" de l'API, à l'exception de :
* `code_departement` (utilisé pour boucler sur les données nationales)

Par exemple :
```python
from cl_hubeau import watercourses_flow
gdf = watercourses_flow.get_all_observations(code_cours_eau="D0110600")
```

### Récupération de la totalité des observations

Cette fonction permet de récupérer les observations de la France entière.

```python
from cl_hubeau import watercourses_flow
gdf = watercourses_flow.get_all_observations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "stations" de l'API, à l'exception de :
* `format` (fixé par défaut au format geojson pour retourner un geodataframe)

Par exemple :
```python
from cl_hubeau import watercourses_flow
gdf = watercourses_flow.get_all_observations(code_cours_eau="D0110600")
```

## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les stations

```python
from cl_hubeau import watercourses_flow
with watercourses_flow.WatercoursesFlowSession() as session:
    df = session.get_stations(code_departement=['02', '59', '60', '62', '80'], format="geojson")
```

### Lister les observations

```python
from cl_hubeau import watercourses_flow
with watercourses_flow.WatercoursesFlowSession() as session:
    df = session.get_observations(code_station="F6640008")
```

### Lister les campagnes

```python
from cl_hubeau import watercourses_flow
with watercourses_flow.WatercoursesFlowSession() as session:
    df = session.get_campaigns(code_departement="59")
```
