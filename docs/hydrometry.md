---
layout: default
title: API Hydrométrie
language: fr
handle: /hydrometry
nav_order: 6

---
# API Hydrométrie

[https://hubeau.eaufrance.fr/page/api-hydrometrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)

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

### Récupération de la totalité des stations hydrométriques

Cette fonction permet de récupérer les stations hydrométriques de la France entière.

```python
from cl_hubeau import hydrometry
gdf = hydrometry.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "stations" de l'API, à l'exception de :
* `format` (fixé par défaut au format geojson pour retourner un geodataframe)
* `code_departement` (utilisé pour boucler sur les données nationales)

Par exemple :
```python
from cl_hubeau import hydrometry
gdf = hydrometry.get_all_stations(en_service=1)
```

### Récupération de la totalité des sites hydrométriques

Cette fonction permet de récupérer les sites hydrométriques de la France entière.

```python
from cl_hubeau import hydrometry
gdf = hydrometry.get_all_sites()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "sites" de l'API, à l'exception de :
* `format` (fixé par défaut au format geojson pour retourner un geodataframe)
* `code_departement` (utilisé pour boucler sur les données nationales)

Par exemple :
```python
from cl_hubeau import hydrometry
gdf = hydrometry.get_all_sites(libelle_cours_eau="seine")
```

### Récupération des observations élaborées "temps différé"

Cette fonction permet de récupérer les observations élaborées (débits moyens journaliers ou mensuels) en temps différé
pour une liste de stations ou de sites.
Ceux-ci doivent être spécifiés sous la forme d'une liste de codes stations ou codes sites (champ unique "code_entite"
du point de sortie "observations élaborées" de l'API).

```python
from cl_hubeau import hydrometry
df = hydrometry.get_observations(
    codes_entites=['H0100020', 'H0210010', 'H0400010', 'H0400020', 'H0800012']
    )
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "observations élaborées" de l'API, à l'exception de `code_entite`.

Par exemple :
```python
from cl_hubeau import hydrometry
df = hydrometry.get_observations(
    codes_entites=['H0100020', 'H0210010', 'H0400010', 'H0400020', 'H0800012'],
    fields=["resultat_obs_elab", "code_site", "date_obs_elab"],
    )
```

### Récupération des chroniques de données "temps réel"

Cette fonction permet de récupérer les observations dites "temps réel" pour une liste de sites ou stations.
Ceux-ci doivent être spécifiés sous la forme d'une liste de codes stations ou codes sites (champ unique "code_entite"
du point de sortie "observations élaborées" de l'API).

Cette fonction utilise un cache avec une expiration fixée à 15 minutes.
Seules les données du dernier mois glissant sont rendues accessibles sur hubeau.

```python
from cl_hubeau import hydrometry
df = hydrometry.get_realtime_observations(
    codes_entites=['H0100020', 'H0210010', 'H0400010', 'H0400020', 'H0800012'],
    )
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "observations temps réel" de l'API, à l'exception de `code_entite`.

```python
from cl_hubeau import hydrometry
df = hydrometry.get_realtime_observations(
    codes_entites=['H0100020', 'H0210010', 'H0400010', 'H0400020', 'H0800012'],
    grandeur_hydro="H",
    )
```

## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les stations hydrométriques

```python
from cl_hubeau import hydrometry
with hydrometry.HydrometrySession() as session:
    df = session.get_stations(code_departement=['02', '59', '60', '62', '80'], format="geojson")
```

### Lister les sites hydrométriques

```python
from cl_hubeau import hydrometry
with hydrometry.HydrometrySession() as session:
    df = session.get_sites(code_departement=['02', '59', '60', '62', '80'], format="geojson")
```

### Lister les observations hydrométriques élaborées

```python
from cl_hubeau import hydrometry
with hydrometry.HydrometrySession() as session:
    df = session.get_observations(
    bbox=[2.08932176, 49.96905897, 4.23125977, 51.08898944],
    date_debut_obs_elab="2024-01-01",
    )
```

### Lister les observations hydrométriques (temps réel)

```python
from cl_hubeau import hydrometry
from datetime import date, timedelta
with hydrometry.HydrometrySession() as session:
    df = session.get_realtime_observations(
    bbox=[2.08932176, 49.96905897, 4.23125977, 51.08898944],
    date_debut_obs=(date.today() -timedelta(days=3)).strftime('%Y-%m-%d'),
    )
```