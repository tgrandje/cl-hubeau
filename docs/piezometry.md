---
layout: default
title: API Piézométrie
language: fr
handle: /piezometry
nav_order: 6

---
# API Piézométrie

[https://hubeau.eaufrance.fr/page/api-piezometrie](https://hubeau.eaufrance.fr/page/api-piezometrie)

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

### Récupération de la totalité des piézomètres

Cette fonction permet de récupérer les piézomètres de la France entière.

```python
from cl_hubeau import piezometry
gdf = piezometry.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "stations" de l'API, à l'exception de :
* `format` (fixé par défaut au format geojson pour retourner un geodataframe)
* `code_departement` (utilisé pour boucler sur les données nationales)

Par exemple :
```python
from cl_hubeau import piezometry
gdf = piezometry.get_all_stations(nb_mesures_piezo_min=500)
```

### Récupération des chroniques de données "temps différé"

Cette fonction permet de récupérer les chroniques de données temps différé pour une liste de piézomètres.
Ceux-ci doivent être spécifiés sous la forme d'une liste de codes bss (seul champ disponible pour
identifier un piézomètre sur ce point de sortie API).

```python
from cl_hubeau import piezometry
df = piezometry.get_chronicles(
    codes_bss=['07011X0117/RN00', '07004X0055/RN10', '07004X0046/D6-20', '07004X0057/D1_20', '06754X0077/F1']
    )
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "chroniques" de l'API, à l'exception de `code_bss`.

Par exemple :
```python
from cl_hubeau import piezometry
df = piezometry.get_chronicles(
    codes_bss=['07011X0117/RN00', '07004X0055/RN10', '07004X0046/D6-20', '07004X0057/D1_20', '06754X0077/F1'],
    date_debut_mesure="2020-01-01",
    )
```

### Récupération des chroniques de données "temps réel"

Cette fonction permet de récupérer les chroniques de données temps réel pour une liste de piézomètres.
Ceux-ci doivent être spécifiés sous la forme d'une liste de codes bss ou d'identifiants BSS 
(les deux options étant mutuellement exclusives).

Cette fonction utilise un cache avec une expiration fixée à 15 minutes.

```python
from cl_hubeau import piezometry
df = piezometry.get_realtime_chronicles(
    codes_bss=['07011X0117/RN00', '07004X0055/RN10', '07004X0046/D6-20', '07004X0057/D1_20', '06754X0077/F1']
    )
```

ou

```python
from cl_hubeau import piezometry
df = piezometry.get_realtime_chronicles(
    bss_ids=['BSS001TULG', 'BSS001TTQY', 'BSS001TTQQ', 'BSS001TTRA', 'BSS001SCTM']
    )
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "chroniques" de l'API, à l'exception de `code_bss` ou `bss_id`.

```python
from cl_hubeau import piezometry
df = piezometry.get_realtime_chronicles(
    codes_bss=['07011X0117/RN00', '07004X0055/RN10', '07004X0046/D6-20', '07004X0057/D1_20', '06754X0077/F1'],
    fields=["date_mesure", "niveau_eau_ngf", "code_bss"]
    )
```

## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les stations de mesure

```python
from cl_hubeau import piezometry
with piezometry.PiezometrySession() as session:
    df = session.get_stations(code_departement=['02', '59', '60', '62', '80'], format="geojson")
```

### Lister les chroniques piézométriques

```python
from cl_hubeau import piezometry
with piezometry.PiezometrySession() as session:
    df = session.get_chronicles(code_bss="07548X0009/F")
```

### Lister les chroniques piézométriques en temps réel

```python
from cl_hubeau import piezometry
with piezometry.PiezometrySession() as session:
    df = session.get_chronicles_real_time(code_bss="07548X0009/F")
```