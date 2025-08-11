---
layout: default
title: ❌ API Poisson
language: fr
handle: /fishes
nav_order: 18

---
# API Poisson

[https://hubeau.eaufrance.fr/page/api-poisson](https://hubeau.eaufrance.fr/page/api-poisson)

`cl-hubeau` définit :

* des fonctions de haut niveau implémentant des boucles basiques ;
* des fonctions de bas niveau qui implémentent directement les différents points d'entrée de l'API.

{: .warning }
Lors de l'utilisation des fonctions de bas niveau, l'utilisateur est responsable
de la consommation de l'API. En particulier, il s'agit d'être vigilant quant au seuil
de 20 000 résultats récupérables d'une seule requête.
Par ailleurs, la gestion du cache par les fonctions de bas niveau est de la responsabilité
de l'utilisateur.

Dans les deux cas, les fonctions implémentées sont conçues pour boucler sur les résultats de la
requête : les arguments optionnels `size` et `page` ou `cursor` ne doivent pas être fournis
au client python.

## Fonctions de haut niveau

### Récupération de la totalité des stations de mesure

Cette fonction permet de récupérer les stations de mesure sur la France entière.

```python
from cl_hubeau import fish
df = fish.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie de l'API.

Par exemple :
```python
from cl_hubeau import fish
gdf = fish.get_all_stations(
    code_departement='02'
    )
```

### Récupération des résultats des operations, observations ou indicateurs

Cette fonction permet de récupérer les opérations

```python
from cl_hubeau import fish
df = fish.get_all_operations(
    code_qualification_operation="1"
)
```

{: .warning }
En l'état, cette fonction implémente déjà une double boucle :
* sur les codes départements

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "operations" de l'API.

Par exemple :
```python
from cl_hubeau import fish
df = fish.get_all_operations(
    codes_dispositifs_collecte=['0400003087', '0600000418'],
    )
```

De même, il exite 2 autres fonctions :
- pour les observations :
```python
from cl_hubeau import fish
df = fish.get_all_observations(
    code_qualification_operation="1"
)
```

- Pour les indicateurs :
```python
from cl_hubeau import fish
df = fish.get_all_indicators()
```


## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les stations

```python
from cl_hubeau import fish
with fish.FishSession() as session:
    df = session.get_stations(code_departement="59")
```

### Lister les observations

```python
from cl_hubeau import fish
with fish.FishSession() as session:
    df = session.get_observations(code_station='...')
```
