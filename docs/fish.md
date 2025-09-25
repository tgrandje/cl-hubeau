---
layout: default
title: API Poisson
language: fr
handle: /fishes
nav_order: 19

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
requête : les arguments optionnels `size` et `page` ne doivent pas être fournis
au client python.

## Fonctions de haut niveau

{: .warning }
> Certaines stations ont des paramètres géographiques incomplets (
> `code_region`, `code_departement`, `code_commune`, `code_bassin_dce`,
> `code_sous_bassin` notamment). Il n'est donc pas possible pour cl-hubeau
> d'implémenter des boucles satisfaisantes en utilisant les critères définis par
> l'API.
>
> A la place, les données **stations** sont requêtées en utilisant une grille
> spatiale basée sur l'enveloppe des territoires présents dans les jeux de
> données AdminExpress de l'IGN. Cette grille est ensuite utilisée pour boucler
> sur les "boîtes" (usage du paramètre `bbox`).
>
> Un post-traitement est ensuite utilisé pour compléter les champs manquants
> dans le dataframe retourné. Cette consolidation est effectuée en deux étapes :
>
> * en premier lieu une jointure spatiale
> * en second lieu une jointure spatiale approximative (dans une limite de 10km)
>
> Ces données consolidées sont ensuite utilisées pour le requêtage de chaque
> endpoint de l'API *Hydrobiologie*. **Aucune consolidation
> n'est effectuée sur les autres jeux de données** : l'utilisateur est à la place
> invité à créer les liaisons pertinentes avec le jeu de données des stations.
>
> Par la suite, toutes les données datées seront requêtées par tranches de
> 1 an pour optimiser le cache ; dans le cas d'atteinte du
> seuil de 20k résultats, les périodes seront automatiquement divisées par deux
> pour garantir l'obtention des résultats.

{: .critical }
Par conséquent, les données des territoires non représentés dans le jeux de
données AdminExpress (certains territoires d'outre-mer) ne peuvent pas être
récupérées à ce jour.

{: .critical }
> Afin de limiter le temps de calcul lié à la consolidation des résultats, tous
> les champs ne sont pas comblés. A ce jour, seules les clefs de requêtage les
> plus courantes ont été implémentées :
>
> * données du code officiel géographique (région, département, commune)
> * bassins DCE et sous-bassins
>
> Dans le cas où vous souhaiteriez voir ajouter une couche de consolidation,
> merci de créer une issue sur le repo, si possible en sourçant un jeu de
> données national APIsé ad hoc.

### Récupération de la totalité des stations de mesure

Cette fonction permet de récupérer les stations de mesure sur la France entière.

```python
from cl_hubeau import fish
df = fish.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie de l'API. Il est également possible d'utiliser l'argument `code_sous_bassin`
qui a été ajouté au client `cl_hubeau` pour compléter les données.

Par exemple :
```python
from cl_hubeau import fish
gdf = fish.get_all_stations(
    code_departement='02'
    )
```

### Récupération des résultats des opérations

Cette fonction permet de récupérer les opérations de prélèvement :

```python
from cl_hubeau import fish
df = fish.get_all_operations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie de l'API. Il est également possible d'utiliser l'argument `code_sous_bassin`
qui a été ajouté au client `cl_hubeau` pour compléter les données.

```python
from cl_hubeau import fish
df = fish.get_all_operations(
    codes_dispositifs_collecte=['0400003087', '0600000418'],
    )
```

Par exemple :
```python
from cl_hubeau import fish
df = fish.get_all_operations(
    codes_dispositifs_collecte=['0400003087', '0600000418'],
    )
```

### Récupération des résultats des observations

Cette fonction permet de récupérer les observations piscicoles :

```python
from cl_hubeau import fish
df = fish.get_all_observations()
```

{: .warning }
Ce type de requêtage induit des résultats volumineux.
S'il est en théorie possible de requêter l'API sans paramétrage via cette
fonction, il est nécessaire d'utiliser des arguments suplémentaires
pour restreindre les résultats (au regard du temps de récupération des données
comme au regard de la gestion de la RAM).

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie de l'API. Il est également possible d'utiliser l'argument `code_sous_bassin`
qui a été ajouté au client `cl_hubeau` pour compléter les données.

Par exemple :
```python
from cl_hubeau import fish
df = fish.get_all_observations(code_departement="75")
```

### Récupération des résultats des indicateurs

Cette fonction permet de récupérer les indicateurs IPR et IPRPlus calculés sur les stations :

```python
from cl_hubeau import fish
df = fish.get_all_indicators()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie de l'API. Il est également possible d'utiliser l'argument `code_sous_bassin`
qui a été ajouté au client `cl_hubeau` pour compléter les données.

Par exemple :
```python
from cl_hubeau import fish
df = fish.get_all_indicators(code_region="32")
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

### Lister les opérations

```python
from cl_hubeau import fish
with fish.FishSession() as session:
    df = session.get_operations(code_station='01001122')
```

### Lister les observations

```python
from cl_hubeau import fish
with fish.FishSession() as session:
    df = session.get_observations(code_station='01001122')
```

### Lister les indicateurs

```python
from cl_hubeau import fish
with fish.FishSession() as session:
    df = session.get_indicators(code_station='01001122')
```
