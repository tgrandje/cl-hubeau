---
layout: default
title: API Vente et achat de produits phytopharmaceutiques
language: fr
handle: /phytopharmaceuticals
nav_order: 6

---
# API Vente et achat de produits phytopharmaceutiques

[https://hubeau.eaufrance.fr/page/api-vente-achat-phytos](https://hubeau.eaufrance.fr/page/api-vente-achat-phytos)

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

{: .warning }
> Les quatres fonctions de haut niveau introduisent deux arguments qui ne figurent pas dans les
> arguments déjà prévus par hubeau :
>
>  * filter_regions
>  * filter_departements
>
> Ces arguments permettent de faciliter la récupération de données territoriales à niveau donné.
> `filter_regions` est utilisé pour filtrer la récupération de données au niveau
> départemental ou code postal ; le champ `filter_departements` est utilisé pour filtrer la
> récupération de données au niveau du code postal. Dans tous les autres cas, l'utilisation
> de ces arguments lèvera des alertes.

Il est également possible d'ajouter des arguments à ces fonctions, en s'appuyant sur la
documentation d'hubeau. Pour en savoir plus, n'hésitez pas à consulter les docstrings via
la commande `help(ma_fonction)`.

Ces fonctions implémentent des doubles boucles pour optimiser l'usage du cache et éviter
d'atteindre le seuil maximal de 20 000 résultats :
* boucle sur les années ;
* boucle sur les territoires.


### Récupération des substances actives achetées

Cette fonction permet de récupérer les achats :
* au niveau France entière
* sur une ou plusieurs régions
* sur un ou plusieurs départements (éventuellement en sélectionnant par code région)
* sur un ou plusieurs codes postaux (éventuellement en sélectionnant par code région ou département)


#### Récupération France entière
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought()
```

#### Récupération de données régionales

Toutes les régions :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Région"
    )
```

Une seule région :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Région", code_territoire="32"
    )
```

Plusieurs régions :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Région", code_territoire=["11", "32"]
    )
```

#### Récupération de données départementales

Toutes les données départementales (long !) :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Département"
    )
```

Un ou plusieurs départements ciblés :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Département", code_territoire=["02", "60", "80"]
    )
```

L'intégralité d'une région (actuelle) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Département", filter_regions="32"
    )
```

#### Récupération de données au code postal

Toutes les données au code postal (très long !) :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Zone postale"
    )
```

Un ou plusieurs code postaux ciblés :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Zone postale", code_territoire=["59000", "59160", "59260", "59777", "59800"]
    )
```

L'intégralité d'une région (actuelle) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Zone postale", filter_regions="32"
    )
```

L'intégralité d'une département (actuel) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="Zone postale", filter_departements="59"
    )
```

### Récupération des produits phytosanitaires achetés

Cette fonction permet de récupérer les achats :
* au niveau France entière
* sur une ou plusieurs régions
* sur un ou plusieurs départements (éventuellement en sélectionnant par code région)
* sur un ou plusieurs codes postaux (éventuellement en sélectionnant par code région ou département)


#### Récupération France entière
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought()
```

#### Récupération de données régionales

Toutes les régions :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Région"
    )
```

Une seule région :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Région", code_territoire="32"
    )
```

Plusieurs régions :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Région", code_territoire=["11", "32"]
    )
```

#### Récupération de données départementales

Toutes les données départementales (long !) :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Département"
    )
```

Un ou plusieurs départements ciblés :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Département", code_territoire=["02", "60", "80"]
    )
```

L'intégralité d'une région (actuelle) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Département", filter_regions="32"
    )
```

#### Récupération de données au code postal

Toutes les données au code postal (très long !) :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Zone postale"
    )
```

Un ou plusieurs code postaux ciblés :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Zone postale", code_territoire=["59000", "59160", "59260", "59777", "59800"]
    )
```

L'intégralité d'une région (actuelle) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Zone postale", filter_regions="32"
    )
```

L'intégralité d'une département (actuel) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="Zone postale", filter_departements="59"
    )
```

### Récupération des substances actives vendues

Cette fonction permet de récupérer les achats :
* au niveau France entière
* sur une ou plusieurs régions
* sur un ou plusieurs départements (éventuellement en sélectionnant par code région)


#### Récupération France entière
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_sold()
```

#### Récupération de données régionales

Toutes les régions :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="Région"
    )
```

Une seule région :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="Région", code_territoire="32"
    )
```

Plusieurs régions :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="Région", code_territoire=["11", "32"]
    )
```

#### Récupération de données départementales

Toutes les données départementales (long !) :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="Département"
    )
```

Un ou plusieurs départements ciblés :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="Département", code_territoire=["02", "60", "80"]
    )
```

L'intégralité d'une région (actuelle) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="Département", filter_regions="32"
    )
```

### Récupération des produits phytosanitaires vendus

Cette fonction permet de récupérer les achats :
* au niveau France entière
* sur une ou plusieurs régions
* sur un ou plusieurs départements (éventuellement en sélectionnant par code région)


#### Récupération France entière
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold()
```

#### Récupération de données régionales

Toutes les régions :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="Région"
    )
```

Une seule région :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="Région", code_territoire="32"
    )
```

Plusieurs régions :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="Région", code_territoire=["11", "32"]
    )
```

#### Récupération de données départementales

Toutes les données départementales (long !) :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="Département"
    )
```

Un ou plusieurs départements ciblés :

```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="Département", code_territoire=["02", "60", "80"]
    )
```

L'intégralité d'une région (actuelle) :
```python
from cl_hubeau import phytopharmaceuticals_transactions
df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="Département", filter_regions="32"
    )
```


## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Récupérer les substances actives vendues

```python
from cl_hubeau import phytopharmaceuticals_transactions
with watercourses_flow.PhytopharmaceuticalsSession() as session:
    df = session.active_substances_sold(
        annee_min=2010,
        annee_max=2015,
        code_territoire=["32"],
        type_territoire="Région",
        )
```

### Récupérer les produits phytosanitaires vendus

```python
from cl_hubeau import phytopharmaceuticals_transactions
with watercourses_flow.phytopharmaceutical_products_sold(
        annee_min=2010,
        annee_max=2015,
        code_territoire=["32"],
        type_territoire="Région",
        eaj="Oui",
        unite="l",
    )
```

### Récupérer les substances actives achetées

```python
from cl_hubeau import phytopharmaceuticals_transactions
with watercourses_flow.active_substances_bought(
        annee_min=2010,
        annee_max=2015,
        code_territoire=["32"],
        type_territoire="Région",
    )
```

### Récupérer les produits phytosanitaires achetés

```python
from cl_hubeau import phytopharmaceuticals_transactions
with watercourses_flow.phytopharmaceutical_products_bought(
        code_territoire=["32"],
        type_territoire="Région",
        eaj="Oui",
        unite="l",
    )
```
