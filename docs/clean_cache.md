---
layout: default
title: Nettoyage du cache
language: fr
handle: /clean_cache
nav_order: 5

---
# Nettoyage du cache

`cl-hubeau` utilise des sessions web héritées du package requests-cache. Celui-ci
exécute des requêtes classiques, mais dont le résultat est conservé en cache jusqu'au
terme d'une période d'expiration ; cela permet d'obtenir de meilleurs performances
dans le cas de tâches répétées, mais aussi de gagner en frugalité (évite la
sur-sollicitation des serveurs).

En règle générale les durées de conservation en cache fixées par `cl-hubeau` sont de

* 30 jours pour les requêtes standard ;
* 15 minutes pour les requêtes concernant des données en temps réel.

Il peut parfois être nécessaire de vider ce cache manuellement.

Pour ce faire, on peut utiliser la commande suivante :

```python
from cl_hubeau.utils import clean_all_cache
clean_all_cache()
```
