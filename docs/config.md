---
layout: default
title: Configuration
language: fr
handle: /config
nav_order: 4

---
# Configuration

## Ajout des clefs API INSEE

`cl-hubeau` utilise `pynsee` pour permettre l'exécution de boucles basées sur le
[code officiel géographique](https://www.insee.fr/fr/information/2560452).
Pour fonctionner, ce package nécessite des clés API INSEE.
Deux variables d'environnement doivent impérativement être spécifiées :

* insee_key
* insee_secret

Merci de se référer à
[la documentation de `pynsee`](https://pynsee.readthedocs.io/en/latest/api_subscription.html)
pour plus d'information sur les clefs API et la configuration.

Pour mémoire, il est tout à fait possible de fixer des variables d'environnement
depuis un environnement python, à l'aide des instructions suivantes :

```python
import os
os.environ["insee_key"] = "ma-clef-applicative"
os.environ["insee_secret"] = "ma-clef-secrete"
```
## Configuration des proxies

Les requêtes web exécutées par  `cl-hubeau` sont de deux types :

* celles exécutées par `pynsee` pour requêter les API de l'INSEE ;
* celles exécutées en propre par `cl-hubeau` pour requêter hubeau.

Dans le cas où l'on souhaiterait utiliser des proxies professionnels
pour connexion internet, il suffit de fixer deux variables d'environnement
supplémentaires :

* http_proxy
* https_proxy

Il est également possible de passer ces arguments directement aux objets sessions
`cl-hubeau`.

Notez néanmoins que `pynsee` conserve clés API et proxies
[dans un fichier de configuration](https://github.com/InseeFrLab/pynsee/blob/0ba3e2e5b753c5c032f2b53d7fc042e995bbef04/pynsee/utils/init_conn.py#L55).

Dans le cas où vous rencontreriez des difficultés de connexion, n'hésitez pas
à effectuer une purge manuelle de ce fichier.
