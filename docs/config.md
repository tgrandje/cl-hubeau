---
layout: default
title: Configuration
language: fr
handle: /config
nav_order: 4

---
# Configuration

## Ajout des clefs API INSEE

{: .warning }
Depuis la version 0.2.0 de `pynsee`, il n'est plus nécessaire de disposer de clefs API.

## Configuration des proxies

Les requêtes web exécutées par  `cl-hubeau` sont de deux types :

* celles exécutées par `pynsee` pour requêter les API de l'INSEE ;
* celles exécutées en propre par `cl-hubeau`.

Dans le cas où l'on souhaiterait utiliser des proxies professionnels
pour connexion internet, il suffit de fixer deux variables d'environnement
supplémentaires :

* http_proxy
* https_proxy

Il est également possible de passer ces arguments directement aux objets sessions
`cl-hubeau`.

Notez néanmoins que `pynsee` conserve ses proxies
[dans un fichier de configuration](https://github.com/InseeFrLab/pynsee/blob/0ba3e2e5b753c5c032f2b53d7fc042e995bbef04/pynsee/utils/init_conn.py#L55).

Dans le cas où vous rencontreriez des difficultés de connexion, n'hésitez pas
à effectuer une purge manuelle de ce fichier.
