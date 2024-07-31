# cl-hubeau

Simple hub'eau client for python

This package is currently under active development.
Every API on [Hub'eau](hubeau.eaufrance.fr/) will be covered by this package in
due time.

## Basic examples

### Clean cache

```python
from cl_hubeau.utils import clean_all_cache
clean_all_cache
```

### Piezometry

3 high level functions are available (and one class for low level operations).

Get all piezometers (uses a 30 days caching):

```python
from cl_hubeau import piezometry
gdf = piezometry.get_all_stations()
```

Get chronicles for the first 100 piezometers (uses a 30 days caching):

```python
df = piezometry.get_chronicles(gdf["code_bss"].head(100).tolist())
```

Get realtime data for the first 100 piezometers:

A small cache is stored to allow for realtime consumption (cache expires after
only 15 minutes). Please, adopt a responsible usage with this functionnality ! 

```python
df = get_realtime_chronicles(gdf["code_bss"].head(100).tolist())
```

Low level class to perform the same tasks:

Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility, noticely for realtime data

```python
with piezometry.PiezometrySession() as session:
    df = session.get_chronicles(code_bss="07548X0009/F")
    df = session.get_stations(code_departement=['02', '59', '60', '62', '80'], format="geojson")
    df = session.get_chronicles_real_time(code_bss="07548X0009/F")
```

### Hydrometry

4 high level functions are available (and one class for low level operations).


Get all stations (uses a 30 days caching):

```python
from cl_hubeau import hydrometry 
gdf = hydrometry.get_all_stations()
```

Get all sites (uses a 30 days caching):

```python
gdf = hydrometry.get_all_sites()
```

Get observations for the first 5 sites (uses a 30 days caching):
_Note that this will also work with stations (instead of sites)._

```python
df = hydrometry.get_observations(gdf["code_site"].head(5).tolist())
```

Get realtime data for the first 5 sites (no cache stored):

A small cache is stored to allow for realtime consumption (cache expires after
only 15 minutes). Please, adopt a responsible usage with this functionnality ! 


```python
df = hydrometry.get_realtime_observations(gdf["code_site"].head(5).tolist())
```

Low level class to perform the same tasks:


Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility, noticely for realtime data

```python
with hydrometry.HydrometrySession() as session:
    df = session.get_stations(code_station="K437311001")
    df = session.get_sites(code_departement=['02', '59', '60', '62', '80'], format="geojson")
    df = session.get_realtime_observations(code_entite="K437311001")
    df = session.get_observations(code_entite="K437311001")

```