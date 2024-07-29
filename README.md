# cl-hubeau

Simple hub'eau client for python

This package is currently under active development.
Every API on [Hub'eau](hubeau.eaufrance.fr/) will be covered by this package in due time.

## Basic examples

### Piezometry

3 high level functions are available (and one class for more low level operations).

```python

from cl_hubeau.piezometry import get_all_stations, get_chronicles, get_realtime_chronicles, PiezometrySession


# Get all piezometers (uses a 30 days caching)

gdf = get_all_stations()

# Get chronicles for the first 100 piezometers (uses a 30 days caching)

df = get_chronicles(gdf["code_bss"].head(100).tolist())

# Get realtime data for the first 100 piezometers (no cache stored)

df = get_realtime_chronicles(gdf["code_bss"].head(100).tolist())

# Low level class to perform the same tasks:
# (note that the API is currently forbidding results > 20k rows and you may need inner loops)

with PiezometrySession() as session:
    df = session.get_chronicles(code_bss="07548X0009/F")
    df = session.get_stations(code_departement=['02', '59', '60', '62', '80'], format="geojson")
    df = session.get_chronicles_real_time(code_bss="07548X0009/F")

```