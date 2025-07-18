# -*- coding: utf-8 -*-

import warnings

import geopandas as gpd
import polars as pl


class NotAGeoDataFrame(ValueError):
    pass


class GeoPolarsDataFrame(pl.DataFrame):
    crs = None

    def __init__(
        self,
        data=None,
        crs=None,
        **kwargs,
    ):
        """
        Small proxy to allow polars with limited support for geo
        functionnalities until geopolars is ready for production.
        This is an EXPERIMENTAL feature.

        To revert to the GeoDataFrame object, please use the to_geopandas()
        method.
        """
        self.crs = crs
        super().__init__(data, **kwargs)

    @property
    def geometry(self):
        try:
            return gpd.GeoSeries.from_wkt(
                self["geometry"].to_list(), crs=self.crs
            )
        except pl.exceptions.ColumnNotFoundError:
            raise NotAGeoDataFrame("No geometry column found")

    def to_crs(self, crs):
        df = self.clone()
        df = df.with_columns(
            geometry=pl.Series(self.geometry.to_crs(crs).tolist())
        )
        return df

    @classmethod
    def from_geopandas(cls, gdf: gpd.GeoDataFrame, **kwargs):
        try:
            crs = gdf.crs
        except AttributeError:
            crs = None
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="Geometry column does not contain geometry"
            )
            try:
                # GeoDataFrame
                gdf["geometry"] = gdf.geometry.to_wkt()
            except AttributeError:
                # DataFrame
                pass
        return cls(gdf, crs=crs, **kwargs)

    def to_geopandas(self):
        try:
            geoms = self.geometry
            gdf = self.drop(["geometry"]).to_pandas()
        except NotAGeoDataFrame:
            return self.to_pandas()
        gdf["geometry"] = geoms
        return gpd.GeoDataFrame(gdf, crs=self.crs)

    def plot(self, *args, **kwargs):
        return self.to_geopandas().plot(*args, **kwargs)

    @classmethod
    def from_features(cls, *args, **kwargs):
        return cls.from_geopandas(
            gpd.GeoDataFrame.from_features(*args, **kwargs)
        )

    def to_wkb(self, *args, **kwargs):
        df = self.clone()
        df = df.with_columns(
            geometry=pl.Series(self.geometry.to_wkb(*args, **kwargs).tolist())
        )
        return df

    def to_wkt(self, *args, **kwargs):
        df = self.clone()
        df = df.with_columns(
            geometry=pl.Series(self.geometry.to_wkt(*args, **kwargs).tolist())
        )
        return df

    def to_arrow(self, *args, **kwargs):
        return self.to_geopandas().to_arrow(*args, **kwargs)

    def to_parquet(self, *args, **kwargs):
        return self.to_geopandas().to_parquet(*args, **kwargs)

    def to_feather(self, *args, **kwargs):
        return self.to_geopandas().to_feather(*args, **kwargs)

    def to_file(self, *args, **kwargs):
        return self.to_geopandas().to_file(*args, **kwargs)

    def dissolve(self, *args, **kwargs):
        return self.to_geopandas().dissolve(*args, **kwargs)

    def explode(self, *args, **kwargs):
        return self.to_geopandas().explode(*args, **kwargs)

    def to_postgis(self, *args, **kwargs):
        return self.to_geopandas().to_postgis(*args, **kwargs)

    def sjoin(self, *args, **kwargs):
        return self.to_geopandas().sjoin(*args, **kwargs)

    def sjoin_nearest(self, *args, **kwargs):
        return self.to_geopandas().sjoin_nearest(*args, **kwargs)

    def clip(self, *args, **kwargs):
        return self.to_geopandas().clip(*args, **kwargs)

    def overlay(self, *args, **kwargs):
        return self.to_geopandas().overlay(*args, **kwargs)

    def affine_transform(self, *args, **kwargs):
        return self.to_geopandas().affine_transform(*args, **kwargs)

    @property
    def area(self):
        return self.to_geopandas().area

    @property
    def boundary(self):
        return self.to_geopandas().boundary

    @property
    def bounds(self):
        return self.to_geopandas().bounds

    def buffer(self, *args, **kwargs):
        return self.to_geopandas().buffer(*args, **kwargs)

    def build_area(self, *args, **kwargs):
        return self.to_geopandas().build_area(*args, **kwargs)

    @property
    def centroid(self):
        return self.to_geopandas().centroid

    def clip_by_rect(self, *args, **kwargs):
        return self.to_geopandas().clip_by_rect(*args, **kwargs)

    def concave_hull(self, *args, **kwargs):
        return self.to_geopandas().concave_hull(*args, **kwargs)

    def contains(self, *args, **kwargs):
        return self.to_geopandas().contains(*args, **kwargs)

    def contains_properly(self, *args, **kwargs):
        return self.to_geopandas().contains_properly(*args, **kwargs)

    @property
    def convex_hull(self):
        return self.to_geopandas().convex_hull

    def count_coordinates(self):
        return self.to_geopandas().count_coordinates()

    def count_geometries(self):
        return self.to_geopandas().count_geometries()

    def count_interior_rings(self):
        return self.to_geopandas().count_interior_rings()

    def covered_by(self, *args, **kwargs):
        return self.to_geopandas().covered_by(*args, **kwargs)

    def covers(self, *args, **kwargs):
        return self.to_geopandas().covers(*args, **kwargs)

    def crosses(self, *args, **kwargs):
        return self.to_geopandas().crosses(*args, **kwargs)

    @property
    def cx(self):
        return self.to_geopandas().cx

    def delaunay_triangles(self, *args, **kwargs):
        return self.to_geopandas().delaunay_triangles(*args, **kwargs)

    def difference(self, *args, **kwargs):
        return self.to_geopandas().difference(*args, **kwargs)

    def disjoint(self, *args, **kwargs):
        return self.to_geopandas().disjoint(*args, **kwargs)

    def distance(self, *args, **kwargs):
        return self.to_geopandas().distance(*args, **kwargs)

    def dwithin(self, *args, **kwargs):
        return self.to_geopandas().dwithin(*args, **kwargs)

    @property
    def envelope(self):
        return self.to_geopandas().envelope

    @property
    def exterior(self):
        return self.to_geopandas().exterior

    def extract_unique_points(self):
        return self.to_geopandas().extract_unique_points()

    def force_2d(self):
        return self.to_geopandas().force_2d()

    def force_3d(self, *args, **kwargs):
        return self.to_geopandas().force_3d(*args, **kwargs)

    def frechet_distance(self, *args, **kwargs):
        return self.to_geopandas().frechet_distance(*args, **kwargs)

    def geom_almost_equals(self, *args, **kwargs):
        return self.to_geopandas().geom_almost_equals(*args, **kwargs)

    def geom_equals(self, *args, **kwargs):
        return self.to_geopandas().geom_equals(*args, **kwargs)

    def geom_equals_exact(self, *args, **kwargs):
        return self.to_geopandas().geom_equals_exact(*args, **kwargs)

    @property
    def geom_type(self):
        return self.to_geopandas().geom_type

    def get_coordinates(self, *args, **kwargs):
        return self.to_geopandas().get_coordinates(*args, **kwargs)

    def get_geometry(self, *args, **kwargs):
        return self.to_geopandas().get_geometry(*args, **kwargs)

    def get_precision(self):
        return self.to_geopandas().get_precision()

    @property
    def has_sindex(self):
        return self.to_geopandas().has_sindex

    @property
    def has_z(self):
        return self.to_geopandas().has_z

    def hausdorff_distance(self, *args, **kwargs):
        return self.to_geopandas().hausdorff_distance(*args, **kwargs)

    def hilbert_distance(self, *args, **kwargs):
        return self.to_geopandas().hilbert_distance(*args, **kwargs)

    @property
    def interiors(self):
        return self.to_geopandas().interiors

    def interpolate(self, *args, **kwargs):
        return self.to_geopandas().interpolate(*args, **kwargs)

    def intersection(self, *args, **kwargs):
        return self.to_geopandas().intersection(*args, **kwargs)

    def intersection_all(self):
        return self.to_geopandas().intersection_all()

    def intersects(self, *args, **kwargs):
        return self.to_geopandas().intersects(*args, **kwargs)

    @property
    def is_ccw(self):
        return self.to_geopandas().is_ccw

    @property
    def is_closed(self):
        return self.to_geopandas().is_closed

    @property
    def is_empty(self):
        return self.to_geopandas().is_empty

    @property
    def is_ring(self):
        return self.to_geopandas().is_ring

    @property
    def is_simple(self):
        return self.to_geopandas().is_simple

    @property
    def is_valid(self):
        return self.to_geopandas().is_valid

    @property
    def is_valid_reason(self):
        return self.to_geopandas().is_valid_reason

    @property
    def length(self):
        return self.to_geopandas().length

    def line_merge(self, *args, **kwargs):
        return self.to_geopandas().line_merge(*args, **kwargs)

    def make_valid(self):
        return self.to_geopandas().make_valid()

    def minimum_bounding_circle(self):
        return self.to_geopandas().minimum_bounding_circle()

    def minimum_bounding_radius(self):
        return self.to_geopandas().minimum_bounding_radius()

    def minimum_clearance(self):
        return self.to_geopandas().minimum_clearance()

    def minimum_rotated_rectangle(self):
        return self.to_geopandas().minimum_rotated_rectangle()

    def normalize(self):
        return self.to_geopandas().normalize()

    def offset_curve(self, *args, **kwargs):
        return self.to_geopandas().offset_curve(*args, **kwargs)

    def overlaps(self, *args, **kwargs):
        return self.to_geopandas().overlaps(*args, **kwargs)

    def polygonize(self, *args, **kwargs):
        return self.to_geopandas().polygonize(*args, **kwargs)

    def project(self, *args, **kwargs):
        return self.to_geopandas().project(*args, **kwargs)

    def relate(self, *args, **kwargs):
        return self.to_geopandas().relate(*args, **kwargs)

    def relate_pattern(self, *args, **kwargs):
        return self.to_geopandas().relate_pattern(*args, **kwargs)

    def remove_repeated_points(self, *args, **kwargs):
        return self.to_geopandas().remove_repeated_points(*args, **kwargs)

    def representative_point(self):
        return self.to_geopandas().representative_point()

    def reverse(self):
        return self.to_geopandas().reverse()

    def rotate(self, *args, **kwargs):
        return self.to_geopandas().rotate(*args, **kwargs)

    def sample_points(self, *args, **kwargs):
        return self.to_geopandas().sample_points(*args, **kwargs)

    def scale(self, *args, **kwargs):
        return self.to_geopandas().scale(*args, **kwargs)

    def segmentize(self, *args, **kwargs):
        return self.to_geopandas().segmentize(*args, **kwargs)

    def set_precision(self, *args, **kwargs):
        return self.to_geopandas().set_precision(*args, **kwargs)

    def shared_paths(self, *args, **kwargs):
        return self.to_geopandas().shared_paths(*args, **kwargs)

    def shortest_line(self, *args, **kwargs):
        return self.to_geopandas().shortest_line(*args, **kwargs)

    def simplify(self, *args, **kwargs):
        return self.to_geopandas().simplify(*args, **kwargs)

    @property
    def sindex(self):
        return self.to_geopandas().sindex

    def skew(self, *args, **kwargs):
        return self.to_geopandas().skew(*args, **kwargs)

    def snap(self, *args, **kwargs):
        return self.to_geopandas().snap(*args, **kwargs)

    def symmetric_difference(self, *args, **kwargs):
        return self.to_geopandas().symmetric_difference(*args, **kwargs)

    @property
    def total_bounds(self):
        return self.to_geopandas().total_bounds

    def touches(self, *args, **kwargs):
        return self.to_geopandas().touches(*args, **kwargs)

    def transform(self, *args, **kwargs):
        return self.to_geopandas().transform(*args, **kwargs)

    def translate(self, *args, **kwargs):
        return self.to_geopandas().translate(*args, **kwargs)

    @property
    def type(self):
        return self.to_geopandas().type

    @property
    def unary_union(self):
        return self.to_geopandas().unary_union

    def union(self, *args, **kwargs):
        return self.to_geopandas().union(*args, **kwargs)

    def union_all(self, *args, **kwargs):
        return self.to_geopandas().union_all(*args, **kwargs)

    def voronoi_polygons(self, *args, **kwargs):
        return self.to_geopandas().voronoi_polygons(*args, **kwargs)

    def within(self, *args, **kwargs):
        return self.to_geopandas().within(*args, **kwargs)


def concat(*args, **kwargs):
    return GeoPolarsDataFrame(pl.concat(*args, **kwargs))
