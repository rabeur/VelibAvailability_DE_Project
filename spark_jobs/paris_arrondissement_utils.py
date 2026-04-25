from __future__ import annotations

"""
Utilities to enrich Paris Velib stations with their arrondissement from latitude / longitude.

The boundaries come from the official Paris Open Data dataset and are stored locally in
`spark_jobs/data/paris_arrondissements.geojson` to keep the enrichment reproducible.
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional

# Repo / Docker layout keeps the geojson under a `data/` subfolder. On
# Dataproc Serverless workers, file_uris land flat next to this module,
# so we resolve at call time across both candidates.
_GEOJSON_FILENAME = "paris_arrondissements.geojson"
_GEOJSON_CANDIDATES = (
    Path(__file__).resolve().parent / "data" / _GEOJSON_FILENAME,
    Path(__file__).resolve().parent / _GEOJSON_FILENAME,
)
DEFAULT_GEOJSON_PATH = _GEOJSON_CANDIDATES[0]


def _format_arrondissement_label(arr_number: int) -> str:
    suffix = "er" if arr_number == 1 else "e"
    return f"Paris {arr_number}{suffix} Arrondissement"


@lru_cache(maxsize=1)
def load_arrondissement_boundaries(geojson_path: Optional[str] = None) -> tuple[dict, ...]:
    """Load Paris arrondissement polygons from the local GeoJSON file."""
    if geojson_path is not None:
        path = Path(geojson_path)
    else:
        path = next(
            (candidate for candidate in _GEOJSON_CANDIDATES if candidate.exists()),
            DEFAULT_GEOJSON_PATH,
        )

    with path.open("r", encoding="utf-8") as file_handle:
        geojson = json.load(file_handle)

    boundaries = []
    for feature in geojson.get("features", []):
        properties = feature.get("properties", {})
        arr_number = int(properties["c_ar"])
        centroid = properties.get("geom_x_y", {})
        boundaries.append(
            {
                "arr_number": arr_number,
                "label": _format_arrondissement_label(arr_number),
                "geometry": feature["geometry"],
                "centroid_lon": centroid.get("lon"),
                "centroid_lat": centroid.get("lat"),
            }
        )

    boundaries.sort(key=lambda item: item["arr_number"])
    return tuple(boundaries)


def _point_in_ring(lon: float, lat: float, ring: list[list[float]]) -> bool:
    """Return True if a point is inside a linear ring using ray casting."""
    if len(ring) < 3:
        return False

    inside = False
    for index, (x1, y1) in enumerate(ring):
        x2, y2 = ring[(index + 1) % len(ring)]

        crosses_latitude = (y1 > lat) != (y2 > lat)
        if not crosses_latitude:
            continue

        denominator = y2 - y1
        if denominator == 0:
            continue

        intersection_lon = ((x2 - x1) * (lat - y1) / denominator) + x1
        if lon < intersection_lon:
            inside = not inside

    return inside


def _point_in_polygon(lon: float, lat: float, polygon_coordinates: list[list[list[float]]]) -> bool:
    """Return True if a point is inside a polygon and not inside one of its holes."""
    if not polygon_coordinates:
        return False

    shell = polygon_coordinates[0]
    if not _point_in_ring(lon, lat, shell):
        return False

    for hole in polygon_coordinates[1:]:
        if _point_in_ring(lon, lat, hole):
            return False

    return True


def _point_in_geometry(lon: float, lat: float, geometry: dict) -> bool:
    """Support Polygon and MultiPolygon geometries from GeoJSON."""
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates", [])

    if geometry_type == "Polygon":
        return _point_in_polygon(lon, lat, coordinates)

    if geometry_type == "MultiPolygon":
        return any(_point_in_polygon(lon, lat, polygon) for polygon in coordinates)

    return False


def get_paris_arrondissement_label(
    latitude: Optional[float],
    longitude: Optional[float],
    geojson_path: Optional[str] = None,
) -> Optional[str]:
    """Map a station coordinate to its Paris arrondissement label."""
    if latitude is None or longitude is None:
        return None

    boundaries = load_arrondissement_boundaries(geojson_path)

    for boundary in boundaries:
        if _point_in_geometry(longitude, latitude, boundary["geometry"]):
            return boundary["label"]

    fallback = min(
        (
            boundary
            for boundary in boundaries
            if boundary["centroid_lon"] is not None and boundary["centroid_lat"] is not None
        ),
        key=lambda boundary: (boundary["centroid_lon"] - longitude) ** 2
        + (boundary["centroid_lat"] - latitude) ** 2,
        default=None,
    )

    return fallback["label"] if fallback else None
