from __future__ import annotations

import math
from dataclasses import dataclass

import geopandas as gpd
import numpy as np
from shapely.affinity import rotate, translate
from shapely.geometry import LineString, Polygon, box
from shapely.ops import unary_union


@dataclass(frozen=True)
class OrthogonalFrontageParams:
    pad_along: float = 12.0     # NEW: meters along street beyond min/max points
    pad_x: float = 10.0            # extend along-street beyond building extent
    pad_y: float = 8.0             # extend away from street beyond building extent
    building_buffer: float = 6.0   # square buffer around buildings (drives "blockiness")
    close_m: float = 35.0        # NEW: bridges gaps
    simplify_tol: float = 1.0      # final simplify tolerance (meters)


def _line_angle_deg(line: LineString) -> float:
    coords = np.asarray(line.coords)
    dx = float(coords[-1, 0] - coords[0, 0])
    dy = float(coords[-1, 1] - coords[0, 1])
    return math.degrees(math.atan2(dy, dx))


def build_orthogonal_frontage_polygon(
    *,
    street_seg: LineString,
    buildings: gpd.GeoDataFrame,
    anchor_geom=None,  # NEW: seeds or points union (truth anchor)
    clamp_poly: Polygon | None = None,
    params: OrthogonalFrontageParams = OrthogonalFrontageParams(),
) -> Polygon:
    """
    Build a street-aligned, mostly orthogonal frontage polygon.

    Key properties:
    - one-sided (we infer side from building centroids after rotating street to horizontal)
    - straight edges (corridor is an axis-aligned box in rotated frame)
    - can be concave (intersection with square-buffered buildings)
    """
    if buildings.empty:
        raise ValueError("build_orthogonal_frontage_polygon: buildings empty")

    origin = street_seg.centroid
    ang = _line_angle_deg(street_seg)
    rot = -ang

    # rotate street + buildings so street is horizontal
    seg_r = rotate(street_seg, rot, origin=origin, use_radians=False)
    b_r = buildings.copy()
    b_r["geometry"] = b_r.geometry.map(lambda g: rotate(g, rot, origin=origin, use_radians=False))

    # translate so street is around y=0 for stable "side" logic
    ry = float(np.mean([c[1] for c in seg_r.coords]))
    seg_r = translate(seg_r, yoff=-ry)
    b_r["geometry"] = b_r.geometry.map(lambda g: translate(g, yoff=-ry))

    # decide side based on majority of building centroid y
    cy = b_r.geometry.centroid.y.values
    pos = int(np.sum(cy >= 0))
    neg = int(np.sum(cy < 0))
    keep_pos = pos >= neg

    if keep_pos:
        b_side = b_r[b_r.geometry.centroid.y >= 0].copy()
        if b_side.empty:
            b_side = b_r.copy()
        y_low, y_high = 0.0, float(b_side.total_bounds[3]) + params.pad_y
        if y_high <= 0:
            y_high = float(np.max(cy)) + params.pad_y
    else:
        b_side = b_r[b_r.geometry.centroid.y <= 0].copy()
        if b_side.empty:
            b_side = b_r.copy()
        y_low, y_high = float(b_side.total_bounds[1]) - params.pad_y, 0.0
        if y_low >= 0:
            y_low = float(np.min(cy)) - params.pad_y

    # along-street extent should come from the anchor (points/seeds) projected onto the street
    if anchor_geom is not None and (not anchor_geom.is_empty):
        # rotate + translate anchor into the same rotated frame
        anc_r = rotate(anchor_geom, rot, origin=origin, use_radians=False)
        anc_r = translate(anc_r, yoff=-ry)

        # project anchor points/centroid-ish samples onto the rotated street
        # If anchor is MultiPoint/Polygon, sample its representative points
        if hasattr(anc_r, "geoms"):
            samples = [g.representative_point() for g in anc_r.geoms]
        else:
            samples = [anc_r.representative_point()]

        dists = [seg_r.project(p) for p in samples]
        a = min(dists)
        b = max(dists)

        x0 = seg_r.interpolate(max(0.0, a - params.pad_along)).x
        x1 = seg_r.interpolate(min(seg_r.length, b + params.pad_along)).x
    else:
        # fallback: use full segment
        seg_xs = [c[0] for c in seg_r.coords]
        x0 = min(seg_xs) - params.pad_along
        x1 = max(seg_xs) + params.pad_along


    corridor = box(x0, y_low, x1, y_high)

    # square-ish buffering around buildings (keeps "polyogonal" feel)
    b_union = unary_union(b_side.geometry)
    b_sq = b_union.buffer(params.building_buffer, cap_style=3, join_style=2).buffer(0)

    # Bridge gaps between nearby frontage buildings BEFORE clipping to corridor
    if params.close_m and params.close_m > 0:
        b_sq = (
            b_sq.buffer(params.close_m, cap_style=3, join_style=2)
                .buffer(-params.close_m, cap_style=3, join_style=2)
                .buffer(0)
        )

    # Base is the corridor itself (continuous strip), then we trim depth using building evidence
    base = corridor

    # Compute a "frontage depth" using building extent on the chosen side
    # (y_low/y_high already represent the allowed side depth)
    # Now build a building envelope to avoid going absurdly wide behind blocks:
    b_env = b_sq  # already square-buffered + optionally closed

    # Union base with building envelope to encourage following building outline,
    # but keep continuity across gaps.
    poly_r = base.union(b_env).intersection(base).buffer(0)


    # Bridge small gaps so frontage becomes a single strip (morphological closing)
    if params.close_m and params.close_m > 0:
        poly_r = (
            poly_r.buffer(params.close_m, cap_style=3, join_style=2)
                .buffer(-params.close_m, cap_style=3, join_style=2)
                .buffer(0)
        )


    # simplify a touch (optional)
    if params.simplify_tol and params.simplify_tol > 0:
        poly_r = poly_r.simplify(params.simplify_tol, preserve_topology=True).buffer(0)

    # rotate back + translate back
    poly = translate(poly_r, yoff=ry)
    poly = rotate(poly, ang, origin=origin, use_radians=False).buffer(0)

    # optional clamp (safety)
    if clamp_poly is not None:
        poly = poly.intersection(clamp_poly).buffer(0)

    if poly.is_empty:
        raise ValueError("Orthogonal frontage polygon became empty.")

    # If we got multiple islands, keep the one closest to the anchor (seeds/points),
    # otherwise fall back to largest area.
    if poly.geom_type == "MultiPolygon":
        parts = list(poly.geoms)

        if anchor_geom is not None and (not anchor_geom.is_empty):
            anc = anchor_geom.centroid
            parts.sort(key=lambda g: g.distance(anc))
            poly = parts[0]
        else:
            parts.sort(key=lambda g: g.area, reverse=True)
            poly = parts[0]


    return poly
