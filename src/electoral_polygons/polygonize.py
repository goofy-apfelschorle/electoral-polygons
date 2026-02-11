# src/electoral_polygons/polygonize.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString, Point
from shapely.ops import unary_union


# =============================================================================
# Auto polygon builder for arbitrary SVs (guaranteed coverage)
# Includes:
#   - Fix A (339): compactness / fill via morphological closing
#   - Fix B (1212): prune huge MST edges + (optionally) drop tiny components
#   - Ensure-all loop: inflate/relax until ALL buildings are covered
# =============================================================================

@dataclass(frozen=True)
class AutoPolygonParams:
    # width selection clamps (initial pass)
    w_min: float = 10.0
    w_max: float = 60.0

    # building buffer clamps (initial pass)
    b_min: float = 3.0
    b_max: float = 12.0

    # simplify clamps (initial pass)
    s_min: float = 1.0
    s_max: float = 6.0

    # Fix B: edge pruning thresholds (1212)
    max_edge_floor: float = 250.0
    edge_factor_d90: float = 4.0
    edge_factor_d50: float = 8.0

    # Fix B: component pruning thresholds (optional)
    min_comp_abs: int = 10
    min_comp_frac: float = 0.10

    # Fix A: “fill/compact” closing radius relative to w (339)
    close_factor_w: float = 1.2
    close_min: float = 10.0
    close_max: float = 60.0

    # Ensure-all loop
    ensure_all: bool = True
    ensure_max_iters: int = 10
    ensure_growth: float = 1.25              # multiply w/b/close when missing
    ensure_tol: float = 0.01                 # meters: buffer poly by this for containment test
    ensure_relax_edges_growth: float = 1.6   # multiply max_edge when missing
    ensure_relax_comp_to_all: bool = True    # if missing, stop dropping components

    # Optional: keep per-iter log in meta (can be large)
    debug_ensure_log: bool = False


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _rep_points(buildings: gpd.GeoDataFrame) -> np.ndarray:
    pts = buildings.geometry.representative_point()
    return np.array([(p.x, p.y) for p in pts], dtype=float)


def _pairwise_distances(coords: np.ndarray) -> np.ndarray:
    diffs = coords[:, None, :] - coords[None, :, :]
    d2 = (diffs ** 2).sum(axis=2)
    return np.sqrt(d2)


def _nearest_neighbor_stats(coords: np.ndarray) -> tuple[float, float]:
    n = coords.shape[0]
    if n < 2:
        return 0.0, 0.0
    D = _pairwise_distances(coords)
    np.fill_diagonal(D, float("inf"))
    nn = D.min(axis=1)
    return float(np.median(nn)), float(np.quantile(nn, 0.90))


def _pca_ratio(coords: np.ndarray) -> float:
    n = coords.shape[0]
    if n < 2:
        return 1.0
    X = coords - coords.mean(axis=0, keepdims=True)
    cov = np.cov(X.T)
    eigvals = np.linalg.eigvalsh(cov)
    lam1 = float(eigvals[-1])
    lam2 = float(eigvals[-2]) if len(eigvals) >= 2 else 0.0
    return lam1 / (lam2 + 1e-9)


def _mst_edges(coords: np.ndarray) -> list[tuple[int, int]]:
    n = coords.shape[0]
    if n <= 1:
        return []

    D = _pairwise_distances(coords)
    np.fill_diagonal(D, float("inf"))

    in_tree = np.zeros(n, dtype=bool)
    in_tree[0] = True
    best_dist = D[0].copy()
    best_parent = np.zeros(n, dtype=int)

    edges: list[tuple[int, int]] = []
    for _ in range(n - 1):
        cand = np.where(~in_tree)[0]
        j = int(cand[np.argmin(best_dist[cand])])
        p = int(best_parent[j])
        edges.append((p, j))
        in_tree[j] = True

        for k in np.where(~in_tree)[0]:
            if D[j, k] < best_dist[k]:
                best_dist[k] = D[j, k]
                best_parent[k] = j

    return edges


def _edge_len(coords: np.ndarray, u: int, v: int) -> float:
    dx = float(coords[u, 0] - coords[v, 0])
    dy = float(coords[u, 1] - coords[v, 1])
    return float((dx * dx + dy * dy) ** 0.5)


def _connected_components(n: int, edges: list[tuple[int, int]]) -> list[list[int]]:
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for u, v in edges:
        union(u, v)

    comps: dict[int, list[int]] = {}
    for i in range(n):
        r = find(i)
        comps.setdefault(r, []).append(i)
    return list(comps.values())


def _missing_count(poly: object, coords: np.ndarray, tol: float) -> int:
    """
    Count how many representative points fall outside polygon.
    Uses a small tolerance buffer for robustness.
    """
    if poly is None or getattr(poly, "is_empty", True):
        return int(coords.shape[0])

    p = poly.buffer(float(tol))
    miss = 0
    for x, y in coords:
        if not p.contains(Point(float(x), float(y))):
            miss += 1
    return miss


def build_auto_polygon(
    buildings: gpd.GeoDataFrame,
    params: AutoPolygonParams = AutoPolygonParams(),
) -> tuple[object, dict[str, Any]]:
    """
    Polygonize an SV from its selected buildings.

    Guarantees (if params.ensure_all=True) that the returned polygon contains ALL
    building representative points, by inflating/relaxing if needed.

    Fixes included:
      - 339: compactness via morphological closing
      - 1212: prune huge MST edges (max_edge) + optional small-component drop
    """
    if buildings.empty:
        raise ValueError("build_auto_polygon: buildings empty")

    coords = _rep_points(buildings)
    d50, d90 = _nearest_neighbor_stats(coords)
    R = _pca_ratio(coords)

    # Choose mode + initial widths
    if R >= 6:
        mode = "corridor"
        w0 = clamp(0.5 * d50, params.w_min, 35.0)
    elif R <= 3:
        mode = "blob"
        w0 = clamp(0.35 * d50, 6.0, 25.0)
    else:
        mode = "multi"
        w0 = clamp(0.8 * d50, 15.0, params.w_max)

    b0 = clamp(0.15 * d50, params.b_min, params.b_max) if d50 > 0 else 6.0
    s0 = clamp(0.08 * d50, params.s_min, params.s_max) if d50 > 0 else 2.0

    buildings_union = unary_union(buildings.geometry)

    # MST once; we prune edges by length threshold
    mst = _mst_edges(coords)

    # Fix B (1212): edge cap derived from local spacing
    base_max_edge = params.max_edge_floor
    if d50 > 0:
        base_max_edge = max(
            params.max_edge_floor,
            params.edge_factor_d90 * d90,
            params.edge_factor_d50 * d50,
        )

    def _build_once(
        *,
        w: float,
        b: float,
        s: float,
        max_edge: float,
        drop_small_components: bool,
    ) -> tuple[object, dict[str, Any]]:
        # Fix B (1212): prune huge edges directly
        edges = [(u, v) for (u, v) in mst if _edge_len(coords, u, v) <= float(max_edge)]

        # Optional: prune tiny components (helps remove spurious far clusters),
        # but can be disabled in ensure loop if it causes missing buildings.
        if drop_small_components and edges:
            comps = _connected_components(len(coords), edges)
            comps.sort(key=len, reverse=True)

            min_keep = max(params.min_comp_abs, int(params.min_comp_frac * len(coords)))
            keep_nodes: set[int] = set()
            for comp in comps:
                if len(comp) >= min_keep:
                    keep_nodes.update(comp)

            # Only apply if it would actually drop something meaningful.
            # If keep_nodes is empty, do nothing (keep all).
            if keep_nodes:
                edges = [(u, v) for (u, v) in edges if u in keep_nodes and v in keep_nodes]

        # Geometry build
        if mode == "blob":
            poly = buildings_union.buffer(float(w))
        else:
            if not edges:
                poly = buildings_union.buffer(float(b))
            else:
                lines = [LineString([tuple(coords[u]), tuple(coords[v])]) for (u, v) in edges]
                skeleton = unary_union(lines)
                corridor = skeleton.buffer(float(w), cap_style=1, join_style=1)
                poly = corridor.union(buildings_union.buffer(float(b)))

        # Fix A (339): compactness / fill holes / reduce “spiky corridors”
        close_r = clamp(params.close_factor_w * float(w), params.close_min, params.close_max)
        poly = poly.buffer(close_r).buffer(-close_r)

        # finalize
        poly = poly.simplify(float(s), preserve_topology=True).buffer(0)

        meta = {
            "mode": mode,
            "d50": float(d50),
            "d90": float(d90),
            "R": float(R),
            "w": float(w),
            "b": float(b),
            "simplify": float(s),
            "max_edge": float(max_edge),
            "close_r": float(close_r),
            "n_buildings": int(len(buildings)),
            "n_edges_kept": int(len(edges)),
            "drop_small_components": bool(drop_small_components),
        }
        return poly, meta

    # First pass
    poly, meta = _build_once(
        w=w0,
        b=b0,
        s=s0,
        max_edge=base_max_edge,
        drop_small_components=True,
    )

    # Ensure-all loop: inflate footprint + relax pruning until fully covering
    ensure_log: list[dict[str, Any]] = []
    if params.ensure_all:
        w, b, s = float(w0), float(b0), float(s0)
        max_edge = float(base_max_edge)
        drop_small = True

        missing0 = _missing_count(poly, coords, params.ensure_tol)

        for it in range(int(params.ensure_max_iters)):
            miss = _missing_count(poly, coords, params.ensure_tol)

            if params.debug_ensure_log:
                ensure_log.append(
                    {
                        "iter": int(it),
                        "missing": int(miss),
                        "w": float(w),
                        "b": float(b),
                        "max_edge": float(max_edge),
                        "drop_small_components": bool(drop_small),
                    }
                )

            if miss == 0:
                break

            # Grow footprint (no hard upper clamp during ensure)
            w = max(params.w_min, w * params.ensure_growth)
            b = max(params.b_min, b * params.ensure_growth)

            # avoid over-smoothing while expanding
            s = float(max(params.s_min, min(s, 2.0)))

            # Relax edge pruning (helps reconnect split skeletons)
            max_edge = max_edge * float(params.ensure_relax_edges_growth)

            # If we’re missing anything, stop dropping components (keep all)
            if params.ensure_relax_comp_to_all:
                drop_small = False

            poly, meta = _build_once(
                w=w,
                b=b,
                s=s,
                max_edge=max_edge,
                drop_small_components=drop_small,
            )

        # Hard guarantee fallback: cover everything by construction
        miss_final = _missing_count(poly, coords, params.ensure_tol)
        if miss_final > 0:
            bb = max(b, params.b_max)
            # union of buffered buildings + stronger closing
            poly = buildings_union.buffer(bb).buffer(2 * bb).buffer(-2 * bb).buffer(0)
            meta["fallback_cover_all"] = True
            meta["fallback_bb"] = float(bb)
        else:
            meta["fallback_cover_all"] = False

        # Keep ensure summary small (safe for CSV joins)
        meta["ensure_missing0"] = int(missing0)
        meta["missing_final"] = int(_missing_count(poly, coords, params.ensure_tol))
        meta["ensure_iters"] = int(len(ensure_log) if params.debug_ensure_log else 0)

    if params.debug_ensure_log:
        meta["ensure_log"] = ensure_log

    return poly, meta
