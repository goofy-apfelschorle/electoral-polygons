from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import geopandas as gpd
from shapely.geometry import LineString
from shapely.ops import unary_union


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _rep_points(gdf: gpd.GeoDataFrame) -> np.ndarray:
    pts = gdf.geometry.representative_point()
    return np.array([(p.x, p.y) for p in pts], dtype=float)


def _pairwise_distances(coords: np.ndarray) -> np.ndarray:
    """
    O(n^2) pairwise distances. Fine for typical SV building counts.
    """
    # (n,1,2) - (1,n,2) -> (n,n,2)
    diffs = coords[:, None, :] - coords[None, :, :]
    d2 = (diffs ** 2).sum(axis=2)
    return np.sqrt(d2)


def _nearest_neighbor_stats(coords: np.ndarray) -> Tuple[float, float]:
    n = coords.shape[0]
    if n < 2:
        return 0.0, 0.0

    D = _pairwise_distances(coords)
    np.fill_diagonal(D, np.inf)
    nn = D.min(axis=1)  # nearest neighbor per point
    d50 = float(np.median(nn))
    d90 = float(np.quantile(nn, 0.90))
    return d50, d90


def _pca_ratio(coords: np.ndarray) -> float:
    n = coords.shape[0]
    if n < 2:
        return 1.0
    X = coords - coords.mean(axis=0, keepdims=True)
    cov = np.cov(X.T)
    eigvals = np.linalg.eigvalsh(cov)  # ascending
    lam1 = float(eigvals[-1])
    lam2 = float(eigvals[-2]) if len(eigvals) >= 2 else 0.0
    return lam1 / (lam2 + 1e-9)


def _count_components(coords: np.ndarray, radius: float) -> int:
    """
    Connect points with edges if distance <= radius. Return #connected components via union-find.
    """
    n = coords.shape[0]
    if n == 0:
        return 0
    if n == 1:
        return 1
    if radius <= 0:
        return n

    D = _pairwise_distances(coords)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # union where within radius
    for i in range(n):
        # only check j>i
        js = np.where(D[i, i + 1 :] <= radius)[0]
        for j_off in js:
            j = i + 1 + int(j_off)
            union(i, j)

    roots = {find(i) for i in range(n)}
    return len(roots)


def _mst_edges(coords: np.ndarray) -> List[Tuple[int, int]]:
    """
    Prim MST on complete graph with Euclidean weights. Returns list of edges (u,v).
    O(n^2) memory/time. Works well for typical SV sizes.
    """
    n = coords.shape[0]
    if n <= 1:
        return []

    D = _pairwise_distances(coords)
    np.fill_diagonal(D, np.inf)

    in_tree = np.zeros(n, dtype=bool)
    in_tree[0] = True

    # best connection to tree for each node
    best_dist = D[0].copy()
    best_parent = np.zeros(n, dtype=int)

    edges: List[Tuple[int, int]] = []
    for _ in range(n - 1):
        # pick node not in tree with smallest best_dist
        candidates = np.where(~in_tree)[0]
        j = candidates[np.argmin(best_dist[candidates])]
        p = int(best_parent[j])
        edges.append((p, int(j)))
        in_tree[j] = True

        # update best distances
        for k in np.where(~in_tree)[0]:
            if D[j, k] < best_dist[k]:
                best_dist[k] = D[j, k]
                best_parent[k] = j

    return edges


@dataclass
class AutoParams:
    mode: str
    d50: float
    d90: float
    R: float
    C: int
    w: float
    b: float
    s: float


def choose_params(coords: np.ndarray) -> AutoParams:
    d50, d90 = _nearest_neighbor_stats(coords)
    R = _pca_ratio(coords)

    # components based on spacing
    r_conn = 1.5 * d50 if d50 > 0 else 0.0
    C = _count_components(coords, r_conn)

    # mode rules
    if R >= 6 and C <= 2:
        mode = "corridor"
        w = clamp(0.5 * d50, 10.0, 35.0)
    elif R <= 3 and C == 1:
        mode = "blob"
        w = clamp(0.35 * d50, 6.0, 25.0)  # used as building-buffer in blob mode
    else:
        mode = "multi"
        w = clamp(0.8 * d50, 15.0, 60.0)

    b = clamp(0.15 * d50, 3.0, 12.0) if d50 > 0 else 6.0
    s = clamp(0.08 * d50, 1.0, 6.0) if d50 > 0 else 2.0

    return AutoParams(mode=mode, d50=d50, d90=d90, R=R, C=C, w=w, b=b, s=s)


def build_auto_polygon(buildings: gpd.GeoDataFrame) -> Tuple[object, AutoParams]:
    coords = _rep_points(buildings)
    params = choose_params(coords)

    buildings_union = unary_union(buildings.geometry)

    if params.mode == "blob":
        # blob: rely mostly on buffered union of footprints
        poly = buildings_union.buffer(params.w)
    else:
        # corridor / multi: MST skeleton buffered by w
        edges = _mst_edges(coords)
        if not edges:
            poly = buildings_union.buffer(params.b)
        else:
            lines = [LineString([tuple(coords[u]), tuple(coords[v])]) for u, v in edges]
            skeleton = unary_union(lines)
            corridor = skeleton.buffer(params.w, cap_style=1, join_style=1)  # round ends/joins
            poly = corridor.union(buildings_union.buffer(params.b))

    # simplify (preserve topology)
    poly = poly.simplify(params.s, preserve_topology=True)
    return poly, params


def main():
    IN_PATH = "src/data/scratch/sv2_selected_buildings_from_links.gpkg"
    IN_LAYER = "sv1_buildings_selected"
    OUT_PATH = "src/data/scratch/sv2_polygon_auto.gpkg"

    b = gpd.read_file(IN_PATH, layer=IN_LAYER)
    if len(b) == 0:
        raise ValueError("No buildings found in selected layer.")

    poly, params = build_auto_polygon(b)

    out = gpd.GeoDataFrame(
        {
            "sv": ["SV2"],
            "mode": [params.mode],
            "d50": [params.d50],
            "d90": [params.d90],
            "pca_R": [params.R],
            "components": [params.C],
            "w": [params.w],
            "b": [params.b],
            "simplify": [params.s],
        },
        geometry=[poly],
        crs=b.crs,
    )
    out.to_file(OUT_PATH, layer="sv1_polygon_auto", driver="GPKG")

    print("Wrote:", OUT_PATH, "layer=sv2_polygon_auto")
    print(
        f"Auto params: mode={params.mode} d50={params.d50:.2f} d90={params.d90:.2f} "
        f"R={params.R:.2f} C={params.C} w={params.w:.2f} b={params.b:.2f} s={params.s:.2f}"
    )


if __name__ == "__main__":
    main()
