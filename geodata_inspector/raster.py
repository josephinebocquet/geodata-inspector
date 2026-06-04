"""
raster.py — Raster file inspection for Geodata Inspector.

Reads raster metadata and statistics with rasterio.
Supported formats: GeoTIFF, ERDAS Imagine, JPEG2000, Arc ASCII Grid, SRTM HGT.
"""

import os
import sys
import math
import io
import base64

# pyproj's C extension initialises PROJ at import time from PROJ_LIB, which may
# already be set to a conflicting PostgreSQL/PostGIS installation.  We override
# both the env var AND call set_data_dir() so the live PROJ context is updated.
_PROJ_CONDA = os.path.join(sys.prefix, "Library", "share", "proj")
if os.path.isdir(_PROJ_CONDA):
    os.environ["PROJ_DATA"] = _PROJ_CONDA
    os.environ["PROJ_LIB"]  = _PROJ_CONDA
    try:
        import pyproj as _pp
        _pp.datadir.set_data_dir(_PROJ_CONDA)
    except Exception:
        pass

import numpy as np
import rasterio
from rasterio.warp import transform_bounds
from rasterio.enums import Resampling
from shapely.geometry import box
import geopandas as gpd


# ---------------------------------------------------------------------------
# File metadata
# ---------------------------------------------------------------------------

def _file_meta(filepath):
    import datetime
    stat = os.stat(filepath)
    ext  = os.path.splitext(filepath)[1].lower()
    types = {
        ".tif": "GeoTIFF", ".tiff": "GeoTIFF",
        ".img": "ERDAS Imagine",
        ".jp2": "JPEG 2000",
        ".asc": "Arc ASCII Grid",
        ".hgt": "SRTM HGT",
        ".nc":  "NetCDF",
        ".vrt": "GDAL Virtual Raster",
    }
    return {
        "size_ko": round(stat.st_size / 1024, 1),
        "date":    datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d"),
        "type":    types.get(ext, f"Raster ({ext})"),
    }


# ---------------------------------------------------------------------------
# Geometry / area helpers
# ---------------------------------------------------------------------------

def _area_km2(bounds_wgs84):
    """
    Approximate bounding-box area in km² using the haversine formula.
    No PROJ database required — works entirely with Python math.
    """
    xmin, ymin, xmax, ymax = bounds_wgs84
    R = 6371.0                                         # Earth mean radius (km)
    clat = math.radians((ymin + ymax) / 2)
    width_km  = math.radians(abs(xmax - xmin)) * R * math.cos(clat)
    height_km = math.radians(abs(ymax - ymin)) * R
    return round(width_km * height_km, 3)


def _cell_size_m(src, crs_obj=None):
    """Return (width_m, height_m) of one pixel, in approximate metres."""
    tr  = src.transform
    pw  = abs(tr[0])
    ph  = abs(tr[4])
    crs = crs_obj or src.crs
    if crs and crs.is_geographic:
        cy  = (src.bounds.top + src.bounds.bottom) / 2
        fac = 111320.0
        pw  = round(pw * fac * math.cos(math.radians(cy)), 1)
        ph  = round(ph * fac, 1)
    elif crs and crs.is_projected:
        pw, ph = round(pw, 2), round(ph, 2)
    else:
        pw, ph = round(pw, 4), round(ph, 4)
    return pw, ph


# ---------------------------------------------------------------------------
# Band statistics & fill rate
# ---------------------------------------------------------------------------

def _read_band_sampled(src, band, max_pixels=4_000_000):
    """Read a band at reduced resolution if the raster is very large."""
    total = src.width * src.height
    if total > max_pixels:
        scale = math.sqrt(max_pixels / total)
        shape = (max(1, int(src.height * scale)), max(1, int(src.width * scale)))
        return src.read(band, out_shape=shape, resampling=Resampling.nearest)
    return src.read(band)


def _valid_mask(data, nodata):
    """
    Boolean mask of valid (non-nodata, non-NaN) pixels.
    Uses relative tolerance for float nodata to handle imprecision
    (e.g. NetCDF fill values like -9e33 that may not compare exactly).
    """
    mask = np.ones(data.shape, dtype=bool)
    if nodata is not None:
        if isinstance(nodata, float) and math.isnan(nodata):
            pass  # handled below
        elif isinstance(nodata, float) and abs(nodata) > 1e10:
            # Large-magnitude fill values: mask anything within 1% relative distance
            mask &= ~(data < nodata * 0.99)  # nodata is large negative → data < 0.99*nodata
        else:
            mask &= data != nodata
    if np.issubdtype(data.dtype, np.floating):
        mask &= ~np.isnan(data)
    return mask


def _band_stats(src, band):
    """Return fill rate and descriptive stats for one band."""
    data   = _read_band_sampled(src, band)
    mask   = _valid_mask(data, src.nodata)
    fill   = round(mask.sum() / mask.size * 100, 2) if mask.size > 0 else 0.0
    valid  = data[mask].astype(np.float64)
    if valid.size == 0:
        return fill, {"min": None, "max": None, "mean": None, "std": None}
    stats = {
        "min":  round(float(valid.min()), 4),
        "max":  round(float(valid.max()), 4),
        "mean": round(float(valid.mean()), 4),
        "std":  round(float(valid.std()),  4),
    }
    return fill, stats


# ---------------------------------------------------------------------------
# Thumbnail generation
# ---------------------------------------------------------------------------

def _thumbnail_b64(src, max_dim=512):
    """
    Return a base64-encoded PNG thumbnail of the raster (first 3 bands or grayscale).
    Normalises values to 0-255 using 2nd–98th percentile stretch.
    Returns None if Pillow is not available or generation fails.
    """
    try:
        from PIL import Image
    except ImportError:
        return None

    try:
        scale  = min(max_dim / src.width, max_dim / src.height, 1.0)
        out_w  = max(1, int(src.width  * scale))
        out_h  = max(1, int(src.height * scale))
        n      = src.count

        def _normalise(arr, nodata):
            a = arr.astype(np.float64)
            m = _valid_mask(a, nodata)
            v = a[m]
            if v.size == 0:
                return np.zeros_like(a, dtype=np.uint8)
            lo, hi = np.percentile(v, [2, 98])
            if hi > lo:
                a = np.clip((a - lo) / (hi - lo) * 255, 0, 255)
            else:
                a = np.zeros_like(a)
            a[~m] = 0
            return a.astype(np.uint8)

        if n >= 3:
            raw  = src.read([1, 2, 3], out_shape=(3, out_h, out_w),
                            resampling=Resampling.bilinear)
            rgb  = np.stack([_normalise(raw[i], src.nodata) for i in range(3)], axis=2)
            img  = Image.fromarray(rgb, "RGB")
        else:
            raw  = src.read(1, out_shape=(out_h, out_w), resampling=Resampling.bilinear)
            gray = _normalise(raw, src.nodata)
            img  = Image.fromarray(gray, "L").convert("RGB")

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception as exc:
        print(f"[Raster] Thumbnail error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Territorial coverage
# ---------------------------------------------------------------------------

def _coverage_pct(bounds_wgs84, gdf_reference):
    if gdf_reference is None:
        return None
    try:
        raster_box = box(*bounds_wgs84)
        ref_4326   = gdf_reference.to_crs(epsg=4326)
        ref_union  = (ref_4326.union_all()
                      if hasattr(ref_4326, "union_all")
                      else ref_4326.geometry.unary_union)
        if ref_union.area <= 0:
            return None
        inter = raster_box.intersection(ref_union)
        return round(inter.area / ref_union.area * 100, 2)
    except Exception as exc:
        print(f"[Raster] Coverage error: {exc}")
        return None


# ---------------------------------------------------------------------------
# NetCDF subdataset helpers
# ---------------------------------------------------------------------------

# Variables that are dimension/coordinate metadata, not spatial data
_NC_SKIP = {"lat", "lon", "latitude", "longitude", "time", "level",
            "times_bnds", "time_bnds", "lat_bnds", "lon_bnds",
            "crs", "x", "y", "z", "depth", "height"}

def _resolve_open_path(filepath):
    """
    For NetCDF files with subdatasets, return (open_path, all_variable_names).
    Skips dimension/coordinate variables and returns the first data variable.
    For other formats, returns (filepath, []).
    """
    ext = os.path.splitext(filepath)[1].lower()
    if ext != ".nc":
        return filepath, []

    with rasterio.open(filepath) as probe:
        subs = probe.subdatasets
    if not subs:
        return filepath, []

    # Parse variable names from "netcdf:path:VarName"
    var_names = [s.rsplit(":", 1)[-1] for s in subs]
    data_vars = [
        (path, name) for path, name in zip(subs, var_names)
        if name.lower() not in _NC_SKIP
    ]
    if not data_vars:
        return subs[0], var_names  # fallback: first subdataset

    open_path   = data_vars[0][0]
    all_data_vars = [name for _, name in data_vars]
    return open_path, all_data_vars


def _infer_crs(src):
    """
    Return (crs_object_or_None, crs_string, was_inferred).
    If CRS is missing but bounds look like geographic lat/lon, assume EPSG:4326.
    """
    if src.crs:
        return src.crs, src.crs.to_string(), False
    b = src.bounds
    if -180 <= b.left <= 180 and -90 <= b.bottom <= 90:
        from rasterio.crs import CRS as _CRS
        # Use WKT to avoid PROJ database lookup (prevents conflicts with system PROJ)
        _WGS84_WKT = (
            'GEOGCS["WGS 84",DATUM["WGS_1984",'
            'SPHEROID["WGS 84",6378137,298.257223563]],'
            'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
        )
        assumed = _CRS.from_wkt(_WGS84_WKT)
        return assumed, "EPSG:4326", True
    return None, "Inconnu", False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def inspect_raster(filepath, gdf_reference=None, metric_crs=4326, wgs84_bounds=None,
                   inferred_label="inferred", crs_override=None):
    """
    Inspect a raster file and return a dict with 'summary' and 'map' keys,
    formatted for the web app's /upload response.

    inferred_label : word to append when CRS is inferred (e.g. "inféré" / "inferred")
    crs_override   : integer EPSG to treat as the file CRS (user correction)
    """
    fmeta    = _file_meta(filepath)
    filename = os.path.basename(filepath)

    # Resolve the actual path to open (handles NetCDF subdatasets)
    open_path, nc_variables = _resolve_open_path(filepath)

    with rasterio.open(open_path) as src:

        # ── CRS (override > file CRS > inference) ─────────────────────────
        if crs_override:
            from rasterio.crs import CRS as _CRS
            try:
                crs_obj = _CRS.from_epsg(crs_override)
            except Exception:
                crs_obj = None
            crs_str    = f"EPSG:{crs_override}"
            crs_inferred = False
        else:
            crs_obj, crs_str, crs_inferred = _infer_crs(src)

        if crs_inferred:
            crs_str = f"{crs_str} ({inferred_label})"

        # ── WGS84 extent ─────────────────────────────────────────────────
        if crs_obj:
            try:
                b84 = transform_bounds(crs_obj, "EPSG:4326", *src.bounds, densify_pts=21)
            except Exception:
                b84 = tuple(src.bounds)
        else:
            b84 = tuple(src.bounds)
        xmin84, ymin84, xmax84, ymax84 = b84

        # ── Area & cell size ──────────────────────────────────────────────
        area_km2       = _area_km2(b84)
        cell_w, cell_h = _cell_size_m(src, crs_obj)

        # ── Band statistics ───────────────────────────────────────────────
        bands_table = []
        fill_rates  = []
        descs = src.descriptions or ([None] * src.count)
        for i in range(1, src.count + 1):
            fill, stats = _band_stats(src, i)
            fill_rates.append(fill)
            name = descs[i - 1] if descs[i - 1] else f"Bande {i}"
            bands_table.append({
                "Bande":           name,
                "Type":            str(src.dtypes[i - 1]),
                "NoData":          src.nodata if src.nodata is not None else "—",
                "Min":             stats["min"],
                "Max":             stats["max"],
                "Moyenne":         stats["mean"],
                "Écart-type":      stats["std"],
                "Remplissage (%)": fill,
            })
        fill_overall = round(sum(fill_rates) / len(fill_rates), 2) if fill_rates else 0.0

        # ── Capture pixel dimensions before exiting context ──────────────
        nc_width, nc_height = src.width, src.height

        # ── Territorial coverage ──────────────────────────────────────────
        cov = _coverage_pct(b84, gdf_reference)

        # ── Thumbnail ─────────────────────────────────────────────────────
        thumb = _thumbnail_b64(src)

    # ── Assemble summary ─────────────────────────────────────────────────
    extent_dict = {
        "_extent_table": True,
        "SW (xmin, ymin)": f"{xmin84:.6f},  {ymin84:.6f}",
        "NW (xmin, ymax)": f"{xmin84:.6f},  {ymax84:.6f}",
        "SE (xmax, ymin)": f"{xmax84:.6f},  {ymin84:.6f}",
        "NE (xmax, ymax)": f"{xmax84:.6f},  {ymax84:.6f}",
    }

    summary = {
        "_raster":                             True,
        "Nom du fichier":                      filename,
        "Taille (Ko)":                         fmeta["size_ko"],
        "Date de création du fichier (Y-M-D)": fmeta["date"],
        "Type de fichier":                     fmeta["type"],
        "CRS":                                 crs_str,
        "Nb bandes":                           len(bands_table),
        "Nb colonnes (pixels)":                nc_width,
        "Nb lignes (pixels)":                  nc_height,
        "Bandes": {"_transposed_table": True, "data": bands_table},
        "Résolution des cellules (m)":         f"{cell_w} × {cell_h}",
        "Étendue (WGS84)":                     extent_dict,
        "Emprise estimée (km2)":               area_km2,
        "Taux de remplissage (%)":             fill_overall,
        "Couverture territoriale (%)":         cov if cov is not None else "N/A",
    }

    # For NetCDF: report all available data variables
    if nc_variables and len(nc_variables) > 1:
        summary["Variables NetCDF disponibles"] = ", ".join(nc_variables)

    map_data = {
        "type":      "raster_extent",
        "bounds":    [[ymin84, xmin84], [ymax84, xmax84]],
        "thumbnail": thumb,
    }

    return {"summary": summary, "map": map_data}
