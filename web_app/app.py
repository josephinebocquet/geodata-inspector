import os
import sys

# ── PROJ database conflict fix ────────────────────────────────────────────────
# PostgreSQL/PostGIS installs its own (old) PROJ database which shadows the
# conda env's PROJ. Force the correct path before any pyproj/rasterio import.
_PROJ_CONDA = os.path.join(sys.prefix, "Library", "share", "proj")
if os.path.isdir(_PROJ_CONDA):
    os.environ["PROJ_DATA"] = _PROJ_CONDA
    os.environ["PROJ_LIB"]  = _PROJ_CONDA
# ─────────────────────────────────────────────────────────────────────────────

import tempfile
import shutil
import zipfile
import traceback
import json
from io import StringIO
from contextlib import redirect_stdout

from flask import Flask, request, jsonify, render_template
import geopandas as gpd
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
import yaml

# Ajoute la racine du repo au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Use DuckDB-optimized inspector for better performance
import geodata_inspector.core as inspector
from config import get_config, get_reference_info, get_ui_labels

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "geodata-inspector-secret-key")
app.config["JSON_SORT_KEYS"] = False  # Preserve column order in JSON responses

# Load configuration
_cfg = get_config()
_reference_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reference_file")
_ref_info = get_reference_info(_cfg, _reference_dir)
UI = get_ui_labels(_cfg)

from config import get_config, get_reference_info, get_result_key_translations, get_geo_key_patterns, get_localisation_params

RESULT_TRANSLATIONS = get_result_key_translations(_cfg)
GEO_KEY_PATTERNS = get_geo_key_patterns(_cfg)
_loc_params = get_localisation_params(_cfg)
WGS84_BOUNDS = _loc_params["wgs84_bounds"]
METRIC_CRS = _loc_params["metric_crs"]

# Upload size limit (None = unlimited)
_max_mb = _cfg.get("server", {}).get("max_upload_size_mb")
app.config["MAX_CONTENT_LENGTH"] = int(_max_mb) * 1024 * 1024 if _max_mb else None

# Pre-load the reference geodataframe once at startup
if _ref_info["available"]:
    gdf_reference = gpd.read_file(_ref_info["path"]).to_crs(epsg=_ref_info["metric_crs"])
    print(f"[Config] Reference: {_ref_info['label']}")
else:
    print(f"[Config] WARNING: Reference file missing. Coverage metrics disabled.")
    gdf_reference = None
    

PREVIEW_DIR = tempfile.mkdtemp(prefix="geodata_preview_")
last_preview_path = {"path": None, "ext": None}

# Batch processing state
batch_state = {
    "running": False,
    "total": 0,
    "done": 0,
    "current": "",
    "errors": [],
    "logs": [],
    "file_paths": {},
    "output_path": None,
    "tmpdir": None,
    "gdfs": {},
    "raster_maps": {},       # {filename: map_data} for raster files
    "stop_requested": False,

}
batch_executor = ThreadPoolExecutor(max_workers=1)

VECTOR_EXTENSIONS = {".csv", ".txt", ".xlsx", ".geojson", ".json", ".shp", ".gpkg", ".zip"}
RASTER_EXTENSIONS = {".tif", ".tiff", ".img", ".jp2", ".asc", ".hgt", ".nc"}
ALLOWED_EXTENSIONS = VECTOR_EXTENSIONS | RASTER_EXTENSIONS


def _check_raster_write_drivers():
    """Return {fmt_key: available} for raster write formats, checked at startup."""
    import rasterio
    try:
        ext_to_driver = rasterio.drivers.raster_driver_extensions()
    except Exception:
        ext_to_driver = {}
    candidates = {
        "tif":  "GTiff",
        "nc":   "netCDF",
        "jp2":  "JP2OpenJPEG",
        "img":  "HFA",
        "asc":  "AAIGrid",
    }
    return {k: (v in ext_to_driver.values() or k == "tif")
            for k, v in candidates.items()}


RASTER_WRITE_DRIVERS = _check_raster_write_drivers()

# Define the desired column order for the summary display
COLUMN_ORDER = [
    # File metadata
    "Nom du fichier",
    "Taille (Ko)",
    "Date de création du fichier (Y-M-D)",
    "Type de fichier",

    # Basic data info
    "Nb lignes",
    "Nb colonnes",
    "Colonnes",
    "Score de complétude global",
    "Analyse temporelle",


    # Geographic info
    "Clés géographiques",
    "Géotransformation",
    "Score de complétude des clés géographique",
    "Score de complétude géographique",

    # Spatial metrics
    "CRS",
    "Types de géométrie",
    "Emprise estimée (km2)",
    "Densité (obj/km2)",
    "Taux de remplissage (%)",
    "Complexite moyenne",
    "Geometries dupliquees (%)",
    "Couverture territoriale (%)",
]

RASTER_COLUMN_ORDER = [
    "Nom du fichier",
    "Taille (Ko)",
    "Date de création du fichier (Y-M-D)",
    "Type de fichier",
    "CRS",
    "Nb bandes",
    "Nb colonnes (pixels)",
    "Nb lignes (pixels)",
    "Bandes",
    "Résolution des cellules (m)",
    "Étendue (WGS84)",
    "Emprise estimée (km2)",
    "Taux de remplissage (%)",
    "Couverture territoriale (%)",
]


def _clean_download_stem(name):
    """
    Return a clean ASCII-only stem safe for Content-Disposition filenames.

    Steps:
    1. Translate subscript/superscript digits to their ASCII equivalents
       (e.g. PM₁₀ → PM10).
    2. NFD-decompose and strip combining diacritics (è → e, à → a, etc.).
    3. Drop any remaining non-ASCII characters.
    4. Collapse runs of whitespace.
    """
    import unicodedata as _ud
    import re as _re

    SUB = str.maketrans("₀₁₂₃₄₅₆₇₈₉", "0123456789")
    SUP = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹", "0123456789")
    name = name.translate(SUB).translate(SUP)

    nfd = _ud.normalize("NFD", name)
    name = "".join(c for c in nfd if not _ud.combining(c))
    name = name.encode("ascii", errors="ignore").decode("ascii")
    name = _re.sub(r"\s+", " ", name).strip()
    return name or "export"


def _sanitize_shp_columns(gdf):
    """
    Rename GeoDataFrame columns to DBF-safe names before shapefile export.

    Rules applied:
    - Strip accents / diacritics → pure ASCII
    - Replace non-alphanumeric characters with underscores
    - Truncate to 10 characters (DBF field-name limit)
    - Deduplicate: append _1, _2 … when truncated names collide

    Returns (renamed GeoDataFrame, {original_name: safe_name} mapping dict).
    A mapping CSV is useful to ship alongside the shapefile.
    """
    import unicodedata
    import re as _re

    def _to_ascii(s):
        nfd = unicodedata.normalize("NFD", str(s))
        ascii_s = "".join(c for c in nfd if not unicodedata.combining(c))
        ascii_s = _re.sub(r"[^\w]", "_", ascii_s)
        ascii_s = _re.sub(r"_+", "_", ascii_s).strip("_")
        return ascii_s or "field"

    mapping = {}
    seen    = {}
    rename  = {}

    for col in gdf.columns:
        if col == "geometry":
            continue
        base = _to_ascii(col)[:10]
        n = seen.get(base, 0)
        seen[base] = n + 1
        if n == 0:
            safe = base
        else:
            suffix = f"_{n}"
            safe = base[: 10 - len(suffix)] + suffix
        mapping[col] = safe
        rename[col]  = safe

    return gdf.rename(columns=rename), mapping


def _make_serializable(obj):
    """Recursively convert numpy/pandas types to JSON-serializable Python types."""
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_serializable(v) for v in obj]
    # NaN check must come BEFORE numpy type conversion, otherwise
    # np.floating NaN gets converted to float('nan') and returned early.
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, np.ndarray):
        return _make_serializable(obj.tolist())
    # Handle pandas Timestamp and datetime objects
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat() if not pd.isna(obj) else None
    if hasattr(obj, 'isoformat'):  # datetime, date, etc.
        return obj.isoformat()
    return obj


def _reorder_summary(result):
    """Reorder summary dict to match COLUMN_ORDER, with translation fallback."""
    translated_order = [RESULT_TRANSLATIONS.get(k, k) for k in COLUMN_ORDER]
    ordered_pairs = []
    result_copy = dict(result)
    for key in translated_order:
        if key in result_copy:
            ordered_pairs.append([key, result_copy.pop(key)])
    for key, value in result_copy.items():
        ordered_pairs.append([key, value])
    return ordered_pairs

def _translate_summary(summary, translations):
    """Translate French result keys to the configured language."""
    if not translations:
        return summary
    return [[translations.get(k, k), v] for k, v in summary]

def _translate_columns_detail(summary, translations):
    """Translate nested column table headers inside the Colonnes entry."""
    if not translations:
        return summary
    translated = []
    for k, v in summary:
        tk = translations.get(k, k)
        # Translate nested table headers
        if isinstance(v, dict) and v.get("_table") and v.get("data"):
            new_data = []
            for row in v["data"]:
                new_row = {translations.get(col, col): val for col, val in row.items()}
                new_data.append(new_row)
            v = {"_table": True, "data": new_data}
        translated.append([tk, v])
    return translated
def _translate_row_dict(row, translations):
    """Translate French keys in a summary row dict to the configured language."""
    if not translations:
        return row
    return {translations.get(k, k): v for k, v in row.items()}

def _translate_values(summary, translations):
    """Translate plain string values that are themselves translation keys."""
    if not translations:
        return summary
    return [[k, translations.get(v, v) if isinstance(v, str) else v] for k, v in summary]   
    
def _extract_map_data():
    """
    Extract geographic data for map visualization using the stored GeoDataFrame.
    Returns GeoJSON-like structure with dataset extent.
    """
    try:
        # Use the GeoDataFrame created during inspection
        if inspector.last_gdf is None or inspector.last_gdf.empty:
            return None

        gdf = inspector.last_gdf
        print(f"[DEBUG] last_gdf.crs = {gdf.crs}, bounds = {gdf.total_bounds}")

        # Ensure CRS is setguess_crs_from_bounds_duckdb 
        if gdf.crs is None:
            return None

        # Bounds must come from the FULL GeoDataFrame, not the sample.
        # For time-series data a sample may contain only one unique location
        # (e.g. a single monitoring station), making bounds collapse to a point.
        gdf_wgs84_full = gdf.to_crs(epsg=4326)
        bounds = gdf_wgs84_full.total_bounds  # [minx, miny, maxx, maxy]

        # Sample only for GeoJSON (keeps response size small)
        sample_size = min(1000, len(gdf))
        gdf_sample_wgs84 = (gdf_wgs84_full.sample(n=sample_size, random_state=42)
                            if len(gdf_wgs84_full) > sample_size
                            else gdf_wgs84_full.copy())

        # Calculate center from full extent
        center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]  # [lat, lon]

        # Convert datetime/timestamp columns to strings for JSON serialization
        for col in gdf_sample_wgs84.columns:
            if col != 'geometry' and pd.api.types.is_datetime64_any_dtype(gdf_sample_wgs84[col]):
                gdf_sample_wgs84[col] = gdf_sample_wgs84[col].astype(str)

        # Convert to GeoJSON (use __geo_interface__ for speed)
        geojson = json.loads(gdf_sample_wgs84.to_json())

        return {
            "center": center,
            "bounds": [[bounds[1], bounds[0]], [bounds[3], bounds[2]]],  # [[south, west], [north, east]]
            "geojson": geojson
        }
    except Exception as e:
        print(f"Error extracting map data: {e}")
        traceback.print_exc()
        return None

@app.route("/")
def index():
    return render_template("index.html", UI=UI)

@app.route("/set_language/<lang>", methods=["POST"])
def set_language(lang):
    global _cfg, UI, RESULT_TRANSLATIONS, GEO_KEY_PATTERNS, WGS84_BOUNDS, METRIC_CRS
    _loc_params = get_localisation_params(_cfg)
    WGS84_BOUNDS = _loc_params["wgs84_bounds"]
    METRIC_CRS = _loc_params["metric_crs"]
    
    if lang not in ("fr", "en"):
        return jsonify({"ok": False, "error": "Unsupported language"}), 400
    try:
        _cfg["language"] = lang
        UI = get_ui_labels(_cfg)
        RESULT_TRANSLATIONS = get_result_key_translations(_cfg)
        # Also write to config.yaml so it persists across restarts
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                current = yaml.safe_load(f) or {}
            current["language"] = lang
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(current, f, allow_unicode=True, default_flow_style=False)
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500
        
    
def _inspect_any_file_for_batch(filepath):
    """
    Inspect a single file (raster or vector) for batch processing.

    Returns (summary_dict, map_data) where:
      - summary_dict : flat/nested dict of metadata (nested values kept as dicts
                       so the batch result viewer can render them as tables)
      - map_data     : raster map dict {type, bounds, thumbnail} or None
    Returns (None, None) on failure.
    """
    ext = os.path.splitext(filepath)[1].lower()
    if ext in RASTER_EXTENSIONS:
        from geodata_inspector.raster import inspect_raster
        result = inspect_raster(filepath, gdf_reference=gdf_reference,
                                metric_crs=METRIC_CRS, wgs84_bounds=WGS84_BOUNDS,
                                inferred_label=UI.get("crs_inferred", "inferred"))
        summary = result["summary"]
        flat = {k: v for k, v in summary.items() if not k.startswith("_")}
        return flat, result["map"]
    else:
        # Vector / tabular path
        if hasattr(inspector, 'summary_rows') and inspector.summary_rows:
            inspector.summary_rows.clear()
        inspector.last_gdf = None
        log_capture = StringIO()
        with redirect_stdout(log_capture):
            inspector.inspect_file(filepath, gdf_reference,
                                   geo_key_patterns=GEO_KEY_PATTERNS,
                                   wgs84_bounds=WGS84_BOUNDS,
                                   metric_crs=METRIC_CRS)
        if inspector.summary_rows:
            return dict(inspector.summary_rows[-1]), None
        return None, None


def _handle_raster_upload(filepath, original_filename):
    """Inspect a raster file and return a JSON response compatible with the upload endpoint."""
    from geodata_inspector.raster import inspect_raster

    try:
        result = inspect_raster(
            filepath,
            gdf_reference=gdf_reference,
            metric_crs=METRIC_CRS,
            wgs84_bounds=WGS84_BOUNDS,
            inferred_label=UI.get("crs_inferred", "inferred"),
        )
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"error": f"Raster inspection failed: {exc}"}), 500

    summary_raw = result["summary"]
    summary_raw["Nom du fichier"] = original_filename

    # Order keys according to RASTER_COLUMN_ORDER
    ordered = []
    raw_copy = dict(summary_raw)
    for key in RASTER_COLUMN_ORDER:
        if key in raw_copy:
            ordered.append([key, raw_copy.pop(key)])
    for key, val in raw_copy.items():
        if not key.startswith("_"):   # skip internal flags
            ordered.append([key, val])

    return jsonify({
        "summary":  _make_serializable(ordered),
        "datetime": None,
        "map":      _make_serializable(result["map"]),
        "logs":     [],
        "glossary": {},
        "glossary_translated": {},
        "raster":   True,
    })


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Empty filename."}), 400

    ext = os.path.splitext(file.filename)[-1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({
            "error": f"Unsupported file type '{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        }), 400

    # Create a temporary directory for this upload
    tmpdir = tempfile.mkdtemp(prefix="geodata_inspect_")

    try:
        saved_path = os.path.join(tmpdir, file.filename)
        print(saved_path)
        file.save(saved_path)
        # Save any sidecar files (e.g. .shx, .dbf, .prj for shapefiles)
        for sidecar in request.files.getlist('sidecars'):
            sidecar_path = os.path.join(tmpdir, sidecar.filename)
            sidecar.save(sidecar_path)
            
        # Save preview for later export (use secure name on disk; keep original for download)
        from werkzeug.utils import secure_filename as _secure
        _safe_name   = _secure(file.filename) or f"upload{ext}"
        preview_copy = os.path.join(PREVIEW_DIR, _safe_name)
        shutil.copy2(saved_path, preview_copy)
        # Strip extension for use as download basename (original name, readable)
        _orig_stem = os.path.splitext(file.filename)[0]
        last_preview_path["path"]          = preview_copy
        last_preview_path["ext"]           = ext
        last_preview_path["original_stem"] = _orig_stem
        
        # If zip, extract and find the data file inside
        filepath_to_inspect = saved_path
        if ext == ".zip":
            filepath_to_inspect = _handle_zip(saved_path, tmpdir)
            if filepath_to_inspect is None:
                return jsonify({
                    "error": "ZIP archive does not contain a supported data file (CSV, XLSX, GeoJSON, SHP, GPKG, GeoTIFF, NC…)."
                }), 400

        # ── Branch: raster vs vector/tabular ─────────────────────────────
        actual_ext = os.path.splitext(filepath_to_inspect)[1].lower()
        if actual_ext in RASTER_EXTENSIONS:
            return _handle_raster_upload(filepath_to_inspect, file.filename)

        if hasattr(inspector, 'summary_rows') and inspector.summary_rows:
            inspector.summary_rows.clear()
        inspector.last_gdf = None

        # Capture stdout from inspection process
        log_capture = StringIO()
        with redirect_stdout(log_capture):
            inspector.inspect_file(filepath_to_inspect, gdf_reference,
                        geo_key_patterns=GEO_KEY_PATTERNS,
                        wgs84_bounds=WGS84_BOUNDS,
                        metric_crs=METRIC_CRS)

        # Get captured logs
        logs = log_capture.getvalue()
        log_lines = [line for line in logs.split('\n') if line.strip()]

        if not inspector.summary_rows:
            return jsonify({"error": "Inspection produced no results. The file may be empty or unreadable."}), 400

        result = inspector.summary_rows[-1]

        # Override folder/filename to show the original upload name (not the tmp path)
        result["Nom du fichier"] = file.filename

        # Reorder the summary to match COLUMN_ORDER
        ordered_result = _translate_values(
            _translate_columns_detail(
                _reorder_summary(result), RESULT_TRANSLATIONS
            ), RESULT_TRANSLATIONS
        )


        # Extract map data from the stored GeoDataFrame
        map_data = _extract_map_data()

        # Extract datetime analysis out of summary for separate rendering
        dt_analysis = None
        ordered_result_clean = []
        for k, v in ordered_result:
            if k == "Analyse temporelle":
                dt_analysis = v
            else:
                ordered_result_clean.append([k, v])

        response = {
            "summary": _make_serializable(ordered_result_clean),
            "datetime": _make_serializable(dt_analysis) if dt_analysis else None,
            "map": _make_serializable(map_data) if map_data else None,
            "logs": log_lines,
            "glossary": UI.get("glossary", {}),
            "glossary_translated": {
                RESULT_TRANSLATIONS.get(k, k): v
                for k, v in UI.get("glossary", {}).items()
            },
        }

        # Add warning for large files using metadata size
        file_size_kb = result.get("Taille (Ko)", 0)
        file_size_mb = file_size_kb / 1024
        is_large_file = (ext == ".xlsx" and file_size_mb > 10) or file_size_mb > 50

        if is_large_file:
            response["warning"] = f"Fichier volumineux ({file_size_mb:.1f} MB) - le traitement peut prendre jusqu'à une minute."

        return jsonify(response)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _handle_zip(zip_path, extract_dir):
    """
    Extract a ZIP and return either:
    - A single file path (if zip contains one supported file, or one dominant data file)
    - A directory path prefixed with 'DIR:' (if zip contains multiple data files → treat as batch)
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    GEO_EXTS    = {".shp", ".gpkg", ".geojson", ".json"}
    TABLE_EXTS  = {".csv", ".txt", ".xlsx"}
    RSTR_EXTS   = RASTER_EXTENSIONS          # {".tif", ".tiff", ".img", ".jp2", ".asc", ".hgt", ".nc"}
    ALL_EXTS    = GEO_EXTS | TABLE_EXTS | RSTR_EXTS

    candidates = []
    for root, _, files in os.walk(extract_dir):
        for fname in files:
            if fname.startswith("._"):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext in ALL_EXTS:
                full_path = os.path.join(root, fname)
                candidates.append((ext, os.path.getsize(full_path), full_path))

    if not candidates:
        return None

    # Single file → read it directly regardless of type
    if len(candidates) == 1:
        return candidates[0][2]

    # Multiple files → check if there is one dominant geospatial / raster file
    geo_files = [c for c in candidates if c[0] in GEO_EXTS | RSTR_EXTS]
    if len(geo_files) == 1:
        return geo_files[0][2]

    # Shapefile components (.shp + .shx + .dbf) count as one file
    shp_files = [c for c in candidates if c[0] == ".shp"]
    if len(shp_files) == 1:
        return shp_files[0][2]

    # Multiple real data files → treat as a batch directory
    return f"DIR:{extract_dir}"
    
@app.route("/preview", methods=["GET"])
def preview():
    """Return the first 10 rows of the last uploaded file as JSON."""
    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext")

    if not path or not os.path.exists(path):
        return jsonify({"error": "No file available for preview."}), 404

    try:
        if ext in [".csv", ".txt"]:
            df = pd.read_csv(path, nrows=10, sep=None, engine="python", encoding_errors="replace")
        elif ext == ".xlsx":
            df = pd.read_excel(path, nrows=10)
        elif ext in [".geojson", ".json", ".shp", ".gpkg"]:
            gdf = gpd.read_file(path, rows=10)
            df = pd.DataFrame(gdf.drop(columns="geometry", errors="ignore"))
        else:
            return jsonify({"error": f"Preview not supported for {ext}"}), 400

        df = df.where(pd.notnull(df), None)
        df.columns = [str(c) for c in df.columns]

        return jsonify({
            "columns": df.columns.tolist(),
            "rows": _make_serializable(df.values.tolist())
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
        
@app.route("/preview_filtered", methods=["POST"])
def preview_filtered():
    """
    Return up to 10 rows of the current vector file after applying a spatial filter.
    Same response format as /preview, plus total_filtered and total_before counts.
    """
    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext", "")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No file available for preview."}), 404
    if ext.lower() in RASTER_EXTENSIONS:
        return jsonify({"error": "Spatial filter not available for raster files."}), 400

    body           = request.get_json(silent=True) or {}
    spatial_filter = body.get("spatial_filter")

    try:
        gdf = _read_full_gdf(path, ext)
        if gdf is None:
            return jsonify({"error": "Could not read file."}), 500

        total_before   = len(gdf)
        total_filtered = total_before

        if spatial_filter and hasattr(gdf, "crs") and gdf.crs is not None:
            geom_wgs84 = _build_filter_geom_wgs84(spatial_filter)
            if geom_wgs84:
                ref_proj = gpd.GeoDataFrame(geometry=[geom_wgs84], crs="EPSG:4326").to_crs(gdf.crs)
                try:
                    mask = gdf.geometry.intersects(ref_proj.geometry.iloc[0])
                    gdf  = gdf[mask.fillna(False)]
                except Exception:
                    pass
            total_filtered = len(gdf)

        geo_col = gdf.geometry.name if hasattr(gdf, "geometry") and gdf.geometry is not None else None
        df = pd.DataFrame(gdf.head(10).drop(columns=[geo_col], errors="ignore") if geo_col else gdf.head(10))
        df = df.where(pd.notnull(df), None)
        df.columns = [str(c) for c in df.columns]

        return jsonify({
            "columns":        df.columns.tolist(),
            "rows":           _make_serializable(df.values.tolist()),
            "total_filtered": total_filtered,
            "total_before":   total_before,
        })

    except Exception as exc:
        traceback.print_exc()
        return jsonify({"error": str(exc)}), 500


@app.route("/batch", methods=["POST"])
def batch_upload():
    """Accept a directory zip or multiple files and process them all."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400

    file = request.files["file"]
    ext  = os.path.splitext(file.filename)[-1].lower()

    if ext != ".zip":
        return jsonify({"error": "Batch mode requires a ZIP archive containing your dataset files."}), 400

    if batch_state["running"]:
        return jsonify({"error": "A batch job is already running. Please wait."}), 409

    # Clean up previous batch tmpdir if it exists
    old_tmpdir = batch_state.get("tmpdir")
    if old_tmpdir and os.path.exists(old_tmpdir):
        shutil.rmtree(old_tmpdir, ignore_errors=True)
        batch_state["tmpdir"] = None
        
    tmpdir = tempfile.mkdtemp(prefix="geodata_batch_")
    zip_path = os.path.join(tmpdir, file.filename)
    file.save(zip_path)

    # Detect contents
    result = _handle_zip(zip_path, tmpdir)

    if result is None:
        shutil.rmtree(tmpdir, ignore_errors=True)
        return jsonify({"error": "ZIP contains no supported data files."}), 400

    if not result.startswith("DIR:"):
        # Single file inside zip — redirect to normal single-file inspection
        shutil.rmtree(tmpdir, ignore_errors=True)
        return jsonify({"error": "ZIP contains only one file. Please use the standard upload button instead."}), 400

    data_dir = result[4:]  # strip "DIR:" prefix

    # Collect all files (vector + raster)
    ALL_EXTS = VECTOR_EXTENSIONS | RASTER_EXTENSIONS
    files_to_process = []
    for root, _, files_in_dir in os.walk(data_dir):
        for fname in sorted(files_in_dir):
            if fname.startswith("._"):
                continue
            if os.path.splitext(fname)[1].lower() in ALL_EXTS:
                files_to_process.append(os.path.join(root, fname))

    if not files_to_process:
        shutil.rmtree(tmpdir, ignore_errors=True)
        return jsonify({"error": "No supported files found in the ZIP (CSV, XLSX, GeoJSON, SHP, GPKG, GeoTIFF, NC…)."}), 400

    # Reset batch state
    batch_state.update({
        "running": True,
        "total": len(files_to_process),
        "done": 0,
        "current": "",
        "errors": [],
        "logs": [],
        "output_path": None,
        "tmpdir": tmpdir,
        "gdfs": {},
        "raster_maps": {},
        "stop_requested": False,

    })
    batch_state["file_paths"] = {os.path.basename(f): f for f in files_to_process}


    output_csv = os.path.join(PREVIEW_DIR, f"batch_results_{os.path.basename(zip_path)}.csv")

    def run_batch():
        try:
            all_rows = []
            for filepath in files_to_process:
                if batch_state.get("stop_requested"):
                    break
                batch_state["current"] = os.path.basename(filepath)
                try:
                    row_dict, raster_map = _inspect_any_file_for_batch(filepath)
                    if row_dict:
                        fname = os.path.basename(filepath)
                        row_dict["_tmpname"] = fname
                        if raster_map:
                            batch_state["raster_maps"][fname] = raster_map
                        elif getattr(inspector, 'last_gdf', None) is not None:
                            batch_state["gdfs"][fname] = inspector.last_gdf.copy()
                        all_rows.append(_translate_row_dict(row_dict, RESULT_TRANSLATIONS))
                        df_out = pd.DataFrame(all_rows)
                        for col in df_out.columns:
                            if df_out[col].dtype == object:
                                df_out[col] = df_out[col].apply(
                                    lambda v: json.dumps(v, ensure_ascii=False)
                                    if isinstance(v, dict) else v
                                )
                        df_out.to_csv(output_csv, index=False, encoding="utf-8-sig")
                        batch_state["output_path"] = output_csv
                except Exception as e:
                    batch_state["errors"].append(f"{os.path.basename(filepath)}: {str(e)}")
                finally:
                    batch_state["done"] += 1

        except Exception as e:
            batch_state["errors"].append(f"Fatal: {str(e)}")
        finally:
            batch_state["running"] = False

    batch_executor.submit(run_batch)

    return jsonify({
        "message": f"Batch started: {len(files_to_process)} files to process.",
        "total": len(files_to_process)
    })


@app.route("/batch/status", methods=["GET"])
def batch_status():
    """Poll batch job progress."""
    return jsonify({
            "running":    batch_state["running"],
            "total":      batch_state["total"],
            "done":       batch_state["done"],
            "current":    batch_state["current"],
            "errors":     batch_state["errors"],
            "logs":       batch_state["logs"],
            "finished":   not batch_state["running"] and batch_state["done"] > 0,
            "has_output": batch_state["output_path"] is not None,
        })


@app.route("/batch/download", methods=["GET"])
def batch_download():
    """Download the batch results CSV."""
    path = batch_state.get("output_path")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No batch results available."}), 404
    from flask import send_file
    return send_file(path, mimetype="text/csv",
                     as_attachment=True, download_name="geodata_batch_results.csv")
    
@app.route("/batch/download/excel", methods=["GET"])
def batch_download_excel():
    """Download the batch results as Excel."""
    path = batch_state.get("output_path")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No batch results available."}), 404
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        excel_path = path.replace(".csv", ".xlsx")
        df.to_excel(excel_path, index=False)
        from flask import send_file
        return send_file(excel_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True, download_name="geodata_batch_results.xlsx")
    except Exception as e:
        return jsonify({"error": str(e)}), 500    

@app.route("/batch/result/<int:index>", methods=["GET"])
def batch_result(index):
    """Return a single batch result by index, re-inspecting for map data."""
    path = batch_state.get("output_path")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No batch results available."}), 404
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        if index < 0 or index >= len(df):
            return jsonify({"error": "Index out of range."}), 404

        row = df.iloc[index].to_dict()

        # Parse JSON strings back to objects (dicts and lists)
        for k in list(row.keys()):
            v = row[k]
            if isinstance(v, str):
                s = v.strip()
                if s.startswith(('{', '[')):
                    try:
                        row[k] = json.loads(s)
                    except Exception:
                        pass

        ordered = _translate_values(
            _translate_columns_detail(
                _reorder_summary(row), RESULT_TRANSLATIONS
            ), RESULT_TRANSLATIONS
        )
        # Get map data from GDF stored during batch processing
        map_data = None
        tmpname = row.get("_tmpname", "")
        filename_key = RESULT_TRANSLATIONS.get("Nom du fichier", "Nom du fichier")
        display_filename = row.get(filename_key, row.get("Nom du fichier", f"File {index+1}"))

        # Raster map takes priority (thumbnail + extent); fall back to vector GDF
        raster_map = batch_state.get("raster_maps", {}).get(tmpname)
        if raster_map:
            map_data = raster_map
        else:
            stored_gdf = batch_state.get("gdfs", {}).get(tmpname)
            if stored_gdf is not None and not stored_gdf.empty:
                try:
                    inspector.last_gdf = stored_gdf
                    map_data = _extract_map_data()
                except Exception:
                    traceback.print_exc()
                finally:
                    inspector.last_gdf = None

        # Extract datetime analysis out of summary for separate rendering
        dt_analysis = None
        ordered_clean = []
        for k, v in ordered:
            if k == "Analyse temporelle":
                dt_analysis = v
            else:
                ordered_clean.append([k, v])

        return jsonify({
            "index": index,
            "total": len(df),
            "filename": display_filename,
            "summary": _make_serializable(ordered_clean),
            "datetime": _make_serializable(dt_analysis) if dt_analysis else None,
            "map": map_data,
            "glossary": UI.get("glossary", {}),
            "glossary_translated": {
                RESULT_TRANSLATIONS.get(k, k): v
                for k, v in UI.get("glossary", {}).items()
            },
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/batch_dir", methods=["POST"])
def batch_dir():
    """Accept multiple uploaded files directly (from webkitdirectory picker)."""
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files received."}), 400

    if batch_state["running"]:
        return jsonify({"error": "A batch job is already running."}), 409

    PRIMARY_EXTS = VECTOR_EXTENSIONS | RASTER_EXTENSIONS   # all supported data files
    SIDECAR_EXTS = {".shx", ".dbf", ".prj", ".cpg", ".qpj"}

    # Clean up previous batch tmpdir if it exists
    old_tmpdir = batch_state.get("tmpdir")
    if old_tmpdir and os.path.exists(old_tmpdir):
        shutil.rmtree(old_tmpdir, ignore_errors=True)
        batch_state["tmpdir"] = None

    # Save all files to a temp dir, skipping hidden/config files
    tmpdir = tempfile.mkdtemp(prefix="geodata_batchdir_")
    saved_paths = []
    for f in files:
        basename = os.path.basename(f.filename)
        ext = os.path.splitext(basename)[1].lower()

        # Skip hidden, system and config files
        if basename.startswith('.') or basename.startswith('__'):
            continue
        if any(pat in basename for pat in ['.local.', '.config.', '.settings.']):
            continue

        safe_name = f.filename.replace("\\", "/").lstrip("/").replace("/", "_")
        dest = os.path.join(tmpdir, safe_name)
        f.save(dest)

        # Only add primary data files to processing list (not sidecars)
        if ext in PRIMARY_EXTS:
            saved_paths.append(dest)

    if not saved_paths:
        shutil.rmtree(tmpdir, ignore_errors=True)
        return jsonify({"error": "Aucun fichier de données compatible trouvé."}), 400

    batch_state.update({
        "running": True,
        "total": len(saved_paths),
        "done": 0,
        "current": "",
        "errors": [],
        "logs": [],
        "output_path": None,
        "tmpdir": tmpdir,
        "gdfs": {},
        "raster_maps": {},
        "stop_requested": False,
    })
    batch_state["file_paths"] = {os.path.basename(f): f for f in saved_paths}

    output_csv = os.path.join(PREVIEW_DIR, "batch_dir_results.csv")

    def run_batch():
        try:
            all_rows = []
            for filepath in saved_paths:
                if batch_state.get("stop_requested"):
                    break
                batch_state["current"] = os.path.basename(filepath)
                try:
                    row_dict, raster_map = _inspect_any_file_for_batch(filepath)
                    if row_dict:
                        fname = os.path.basename(filepath)
                        row_dict["_tmpname"] = fname
                        if raster_map:
                            batch_state["raster_maps"][fname] = raster_map
                        elif getattr(inspector, 'last_gdf', None) is not None:
                            batch_state["gdfs"][fname] = inspector.last_gdf.copy()
                        all_rows.append(_translate_row_dict(row_dict, RESULT_TRANSLATIONS))
                        df_out = pd.DataFrame(all_rows)
                        for col in df_out.columns:
                            if df_out[col].dtype == object:
                                df_out[col] = df_out[col].apply(
                                    lambda v: json.dumps(v, ensure_ascii=False)
                                    if isinstance(v, dict) else v
                                )
                        df_out.to_csv(output_csv, index=False, encoding="utf-8-sig")
                        batch_state["output_path"] = output_csv
                except Exception as e:
                    batch_state["errors"].append(f"{os.path.basename(filepath)}: {str(e)}")
                finally:
                    batch_state["done"] += 1
        except Exception as e:
            batch_state["errors"].append(f"Fatal: {str(e)}")
        finally:
            batch_state["running"] = False

    batch_executor.submit(run_batch)
    return jsonify({
        "message": f"Batch started: {len(saved_paths)} files.",
        "total": len(saved_paths)
    })
def _df_to_duckdb(df):
    """Register a DataFrame in a fresh DuckDB connection and return the connection."""
    import duckdb
    conn = duckdb.connect()
    conn.register("excel_tbl", df)
    return conn
    
@app.route("/datetime_analysis", methods=["GET"])
def datetime_analysis():
    """Return the datetime analysis for the last inspected file."""
    if not inspector.summary_rows:
        return jsonify({"error": "No result available."}), 404
    result = inspector.summary_rows[-1]
    dt = result.get("Analyse temporelle")
    if not dt:
        return jsonify({"available": False, "message": "No date columns detected."})
    return jsonify({"available": True, **_make_serializable(dt)})


@app.route("/temporal_plot", methods=["POST"])
def temporal_plot():
    """
    Compute presence/occupancy of a column over time by re-reading the cached file.

    JSON body:
      date_col     : start (or single) date column  (required)
      end_date_col : end date column — if set, uses occupancy-curve mode (optional)
      value_col    : column whose non-null presence to measure; null = all rows
      date_start   : ISO date lower bound  (optional)
      date_end     : ISO date upper bound  (optional)
      granularity  : "year"|"month"|"day"  (optional, null = auto)
    """
    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext", "")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No file available. Upload a file first."}), 404

    body         = request.get_json(silent=True) or {}
    date_col     = body.get("date_col")
    end_date_col = body.get("end_date_col") or None
    value_col    = body.get("value_col") or None
    aggregation  = body.get("aggregation", "presence")
    date_start   = body.get("date_start") or None
    date_end     = body.get("date_end")   or None
    granularity  = body.get("granularity") or None

    if not date_col:
        return jsonify({"error": "date_col is required"}), 400
    if granularity not in (None, "year", "month", "day"):
        return jsonify({"error": "granularity must be 'year', 'month', 'day', or null"}), 400
    if aggregation not in ("presence", "mean", "sum", "min", "max"):
        return jsonify({"error": "aggregation must be one of: presence, mean, sum, min, max"}), 400

    raw_formats = (inspector.last_dt_raw or {}).get("raw_formats", {})

    result = inspector.compute_column_occupancy(
        filepath=path, file_ext=ext,
        date_col=date_col, end_date_col=end_date_col,
        value_col=value_col, aggregation=aggregation,
        date_start=date_start, date_end=date_end,
        granularity=granularity, raw_formats=raw_formats,
    )
    if result is None:
        return jsonify({"error": "Could not compute temporal chart. Check column names and date format."}), 500

    return jsonify(_make_serializable(result))


@app.route("/temporal_filter", methods=["POST"])
def temporal_filter():
    """
    Re-compute temporal charts from cached raw data with optional filters.

    Expected JSON body:
      {
        "columns":    ["col1", "col2"],   // null or omitted = all columns
        "date_start": "2010-01-01",       // null or omitted = no lower bound
        "date_end":   "2020-12-31",       // null or omitted = no upper bound
        "granularity": "month"            // "year"|"month"|"day"|null = auto
      }
    """
    if inspector.last_dt_raw is None:
        return jsonify({"error": "No temporal data cached. Upload a file first."}), 404

    body = request.get_json(silent=True) or {}
    columns     = body.get("columns")      or None
    date_start  = body.get("date_start")   or None
    date_end    = body.get("date_end")     or None
    granularity = body.get("granularity")  or None

    if granularity not in (None, "year", "month", "day"):
        return jsonify({"error": "granularity must be 'year', 'month', 'day', or null"}), 400

    result = inspector.recompute_temporal_charts(
        columns=columns,
        date_start=date_start,
        date_end=date_end,
        granularity=granularity,
    )
    if result is None:
        return jsonify({"error": "Re-computation failed (no cached data)."}), 500

    return jsonify(_make_serializable(result))


@app.route("/export_info", methods=["GET"])
def export_info():
    """Return available export formats, column/band list, and date range for the current file."""
    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext", "")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No file loaded."}), 404

    is_raster  = ext.lower() in RASTER_EXTENSIONS
    has_geometry = inspector.last_gdf is not None if not is_raster else False

    # ── Format availability ───────────────────────────────────────────────
    vector_fmts = {
        "geojson":     {"available": not is_raster and has_geometry, "label": "GeoJSON",       "ext": ".geojson"},
        "gpkg":        {"available": not is_raster and has_geometry, "label": "GeoPackage",     "ext": ".gpkg"},
        "shp":         {"available": not is_raster and has_geometry, "label": "Shapefile (.zip)","ext": ".zip"},
        "geoparquet":  {"available": not is_raster and has_geometry, "label": "GeoParquet",     "ext": ".parquet"},
        "csv":         {"available": not is_raster,                  "label": "CSV + WKT",      "ext": ".csv"},
    }
    raster_fmts = {k: {"available": is_raster and v, "label": lbl, "ext": e}
                   for (k, v), lbl, e in zip(
                       RASTER_WRITE_DRIVERS.items(),
                       ["GeoTIFF", "NetCDF", "JPEG2000", "ERDAS Imagine", "ASCII Grid"],
                       [".tif", ".nc", ".jp2", ".img", ".asc"]
                   )}

    # ── Column / band list ────────────────────────────────────────────────
    columns, bands = [], []
    if is_raster:
        # Get band names from last inspection summary
        if inspector.summary_rows:
            last = inspector.summary_rows[-1]
            bandes = last.get("Bandes", {})
            if isinstance(bandes, dict) and "data" in bandes:
                bands = [{"index": i + 1, "name": b.get("Bande", f"Band {i+1}")}
                         for i, b in enumerate(bandes["data"])]
        if not bands:
            bands = [{"index": 1, "name": "Band 1"}]
    else:
        # Priority 1: all_columns from last_dt_raw — populated during every DuckDB inspection
        # and contains ALL columns from the original file schema (not just geometry columns).
        if inspector.last_dt_raw and inspector.last_dt_raw.get("all_columns"):
            columns = [c["name"] for c in inspector.last_dt_raw["all_columns"]]
        # Priority 2: Colonnes table from summary_rows (French or English key)
        elif inspector.summary_rows:
            last     = inspector.summary_rows[-1]
            col_raw  = last.get("Colonnes", {})
            if isinstance(col_raw, dict) and "data" in col_raw:
                data = col_raw["data"]
                if data:
                    name_key = list(data[0].keys())[0]   # first key = column name
                    columns  = [row.get(name_key, "") for row in data if row.get(name_key)]
        # Priority 3: GeoDataFrame columns (may be incomplete for lat/lon CSVs)
        elif inspector.last_gdf is not None:
            columns = [c for c in inspector.last_gdf.columns if c != "geometry"]

    # ── Date range (for temporal filter) ─────────────────────────────────
    date_info = None
    if not is_raster and inspector.last_dt_raw:
        all_date_cols = inspector.last_dt_raw.get("all_date_cols", [])
        singles       = inspector.last_dt_raw.get("singles", {})
        if all_date_cols:
            # Global min/max across all detected date columns
            g_min, g_max = None, None
            for raw in singles.values():
                d_min = raw.get("global_min", "")[:10]
                d_max = raw.get("global_max", "")[:10]
                if d_min and (g_min is None or d_min < g_min): g_min = d_min
                if d_max and (g_max is None or d_max > g_max): g_max = d_max
            date_info = {
                "column":      all_date_cols[0],
                "global_min":  g_min or "",
                "global_max":  g_max or "",
                "all_columns": all_date_cols,
            }

    return jsonify({
        "is_raster":   is_raster,
        "has_geometry": has_geometry,
        "formats": {**vector_fmts, **raster_fmts},
        "columns":     columns,
        "bands":       bands,
        "date_info":   date_info,
    })


# ── Helpers for /export_filtered ─────────────────────────────────────────

def _utm_epsg(lat, lon):
    zone = int((lon + 180) / 6) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone


def _build_filter_geom_wgs84(spatial_filter):
    """Return a Shapely geometry in WGS84 from a spatial filter spec."""
    from shapely.geometry import Point, shape
    from shapely.ops import transform as shp_transform
    import pyproj

    sf_type = spatial_filter.get("type")
    if sf_type == "buffer":
        lat, lon = spatial_filter["lat"], spatial_filter["lon"]
        radius_m = float(spatial_filter.get("radius_km", 1)) * 1000
        utm = _utm_epsg(lat, lon)
        fwd = pyproj.Transformer.from_crs("EPSG:4326", f"EPSG:{utm}", always_xy=True).transform
        inv = pyproj.Transformer.from_crs(f"EPSG:{utm}", "EPSG:4326", always_xy=True).transform
        center_m = shp_transform(fwd, Point(lon, lat))
        return shp_transform(inv, center_m.buffer(radius_m))
    elif sf_type == "polygon":
        return shape(spatial_filter["geojson"])
    return None


def _read_full_gdf(path, ext):
    """
    Re-read the full dataset as a GeoDataFrame for export.
    Does not call internal inspection helpers — reads the file directly.
    """
    try:
        # ── Native geo formats ────────────────────────────────────────────
        if ext in (".geojson", ".json", ".shp", ".gpkg"):
            return gpd.read_file(path)

        # ── Tabular formats ───────────────────────────────────────────────
        if ext == ".xlsx":
            df = pd.read_excel(path)
        elif ext in (".csv", ".txt"):
            # Detect encoding
            with open(path, "rb") as _f:
                raw = _f.read(8192)
            try:
                import chardet as _cd
                enc = _cd.detect(raw).get("encoding") or "utf-8"
            except Exception:
                enc = "utf-8"
            # Detect separator
            try:
                first = raw.decode(enc, errors="replace")
            except Exception:
                first = raw.decode("utf-8", errors="replace")
            sep = ";" if first.count(";") > first.count(",") else ","
            df = pd.read_csv(path, sep=sep, encoding=enc, encoding_errors="replace")
        else:
            return None

        # ── Reconstruct geometry from lat/lon if detected during inspection ─
        src_crs = None
        if inspector.last_gdf is not None and inspector.last_gdf.crs is not None:
            src_crs = inspector.last_gdf.crs

        _LAT = {"latitude", "lat", "y", "ylat", "ycoord", "y_coord", "y_wgs84"}
        _LON = {"longitude", "lon", "lng", "x", "xlon", "xcoord", "x_coord", "x_wgs84"}

        lat_col = next((c for c in df.columns if c.strip().lower() in _LAT), None)
        lon_col = next((c for c in df.columns if c.strip().lower() in _LON), None)

        if lat_col and lon_col:
            lats = pd.to_numeric(df[lat_col], errors="coerce")
            lons = pd.to_numeric(df[lon_col], errors="coerce")
            geometry = gpd.points_from_xy(lons, lats)
            return gpd.GeoDataFrame(df, geometry=geometry,
                                    crs=src_crs or "EPSG:4326")

        # No geometry found — return plain GeoDataFrame (CSV export still works)
        return gpd.GeoDataFrame(df)

    except Exception as exc:
        print(f"[Export] _read_full_gdf failed: {exc}")
        traceback.print_exc()
        return None


@app.route("/export_filtered", methods=["POST"])
def export_filtered():
    """
    Export the current file with optional filters and format conversion.

    JSON body:
      format        : export format key (geojson, gpkg, shp, geoparquet, csv, tif, nc, jp2, img, asc)
      columns       : list of column names to include (null = all)  — vector only
      bands         : list of band indices [1-based] to include     — raster only
      value_filters : [{column, value}]
      time_filter   : {column, date_start, date_end} or null
      spatial_filter: {type:"buffer", lat, lon, radius_km} or
                      {type:"polygon", geojson:<GeoJSON geometry>} or null
    """
    import csv as _csv

    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext", "")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No file loaded."}), 404

    body         = request.get_json(silent=True) or {}
    fmt          = body.get("format", "geojson").lower()
    columns      = body.get("columns")      or None
    bands        = body.get("bands")        or None
    val_filters  = body.get("value_filters") or []
    time_filter  = body.get("time_filter")  or None
    spatial_filt = body.get("spatial_filter") or None

    is_raster = ext.lower() in RASTER_EXTENSIONS

    import re as _re
    orig_stem   = last_preview_path.get("original_stem") or \
                  os.path.splitext(os.path.basename(path))[0]
    clean_stem  = _clean_download_stem(orig_stem)
    safe_slug   = _re.sub(r"[^\w]", "_", clean_stem).strip("_") or "export"

    try:
        # ── RASTER EXPORT ─────────────────────────────────────────────────
        if is_raster:
            import rasterio
            from geodata_inspector.raster import _resolve_open_path

            driver_map = {"tif": "GTiff", "nc": "netCDF",
                          "jp2": "JP2OpenJPEG", "img": "HFA", "asc": "AAIGrid"}
            ext_map    = {"tif": ".tif", "nc": ".nc",
                          "jp2": ".jp2", "img": ".img", "asc": ".asc"}
            driver     = driver_map.get(fmt, "GTiff")
            out_ext    = ext_map.get(fmt, ".tif")
            export_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_export{out_ext}")

            open_path, _ = _resolve_open_path(path)
            with rasterio.open(open_path) as src:
                band_list = bands if bands else list(range(1, src.count + 1))
                band_list = [b for b in band_list if 1 <= b <= src.count]

                out_meta = src.meta.copy()
                out_meta.update({"driver": driver, "count": len(band_list)})

                if spatial_filt:
                    from rasterio.mask import mask as rio_mask
                    from shapely.geometry import mapping
                    from shapely.ops import transform as shp_transform
                    import pyproj

                    geom_wgs84 = _build_filter_geom_wgs84(spatial_filt)
                    if geom_wgs84 and src.crs:
                        inv = pyproj.Transformer.from_crs(
                            "EPSG:4326", src.crs.to_epsg() or "EPSG:4326",
                            always_xy=True).transform
                        geom_proj = shp_transform(inv, geom_wgs84)
                        out_img, out_tf = rio_mask(src, [mapping(geom_proj)],
                                                   crop=True, indexes=band_list)
                        out_meta.update({"height": out_img.shape[1],
                                         "width":  out_img.shape[2],
                                         "transform": out_tf})
                    else:
                        out_img = src.read(band_list)
                else:
                    out_img = src.read(band_list)

                with rasterio.open(export_path, "w", **out_meta) as dst:
                    dst.write(out_img)

            ext_label = out_ext.lstrip(".")
            mime      = "application/octet-stream"
            dl_name   = f"{clean_stem}.{ext_label}"

        # ── VECTOR EXPORT ─────────────────────────────────────────────────
        else:
            gdf = _read_full_gdf(path, ext)
            if gdf is None:
                return jsonify({"error": "Could not re-read file for export."}), 500

            # 1 — value filters (with operator: =, !=, <, <=, >, >=, contains)
            for f in val_filters:
                col = f.get("column")
                op  = f.get("operator", "=")
                val = f.get("value", "")
                if not col or col not in gdf.columns or val == "":
                    continue
                series = gdf[col]
                # Try numeric comparison first, fall back to string
                try:
                    num_val = float(val)
                    num_ser = pd.to_numeric(series, errors="coerce")
                    if op == "=":        mask = num_ser == num_val
                    elif op == "!=":     mask = num_ser != num_val
                    elif op == "<":      mask = num_ser <  num_val
                    elif op == "<=":     mask = num_ser <= num_val
                    elif op == ">":      mask = num_ser >  num_val
                    elif op == ">=":     mask = num_ser >= num_val
                    elif op == "contains": mask = series.astype(str).str.contains(str(val), case=False, na=False)
                    else:                mask = num_ser == num_val
                except (ValueError, TypeError):
                    s = series.astype(str).str.strip()
                    v = str(val).strip()
                    if op == "=":         mask = s == v
                    elif op == "!=":      mask = s != v
                    elif op == "contains": mask = s.str.contains(v, case=False, na=False)
                    else:                  mask = s == v   # fallback for <,>,<=,>= on strings
                gdf = gdf[mask.fillna(False)]

            # 2 — temporal filter
            if time_filter and time_filter.get("column"):
                tcol = time_filter["column"]
                if tcol in gdf.columns:
                    gdf[tcol] = pd.to_datetime(gdf[tcol], errors="coerce", dayfirst=True)
                    if time_filter.get("date_start"):
                        gdf = gdf[gdf[tcol] >= pd.Timestamp(time_filter["date_start"])]
                    if time_filter.get("date_end"):
                        gdf = gdf[gdf[tcol] <= pd.Timestamp(time_filter["date_end"]
                                                              + " 23:59:59")]

            # 3 — spatial filter
            if spatial_filt and hasattr(gdf, "geometry") and gdf.crs is not None:
                geom_wgs84 = _build_filter_geom_wgs84(spatial_filt)
                if geom_wgs84:
                    ref_gdf = gpd.GeoDataFrame(geometry=[geom_wgs84], crs="EPSG:4326")
                    ref_proj = ref_gdf.to_crs(gdf.crs)
                    gdf = gdf[gdf.geometry.intersects(ref_proj.geometry.iloc[0])]

            # 4 — column selection
            if columns:
                geo_col = gdf.geometry.name if hasattr(gdf, "geometry") else None
                keep = [c for c in columns if c in gdf.columns]
                if geo_col and geo_col not in keep:
                    keep.append(geo_col)
                gdf = gdf[keep]

            # 5 — export
            if fmt == "geojson":
                export_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_filtered.geojson")
                gdf.to_crs(epsg=4326).to_file(export_path, driver="GeoJSON")
                mime, dl_name = "application/geo+json", f"{clean_stem}.geojson"

            elif fmt == "gpkg":
                export_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_filtered.gpkg")
                gdf.to_crs(epsg=2154).to_file(export_path, driver="GPKG", layer=safe_slug[:50])
                mime, dl_name = "application/geopackage+sqlite3", f"{clean_stem}.gpkg"

            elif fmt == "shp":
                shp_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_filtered.shp")
                shp_base = os.path.splitext(shp_path)[0]
                gdf_shp, col_mapping = _sanitize_shp_columns(gdf.to_crs(epsg=2154).copy())
                gdf_shp.to_file(shp_path, driver="ESRI Shapefile", encoding="utf-8")
                with open(shp_base + ".cpg", "w") as _f: _f.write("UTF-8")
                mapping_path = shp_base + "_field_names.csv"
                with open(mapping_path, "w", encoding="utf-8-sig", newline="") as _f:
                    w = _csv.writer(_f)
                    w.writerow(["original_name", "shapefile_name"])
                    for o, s in col_mapping.items(): w.writerow([o, s])
                zip_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_filtered_shp.zip")
                with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for suf in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                        fp = shp_base + suf
                        if os.path.exists(fp): zf.write(fp, os.path.basename(fp))
                    zf.write(mapping_path, f"{safe_slug}_field_names.csv")
                export_path = zip_path
                mime, dl_name = "application/zip", f"{clean_stem}_shp.zip"

            elif fmt == "geoparquet":
                export_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_filtered.parquet")
                gdf.to_crs(epsg=4326).to_parquet(export_path, index=False)
                mime, dl_name = "application/vnd.apache.parquet", f"{clean_stem}.parquet"

            elif fmt == "csv":
                export_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_filtered_geo.csv")
                gdf_out = gdf.to_crs(epsg=4326).copy() if gdf.crs else gdf.copy()
                if hasattr(gdf_out, "geometry") and gdf_out.geometry.name in gdf_out.columns:
                    gdf_out["geometry_wkt"] = gdf_out.geometry.apply(
                        lambda g: g.wkt if g is not None else None)
                    gdf_out = pd.DataFrame(gdf_out.drop(columns=gdf_out.geometry.name))
                gdf_out.to_csv(export_path, index=False, encoding="utf-8-sig")
                mime, dl_name = "text/csv", f"{clean_stem}_geo.csv"

            else:
                return jsonify({"error": f"Unknown format: {fmt}"}), 400

        from flask import send_file
        return send_file(export_path, mimetype=mime, as_attachment=True, download_name=dl_name)

    except Exception as exc:
        traceback.print_exc()
        return jsonify({"error": str(exc)}), 500


@app.route("/export", methods=["GET"])
def export():
    """Export the full dataset as a geospatial file by re-reading the original."""
    fmt  = request.args.get("format", "geojson").lower()
    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext")

    if not path or not os.path.exists(path):
        return jsonify({"error": "No file available for export."}), 404

    SUPPORTED_FORMATS = {"geojson", "gpkg", "shp", "geoparquet", "csv"}
    if fmt not in SUPPORTED_FORMATS:
        return jsonify({"error": f"Unsupported format '{fmt}'. Choose from: {', '.join(sorted(SUPPORTED_FORMATS))}"}), 400

    try:
        # ── Re-read the FULL file ──────────────────────────────────────────
        gdf = None

        if ext in [".geojson", ".json", ".shp", ".gpkg"]:
            # Already a geospatial format — read directly
            gdf = gpd.read_file(path)

        elif ext in [".csv", ".txt"]:
            import duckdb
            conn = duckdb.connect()
            conn.execute("INSTALL spatial; LOAD spatial;")
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{path}')")

            # Detect geometry columns the same way the inspector does
            res_geom = inspector.get_geo_columns_duckdb(conn, "data")

            if res_geom['columns'] and res_geom['method'] == 'points_from_xy':
                x_col, y_col = res_geom['columns']
                # Detect source CRS
                detected_crs = inspector.guess_crs_from_coords_duckdb(conn, "data", x_col, y_col) or 4326
                df = conn.execute("SELECT * FROM data").fetchdf()
                geometry = gpd.points_from_xy(df[x_col], df[y_col])
                gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=f"EPSG:{detected_crs}")

            elif res_geom['columns'] and res_geom['method'] == 'linestring_coords':
                x_start, y_start, x_end, y_end = res_geom['columns']
                detected_crs = inspector.guess_crs_from_coords_duckdb(conn, "data", x_start, y_start) or 4326
                df = conn.execute("SELECT * FROM data").fetchdf()
                from shapely.geometry import LineString
                def make_line(row):
                    try:
                        return LineString([(row[x_start], row[y_start]),
                                           (row[x_end],   row[y_end])])
                    except Exception:
                        return None
                geometry = df.apply(make_line, axis=1)
                gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=f"EPSG:{detected_crs}")

            elif res_geom['columns'] and res_geom['method'] in ['from_wkt', 'geojson', 'geopoint']:
                df = conn.execute("SELECT * FROM data").fetchdf()
                gdf = inspector.create_geodataframe_from_result(df, res_geom)

            conn.close()

        elif ext == ".xlsx":
            # Re-read with pandas then reconstruct geometry
            df = pd.read_excel(path)
            res_geom = inspector.get_geo_columns_duckdb(
                _df_to_duckdb(df), "excel_tbl"
            )
            if res_geom['columns'] and res_geom['method'] == 'points_from_xy':
                x_col, y_col = res_geom['columns']
                import duckdb
                conn = duckdb.connect()
                conn.register("excel_tbl", df)
                detected_crs = inspector.guess_crs_from_coords_duckdb(conn, "excel_tbl", x_col, y_col) or 4326
                conn.close()
                geometry = gpd.points_from_xy(df[x_col], df[y_col])
                gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=f"EPSG:{detected_crs}")
            else:
                gdf = inspector.create_geodataframe_from_result(df, res_geom)

        if gdf is None or gdf.empty:
            return jsonify({"error": "Could not reconstruct geographic data from this file."}), 400

        # Drop rows with null geometry
        gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

        if len(gdf) == 0:
            return jsonify({"error": "No valid geometries found after filtering nulls."}), 400

        # If CRS is still not set, assume WGS84 as fallback
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)

        # Use the original upload filename stem for download names (human-readable).
        # Use an ASCII-safe slug for internal temp-file paths (avoids FS issues).
        import re as _re
        orig_stem  = last_preview_path.get("original_stem") or \
                     os.path.splitext(os.path.basename(path))[0]
        # clean_stem: ASCII-only, readable (diacritics stripped, subscripts normalised)
        clean_stem = _clean_download_stem(orig_stem)
        # safe_slug: filesystem-safe version for internal temp-file names
        safe_slug  = _re.sub(r"[^\w]", "_", clean_stem).strip("_") or "export"

        if fmt == "geojson":
            export_path   = os.path.join(PREVIEW_DIR, f"{safe_slug}_export.geojson")
            gdf.to_crs(epsg=4326).to_file(export_path, driver="GeoJSON")
            mime          = "application/geo+json"
            download_name = f"{clean_stem}.geojson"

        elif fmt == "gpkg":
            export_path   = os.path.join(PREVIEW_DIR, f"{safe_slug}_export.gpkg")
            gdf.to_crs(epsg=2154).to_file(export_path, driver="GPKG", layer=safe_slug[:50])
            mime          = "application/geopackage+sqlite3"
            download_name = f"{clean_stem}.gpkg"

        elif fmt == "shp":
            shp_path  = os.path.join(PREVIEW_DIR, f"{safe_slug}_export.shp")
            shp_base  = os.path.splitext(shp_path)[0]

            gdf_proj = gdf.to_crs(epsg=2154).copy()

            # Cast any object columns that look like dates to proper datetime so
            # fiona writes them as Date fields instead of String.
            for col in gdf_proj.columns:
                if col == "geometry":
                    continue
                if gdf_proj[col].dtype == object:
                    try:
                        converted = pd.to_datetime(gdf_proj[col], dayfirst=True, errors="coerce")
                        # Only replace if most values parsed successfully
                        if converted.notna().mean() > 0.5:
                            gdf_proj[col] = converted
                    except Exception:
                        pass

            # Sanitize column names: strip accents, ASCII-only, max 10 chars, no dupes
            gdf_shp, col_mapping = _sanitize_shp_columns(gdf_proj)

            # Write shapefile with explicit UTF-8 encoding
            gdf_shp.to_file(shp_path, driver="ESRI Shapefile", encoding="utf-8")

            # Write .cpg sidecar so GIS tools know the encoding
            with open(shp_base + ".cpg", "w") as _cpg:
                _cpg.write("UTF-8")

            # Bundle a field-name mapping CSV so users can reconstruct original names
            import csv as _csv
            mapping_path = shp_base + "_field_names.csv"
            with open(mapping_path, "w", encoding="utf-8-sig", newline="") as _mf:
                writer = _csv.writer(_mf)
                writer.writerow(["original_name", "shapefile_name"])
                for orig, safe in col_mapping.items():
                    writer.writerow([orig, safe])

            zip_path = os.path.join(PREVIEW_DIR, f"{safe_slug}_shp.zip")
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for suffix in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                    fp = shp_base + suffix
                    if os.path.exists(fp):
                        zf.write(fp, os.path.basename(fp))
                zf.write(mapping_path, f"{safe_slug}_field_names.csv")

            export_path   = zip_path
            mime          = "application/zip"
            download_name = f"{clean_stem}_shp.zip"

        elif fmt == "geoparquet":
            export_path   = os.path.join(PREVIEW_DIR, f"{safe_slug}_export.parquet")
            # GeoParquet preserves full column names, UTF-8, and all dtypes natively
            gdf.to_crs(epsg=4326).to_parquet(export_path, index=False)
            mime          = "application/vnd.apache.parquet"
            download_name = f"{clean_stem}.parquet"

        elif fmt == "csv":
            export_path   = os.path.join(PREVIEW_DIR, f"{safe_slug}_export_geo.csv")
            gdf_out = gdf.to_crs(epsg=4326).copy()
            gdf_out["geometry_wkt"] = gdf_out.geometry.apply(
                lambda g: g.wkt if g else None)
            pd.DataFrame(gdf_out.drop(columns="geometry")).to_csv(
                export_path, index=False, encoding="utf-8-sig")
            mime          = "text/csv"
            download_name = f"{clean_stem}_geo.csv"

        from flask import send_file
        return send_file(export_path, mimetype=mime,as_attachment=True, download_name=download_name)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/result/download", methods=["GET"])
def result_download():
    """Download the last single-file inspection result as CSV."""
    if not inspector.summary_rows:
        return jsonify({"error": "No result available."}), 404
    try:
        result = dict(inspector.summary_rows[-1])
        # Flatten nested objects to JSON strings for CSV
        flat = {}
        for k, v in result.items():
            if k.startswith("_"):
                continue
            tk = RESULT_TRANSLATIONS.get(k, k)
            if isinstance(v, dict):
                flat[tk] = json.dumps(v, ensure_ascii=False)
            else:
                flat[tk] = v
        df = pd.DataFrame([flat])
        csv_path = os.path.join(PREVIEW_DIR, "single_result.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        filename = result.get("Nom du fichier", "result")
        base = os.path.splitext(os.path.basename(filename))[0]
        from flask import send_file
        return send_file(csv_path, mimetype="text/csv",
                         as_attachment=True,
                         download_name=f"{base}_inspection.csv")
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/batch/stop", methods=["POST"])
def batch_stop():
    """Request the running batch to stop after the current file."""
    batch_state["stop_requested"] = True
    return jsonify({"ok": True})

@app.route("/raster_remap", methods=["POST"])
def raster_remap():
    """Re-inspect the last raster with a user-specified CRS override."""
    from geodata_inspector.raster import inspect_raster

    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext", "")
    if not path or not os.path.exists(path) or ext.lower() not in RASTER_EXTENSIONS:
        return jsonify({"error": "No raster file available."}), 404

    body = request.get_json(silent=True) or {}
    try:
        epsg = int(body.get("epsg", 0))
        if epsg <= 0:
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({"error": "A valid EPSG integer is required."}), 400

    try:
        result = inspect_raster(
            path,
            gdf_reference=gdf_reference,
            metric_crs=METRIC_CRS,
            wgs84_bounds=WGS84_BOUNDS,
            inferred_label=UI.get("crs_inferred", "inferred"),
            crs_override=epsg,
        )
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"error": str(exc)}), 500

    return jsonify(_make_serializable({
        "type":      "raster_extent",
        "bounds":    result["map"]["bounds"],
        "thumbnail": result["map"]["thumbnail"],
        "crs":       result["summary"].get("CRS", f"EPSG:{epsg}"),
    }))


@app.route("/remap", methods=["POST"])
def remap():
    try:
        epsg = int(request.json.get("epsg", 0))
        if epsg <= 0:
            return jsonify({"error": "Invalid EPSG code"}), 400

        gdf = inspector.last_gdf
        if gdf is None or gdf.empty:
            return jsonify({"error": "No geodata loaded"}), 400

        gdf_override = gdf.set_crs(epsg=epsg, allow_override=True).to_crs(epsg=4326)

        bounds = gdf_override.total_bounds
        if not all(np.isfinite(bounds)):
            return jsonify({"error": f"EPSG:{epsg} produced invalid bounds — check the CRS code"}), 400

        center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]

        for col in gdf_override.columns:
            if col != "geometry" and pd.api.types.is_datetime64_any_dtype(gdf_override[col]):
                gdf_override[col] = gdf_override[col].astype(str)

        geojson = json.loads(gdf_override.to_json())

        return jsonify({
            "center": center,
            "bounds": [[bounds[1], bounds[0]], [bounds[3], bounds[2]]],
            "geojson": geojson,
            "epsg": epsg,
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.errorhandler(413)
def request_too_large(e):
    limit = app.config.get("MAX_CONTENT_LENGTH")
    limit_mb = f"{limit // (1024*1024)} MB" if limit else "unknown"
    return jsonify({
        "error": f"File exceeds the configured upload limit ({limit_mb}). "
                 f"Increase max_upload_size_mb in web_app/config.yaml or set it to null for no limit."
    }), 413


if __name__ == "__main__":
    _server = _cfg.get("server", {})
    _host   = _server.get("host", "localhost")
    _port   = int(_server.get("port", 5050))
    _debug  = bool(_server.get("debug", False))
    print("=" * 70)
    print("Geodata Inspector - DuckDB-Optimized Version")
    print("=" * 70)
    print(f"  Localisation : {_ref_info['label']}")
    print(f"  Language     : {_cfg.get('language', 'fr')}")
    print(f"  Server       : http://{_host}:{_port}/")
    print("=" * 70)
    app.run(debug=_debug, host=_host, port=_port)
    
# if __name__ == "__main__":
#     print("=" * 70)
#     print("Geodata Inspector - DuckDB-Optimized Version")
#     print("=" * 70)
#     print("Features:")
#     print("  - Fast CSV/Excel processing with DuckDB")
#     print("  - LineString geometry detection (road networks, etc.)")
#     print("  - Point, LineString, and Polygon support")
#     print("  - French decimal separator handling")
#     print("  - Automatic CRS detection and transformation")
#     print("=" * 70)
#     print("\nStarting web server...")
#     print("Open http://10.149.201.62:5050/ in your browser")
#     print("=" * 70) #http://10.149.201.62/ #127.0.0.1
#     app.run(debug=True, host="10.149.201.62", port=5050)