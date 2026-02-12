import os
import sys
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

# Ensure this directory is on the path so imports resolve
sys.path.insert(0, os.path.dirname(__file__))

# Use DuckDB-optimized inspector for better performance
import inspect_geodata_duckdb as inspector

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB max upload
app.config["JSON_SORT_KEYS"] = False  # Preserve column order in JSON responses

# Pre-load the reference geodataframe once at startup
REFERENCE_PATH = os.path.join(os.path.dirname(__file__), "data", "regions.geojson")
gdf_reference = gpd.read_file(REFERENCE_PATH).to_crs(epsg=2154)

ALLOWED_EXTENSIONS = {".csv", ".txt", ".xlsx", ".geojson", ".json", ".shp", ".gpkg", ".zip"}

# Define the desired column order for the summary display
COLUMN_ORDER = [
    # File metadata
    "Dossier",
    "Nom du fichier",
    "Taille (Ko)",
    "Date de création du fichier (Y-M-D)",
    "Type de fichier",

    # Basic data info
    "Nb lignes",
    "Nb colonnes",
    "Colonnes",
    "Score de complétude global",

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
    "Couverture territoriale hexagonale (%)",
]


def _make_serializable(obj):
    """Recursively convert numpy/pandas types to JSON-serializable Python types."""
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_serializable(v) for v in obj]
    # NaN check must come BEFORE numpy type conversion, otherwise
    # np.floating NaN gets converted to float('nan') and returned early.
    if isinstance(obj, float) and np.isnan(obj):
        return None
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj):
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


def _reorder_summary(summary):
    """Reorder summary dictionary to match COLUMN_ORDER and return as list of [key, value] pairs."""
    ordered_pairs = []
    added_keys = set()

    # Add columns in the defined order
    for key in COLUMN_ORDER:
        if key in summary:
            ordered_pairs.append([key, summary[key]])
            added_keys.add(key)

    # Add any remaining columns not in COLUMN_ORDER
    for key, value in summary.items():
        if key not in added_keys:
            ordered_pairs.append([key, value])

    return ordered_pairs


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

        # Ensure CRS is set
        if gdf.crs is None:
            return None

        # OPTIMIZATION: Sample FIRST, then reproject only the sample
        sample_size = min(1000, len(gdf))
        gdf_sample = gdf.sample(n=sample_size, random_state=42) if len(gdf) > sample_size else gdf.copy()

        # Convert only the sample to WGS84 for web mapping
        gdf_sample_wgs84 = gdf_sample.to_crs(epsg=4326)

        # Get bounding box from sample (good enough approximation)
        bounds = gdf_sample_wgs84.total_bounds  # [minx, miny, maxx, maxy]

        # Calculate center
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
    return render_template("index.html")


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
        file.save(saved_path)

        # If zip, extract and find the data file inside
        filepath_to_inspect = saved_path
        if ext == ".zip":
            filepath_to_inspect = _handle_zip(saved_path, tmpdir)
            if filepath_to_inspect is None:
                return jsonify({
                    "error": "ZIP archive does not contain a supported data file (.shp, .geojson, .csv, .xlsx, .gpkg)."
                }), 400

        # Clear global summary list and last GeoDataFrame
        inspector.summary_rows.clear()
        inspector.last_gdf = None

        # Capture stdout from inspection process
        log_capture = StringIO()
        with redirect_stdout(log_capture):
            inspector.inspect_file(filepath_to_inspect, gdf_reference)

        # Get captured logs
        logs = log_capture.getvalue()
        log_lines = [line for line in logs.split('\n') if line.strip()]

        if not inspector.summary_rows:
            return jsonify({"error": "Inspection produced no results. The file may be empty or unreadable."}), 400

        result = inspector.summary_rows[-1]

        # Override folder/filename to show the original upload name (not the tmp path)
        result["Dossier"] = "(uploaded)"
        result["Nom du fichier"] = file.filename

        # Reorder the summary to match COLUMN_ORDER
        ordered_result = _reorder_summary(result)

        # Extract map data from the stored GeoDataFrame
        map_data = _extract_map_data()

        # Build response
        response = {
            "summary": _make_serializable(ordered_result),
            "map": _make_serializable(map_data) if map_data else None,
            "logs": log_lines
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
    """Extract a ZIP and return the path to the first supported data file."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    # Priority order for file detection inside the zip
    priority = [".shp", ".gpkg", ".geojson", ".json", ".csv", ".txt", ".xlsx"]

    for ext in priority:
        for root, _, files in os.walk(extract_dir):
            for fname in files:
                if fname.lower().endswith(ext) and not fname.startswith("._"):
                    return os.path.join(root, fname)
    return None


if __name__ == "__main__":
    print("=" * 70)
    print("Geodata Inspector - DuckDB-Optimized Version")
    print("=" * 70)
    print("Features:")
    print("  - Fast CSV/Excel processing with DuckDB")
    print("  - LineString geometry detection (road networks, etc.)")
    print("  - Point, LineString, and Polygon support")
    print("  - French decimal separator handling")
    print("  - Automatic CRS detection and transformation")
    print("=" * 70)
    print("\nStarting web server...")
    print("Open http://127.0.0.1:5000 in your browser")
    print("=" * 70)
    app.run(debug=True, host="127.0.0.1", port=5000)
