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
from concurrent.futures import ThreadPoolExecutor
import threading
# Ajoute la racine du repo au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# Ensure this directory is on the path so imports resolve
# sys.path.insert(0, os.path.dirname(__file__))

# Use DuckDB-optimized inspector for better performance
import geodata_inspector.core as inspector

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024 * 1024  # 2 GB max upload
app.config["JSON_SORT_KEYS"] = False  # Preserve column order in JSON responses

# Pre-load the reference geodataframe once at startup
REFERENCE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reference_file", "regions.geojson")
gdf_reference = gpd.read_file(REFERENCE_PATH).to_crs(epsg=2154)

PREVIEW_DIR = tempfile.mkdtemp(prefix="geodata_preview_")
last_preview_path = {"path": None, "ext": None}

# Batch processing state
batch_state = {
    "running": False,
    "total": 0,
    "done": 0,
    "current": "",
    "errors": [],
    "output_path": None,
}
batch_executor = ThreadPoolExecutor(max_workers=1)

ALLOWED_EXTENSIONS = {".csv", ".txt", ".xlsx", ".geojson", ".json", ".shp", ".gpkg", ".zip"}

# Define the desired column order for the summary display
COLUMN_ORDER = [
    # File metadata
    # "Dossier",
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
        # Save any sidecar files (e.g. .shx, .dbf, .prj for shapefiles)
        for sidecar in request.files.getlist('sidecars'):
            sidecar_path = os.path.join(tmpdir, sidecar.filename)
            sidecar.save(sidecar_path)
            
        ##Save preview 
        preview_copy = os.path.join(PREVIEW_DIR, file.filename)
        shutil.copy2(saved_path, preview_copy)
        last_preview_path["path"] = preview_copy
        last_preview_path["ext"] = ext
        
        # If zip, extract and find the data file inside
        filepath_to_inspect = saved_path
        if ext == ".zip":
            filepath_to_inspect = _handle_zip(saved_path, tmpdir)
            if filepath_to_inspect is None:
                return jsonify({
                    "error": "ZIP archive does not contain a supported data file (.shp, .geojson, .csv, .xlsx, .gpkg)."
                }), 400

        if hasattr(inspector, 'summary_rows') and inspector.summary_rows:
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
    """
    Extract a ZIP and return either:
    - A single file path (if zip contains one supported file, or one dominant data file)
    - A directory path prefixed with 'DIR:' (if zip contains multiple data files → treat as batch)
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    GEO_EXTS   = {".shp", ".gpkg", ".geojson", ".json"}
    TABLE_EXTS = {".csv", ".txt", ".xlsx"}
    ALL_EXTS   = GEO_EXTS | TABLE_EXTS

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

    # Multiple files → check if there is one dominant geospatial file
    geo_files = [c for c in candidates if c[0] in GEO_EXTS]
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

    # Collect all files
    ALL_EXTS = {".csv", ".txt", ".xlsx", ".geojson", ".json", ".shp", ".gpkg"}
    files_to_process = []
    for root, _, files_in_dir in os.walk(data_dir):
        for fname in sorted(files_in_dir):
            if fname.startswith("._"):
                continue
            if os.path.splitext(fname)[1].lower() in ALL_EXTS:
                files_to_process.append(os.path.join(root, fname))

    if not files_to_process:
        shutil.rmtree(tmpdir, ignore_errors=True)
        return jsonify({"error": "No supported files found in the ZIP."}), 400

    # Reset batch state
    batch_state.update({
        "running": True,
        "total": len(files_to_process),
        "done": 0,
        "current": "",
        "errors": [],
        "output_path": None,
    })

    output_csv = os.path.join(PREVIEW_DIR, f"batch_results_{os.path.basename(zip_path)}.csv")

    def run_batch():
        try:
            all_rows = []
            for filepath in files_to_process:
                batch_state["current"] = os.path.basename(filepath)
                try:
                    if hasattr(inspector, 'summary_rows') and inspector.summary_rows:
                        inspector.summary_rows.clear()
                    inspector.last_gdf = None

                    inspector.inspect_file(filepath, gdf_reference)

                    if inspector.summary_rows:
                        all_rows.append(dict(inspector.summary_rows[-1]))
                except Exception as e:
                    batch_state["errors"].append(f"{os.path.basename(filepath)}: {str(e)}")
                finally:
                    batch_state["done"] += 1

            # Write CSV output
            if all_rows:
                df_out = pd.DataFrame(all_rows)
                # Flatten dict columns to JSON strings for CSV
                for col in df_out.columns:
                    if df_out[col].dtype == object:
                        df_out[col] = df_out[col].apply(
                            lambda v: json.dumps(v, ensure_ascii=False)
                            if isinstance(v, dict) else v
                        )
                df_out.to_csv(output_csv, index=False, encoding="utf-8-sig")
                batch_state["output_path"] = output_csv

        except Exception as e:
            batch_state["errors"].append(f"Fatal: {str(e)}")
        finally:
            batch_state["running"] = False
            shutil.rmtree(tmpdir, ignore_errors=True)

    batch_executor.submit(run_batch)

    return jsonify({
        "message": f"Batch started: {len(files_to_process)} files to process.",
        "total": len(files_to_process)
    })


@app.route("/batch/status", methods=["GET"])
def batch_status():
    """Poll batch job progress."""
    return jsonify({
        "running":  batch_state["running"],
        "total":    batch_state["total"],
        "done":     batch_state["done"],
        "current":  batch_state["current"],
        "errors":   batch_state["errors"],
        "finished": not batch_state["running"] and batch_state["done"] > 0,
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
    """Return a single batch result by index for display in the UI."""
    path = batch_state.get("output_path")
    if not path or not os.path.exists(path):
        return jsonify({"error": "No batch results available."}), 404
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        if index < 0 or index >= len(df):
            return jsonify({"error": "Index out of range."}), 404

        row = df.iloc[index].to_dict()

        # Try to parse JSON strings back to objects for display
        for k, v in row.items():
            if isinstance(v, str) and v.strip().startswith('{'):
                try:
                    row[k] = json.loads(v)
                except Exception:
                    pass

        # Reorder to match COLUMN_ORDER
        ordered = _reorder_summary(row)

        return jsonify({
            "index": index,
            "total": len(df),
            "filename": row.get("Nom du fichier", f"Fichier {index+1}"),
            "summary": _make_serializable(ordered)
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

    # Save all files to a temp dir
    tmpdir = tempfile.mkdtemp(prefix="geodata_batchdir_")
    saved_paths = []
    for f in files:
        # Flatten subdirectory structure using underscores to avoid collisions
        safe_name = f.filename.replace("\\", "/").lstrip("/").replace("/", "_")
        dest = os.path.join(tmpdir, safe_name)
        f.save(dest)
        saved_paths.append(dest)

    batch_state.update({
        "running": True,
        "total": len(saved_paths),
        "done": 0,
        "current": "",
        "errors": [],
        "output_path": None,
    })

    output_csv = os.path.join(PREVIEW_DIR, "batch_dir_results.csv")

    def run_batch():
        try:
            all_rows = []
            for filepath in saved_paths:
                batch_state["current"] = os.path.basename(filepath)
                try:
                    if hasattr(inspector, 'summary_rows') and inspector.summary_rows:
                        inspector.summary_rows.clear()
                    inspector.last_gdf = None
                    inspector.inspect_file(filepath, gdf_reference)
                    if inspector.summary_rows:
                        all_rows.append(dict(inspector.summary_rows[-1]))
                except Exception as e:
                    batch_state["errors"].append(f"{os.path.basename(filepath)}: {str(e)}")
                finally:
                    batch_state["done"] += 1

            if all_rows:
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
            batch_state["errors"].append(f"Fatal: {str(e)}")
        finally:
            batch_state["running"] = False
            shutil.rmtree(tmpdir, ignore_errors=True)

    batch_executor.submit(run_batch)
    return jsonify({"message": f"Batch started: {len(saved_paths)} files.", "total": len(saved_paths)})   
    
def _df_to_duckdb(df):
    """Register a DataFrame in a fresh DuckDB connection and return the connection."""
    import duckdb
    conn = duckdb.connect()
    conn.register("excel_tbl", df)
    return conn

@app.route("/export", methods=["GET"])
def export():
    """Export the full dataset as a geospatial file by re-reading the original."""
    fmt  = request.args.get("format", "geojson").lower()
    path = last_preview_path.get("path")
    ext  = last_preview_path.get("ext")

    if not path or not os.path.exists(path):
        return jsonify({"error": "No file available for export."}), 404

    SUPPORTED_FORMATS = {"geojson", "gpkg", "shp", "csv"}
    if fmt not in SUPPORTED_FORMATS:
        return jsonify({"error": f"Unsupported format '{fmt}'. Choose from: {', '.join(SUPPORTED_FORMATS)}"}), 400

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

        # Reproject to EPSG:2154 for storage formats, WGS84 for GeoJSON/CSV
        base_name = os.path.splitext(os.path.basename(path))[0]

        if fmt == "geojson":
            export_path = os.path.join(PREVIEW_DIR, f"{base_name}_export.geojson")
            gdf.to_crs(epsg=4326).to_file(export_path, driver="GeoJSON")
            mime = "application/geo+json"
            download_name = f"{base_name}.geojson"

        elif fmt == "gpkg":
            export_path = os.path.join(PREVIEW_DIR, f"{base_name}_export.gpkg")
            gdf.to_crs(epsg=2154).to_file(export_path, driver="GPKG", layer=base_name)
            mime = "application/geopackage+sqlite3"
            download_name = f"{base_name}.gpkg"

        elif fmt == "shp":
            shp_path = os.path.join(PREVIEW_DIR, f"{base_name}_export.shp")
            gdf.to_crs(epsg=2154).to_file(shp_path, driver="ESRI Shapefile")
            zip_path = os.path.join(PREVIEW_DIR, f"{base_name}_shp.zip")
            shp_base = os.path.splitext(shp_path)[0]
            with zipfile.ZipFile(zip_path, "w") as zf:
                for suffix in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                    f = shp_base + suffix
                    if os.path.exists(f):
                        zf.write(f, os.path.basename(f))
            export_path = zip_path
            mime = "application/zip"
            download_name = f"{base_name}_shp.zip"

        elif fmt == "csv":
            export_path = os.path.join(PREVIEW_DIR, f"{base_name}_export_geo.csv")
            gdf_out = gdf.to_crs(epsg=4326).copy()
            gdf_out["geometry_wkt"] = gdf_out.geometry.apply(
                lambda g: g.wkt if g else None)
            pd.DataFrame(gdf_out.drop(columns="geometry")).to_csv(
                export_path, index=False, encoding="utf-8-sig")
            mime = "text/csv"
            download_name = f"{base_name}_geo.csv"

        from flask import send_file
        return send_file(export_path, mimetype=mime,as_attachment=True, download_name=download_name)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    
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
    print("Open http://10.149.201.62:5050/ in your browser")
    print("=" * 70) #http://10.149.201.62/ #127.0.0.1
    app.run(debug=True, host="10.149.201.62", port=5050)
