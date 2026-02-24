"""
DuckDB-optimized Geodata Inspector
==================================
This module provides DuckDB-accelerated versions of the geodata inspection functions.
DuckDB offers significant performance improvements for:
- CSV reading (10-100x faster than pandas for large files)
- SQL-based aggregations and analysis
- Spatial operations via the spatial extension

Usage:
    python core.py [file_path]
"""

import os
import time
import duckdb
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import datetime
from shapely.geometry import Point, LineString, shape
from shapely import wkt
from shapely.validation import make_valid
import re
import json

# Import spatial metrics (reuse existing)
from . import spatial 
from .spatial import taux_de_remplissage, complexite_moyenne, pourcentage_geometries_dupliquees

# ============================================================================
# CONFIGURATION & STATE
# ============================================================================
summary_rows = []
last_gdf = None

# Initialize DuckDB with spatial extension
def get_duckdb_connection():
    """Create DuckDB connection with spatial extension loaded."""
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL spatial; LOAD spatial;")
    return conn


# ============================================================================
# DUCKDB UTILITY FUNCTIONS
# ============================================================================
def get_file_metadata(filepath):
    """Extract file metadata."""
    stats = os.stat(filepath)
    return {
        "Dossier": os.path.dirname(filepath),
        "Nom du fichier": os.path.basename(filepath),
        "Taille (Ko)": round(stats.st_size / 1024, 2),
        "Date de création du fichier (Y-M-D)": datetime.fromtimestamp(stats.st_ctime).strftime('%Y-%m-%d')
    }


def detect_csv_dialect_duckdb(filepath, sample_size=10000):
    """
    Detect CSV encoding and delimiter using DuckDB's sniffer.
    DuckDB automatically handles most CSV formats.
    """
    conn = duckdb.connect(':memory:')

    # Try common delimiters
    for delimiter in [',', ';', '\t', '|']:
        try:
            # Use DuckDB's read_csv with explicit delimiter
            result = conn.execute(f"""
                SELECT * FROM read_csv('{filepath}',
                    delim='{delimiter}',
                    header=true,
                    sample_size={sample_size},
                    ignore_errors=true
                ) LIMIT 5
            """).fetchdf()

            # If we got more than 1 column, likely correct delimiter
            if len(result.columns) > 1:
                conn.close()
                return {'delimiter': delimiter, 'encoding': 'utf-8'}
        except Exception:
            continue

    conn.close()
    return {'delimiter': ',', 'encoding': 'utf-8'}


def completeness_score_duckdb(conn, table_name):
    """
    Calculate completeness score using DuckDB SQL.
    Much faster than pandas for large datasets.
    """
    # Get column names
    columns = conn.execute(f"DESCRIBE {table_name}").fetchdf()['column_name'].tolist()

    if not columns:
        return {"Score de complétude moyen": 0, "Score de complétude std": 0}

    # Build SQL for null counts per column
    null_counts_sql = ", ".join([
        f"SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)::DOUBLE as null_ratio_{i}"
        for i, col in enumerate(columns)
    ])

    result = conn.execute(f"SELECT {null_counts_sql} FROM {table_name}").fetchone()

    if result is None:
        return {"Score de complétude moyen": 0, "Score de complétude std": 0}

    null_ratios = [r for r in result if r is not None]

    if not null_ratios:
        return {"Score de complétude moyen": 1.0, "Score de complétude std": 0}

    mean_completeness = 1 - np.mean(null_ratios)
    std_completeness = np.std(null_ratios)

    return {
        "Score de complétude moyen": round(mean_completeness, 2),
        "Score de complétude std": round(std_completeness, 2)
    }

def completeness_score_duckdb_cols(conn, table_name, columns):
    """Calculate completeness score restricted to a specific list of columns."""
    if not columns:
        return {"Score de complétude moyen": "N/A", "Score de complétude std": "N/A"}

    null_counts_sql = ", ".join([
        f"SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)::DOUBLE as null_ratio_{i}"
        for i, col in enumerate(columns)
    ])

    result = conn.execute(f"SELECT {null_counts_sql} FROM {table_name}").fetchone()

    if result is None:
        return {"Score de complétude moyen": 0, "Score de complétude std": 0}

    null_ratios = [r for r in result if r is not None]
    mean_completeness = 1 - np.mean(null_ratios)
    std_completeness = np.std(null_ratios)

    return {
        "Score de complétude moyen": round(mean_completeness, 2),
        "Score de complétude std": round(std_completeness, 2)
    }

def build_columns_detail_duckdb(conn, table_name, limit=5):
    """Build column details using DuckDB."""
    schema = conn.execute(f"DESCRIBE {table_name}").fetchdf()

    details = []
    for _, row in schema.iterrows():
        col_name = row['column_name']
        col_type = row['column_type']

        # Get sample value and null count
        try:
            sample = conn.execute(f"""
                SELECT "{col_name}" FROM {table_name}
                WHERE "{col_name}" IS NOT NULL
                LIMIT 1
            """).fetchone()
            sample_val = str(sample[0]) if sample else "N/A"

            null_count = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE "{col_name}" IS NULL
            """).fetchone()[0]
        except Exception:
            sample_val = "N/A"
            null_count = 0

        details.append({
            "Colonne": col_name,
            "Exemple": sample_val[:100] if len(sample_val) > 100 else sample_val,
            "Type": col_type,
            "Valeurs manquantes": null_count
        })

    return details


# ============================================================================
# GEOGRAPHIC DETECTION (reused from original with minor optimizations)
# ============================================================================
def detect_geo_join_keys_duckdb(conn, table_name):
    """Detect geographic join key columns using DuckDB."""
    candidates = []
    pattern = r'(dep|reg|insee|com|code|postal|commune|departement|département)'
    schema = conn.execute(f"DESCRIBE {table_name}").fetchdf()

    for _, row in schema.iterrows():
        col = row['column_name']
        col_lc = col.lower()

        match = re.search(pattern, col_lc)
        if not match:
            continue

        try:
            stats = conn.execute(f"""
                SELECT
                    AVG(LENGTH(CAST("{col}" AS VARCHAR))) as avg_len,
                    COUNT(*) - COUNT("{col}") as null_count,
                    COUNT(*) as total
                FROM {table_name}
            """).fetchone()

            if stats[2] == stats[1]:  # All null
                continue

            avg_len = stats[0] or 0
            is_numeric = 'INT' in row['column_type'].upper() or 'DOUBLE' in row['column_type'].upper()
            w_match = col_lc[match.start():match.end()]

            # Pour les numériques, cast en BIGINT pour supprimer le ".0" du float
            if is_numeric:
                clean_len = conn.execute(f"""
                    SELECT AVG(LENGTH(CAST(CAST("{col}" AS BIGINT) AS VARCHAR)))
                    FROM {table_name}
                    WHERE "{col}" IS NOT NULL
                """).fetchone()[0] or 0
            else:
                clean_len = avg_len

            # Détecter le pattern "Nom (Code)" — extraire la longueur du code entre parenthèses
            code_in_parens_len = conn.execute(f"""
                SELECT AVG(LENGTH(REGEXP_EXTRACT(CAST("{col}" AS VARCHAR), '\\(([^)]+)\\)', 1)))
                FROM {table_name}
                WHERE "{col}" IS NOT NULL
                  AND REGEXP_EXTRACT(CAST("{col}" AS VARCHAR), '\\(([^)]+)\\)', 1) != ''
            """).fetchone()[0]

            if code_in_parens_len:
                clean_len = code_in_parens_len  # Utiliser la longueur du code extrait

            geo_type = None

            if 4.5 <= clean_len <= 5.5:
                geo_type = "Code INSEE ou commune (zéros perdus)" if is_numeric else "Code INSEE ou commmune"
            elif 3.5 <= clean_len <= 4.5:
                geo_type = "Code INSEE ou commune (zéros perdus)"
            elif 1.5 <= clean_len <= 3.5:
                if 'reg' in w_match:
                    geo_type = "Code region"
                elif 'dep' in w_match or 'departement' in w_match or 'département' in w_match:
                    geo_type = "Code departement (extrait)"
                elif 'com' in w_match or 'commune' in w_match:
                    geo_type = "Code INSEE ou commune (extrait)"
                else:
                    geo_type = "Code region ou departement"

            if geo_type:
                candidates.append(f"{col} ({geo_type})")

        except Exception:
            continue

    return candidates

# def detect_geo_join_keys_duckdb(conn, table_name):
#     """Detect geographic join key columns using DuckDB."""
#     candidates = []
#     pattern = r'(dep|reg|insee|com|code|postal)'
#     schema = conn.execute(f"DESCRIBE {table_name}").fetchdf()

#     for _, row in schema.iterrows():
#         col = row['column_name']
#         col_lc = col.lower()

#         match = re.search(pattern, col_lc)
#         if not match:
#             continue

#         try:
#             stats = conn.execute(f"""
#                 SELECT
#                     AVG(LENGTH(CAST("{col}" AS VARCHAR))) as avg_len,
#                     COUNT(*) - COUNT("{col}") as null_count,
#                     COUNT(*) as total
#                 FROM {table_name}
#             """).fetchone()

#             if stats[2] == stats[1]:  # All null
#                 continue

#             avg_len = stats[0] or 0
#             is_numeric = 'INT' in row['column_type'].upper() or 'DOUBLE' in row['column_type'].upper()
#             w_match = col_lc[match.start():match.end()]

#             # Pour les numériques, cast en BIGINT pour supprimer le ".0" du float
#             if is_numeric:
#                 clean_len = conn.execute(f"""
#                     SELECT AVG(LENGTH(CAST(CAST("{col}" AS BIGINT) AS VARCHAR)))
#                     FROM {table_name}
#                     WHERE "{col}" IS NOT NULL
#                 """).fetchone()[0] or 0
#             else:
#                 clean_len = avg_len

#             geo_type = None

#             if 4.5 <= clean_len <= 5.5:
#                 # 5 chiffres → code INSEE commune ou code postal
#                 if 'postal' in col_lc or 'cp' == col_lc:
#                     geo_type = "code_postal"
#                 else:
#                     geo_type = "code_insee_commune (zéros perdus)" if is_numeric else "code_insee_commune"
#             elif 3.5 <= clean_len <= 4.5:
#                 # 4 chiffres → code INSEE numérique sans zéro (ex: 1001 au lieu de 01001)
#                 geo_type = "code_insee_commune (zéros perdus)"
#             elif 1.5 <= clean_len <= 3.5:
#                 # 2-3 chiffres → département ou région
#                 if 'reg' in w_match:
#                     geo_type = "code_region"
#                 elif 'dep' in w_match:
#                     geo_type = "code_departement"
#                 else:
#                     geo_type = "code_departement_or_region"

#             if geo_type:
#                 candidates.append(f"{col} ({geo_type})")

#         except Exception:
#             continue

#     return candidates

def get_geo_columns_duckdb(conn, table_name):
    """Identify geometry columns using DuckDB queries."""
    lat_pattern = r'\b(lat|latitude)\b'
    lon_pattern = r'\b(lon|long|lng|longitude)\b'
    x_pattern = r'(^x[^a-zA-Z0-9]?|[^a-zA-Z0-9]x$)'
    y_pattern = r'(^y[^a-zA-Z0-9]?|[^a-zA-Z0-9]y$)'
    geom_pattern = r'\b(geometry|geom|shape|point|polygon)\b'
    addr_pattern = r'adresse'
    insee_pattern = r'(dep|reg|insee|com)'

    result = {
        'columns': [],
        'type': None,
        'method': None,
        'geo_keys': [],
        'geotrans': 'Aucune géométrie',
    }

    schema = conn.execute(f"DESCRIBE {table_name}").fetchdf()
    columns = schema['column_name'].tolist()

    # Check for geometry columns (GeoJSON/WKT)
    for col in columns:
        col_lc = col.lower()
        if not re.search(geom_pattern, col_lc):
            continue

        try:
            sample = conn.execute(f"""
                SELECT "{col}" FROM {table_name}
                WHERE "{col}" IS NOT NULL
                LIMIT 1
            """).fetchone()

            if sample is None:
                continue

            sample_val = sample[0]

            # Check geo_point format
            if 'point' in col_lc and 'geo' in col_lc and isinstance(sample_val, str) and ',' in sample_val:
                return {**result, 'type': 'Point', 'method': 'geopoint', 'columns': [col], 'geotrans': "Présence géométrie"}

            # Check GeoJSON
            try:
                val = json.loads(sample_val) if isinstance(sample_val, str) else sample_val
                if isinstance(val, dict) and 'type' in val and 'coordinates' in val:
                    return {**result, 'type': val.get('type', 'Unknown'), 'method': 'geojson', 'columns': [col], 'geotrans': "Présence géométrie"}
            except Exception:
                pass

            # Check WKT
            if isinstance(sample_val, str):
                sample_upper = sample_val.upper()
                for geom_type in ['POINT', 'LINESTRING', 'POLYGON']:
                    if geom_type in sample_upper:
                        return {**result, 'type': geom_type.title(), 'method': 'from_wkt', 'columns': [col], 'geotrans': "Présence géométrie"}
        except Exception:
            continue

    # Check for Lat/Lon pairs
    lat_col = lon_col = None
    for col in columns:
        col_lc = col.lower()
        if re.search(lat_pattern, col_lc) and lat_col is None:
            lat_col = col
        if re.search(lon_pattern, col_lc) and lon_col is None:
            lon_col = col

    if lat_col and lon_col:
        return {**result, 'type': 'Point', 'method': 'points_from_xy', 'columns': [lon_col, lat_col], 'geotrans': "Présence géométrie séparée (x,y)"}

    # Check for X/Y pairs
    x_cols = [col for col in columns if re.search(x_pattern, col.lower())]
    y_cols = [col for col in columns if re.search(y_pattern, col.lower())]

    if x_cols and y_cols:
        # Match X/Y pairs by suffix and check for start/end coordinate patterns (LineString)
        for x_col in x_cols:
            for y_col in y_cols:
                x_suffix = x_col.lower().replace('x', '')
                y_suffix = y_col.lower().replace('y', '')

                if x_suffix == y_suffix or (not x_suffix and not y_suffix):
                    # Check if it's start/end coordinates for LineString
                    if any(k in x_col.lower() for k in ['d', 'debut', 'start']):
                        for x_end in x_cols:
                            if x_end != x_col and any(k in x_end.lower() for k in ['f', 'fin', 'end']):
                                for y_end in y_cols:
                                    if y_end != y_col and any(k in y_end.lower() for k in ['f', 'fin', 'end']):
                                        return {**result, 'type': 'LineString', 'method': 'linestring_coords',
                                                'columns': [x_col, y_col, x_end, y_end],
                                                'geotrans': "Présence géométrie multiples (x1,y1), (x2,y2)"}

                    # Otherwise, simple Point pair
                    return {**result, 'type': 'Point', 'method': 'points_from_xy', 'columns': [x_col, y_col], 'geotrans': "Présence géométrie séparée (x,y)"}

    # Check for address columns
    for col in columns:
        if re.search(addr_pattern, col.lower()):
            return {**result, 'type': 'Address', 'method': 'geocoding_required', 'columns': [col], 'geotrans': "Géocodage de l'adresse"}

    # Collect INSEE/geographic keys
    for col in columns:
        if re.search(insee_pattern, col.lower()):
            result['geo_keys'].append(col)

    if result['geo_keys']:
        result['type'] = 'Administrative'
        result['method'] = 'join_required'
        result['columns'] = result['geo_keys']
        result['geotrans'] = "Jointure spatiale à l'aide de clés géographiques"

    return result


# ============================================================================
# DUCKDB CSV INSPECTION
# ============================================================================
def guess_crs_from_coords_duckdb(conn, table_name, x_col, y_col):
    """
    Guess CRS from coordinate columns using median values.
    Same logic as guess_crs_from_bounds but for x,y columns.
    """
    try:
        result = conn.execute(f"""
            SELECT
                MEDIAN("{x_col}") as median_x,
                MEDIAN("{y_col}") as median_y
            FROM {table_name}
            WHERE "{x_col}" IS NOT NULL AND "{y_col}" IS NOT NULL
        """).fetchone()

        median_x, median_y = result

        if median_x is None or median_y is None:
            return None

        # Same logic as original guess_crs_from_bounds
        if -10 < median_x < 10 and 40 < median_y < 60:
            return 4326  # WGS84
        elif 100000 < median_x < 1300000 and 6000000 < median_y < 7400000:
            return 2154  # Lambert 93
        elif -2.2e6 < median_x < 2.2e6 and -2.2e6 < median_y < 2.2e6:
            return 3857  # Web Mercator

        return None
    except Exception:
        return None


def process_geometry_duckdb_points(conn, table_name, x_col, y_col, gdf_metro):
    """
    Process point geometry (from x,y columns) entirely in DuckDB using spatial functions.
    Much faster than GeoPandas for large datasets.

    Detects CRS using guess_crs_from_bounds logic and transforms to EPSG:2154.
    Filters to metropolitan France (hexagonal territory) bounds to exclude overseas territories.
    """
    print(f"[DuckDB Spatial] Processing point geometry with DuckDB spatial extension...")
    start = time.time()

    # Metropolitan France bounds in Lambert 93 (EPSG:2154)
    METRO_BOUNDS = {
        'minx': 100000,
        'miny': 6000000,
        'maxx': 1250000,
        'maxy': 7150000
    }

    # PROJ strings for accurate transformation
    PROJ_STRINGS = {
        4326: '+proj=longlat +datum=WGS84',
        2154: '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m',
        3857: '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m'
    }

    # Detect CRS from coordinate values
    detected_crs = guess_crs_from_coords_duckdb(conn, table_name, x_col, y_col)

    if detected_crs is None:
        detected_crs = 2154  # Default to Lambert 93

    print(f"[DuckDB Spatial] Detected CRS: EPSG:{detected_crs}")

    # Create points and transform to Lambert 93 if needed
    if detected_crs != 2154:
        source_proj = PROJ_STRINGS.get(detected_crs, f'EPSG:{detected_crs}')
        target_proj = PROJ_STRINGS[2154]

        print(f"[DuckDB Spatial] Transforming from EPSG:{detected_crs} to EPSG:2154...")
        conn.execute(f"""
            CREATE TABLE geo_points AS
            SELECT *,
                ST_Transform(ST_Point("{x_col}", "{y_col}"), '{source_proj}', '{target_proj}') as geom
            FROM {table_name}
            WHERE "{x_col}" IS NOT NULL AND "{y_col}" IS NOT NULL
        """)
    else:
        conn.execute(f"""
            CREATE TABLE geo_points AS
            SELECT *, ST_Point("{x_col}", "{y_col}") as geom
            FROM {table_name}
            WHERE "{x_col}" IS NOT NULL AND "{y_col}" IS NOT NULL
        """)

    # Filter to metro France (in Lambert 93 coordinates)
    conn.execute(f"""
        CREATE TABLE geo_processed AS
        SELECT * FROM geo_points
        WHERE ST_X(geom) BETWEEN {METRO_BOUNDS['minx']} AND {METRO_BOUNDS['maxx']}
          AND ST_Y(geom) BETWEEN {METRO_BOUNDS['miny']} AND {METRO_BOUNDS['maxy']}
    """)

    filtered_count = conn.execute("SELECT COUNT(*) FROM geo_processed").fetchone()[0]
    total_with_coords = conn.execute("SELECT COUNT(*) FROM geo_points").fetchone()[0]

    if filtered_count < total_with_coords:
        excluded = total_with_coords - filtered_count
        print(f"[DuckDB Spatial] Filtered to metropolitan France: {filtered_count:,} points ({excluded:,} outre-mer excluded)")

    return _compute_spatial_metrics_duckdb(conn, "geo_processed", "Point", gdf_metro, start,source_crs=detected_crs)


def process_geometry_duckdb_linestrings(conn, table_name, x_start, y_start, x_end, y_end, gdf_metro):
    """
    Process LineString geometry from start/end coordinate columns using DuckDB spatial.
    Uses ST_MakeLine to build LineStrings from (xD,yD)→(xF,yF) pairs.
    Handles French decimal separators (comma) via REPLACE normalization.
    Detects CRS and transforms to EPSG:2154.
    """
    print(f"[DuckDB Spatial] Processing LineString geometry with DuckDB spatial extension...")
    start = time.time()

    METRO_BOUNDS = {
        'minx': 100000,
        'miny': 6000000,
        'maxx': 1250000,
        'maxy': 7150000
    }

    PROJ_STRINGS = {
        4326: '+proj=longlat +datum=WGS84',
        2154: '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m',
        3857: '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m'
    }

    # Safe cast: handles both numeric columns and VARCHAR with comma decimal separator
    def safe_cast(col):
        return f"TRY_CAST(REPLACE(CAST(\"{col}\" AS VARCHAR), ',', '.') AS DOUBLE)"

    # Materialize normalized coordinates
    conn.execute(f"""
        CREATE TABLE geo_coords AS
        SELECT
            {safe_cast(x_start)} as x_s,
            {safe_cast(y_start)} as y_s,
            {safe_cast(x_end)} as x_e,
            {safe_cast(y_end)} as y_e
        FROM {table_name}
        WHERE {safe_cast(x_start)} IS NOT NULL AND {safe_cast(y_start)} IS NOT NULL
          AND {safe_cast(x_end)} IS NOT NULL AND {safe_cast(y_end)} IS NOT NULL
    """)

    # Detect CRS from start coordinates
    detected_crs = guess_crs_from_coords_duckdb(conn, "geo_coords", "x_s", "y_s")
    if detected_crs is None:
        detected_crs = 2154

    print(f"[DuckDB Spatial] Detected CRS: EPSG:{detected_crs}")

    # Create LineStrings and reproject if needed
    if detected_crs != 2154:
        source_proj = PROJ_STRINGS.get(detected_crs, f'EPSG:{detected_crs}')
        target_proj = PROJ_STRINGS[2154]

        print(f"[DuckDB Spatial] Transforming from EPSG:{detected_crs} to EPSG:2154...")
        conn.execute(f"""
            CREATE TABLE geo_lines AS
            SELECT *,
                ST_Transform(
                    ST_MakeLine(ST_Point(x_s, y_s), ST_Point(x_e, y_e)),
                    '{source_proj}', '{target_proj}'
                ) as geom
            FROM geo_coords
        """)
    else:
        conn.execute(f"""
            CREATE TABLE geo_lines AS
            SELECT *,
                ST_MakeLine(ST_Point(x_s, y_s), ST_Point(x_e, y_e)) as geom
            FROM geo_coords
        """)

    conn.execute("DROP TABLE IF EXISTS geo_coords")

    total_with_coords = conn.execute("SELECT COUNT(*) FROM geo_lines").fetchone()[0]

    # Filter to metropolitan France using line midpoint
    conn.execute(f"""
        CREATE TABLE geo_processed AS
        SELECT * FROM geo_lines
        WHERE ST_X(ST_Centroid(geom)) BETWEEN {METRO_BOUNDS['minx']} AND {METRO_BOUNDS['maxx']}
          AND ST_Y(ST_Centroid(geom)) BETWEEN {METRO_BOUNDS['miny']} AND {METRO_BOUNDS['maxy']}
    """)

    conn.execute("DROP TABLE IF EXISTS geo_lines")

    filtered_count = conn.execute("SELECT COUNT(*) FROM geo_processed").fetchone()[0]

    if filtered_count < total_with_coords:
        excluded = total_with_coords - filtered_count
        print(f"[DuckDB Spatial] Filtered to metropolitan France: {filtered_count:,} lines ({excluded:,} outre-mer excluded)")

    return _compute_spatial_metrics_duckdb(conn, "geo_processed", "LineString", gdf_metro, start,source_crs=detected_crs)


def guess_crs_from_bounds_duckdb(conn, table_name, geom_col):
    """
    Guess CRS from bounding box coordinates using DuckDB.
    Same logic as the original guess_crs_from_bounds function.
    """
    try:
        # Get median coordinates
        result = conn.execute(f"""
            SELECT
                MEDIAN(ST_X(ST_Centroid("{geom_col}"))) as median_x,
                MEDIAN(ST_Y(ST_Centroid("{geom_col}"))) as median_y
            FROM {table_name}
            WHERE "{geom_col}" IS NOT NULL
        """).fetchone()

        median_x, median_y = result

        if median_x is None or median_y is None:
            return None

        # Same logic as original guess_crs_from_bounds
        if -10 < median_x < 10 and 40 < median_y < 60:
            return 4326  # WGS84
        elif 100000 < median_x < 1300000 and 6000000 < median_y < 7400000:
            return 2154  # Lambert 93
        elif -2.2e6 < median_x < 2.2e6 and -2.2e6 < median_y < 2.2e6:
            return 3857  # Web Mercator

        return None
    except Exception:
        return None


def process_geometry_duckdb_native(conn, table_name, geom_col, gdf_metro):
    """
    Process native geometry column (from geospatial files) using DuckDB spatial.
    Handles Points, LineStrings, Polygons, etc.
    Detects CRS using guess_crs_from_bounds logic and transforms to EPSG:2154.
    """
    print(f"[DuckDB Spatial] Processing native geometry with DuckDB spatial extension...")
    start = time.time()

    # Metropolitan France bounds in Lambert 93 (EPSG:2154)
    METRO_BOUNDS = {
        'minx': 100000,
        'miny': 6000000,
        'maxx': 1250000,
        'maxy': 7150000
    }

    # Get geometry type
    geom_type_result = conn.execute(f"""
        SELECT ST_GeometryType("{geom_col}") FROM {table_name}
        WHERE "{geom_col}" IS NOT NULL LIMIT 1
    """).fetchone()
    geom_type = geom_type_result[0] if geom_type_result else "GEOMETRY"

    # Detect CRS using the same logic as guess_crs_from_bounds
    detected_crs = guess_crs_from_bounds_duckdb(conn, table_name, geom_col)

    if detected_crs is None:
        detected_crs = 2154  # Default to Lambert 93

    print(f"[DuckDB Spatial] Detected CRS: EPSG:{detected_crs}")

    # PROJ strings for accurate transformation (DuckDB EPSG codes can have axis issues)
    PROJ_STRINGS = {
        4326: '+proj=longlat +datum=WGS84',
        2154: '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m',
        3857: '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m'
    }

    # Transform to Lambert 93 (EPSG:2154) if needed
    if detected_crs != 2154:
        source_proj = PROJ_STRINGS.get(detected_crs, f'EPSG:{detected_crs}')
        target_proj = PROJ_STRINGS[2154]

        print(f"[DuckDB Spatial] Transforming from EPSG:{detected_crs} to EPSG:2154...")
        conn.execute(f"""
            CREATE TABLE geo_transformed AS
            SELECT *,
                ST_Transform("{geom_col}", '{source_proj}', '{target_proj}') as geom_2154
            FROM {table_name}
            WHERE "{geom_col}" IS NOT NULL
        """)
        work_geom_col = "geom_2154"
        work_table = "geo_transformed"
    else:
        conn.execute(f"""
            CREATE TABLE geo_transformed AS
            SELECT *, "{geom_col}" as geom_2154
            FROM {table_name}
            WHERE "{geom_col}" IS NOT NULL
        """)
        work_geom_col = "geom_2154"
        work_table = "geo_transformed"

    # Create processed table with centroid for filtering
    # Note: Must exclude original geom column to avoid name conflict with transformed geom
    conn.execute(f"""
        CREATE TABLE geo_processed AS
        SELECT * EXCLUDE ("{geom_col}"),
            CASE
                WHEN ST_GeometryType({work_geom_col}) = 'POINT' THEN {work_geom_col}
                ELSE ST_Centroid({work_geom_col})
            END as geom_center,
            {work_geom_col} as geom
        FROM {work_table}
    """)

    # Filter to metropolitan France using centroid in Lambert 93
    conn.execute(f"""
        CREATE TABLE geo_filtered AS
        SELECT * FROM geo_processed
        WHERE ST_X(geom_center) BETWEEN {METRO_BOUNDS['minx']} AND {METRO_BOUNDS['maxx']}
          AND ST_Y(geom_center) BETWEEN {METRO_BOUNDS['miny']} AND {METRO_BOUNDS['maxy']}
    """)

    filtered_count = conn.execute("SELECT COUNT(*) FROM geo_filtered").fetchone()[0]
    total_count = conn.execute("SELECT COUNT(*) FROM geo_processed").fetchone()[0]

    if filtered_count < total_count:
        excluded = total_count - filtered_count
        print(f"[DuckDB Spatial] Filtered to metropolitan France: {filtered_count:,} geometries ({excluded:,} outre-mer excluded)")

    # Use filtered table
    conn.execute("DROP TABLE IF EXISTS geo_processed")
    conn.execute("ALTER TABLE geo_filtered RENAME TO geo_processed")

    return _compute_spatial_metrics_duckdb(conn, "geo_processed", geom_type, gdf_metro, start,source_crs=detected_crs)
    
def _compute_spatial_metrics_duckdb(conn, table_name, geom_type, gdf_metro, start_time, source_crs=None):
    """
    Compute spatial metrics using DuckDB spatial functions.
    Shared logic for both point and native geometry processing.
    """
    total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    if total == 0:
        return get_default_geo_summary()

    non_empty = total  # All rows have geometry by construction

    # Validate geometries
    valid_count = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ST_IsValid(geom)").fetchone()[0]

    hull_row = conn.execute(f"""
    SELECT
        ST_AsText(ST_Envelope_Agg(geom)) AS hull_wkt,
        ST_Area(ST_Envelope_Agg(geom)) / 1e6 AS hull_area_km2
    FROM {table_name}
    """).fetchone()

    hull_wkt = hull_row[0] if hull_row and hull_row[0] is not None else None
    hull_area_km2 = hull_row[1] if hull_row and hull_row[1] is not None else 0
    # Compute bounding box
    bbox = conn.execute(f"""
    SELECT
        MIN(ST_X(ST_Centroid(geom))) as minx, MIN(ST_Y(ST_Centroid(geom))) as miny,
        MAX(ST_X(ST_Centroid(geom))) as maxx, MAX(ST_Y(ST_Centroid(geom))) as maxy
    FROM {table_name}""").fetchone()

    minx, miny, maxx, maxy = bbox

    if 'POINT' in geom_type.upper():
        # For points: use hull area from precomputed hull
        area_km2 = hull_area_km2
        complexity = "None : POINT"

        
    elif 'LINESTRING' or 'LINE' in geom_type.upper():
        # For lines: buffer and union
        area_km2 = hull_area_km2
        # Complexity remains computed as you already do
        complexity_result = conn.execute(f"""
            SELECT AVG(ST_NPoints(geom)) FROM {table_name}
        """).fetchone()
        complexity = round(complexity_result[0], 2) if complexity_result[0] else 0

    else:
        # For polygons: union area
        try:
            area_result = conn.execute(f"""
                SELECT ST_Area(ST_Union_Agg(geom)) / 1e6 as area_km2
                FROM {table_name}
            """).fetchone()
            area_km2 = area_result[0] if area_result[0] else hull_area_km2
        except Exception:
            area_km2 = hull_area_km2

        complexity_result = conn.execute(f"""
            SELECT AVG(ST_NPoints(geom)) FROM {table_name}
        """).fetchone()
        complexity = round(complexity_result[0], 2) if complexity_result[0] else 0

    # Density
    density = total / area_km2 if area_km2 > 0 else 0

    # Duplicate detection using WKT
    dup_result = conn.execute(f"""
        WITH wkt_geoms AS (
            SELECT ST_AsText(geom) as wkt FROM {table_name}
        )
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT wkt) as unique_count
        FROM wkt_geoms
    """).fetchone()
    dup_pct = (dup_result[0] - dup_result[1]) / dup_result[0] * 100 if dup_result[0] > 0 else 0

    # Compute fill rate (area vs bounding box area)
    bbox_area = (maxx - minx) * (maxy - miny) / 1e6 if (maxx > minx and maxy > miny) else 0
    fill_rate = (area_km2 / bbox_area * 100) if bbox_area > 0 else 0

    # Compute coverage against reference using precomputed hull_wkt
    coverage_pct = 0
    try:
        if hull_wkt and gdf_metro is not None:
            from shapely import wkt as shapely_wkt
            hull_geom = shapely_wkt.loads(hull_wkt)
            ref_dissolved = gdf_metro.union_all()
            intersection = hull_geom.intersection(ref_dissolved)
            coverage_pct = (intersection.area / ref_dissolved.area) * 100 if ref_dissolved.area > 0 else 0
    except Exception as e:
        print(f"[DuckDB Spatial] Coverage calculation error: {e}")


    processing_time = time.time() - start_time
    print(f"[DuckDB Spatial] Geometry processing done in {processing_time:.2f}s")

    # Clean geometry type for display
    display_geom_type = geom_type.replace("ST_", "").title()

    return {
        "Score de complétude géographique": f"présentes: {round(non_empty/total, 2)}, valides: {round(valid_count/total, 2)}",
        "CRS": f"EPSG:{source_crs}" if source_crs and source_crs != 2154 else "EPSG:2154",
        "Types de géométrie": display_geom_type,
        "Emprise estimée (km2)": round(area_km2, 2),
        "Densité (obj/km2)": round(density, 2),
        "Taux de remplissage géométrique (%)": round(fill_rate, 2),
        "Complexité moyenne des géométries": complexity,
        "Part des geometries dupliquees (%)": round(dup_pct, 2),
        "Couverture territoriale hexagonale (%)": round(coverage_pct, 2),
    }


def process_geometry_duckdb(conn, table_name, x_col, y_col, gdf_metro):
    """Backward compatible wrapper for point geometry processing."""
    return process_geometry_duckdb_points(conn, table_name, x_col, y_col, gdf_metro)

    total = conn.execute("SELECT COUNT(*) FROM geo_processed").fetchone()[0]
    non_empty = total  # All rows have geometry by construction

    # Validate geometries
    valid_count = conn.execute("SELECT COUNT(*) FROM geo_processed WHERE ST_IsValid(geom)").fetchone()[0]

    # Compute bounding box and area
    bbox = conn.execute("""
        SELECT
            MIN(ST_X(geom)) as minx, MIN(ST_Y(geom)) as miny,
            MAX(ST_X(geom)) as maxx, MAX(ST_Y(geom)) as maxy
        FROM geo_processed
    """).fetchone()

    minx, miny, maxx, maxy = bbox

    # Compute convex hull area
    hull_result = conn.execute("""
        SELECT ST_Area(ST_ConvexHull(ST_Union_Agg(geom))) / 1e6 as hull_area_km2
        FROM geo_processed
    """).fetchone()
    area_km2 = hull_result[0] if hull_result[0] else 0

    # Density
    density = total / area_km2 if area_km2 > 0 else 0

    # Duplicate detection using WKT
    dup_result = conn.execute("""
        WITH wkt_geoms AS (
            SELECT ST_AsText(geom) as wkt FROM geo_processed
        )
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT wkt) as unique_count
        FROM wkt_geoms
    """).fetchone()
    dup_pct = (dup_result[0] - dup_result[1]) / dup_result[0] * 100 if dup_result[0] > 0 else 0

    # Compute fill rate (convex hull area vs bounding box area)
    bbox_area = (maxx - minx) * (maxy - miny) / 1e6 if (maxx > minx and maxy > miny) else 0
    fill_rate = (area_km2 / bbox_area * 100) if bbox_area > 0 else 0

    # Compute coverage against reference (intersection with metro France)
    # For this we need to use GeoPandas since DuckDB doesn't have the reference loaded
    coverage_pct = 0
    try:
        # Get convex hull as WKT and intersect with reference
        hull_wkt = conn.execute("""
            SELECT ST_AsText(ST_ConvexHull(ST_Union_Agg(geom))) FROM geo_processed
        """).fetchone()[0]

        if hull_wkt and gdf_metro is not None:
            from shapely import wkt as shapely_wkt
            hull_geom = shapely_wkt.loads(hull_wkt)
            ref_dissolved = gdf_metro.union_all()
            intersection = hull_geom.intersection(ref_dissolved)
            coverage_pct = (intersection.area / ref_dissolved.area) * 100 if ref_dissolved.area > 0 else 0
    except Exception as e:
        print(f"[DuckDB Spatial] Coverage calculation error: {e}")

    processing_time = time.time() - start
    print(f"[DuckDB Spatial] Geometry processing done in {processing_time:.2f}s")

    return {
        "Score de complétude géographique": f"présentes: {round(non_empty/total, 2)}, valides: {round(valid_count/total, 2)}",
        "CRS": "EPSG:2154",  # Assumed Lambert 93 based on coordinate ranges
        "Types de géométrie": "Point",
        "Emprise estimée (km2)": round(area_km2, 2),
        "Densité (obj/km2)": round(density, 2),
        "Taux de remplissage géométrique (%)": round(fill_rate, 2),
        "Complexité moyenne des géométries": "None : POINT",
        "Part des geometries dupliquees (%)": round(dup_pct, 2),
        "Couverture territoriale hexagonale (%)": round(coverage_pct, 2),
    }


def inspect_csv_duckdb(filepath, gdf_metro):
    """
    Inspect CSV file using DuckDB for faster processing.

    Performance improvements:
    - 10-100x faster CSV reading for large files
    - SQL-based aggregations instead of pandas
    - DuckDB spatial for geometry processing (much faster than GeoPandas)
    """
    global last_gdf

    print(f"[DuckDB] Inspecting CSV: {filepath}")
    start_time = time.time()

    meta = get_file_metadata(filepath)

    # Create DuckDB connection
    conn = get_duckdb_connection()

    # Read CSV directly with DuckDB (auto-detects format)
    try:
        conn.execute(f"""
            CREATE TABLE csv_data AS
            SELECT * FROM read_csv('{filepath}',
                header=true,
                auto_detect=true,
                ignore_errors=true,
                sample_size=100000
            )
        """)
    except Exception as e:
        print(f"[DuckDB] Error reading CSV: {e}")
        # Fallback: try with different settings
        conn.execute(f"""
            CREATE TABLE csv_data AS
            SELECT * FROM read_csv('{filepath}',
                header=true,
                all_varchar=true,
                ignore_errors=true
            )
        """)

    read_time = time.time() - start_time
    print(f"[DuckDB] CSV read in {read_time:.2f}s")

    # Get row and column counts
    row_count = conn.execute("SELECT COUNT(*) FROM csv_data").fetchone()[0]
    col_count = len(conn.execute("DESCRIBE csv_data").fetchdf())

    # Detect geo info
    geo_keys = detect_geo_join_keys_duckdb(conn, "csv_data")
    res_geom = get_geo_columns_duckdb(conn, "csv_data")

    geo_key_cols = [k.split(" (")[0] for k in geo_keys]
    geo_key_completeness = completeness_score_duckdb_cols(conn, "csv_data", geo_key_cols)

    # Build base summary
    base_summary = {
        **meta,
        "Type de fichier": "CSV (DuckDB)",
        "Nb lignes": row_count,
        "Nb colonnes": col_count,
        "Colonnes": {"_table": True, "data": build_columns_detail_duckdb(conn, "csv_data")},
        "Score de complétude global": completeness_score_duckdb(conn, "csv_data"),
        "Clés géographiques": ", ".join(geo_keys) if geo_keys else "Aucune",
        "Géotransformation": res_geom['geotrans'],
        "Score de complétude des clés géographique": geo_key_completeness,
    }

    geo_summary = get_default_geo_summary()

    # Process geometry if detected - use DuckDB spatial for points_from_xy
    if res_geom['columns'] and res_geom['method'] == 'points_from_xy':
        print(f"[DuckDB] Geometry detected: {res_geom['columns']} ({res_geom['method']})")

        x_col, y_col = res_geom['columns']

        # Use DuckDB spatial for all geometry processing
        geo_summary = process_geometry_duckdb(conn, "csv_data", x_col, y_col, gdf_metro)
        base_summary["Type de fichier"] = "CSV with Geometry (DuckDB Spatial)"

        # Create GeoDataFrame for map display (sample only)
        sample_df = conn.execute(f"""
            SELECT * FROM csv_data
            WHERE "{x_col}" IS NOT NULL AND "{y_col}" IS NOT NULL
            USING SAMPLE 1000
        """).fetchdf()

        if len(sample_df) > 0:
            detected_crs = guess_crs_from_coords_duckdb(conn, "csv_data", x_col, y_col) or 2154
            geometry = gpd.points_from_xy(sample_df[x_col], sample_df[y_col])
            last_gdf = gpd.GeoDataFrame(sample_df, geometry=geometry, crs=f"EPSG:{detected_crs}")
            if detected_crs != 2154:
                last_gdf = last_gdf.to_crs(epsg=2154)              

    elif res_geom['columns'] and res_geom['method'] == 'linestring_coords':
        print(f"[DuckDB] Geometry detected: {res_geom['columns']} ({res_geom['method']})")

        x_start, y_start, x_end, y_end = res_geom['columns']
        geo_summary = process_geometry_duckdb_linestrings(conn, "csv_data", x_start, y_start, x_end, y_end, gdf_metro)
        base_summary["Type de fichier"] = "CSV with Geometry (DuckDB Spatial)"

        # Create sample GeoDataFrame for map display (from already-projected data)
        try:
            sample_wkt = conn.execute("""
                SELECT ST_AsText(geom) as wkt FROM geo_processed
                USING SAMPLE 1000
            """).fetchdf()

            if len(sample_wkt) > 0:
                from shapely import wkt as shapely_wkt
                geometries = [shapely_wkt.loads(w) for w in sample_wkt['wkt'] if w]
                last_gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:2154")
        except Exception as e:
            print(f"[DuckDB] Sample GeoDataFrame creation error: {e}")

    elif res_geom['columns'] and res_geom['method'] in ['from_wkt', 'geojson', 'geopoint']:
        # For WKT/GeoJSON, fall back to GeoPandas (more complex parsing)
        print(f"[DuckDB] Geometry detected: {res_geom['columns']} ({res_geom['method']})")
        df = conn.execute("SELECT * FROM csv_data").fetchdf()
        gdf = create_geodataframe_from_result(df, res_geom)
        gdf_proj, geo_metrics = process_geodataframe(gdf, gdf_metro)

        if geo_metrics:
            geo_summary = geo_metrics
            base_summary["Type de fichier"] = "CSV with Geometry (DuckDB)"
            last_gdf = gdf_proj

    conn.close()

    total_time = time.time() - start_time
    print(f"[DuckDB] Total inspection time: {total_time:.2f}s")
    granularite = detect_granularite(
        base_summary.get("Clés géographiques", "Aucune"),
        geo_summary
    )
    summary_rows.append({
        **base_summary,
        **geo_summary,
        "Granularité": granularite,
    })
    print(f"\n{filepath} done\n")

# ============================================================================
# EXCEL HELPERS
# ============================================================================

def find_best_data_sheet(wb, sheet_names):
    """Find the sheet most likely to contain data."""
    if len(sheet_names) == 1:
        return sheet_names[0]

    metadata_patterns = ['readme', 'lisez', 'info', 'indic', 'description', 'metadata', 'note', 'about', 'legend', 'source']
    data_patterns = ['data', 'donnee', 'donnée', 'tableau', 'main', 'result', 'mesure', 'values', 'export']

    best_sheet = sheet_names[0]
    best_score = -1

    for sheet_name in sheet_names:
        ws = wb[sheet_name]
        score = 0
        name_lower = sheet_name.lower()

        if any(p in name_lower for p in metadata_patterns):
            score -= 10
        if any(p in name_lower for p in data_patterns):
            score += 10

        score += min((ws.max_row or 0) / 100, 50)
        score += min(ws.max_column or 0, 20)

        if score > best_score:
            best_score = score
            best_sheet = sheet_name

    return best_sheet
    
def find_header_row(ws, max_rows_to_check=30):
    """Detect header row from worksheet."""
    rows_data = list(ws.iter_rows(max_row=max_rows_to_check, values_only=True))
    if not rows_data:
        return 0

    best_header_row = 0
    best_score = 0

    for row_idx, row in enumerate(rows_data[:-1]):
        non_empty = sum(1 for c in row if c is not None and str(c).strip())
        if non_empty < 2:
            continue

        string_cells = sum(
            1 for c in row
            if c is not None and isinstance(c, str)
            and not str(c).replace('.', '').replace('-', '').replace(',', '').isdigit()
        )

        next_row = rows_data[row_idx + 1]
        next_non_empty = sum(1 for c in next_row if c is not None and str(c).strip())

        score = non_empty + string_cells * 0.5 + (next_non_empty * 0.3 if next_non_empty > 0 else 0)
        if non_empty >= 3 and next_non_empty >= non_empty * 0.5:
            score += 5

        if score > best_score:
            best_score = score
            best_header_row = row_idx

    return best_header_row
def get_geo_columns(df):
    """Identify geometry columns and return structured info."""
    lat_pattern = r'\b(lat|latitude)\b'
    lon_pattern = r'\b(lon|long|lng|longitude)\b'
    x_pattern = r'(^x[^a-zA-Z0-9]?|[^a-zA-Z0-9]x$)'
    y_pattern = r'(^y[^a-zA-Z0-9]?|[^a-zA-Z0-9]y$)'
    geom_pattern = r'\b(geometry|geom|shape|point|polygon)\b'
    addr_pattern = r'adresse'
    insee_pattern = r'(dep|reg|insee|com)'

    result = {
        'columns': [],
        'type': None,
        'method': None,
        'geo_keys': [],
        'geotrans': 'Aucune géométrie',
    }

    # 1. Check for GeoJSON/WKT geometry columns
    for col in df.columns:
        col_lc = col.lower()
        if not re.search(geom_pattern, col_lc):
            continue

        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
        if sample is None:
            continue

        # Check geo_point format
        if 'point' in col_lc and 'geo' in col_lc and isinstance(sample, str) and ',' in sample:
            return {**result, 'type': 'Point', 'method': 'geopoint', 'columns': [col], 'geotrans': "Présence géométrie"}

        # Check GeoJSON
        try:
            val = json.loads(sample) if isinstance(sample, str) else sample
            if isinstance(val, dict) and 'type' in val and 'coordinates' in val:
                return {**result, 'type': val.get('type', 'Unknown'), 'method': 'geojson', 'columns': [col], 'geotrans': "Présence géométrie"}
        except:
            pass

        # Check WKT
        if isinstance(sample, str):
            sample_upper = sample.upper()
            for geom_type in ['POINT', 'LINESTRING', 'POLYGON']:
                if geom_type in sample_upper:
                    return {**result, 'type': geom_type.title(), 'method': 'from_wkt', 'columns': [col], 'geotrans': "Présence géométrie"}

    # 2. Check for Lat/Lon pairs
    lat_col = lon_col = None
    for col in df.columns:
        col_lc = col.lower()
        if re.search(lat_pattern, col_lc) and lat_col is None:
            lat_col = col
        if re.search(lon_pattern, col_lc) and lon_col is None:
            lon_col = col

    if lat_col and lon_col:
        return {**result, 'type': 'Point', 'method': 'points_from_xy', 'columns': [lon_col, lat_col], 'geotrans': "Présence géométrie séparée (x,y)"}

    # 3. Check for X/Y pairs
    x_cols = [col for col in df.columns if re.search(x_pattern, col.lower())]
    y_cols = [col for col in df.columns if re.search(y_pattern, col.lower())]

    if x_cols and y_cols:
        for x_col in x_cols:
            for y_col in y_cols:
                x_suffix = x_col.lower().replace('x', '')
                y_suffix = y_col.lower().replace('y', '')

                if x_suffix == y_suffix or (not x_suffix and not y_suffix):
                    # Check for LineString (start/end coords)
                    if any(k in x_col.lower() for k in ['d', 'debut', 'start']):
                        for x_end in x_cols:
                            if x_end != x_col and any(k in x_end.lower() for k in ['f', 'fin', 'end']):
                                for y_end in y_cols:
                                    if y_end != y_col and any(k in y_end.lower() for k in ['f', 'fin', 'end']):
                                        return {**result, 'type': 'LineString', 'method': 'linestring_coords', 
                                                'columns': [x_col, y_col, x_end, y_end], 'geotrans': "Présence géométrie multiples (x1,y1), (x2,y2)"}

                    return {**result, 'type': 'Point', 'method': 'points_from_xy', 'columns': [x_col, y_col], 'geotrans': "Présence géométrie séparée (x,y)"}

    # 4. Check for address columns
    for col in df.columns:
        if re.search(addr_pattern, col.lower()):
            return {**result, 'type': 'Address', 'method': 'geocoding_required', 'columns': [col], 'geotrans': "Géocodage de l'adresse"}

    # 5. Collect INSEE/geographic keys (BUG FIX: this was unreachable before)
    for col in df.columns:
        if re.search(insee_pattern, col.lower()):
            result['geo_keys'].append(col)

    if result['geo_keys']:
        result['type'] = 'Administrative'
        result['method'] = 'join_required'
        result['columns'] = result['geo_keys']
        result['geotrans'] = "Jointure spatiale à l'aide de clés géographiques"

    return result
# ============================================================================
# GEOGRAPHIC DETECTION
# ============================================================================
def detect_geo_join_keys(df):
    """Detect geographic join key columns."""
    candidates = []
    pattern = r'(dep|reg|insee|com|code)'

    for col in df.columns:
        col_lc = col.lower()
        match = re.search(pattern, col_lc)
        if not match:
            continue

        if df[col].notna().sum() == 0:
            continue

        avg_len = df[col].astype(str).str.len().mean()
        is_numeric = pd.api.types.is_numeric_dtype(df[col])
        w_match = col_lc[match.start():match.end()]

        geo_type = None
        if 3.5 <= avg_len <= 5.5:
            geo_type = "code_insee_commune (zéros perdus)" if is_numeric and avg_len < 4.5 else "code_insee_commune"
        elif 1.5 <= avg_len <= 2.5:
            if 'reg' in w_match:
                geo_type = "code_region"
            elif 'dep' in w_match:
                geo_type = "code_departement"
            else:
                geo_type = "code_departement_or_region"

        if geo_type:
            candidates.append(f"{col} ({geo_type})")

    return candidates
    
def fix_insee_codes(df):
    """Fix INSEE codes that were read as integers."""
    fixed_columns = []
    pattern = r'(dep|reg|insee|com|code)'

    for col in df.columns:
        if not re.search(pattern, col.lower()):
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        if df[col].notna().sum() == 0:
            continue

        avg_len = df[col].astype(str).str.len().mean()

        if 3.5 <= avg_len < 4.5:
            df[col] = df[col].apply(lambda x: str(int(x)).zfill(5) if pd.notna(x) else x)
            fixed_columns.append((col, 5, "code_insee_commune"))
        elif 1.0 <= avg_len < 1.5:
            df[col] = df[col].apply(lambda x: str(int(x)).zfill(2) if pd.notna(x) else x)
            fixed_columns.append((col, 2, "code_departement"))

    return df, fixed_columns

def create_geodataframe_from_result(df, res_geom):
    """Create GeoDataFrame from detection result."""
    method = res_geom['method']
    cols = res_geom['columns']

    if method == 'points_from_xy':
        x_col, y_col = cols
        geometry = gpd.points_from_xy(df[x_col], df[y_col])
        return gpd.GeoDataFrame(df, geometry=geometry)

    elif method == 'geojson':
        def parse_geojson(val):
            if pd.isna(val):
                return None
            try:
                geom_dict = json.loads(val) if isinstance(val, str) else val
                return shape(geom_dict)
            except:
                return None

        df = df.copy()
        df['geometry'] = df[cols[0]].apply(parse_geojson)
        return gpd.GeoDataFrame(df, geometry='geometry')

    elif method == 'geopoint':
        def parse_geopoint(val):
            if pd.isna(val) or not val:
                return None
            try:
                parts = str(val).split(',')
                if len(parts) == 2:
                    return Point(float(parts[1].strip()), float(parts[0].strip()))
            except:
                return None

        df = df.copy()
        df['geometry'] = df[cols[0]].apply(parse_geopoint)
        return gpd.GeoDataFrame(df, geometry='geometry')

    elif method == 'from_wkt':
        def parse_wkt(val):
            if pd.isna(val):
                return None
            try:
                return wkt.loads(str(val))
            except:
                return None

        df = df.copy()
        df['geometry'] = df[cols[0]].apply(parse_wkt)
        return gpd.GeoDataFrame(df, geometry='geometry')

    elif method == 'linestring_coords':
        x_start, y_start, x_end, y_end = cols

        def create_linestring(row):
            try:
                def to_float(val):
                    if pd.isna(val):
                        return None
                    if isinstance(val, str):
                        val = val.replace(',', '.')
                    return float(val)

                coords = [to_float(row[c]) for c in [x_start, y_start, x_end, y_end]]
                if any(v is None for v in coords):
                    return None
                return LineString([(coords[0], coords[1]), (coords[2], coords[3])])
            except:
                return None

        df = df.copy()
        df['geometry'] = df.apply(create_linestring, axis=1)
        return gpd.GeoDataFrame(df, geometry='geometry')

    else:
        raise ValueError(f"Unknown method: {method}")

        
def detect_granularite(geo_keys, geo_summary):
    """Déduit la granularité spatiale des données à partir des clés géo et des métriques."""
    
    granularites = []

    # Granularité géométrique
    geom_types = geo_summary.get("Types de géométrie", "N/A")
    if geom_types and geom_types != "N/A":
        geom_lc = geom_types.lower()
        if "point" in geom_lc:
            granularites.append("Ponctuelle (géométrie)")
        elif "linestring" in geom_lc or "line" in geom_lc:
            granularites.append("Linéaire (géométrie)")
        elif "polygon" in geom_lc:
            granularites.append("Surfacique (géométrie)")

    # Granularité des clés géographiques
    if geo_keys and geo_keys != "Aucune":
        keys_lc = geo_keys.lower()
        # Matcher toutes les variantes possibles des labels
        if any(x in keys_lc for x in ["insee", "code_insee", "commune", "postal"]):
            granularites.append("Commune / INSEE")
        elif any(x in keys_lc for x in ["departement", "département", "code_departement"]):
            granularites.append("Département")
        elif any(x in keys_lc for x in ["region", "région", "code_region"]):
            granularites.append("Région")

    return " + ".join(granularites) if granularites else "Inconnue"

def inspect_excel(filepath, gdf_metro, sample_size=5000):
    """
    Inspect Excel file using original optimized reading + DuckDB spatial for geometry.

    Strategy:
    - Use original optimized Excel reader (openpyxl + calamine + smart sampling)
    - Use DuckDB spatial for geometry processing (much faster than GeoPandas)
    """
    global last_gdf

    print(f"[DuckDB] Inspecting Excel: {filepath}")
    start_time = time.time()

    meta = get_file_metadata(filepath)

    # Use original optimized Excel reading logic
    import openpyxl

    wb = openpyxl.load_workbook(filepath, data_only=True, read_only=True)
    sheet_names = wb.sheetnames
    best_sheet = find_best_data_sheet(wb, sheet_names)
    ws = wb[best_sheet]

    total_rows = ws.max_row or 0
    total_cols = ws.max_column or 0
    header_row = find_header_row(ws, max_rows_to_check=30)
    wb.close()

    total_data_rows = total_rows - header_row - 1
    duplicate_pct = 0

    print(f"[DuckDB] Sheet scan done - using '{best_sheet}' ({total_data_rows} rows)")

    # Step 2: Read data with original optimized method
    read_start = time.time()

    if total_data_rows <= sample_size:
        df = pd.read_excel(filepath, sheet_name=best_sheet, header=header_row, engine='calamine')
        res_geom = None
    else:
        print(f"[DuckDB] Large file ({total_data_rows} rows). Using smart sampling...")

        # Read sample for geometry detection
        df_sample = pd.read_excel(filepath, sheet_name=best_sheet, header=header_row, nrows=100, engine='openpyxl')
        df_sample.columns = [str(c).replace('\r\n', ' ').replace('\n', ' ').strip() for c in df_sample.columns]
        res_geom = get_geo_columns(df_sample)

        if res_geom['columns'] and res_geom['method'] in ['points_from_xy', 'linestring_coords']:
            # Smart sampling: read only geometry columns first
            coord_df = pd.read_excel(filepath, sheet_name=best_sheet, header=header_row,
                                    usecols=res_geom['columns'], engine='calamine')

            coord_df['_idx'] = coord_df.index
            duplicate_pct = coord_df.duplicated(subset=res_geom['columns'], keep='first').sum() / len(coord_df) * 100
            coord_unique = coord_df.drop_duplicates(subset=res_geom['columns'], keep='first')

            if len(coord_unique) > sample_size:
                coord_unique = coord_unique.sample(n=sample_size, random_state=42)

            indices = sorted(coord_unique['_idx'].tolist())
            skip_rows = set(range(header_row + 1, total_rows)) - set([header_row + 1 + i for i in indices])

            df = pd.read_excel(filepath, sheet_name=best_sheet, header=header_row,
                              skiprows=list(skip_rows), engine='calamine')
        else:
            df = pd.read_excel(filepath, sheet_name=best_sheet, header=header_row,
                              nrows=sample_size, engine='calamine')

    read_time = time.time() - read_start
    print(f"[DuckDB] Excel read in {read_time:.2f}s")

    # Clean columns
    df.columns = [str(c).replace('\r\n', ' ').replace('\n', ' ').strip() for c in df.columns]
    df, _ = fix_insee_codes(df)

    # Get geo columns if not already detected
    if res_geom is None:
        res_geom = get_geo_columns(df)

    # Step 3: Load into DuckDB for aggregations
    conn = get_duckdb_connection()
    conn.register('excel_data', df)
    conn.execute("CREATE TABLE excel_tbl AS SELECT * FROM excel_data")



    geo_keys = detect_geo_join_keys_duckdb(conn, "excel_data")
    res_geom = get_geo_columns_duckdb(conn, "excel_data")

    geo_key_cols = [k.split(" (")[0] for k in geo_keys]
    geo_key_completeness = completeness_score_duckdb_cols(conn, "excel_data", geo_key_cols)

    # Build sheet info
    sheet_info = f"Feuilles: {len(sheet_names)}, analysée: {best_sheet}"
    if header_row > 0:
        sheet_info += f", en-tête ligne {header_row + 1}"

    base_summary = {
        **meta,
        "Type de fichier": f"EXCEL (DuckDB Spatial) ({sheet_info})",
        "Nb lignes": total_data_rows,
        "Nb colonnes": total_cols,
        "Colonnes": {"_table": True, "data": build_columns_detail_duckdb(conn, "excel_tbl")},
        "Score de complétude global": completeness_score_duckdb(conn, "excel_tbl"),
        "Clés géographiques": ", ".join(geo_keys) if geo_keys else "Aucune",
        "Géotransformation": res_geom['geotrans'],
        "Score de complétude des clés géographique": geo_key_completeness,
    }

    geo_summary = get_default_geo_summary()

    # Step 4: Use DuckDB spatial for geometry processing
    if res_geom['columns'] and res_geom['method'] == 'points_from_xy':
        print(f"[DuckDB] Geometry: {res_geom['columns']} ({res_geom['method']})")

        x_col, y_col = res_geom['columns']
        geo_summary = process_geometry_duckdb_points(conn, "excel_tbl", x_col, y_col, gdf_metro)
        geo_summary['Part des geometries dupliquees (%)'] = round(duplicate_pct, 2)  # Use pre-computed value
        base_summary["Type de fichier"] = f"EXCEL with Geometry (DuckDB Spatial) ({sheet_info})"

        # Create sample GeoDataFrame for map display
        sample_df = df.sample(n=min(1000, len(df)), random_state=42)
        detected_crs = guess_crs_from_coords_duckdb(conn, "excel_tbl", x_col, y_col) or 2154
        geometry = gpd.points_from_xy(sample_df[x_col], sample_df[y_col])
        last_gdf = gpd.GeoDataFrame(sample_df, geometry=geometry, crs=f"EPSG:{detected_crs}")
        
        if detected_crs != 2154:
            last_gdf = last_gdf.to_crs(epsg=2154)

    elif res_geom['columns'] and res_geom['method'] == 'linestring_coords':
        print(f"[DuckDB] Geometry: {res_geom['columns']} ({res_geom['method']})")

        x_start, y_start, x_end, y_end = res_geom['columns']
        geo_summary = process_geometry_duckdb_linestrings(conn, "excel_tbl", x_start, y_start, x_end, y_end, gdf_metro)
        geo_summary['Part des geometries dupliquees (%)'] = round(duplicate_pct, 2)
        base_summary["Type de fichier"] = f"EXCEL with Geometry (DuckDB Spatial) ({sheet_info})"

        # Create sample GeoDataFrame for map display
        try:
            sample_wkt = conn.execute("""
                SELECT ST_AsText(geom) as wkt FROM geo_processed
                USING SAMPLE 1000
            """).fetchdf()

            if len(sample_wkt) > 0:
                from shapely import wkt as shapely_wkt
                geometries = [shapely_wkt.loads(w) for w in sample_wkt['wkt'] if w]
                last_gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:2154")
        except Exception as e:
            print(f"[DuckDB] Sample GeoDataFrame creation error: {e}")

    elif res_geom['columns'] and res_geom['method'] in ['from_wkt', 'geojson', 'geopoint']:
        # For complex geometry types, use GeoPandas
        print(f"[DuckDB] Geometry: {res_geom['columns']} ({res_geom['method']})")
        gdf = create_geodataframe_from_result(df, res_geom)
        gdf_proj, geo_metrics = process_geodataframe(gdf, gdf_metro, compute_duplicates=False)

        if geo_metrics:
            geo_metrics['Part des geometries dupliquees (%)'] = round(duplicate_pct, 2)
            geo_summary = geo_metrics
            base_summary["Type de fichier"] = f"EXCEL with Geometry (DuckDB) ({sheet_info})"
            last_gdf = gdf_proj

    conn.close()

    total_time = time.time() - start_time
    print(f"[DuckDB] Total inspection time: {total_time:.2f}s")

    granularite = detect_granularite(
        base_summary.get("Clés géographiques", "Aucune"),
            geo_summary)
    
    summary_rows.append({
        **base_summary,
        **geo_summary,
        "Granularité": granularite,
    })   
    print(f"\n{filepath} done\n")

def inspect_geospatial_duckdb(filepath, gdf_metro):
    """
    Inspect geospatial file using DuckDB spatial extension for both reading and processing.

    Supports: GeoJSON, Shapefile, GeoPackage, etc.
    Uses DuckDB spatial for all geometry operations (much faster than GeoPandas).
    """
    global last_gdf

    print(f"[DuckDB] Inspecting geospatial file: {filepath}")
    start_time = time.time()

    conn = get_duckdb_connection()

    try:
        # Use DuckDB's spatial extension to read the file
        conn.execute(f"""
            CREATE TABLE geo_data AS
            SELECT * FROM st_read('{filepath}')
        """)

        read_time = time.time() - start_time
        print(f"[DuckDB] Geospatial file read in {read_time:.2f}s")

        meta = get_file_metadata(filepath)

        # Get row count and schema
        row_count = conn.execute("SELECT COUNT(*) FROM geo_data").fetchone()[0]
        schema = conn.execute("DESCRIBE geo_data").fetchdf()
        col_count = len(schema)

        # Find geometry column
        geom_col = None
        
        # 2. Chercher par nom commun si pas trouvé par type
        if geom_col is None:
            geom_name_candidates = ['geom', 'geometry', 'wkb_geometry', 'shape', 'the_geom']
            col_names_lower = {col.lower(): col for col in schema['column_name'].tolist()}
            for candidate in geom_name_candidates:
                if candidate in col_names_lower:
                    geom_col = col_names_lower[candidate]
                    break

        # 3. Abandon propre si aucune géométrie trouvée
        if geom_col is None:
            print(f"[WARN] Aucune colonne géométrique trouvée. Colonnes disponibles: {schema['column_name'].tolist()}")

        # Detect geo join keys

        geo_keys = detect_geo_join_keys_duckdb(conn, "geo_data")
        res_geom = get_geo_columns_duckdb(conn, "geo_data")
    
        geo_key_cols = [k.split(" (")[0] for k in geo_keys]
        geo_key_completeness = completeness_score_duckdb_cols(conn, "geo_data", geo_key_cols)
        
        # Get completeness and column details
        completeness = completeness_score_duckdb(conn, "geo_data")
        
        columns_detail = build_columns_detail_duckdb(conn, "geo_data")

        
        # Use DuckDB spatial for geometry processing
        # geo_summary = process_geometry_duckdb_native(conn, "geo_data", geom_col, gdf_metro)
        if geom_col is not None:
            geo_summary = process_geometry_duckdb_native(conn, "geo_data", geom_col, gdf_metro)
        else:
            geo_summary = get_default_geo_summary()

        # Create sample GeoDataFrame for map display (using GeoPandas for simplicity)
        try:
            # Use geo_processed table which has transformed geometries in EPSG:2154
            sample_wkt = conn.execute("""
                SELECT ST_AsText(geom) as wkt FROM geo_processed
                USING SAMPLE 1000
            """).fetchdf()

            if len(sample_wkt) > 0:
                from shapely import wkt as shapely_wkt
                geometries = [shapely_wkt.loads(w) for w in sample_wkt['wkt'] if w]
                last_gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:2154")
                
        except Exception as e:
            print(f"[DuckDB] Sample GeoDataFrame creation error: {e}")
            # Fallback: read with GeoPandas for map display
            gdf = gpd.read_file(filepath)
            if len(gdf) > 1000:
                gdf = gdf.sample(n=1000, random_state=42)
            if gdf.crs is not None and gdf.crs.to_string() != "EPSG:2154":
                gdf = gdf.to_crs(epsg=2154)
            last_gdf = gdf

        conn.close()

        base_summary ={
            **meta,
            "Type de fichier": "Geospatial (DuckDB Spatial)",
            "Nb lignes": row_count,
            "Nb colonnes": col_count,
            "Colonnes": {"_table": True, "data": columns_detail},
            "Score de complétude global": completeness,
            "Clés géographiques": ", ".join(geo_keys) if geo_keys else "Aucune",
            "Géotransformation": "Données géographiques",
            "Score de complétude des clés géographique": geo_key_completeness
        }

        granularite = detect_granularite(
            base_summary.get("Clés géographiques", "Aucune"),
                geo_summary)
        
        summary_rows.append({
            **base_summary,
            **geo_summary,
            "Granularité": granularite,
        })
        total_time = time.time() - start_time
        print(f"[DuckDB] Total inspection time: {total_time:.2f}s")
        print(summary_rows)

        print(f"\n{filepath} done\n")

    except Exception as e:
        conn.close()
        print(f"[DuckDB] Error: {e}")
        import traceback
        traceback.print_exc()
        raise

# ============================================================================
# SHARED FUNCTIONS (from original)
# ============================================================================
def create_geodataframe_from_result(df, res_geom):
    """Create GeoDataFrame from detection result."""
    method = res_geom['method']
    cols = res_geom['columns']

    if method == 'points_from_xy':
        x_col, y_col = cols
        geometry = gpd.points_from_xy(df[x_col], df[y_col])
        return gpd.GeoDataFrame(df, geometry=geometry)

    elif method == 'geojson':
        def parse_geojson(val):
            if pd.isna(val):
                return None
            try:
                geom_dict = json.loads(val) if isinstance(val, str) else val
                return shape(geom_dict)
            except Exception:
                return None

        df = df.copy()
        df['geometry'] = df[cols[0]].apply(parse_geojson)
        return gpd.GeoDataFrame(df, geometry='geometry')

    elif method == 'geopoint':
        def parse_geopoint(val):
            if pd.isna(val) or not val:
                return None
            try:
                parts = str(val).split(',')
                if len(parts) == 2:
                    return Point(float(parts[1].strip()), float(parts[0].strip()))
            except Exception:
                return None

        df = df.copy()
        df['geometry'] = df[cols[0]].apply(parse_geopoint)
        return gpd.GeoDataFrame(df, geometry='geometry')

    elif method == 'from_wkt':
        def parse_wkt(val):
            if pd.isna(val):
                return None
            try:
                return wkt.loads(str(val))
            except Exception:
                return None

        df = df.copy()
        df['geometry'] = df[cols[0]].apply(parse_wkt)
        return gpd.GeoDataFrame(df, geometry='geometry')

    elif method == 'linestring_coords':
        x_start, y_start, x_end, y_end = cols

        def create_linestring(row):
            try:
                def to_float(val):
                    if pd.isna(val):
                        return None
                    if isinstance(val, str):
                        val = val.replace(',', '.')
                    return float(val)

                coords = [to_float(row[c]) for c in [x_start, y_start, x_end, y_end]]
                if any(v is None for v in coords):
                    return None
                return LineString([(coords[0], coords[1]), (coords[2], coords[3])])
            except Exception:
                return None

        df = df.copy()
        df['geometry'] = df.apply(create_linestring, axis=1)
        return gpd.GeoDataFrame(df, geometry='geometry')

    else:
        raise ValueError(f"Unknown method: {method}")


def guess_crs_from_bounds(gdf):
    """Guess CRS from bounding box coordinates."""
    if gdf.empty or gdf.geometry.is_empty.all():
        return None

    try:
        geom_type = gdf.geometry.iloc[0].geom_type
        if geom_type == 'Point':
            xs, ys = gdf.geometry.x, gdf.geometry.y
        else:
            centroids = gdf.geometry.centroid
            xs, ys = centroids.x, centroids.y

        median_x, median_y = xs.median(), ys.median()

        if -10 < median_x < 10 and 40 < median_y < 60:
            return 4326
        elif 100000 < median_x < 1300000 and 6000000 < median_y < 7400000:
            return 2154
        elif -2.2e6 < median_x < 2.2e6 and -2.2e6 < median_y < 2.2e6:
            return 3857
        return None
    except Exception:
        return None


def process_geodataframe(gdf, gdf_metro, compute_duplicates=True):
    """Process GeoDataFrame: reproject, validate, compute metrics."""
    try:
        if gdf.crs is None:
            epsg_crs = guess_crs_from_bounds(gdf)
            if epsg_crs:
                gdf = gdf.set_crs(epsg=epsg_crs)

        if gdf.crs is not None and gdf.crs.to_string() != "EPSG:2154":
            gdf_proj = gdf.to_crs(epsg=2154)
        else:
            gdf_proj = gdf

        non_empty = (~gdf_proj.geometry.is_empty & gdf_proj.geometry.notna()).sum()
        valid_count = gdf_proj.geometry.dropna().apply(lambda g: g.is_valid).sum()
        total = len(gdf_proj)

        invalid_mask = ~gdf_proj.geometry.is_valid
        if invalid_mask.any():
            gdf_proj = gdf_proj.copy()
            gdf_proj.loc[invalid_mask, 'geometry'] = gdf_proj.loc[invalid_mask, 'geometry'].apply(make_valid)

        merged = gdf_proj.geometry.union_all()
        hull = gpd.GeoSeries([merged], crs=gdf_proj.crs).concave_hull(ratio=0.1, allow_holes=False).iloc[0]
        area_km2 = hull.area / 1e6 if hull and hull.area > 0 else 0
        density = len(gdf_proj) / area_km2 if area_km2 > 0 else 0

        remplissage = taux_de_remplissage(gdf_proj, gdf_metro)
        complexite = complexite_moyenne(gdf_proj)
        doublons = pourcentage_geometries_dupliquees(gdf_proj) if compute_duplicates else {'Part des geometries dupliquees (%)': 0}

        geo_metrics = {
            "Score de complétude géographique": f"présentes: {round(non_empty/total, 2)}, valides: {round(valid_count/total, 2)}",
            "CRS": gdf_proj.crs.to_string() if gdf_proj.crs else "Non défini",
            "Types de géométrie": ", ".join(gdf_proj.geom_type.value_counts().index.tolist()),
            "Emprise estimée (km2)": round(area_km2, 2),
            "Densité (obj/km2)": round(density, 2),
            "Taux de remplissage géométrique (%)": remplissage['Taux de remplissage (%)'],
            "Complexité moyenne des géométries": complexite['Complexite moyenne (sommets)'],
            "Part des geometries dupliquees (%)": doublons['Geometries dupliquees (%)'],
            "Couverture territoriale hexagonale (%)": remplissage['Couverture territoriale (%)'],
        }

        return gdf_proj, geo_metrics

    except Exception as e:
        print(f"Error processing GeoDataFrame: {e}")
        return None, None


def get_default_geo_summary():
    """Return default geo summary with N/A values."""
    return {
        "Score de complétude géographique": "N/A",
        "CRS": "N/A",
        "Types de géométrie": "N/A",
        "Emprise estimée (km2)": None,
        "Densité (obj/km2)": None,
        "Taux de remplissage géométrique (%)": None,
        "Complexité moyenne des géométries": None,
        "Part des geometries dupliquees (%)": None,
        "Couverture territoriale hexagonale (%)": None,
    }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
def inspect_file(filepath, gdf_metro):
    """Main entry point - dispatch to appropriate DuckDB inspector."""
    ext = os.path.splitext(filepath)[-1].lower()

    if ext in ['.csv', '.txt']:
        inspect_csv_duckdb(filepath, gdf_metro)
    elif ext in ['.geojson', '.json', '.shp', '.gpkg']:
        inspect_geospatial_duckdb(filepath, gdf_metro)
    elif ext == ".xlsx":
        inspect_excel(filepath, gdf_metro)


# ============================================================================
# CLI
# ============================================================================
if __name__ == "__main__":
    import sys

    # Load reference data
    REFERENCE_PATH = os.path.join(os.path.dirname(__file__), "data", "regions.geojson")
    gdf_reference = gpd.read_file(REFERENCE_PATH).to_crs(epsg=2154)

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python core.py <file_path>           # Inspect file")
        print()
        print("Examples:")
        print("  python core.py data/pesticides-2002-2022-v03-2024-vf.csv")
        sys.exit(1)

    else:
        inspect_file(sys.argv[1], gdf_reference)

        if summary_rows:
            print("\n--- Summary ---")
            for key, value in summary_rows[-1].items():
                if key != "Colonnes":
                    print(f"  {key}: {value}")
