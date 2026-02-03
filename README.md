# Geodata Inspector - DuckDB Optimized

A high-performance geodata inspection tool using DuckDB for fast CSV/Excel processing and comprehensive spatial analysis.

## Features

- **Fast Processing**: DuckDB-powered CSV/Excel reading (10-100x faster for large files)
- **Comprehensive Geometry Support**:
  - Points (lat/lon, x/y coordinates)
  - LineStrings (road networks with start/end coordinates)
  - Polygons (regions, zones)
- **Smart Detection**:
  - Automatic geometry type detection
  - CRS detection and transformation to EPSG:2154
  - French decimal separator handling (comma → dot)
- **Spatial Metrics**:
  - Fill rate, coverage, complexity
  - Duplicate detection
  - Metropolitan France filtering

## Installation

### Prerequisites

Ensure you have conda/mamba installed and the `geodata_env` environment set up:

```bash
conda env create -f environment.yml
conda activate geodata_env
```

### Required Packages

```bash
# Core dependencies
pip install duckdb geopandas pandas numpy flask openpyxl

# Spatial extensions
# DuckDB spatial extension is installed automatically on first use
```

## Usage

### 1. Web Application

Start the Flask web interface for interactive file inspection:

```bash
conda activate geodata_env
python app.py
```

Then open http://127.0.0.1:5000 in your browser.

**Supported file formats**: CSV, Excel (.xlsx), GeoJSON, Shapefile (.shp), GeoPackage (.gpkg), ZIP archives

### 2. Python Library (Recommended)

Use the high-level library for extracting metadata:

```python
from geodata_metadata import MetadataExtractor

# Initialize extractor
extractor = MetadataExtractor(reference_file="data/regions.geojson")

# Extract metadata from a single file
result = extractor.extract("data/your_file.csv")

if result.success:
    metadata = result.metadata
    print(f"Rows: {metadata['Nb lignes']}")
    print(f"Geometry: {metadata['Types de géométrie']}")
    print(f"CRS: {metadata['CRS']}")

# Batch processing
results = extractor.extract_batch(["file1.csv", "file2.geojson"])

# Export to various formats
extractor.to_csv(results, "metadata.csv")
extractor.to_json(results, "metadata.json")
extractor.to_excel(results, "metadata.xlsx")

# Get summary statistics
stats = extractor.get_summary_stats(results)
print(f"Processed {stats['total_files']} files in {stats['total_time']:.2f}s")
```

**See [LIBRARY_SUMMARY.md](LIBRARY_SUMMARY.md) for complete library documentation.**

### 3. Direct API Usage

For low-level access to the inspection module:

```python
import inspect_geodata_duckdb as inspector
import geopandas as gpd

# Load reference data
gdf_reference = gpd.read_file('./data/regions.geojson').to_crs(epsg=2154)

# Inspect a file
inspector.summary_rows.clear()
inspector.inspect_file('path/to/your/file.csv', gdf_reference)

# Get results
if inspector.summary_rows:
    summary = inspector.summary_rows[0]
    print(f"Geometry type: {summary['Types de géométrie']}")
    print(f"CRS: {summary['CRS']}")
    print(f"Rows: {summary['Nb lignes']}")
```

## Project Structure

```
geodata_inspector/
├── app.py                       # Flask web application
├── geodata_metadata.py          # 🆕 Python library for metadata extraction
├── inspect_geodata_duckdb.py    # Core inspection logic (DuckDB-optimized)
├── spatial_metrics.py           # Spatial analysis functions
├── example_usage.py             # 🆕 Library usage examples
├── test_library.py              # 🆕 Library test script
├── data/
│   └── regions.geojson          # Reference data for coverage analysis
├── templates/
│   └── index.html               # Web UI template
├── README.md                    # This file
├── README_LIBRARY.md            # 🆕 Complete library documentation
└── LIBRARY_SUMMARY.md           # 🆕 Library quick start guide
```

## Key Improvements Over Original

1. **LineString Detection**: Properly detects road network data with start/end coordinates (xD/yD → xF/yF)
2. **DuckDB Spatial**: Uses `ST_MakeLine`, `ST_Point`, `ST_Transform` for native geometry processing
3. **French Data Support**: Handles comma decimal separators (e.g., "511656,78")
4. **Performance**:
   - CSV reading: 0.1-0.5s for files with thousands of rows
   - Geometry processing: 0.2-0.4s for LineString creation and transformation
   - Metropolitan France filtering: Automatic filtering of overseas territories

## Examples

### TMJA Road Network Files

The inspector correctly handles French road traffic data (TMJA = Trafic Moyen Journalier Annuel):

```
File: tmja-2019.csv
- Detected: LineString geometry from xD/yD → xF/yF columns
- CRS: EPSG:2154 (Lambert 93)
- Rows: 4,695 road segments
- Processing time: 0.24s
- Complexity: 2.0 (straight-line segments)
```

### Point Data (Monitoring Stations)

```
File: result.csv
- Detected: Point geometry from x_wgs84/y_wgs84
- CRS: EPSG:4326 → EPSG:2154
- Rows: 36,771 points
- Processing time: 0.51s
```

### Polygon Data (Administrative Regions)

```
File: irsn_radon_metropole.shp
- Detected: Native Polygon geometry
- CRS: EPSG:4326 → EPSG:2154
- Processing time: Native DuckDB spatial processing
```

## Technical Details

### Geometry Detection Logic

The `get_geo_columns_duckdb()` function detects geometry through:

1. **Column Name Patterns**:
   - `lat/latitude`, `lon/longitude` → Point
   - `xD/yD` + `xF/yF` → LineString (start/end coordinates)
   - `x/y` pairs → Point
   - `geometry`, `geom`, `shape` → WKT/GeoJSON

2. **Content Analysis**:
   - Samples first row to identify WKT, GeoJSON, or geopoint format
   - Checks for start/end patterns (d/debut/start, f/fin/end)

3. **Suffix Matching**:
   - Pairs coordinates by suffix: `xD` with `yD`, `xF` with `yF`
   - Ensures consistent coordinate pairing

### DuckDB Spatial Functions

- `ST_Point(x, y)`: Create point geometries
- `ST_MakeLine(point1, point2)`: Create LineString from two points
- `ST_Transform(geom, from_crs, to_crs)`: Reproject geometries
- `ST_Centroid(geom)`: Get geometry center for filtering
- `ST_Union_Agg(geom)`: Merge geometries for extent calculation

### Performance Optimization

- **Smart Sampling**: For large Excel files (>5000 rows), samples unique geometries
- **Duplicate Pre-filtering**: Detects duplicates before full processing
- **Lazy Projection**: Only reprojects data when CRS transformation needed
- **Vectorized Operations**: Uses DuckDB's columnar processing for coordinates

## Troubleshooting

### DuckDB Spatial Extension

If you get "spatial extension not found":

```python
import duckdb
conn = duckdb.connect(':memory:')
conn.execute("INSTALL spatial; LOAD spatial;")
```

### Memory Issues

For very large files (>100MB), increase sampling:

```python
# In inspect_geodata_duckdb.py, modify:
sample_size = 10000  # Reduce from default 5000
```

### CRS Detection

If CRS is not detected correctly, it defaults to EPSG:2154 (Lambert 93 for France). You can override by setting CRS explicitly in the code.

## Contributing

To add support for new geometry types or data formats:

1. Add pattern detection in `get_geo_columns_duckdb()`
2. Implement processing in `process_geometry_duckdb_*()` function
3. Add test cases with sample data

## License

This project uses:
- DuckDB (MIT License)
- GeoPandas (BSD License)
- Flask (BSD License)

## Credits

Developed for the GeoCancer project for efficient geodata quality assessment in cancer research data analysis.
