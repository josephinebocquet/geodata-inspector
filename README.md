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

## Troubleshooting

### DuckDB Spatial Extension

If you get "spatial extension not found":

```python
import duckdb
conn = duckdb.connect(':memory:')
conn.execute("INSTALL spatial; LOAD spatial;")
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

