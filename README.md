# Geodata Inspector

A high-performance geodata inspection tool using DuckDB for fast CSV/Excel processing and comprehensive spatial analysis. Available as a web application and a Python library.

## Features

- **Multi-format Support**: CSV, TXT, Excel (.xlsx), GeoJSON, Shapefile, GeoPackage, ZIP
- **Automatic Geometry Detection**: Points, LineStrings, Polygons, WKT, GeoJSON, native geometry columns
- **CRS Detection**: Automatically detects coordinate reference systems; area and density always computed in km² regardless of source CRS
- **Multi-country Support**: Built-in reference files and geographic key patterns for France, UK, Germany, Italy, Spain, USA, and Europe (NUTS)
- **Spatial Metrics**: Area, density, coverage, complexity, duplicates, fill rate
- **Temporal Analysis**: Automatic detection of date columns with interactive occupancy curves and histograms; filter by date range, column selection, and granularity (year/month/day)
- **Bilingual Interface**: French/English with hover tooltips (ⓘ) explaining each metric
- **Batch Processing**: Process directories or ZIP archives via the web UI or library
- **Multiple Export Formats**: GeoJSON, GeoPackage, Shapefile, CSV+WKT

## Installation

### Option 1 — Conda (recommended)

```bash
# Create and populate the environment from the lock file
conda env create -f environment.yml

# Activate it
conda activate geodata_env
```

To update an existing environment after changes to `environment.yml`:

```bash
conda env update -f environment.yml --prune
```

### Option 2 — pip only

```bash
pip install -r requirements.txt
# or (editable install of the library)
pip install -e .
```

## Project Structure

```
geodata_inspector/
├── README.md                          # This file
├── README_LIBRARY.md                  # Complete library documentation
├── LIBRARY_SUMMARY.md                 # Library quick start guide
├── setup.py
├── requirements.txt
├── pyproject.toml
├── MANIFEST
├── LICENCE
├── examples/
│   └── example_usage.py               # Usage examples
├── geodata_inspector/                 # Core Python package
│   ├── __init__.py
│   ├── core.py                        # Core inspection logic (DuckDB-optimized)
│   ├── metadata.py                    # MetadataExtractor library
│   └── spatial.py                     # Spatial analysis functions
├── reference_file/                    # Reference boundary files (one per country)
│   ├── fr_regions.geojson
│   ├── uk_regions.geojson
│   ├── germany_states.geojson
│   ├── italy_regions.geojson
│   ├── spain_regions.geojson
│   ├── usa_states.geojson
│   └── europe_geographic.geojson
└── web_app/                           # Flask web application
    ├── app.py
    ├── config.py                      # Configuration loader + LOCALISATIONS registry
    ├── config.yaml                    # User configuration (language, country, server)
    └── templates/
        └── index.html                 # Web UI template
```

## Configuration

All settings are stored in `web_app/config.yaml`:

```yaml
language: fr          # fr or en
localisation:
  country: france     # see supported countries below
  custom_reference_path: ''
  custom_metric_crs: 2154
server:
  host: localhost
  port: 5050
  debug: false
```

### Supported Countries

The `country` key selects the reference file, metric CRS, geographic bounds, and geographic key patterns automatically. No file path needs to be specified manually.

| `country` | Reference file | Metric CRS | Geographic keys detected |
|---|---|---|---|
| `france` | `fr_regions.geojson` | EPSG:2154 (Lambert 93) | INSEE commune, postal, département, région, IRIS, EPCI |
| `uk` | `uk_regions.geojson` | EPSG:27700 (BNG) | Postcode, ONS area code, LSOA, MSOA, Local Authority |
| `germany` | `germany_states.geojson` | EPSG:25832 (UTM 32N) | PLZ, Gemeindeschlüssel, Kreisschlüssel, Bundesland, NUTS |
| `italy` | `italy_regions.geojson` | EPSG:25832 (UTM 32N) | CAP, ISTAT comune, provincia, regione, NUTS |
| `spain` | `spain_regions.geojson` | EPSG:2062 (Madrid 1870) | Código postal, INE municipio, provincia, comunidad, NUTS |
| `usa` | `usa_states.geojson` | EPSG:4326 (WGS84) | ZIP code, FIPS state/county, Census tract, state abbreviation |
| `europe` | `europe_geographic.geojson` | EPSG:3035 (ETRS89-LAEA) | NUTS 0–3, LAU code |
| `custom` | `custom_reference_path` | `custom_metric_crs` | None (fallback heuristics) |

### Custom Reference File

```yaml
localisation:
  country: custom
  custom_reference_path: /path/to/my_reference.geojson
  custom_metric_crs: 2154
```

## Usage

### 1. Web Application

```bash
cd web_app
python app.py
```

Open the URL shown in the terminal (default: `http://localhost:5050`).

**Features:**
- Single file inspection with interactive map preview
- Batch processing (ZIP or folder) with file navigator
- Export results to CSV/Excel
- Export geometry to GeoJSON, GeoPackage, Shapefile, or CSV+WKT
- FR/EN language switch with metric tooltips

**Supported file formats**: CSV, TXT, XLSX, GeoJSON, JSON, SHP (+ sidecars), GPKG, ZIP

### 2. Python Library

```python
from geodata_inspector.metadata import MetadataExtractor

# Country and reference file resolved automatically from web_app/config.yaml
extractor = MetadataExtractor()

# Or override country at runtime
extractor = MetadataExtractor(country="uk")

result = extractor.extract("data/your_file.csv")
if result.success:
    m = result.metadata
    print(f"Rows: {m['Nb lignes']}")
    print(f"CRS: {m['CRS']}")
    print(f"Area: {m['Emprise estimée (km2)']} km²")
    print(f"Density: {m['Densité (obj/km2)']} obj/km²")

# Batch processing
results = extractor.extract_batch(["file1.csv", "file2.geojson"])
extractor.to_csv(results, "metadata.csv")
extractor.to_excel(results, "metadata.xlsx")
```

See [README_LIBRARY.md](README_LIBRARY.md) for the full library API reference.

### 3. Direct Core API

```python
import geodata_inspector.core as inspector
from web_app.config import get_config, get_reference_info, get_geo_key_patterns, get_localisation_params
import geopandas as gpd
import os

cfg = get_config()
reference_dir = os.path.join(os.path.dirname(__file__), "reference_file")
ref_info = get_reference_info(cfg, reference_dir)
gdf_reference = gpd.read_file(ref_info["path"]).to_crs(epsg=ref_info["metric_crs"])

inspector.summary_rows.clear()
inspector.inspect_file(
    "path/to/your/file.csv",
    gdf_reference,
    geo_key_patterns=get_geo_key_patterns(cfg),
    wgs84_bounds=get_localisation_params(cfg)["wgs84_bounds"],
    metric_crs=get_localisation_params(cfg)["metric_crs"],
)

if inspector.summary_rows:
    summary = inspector.summary_rows[0]
    print(f"CRS: {summary['CRS']}")
    print(f"Rows: {summary['Nb lignes']}")
```

## Temporal Analysis

When date or timestamp columns are detected in a file, the web UI shows an interactive **Temporal analysis** section below the spatial metrics.

### Chart types

| Detected pattern | Chart |
|---|---|
| A pair of columns matching start/end keywords (e.g. `date_debut` / `date_fin`) | **Occupancy curve** — count of active intervals at each time point |
| Any other date column | **Distribution histogram** — row count per time period |

The granularity (year / month / day) is chosen automatically based on the data's time span, but can be overridden manually.

### Interactive filters

| Control | Effect |
|---|---|
| **Column checkboxes** | Show or hide individual date column charts |
| **From / To date inputs** | Restrict the displayed period |
| **Granularity buttons** | Force year / month / day aggregation (Auto = adaptive) |
| **Apply** | Re-compute charts with the current filter settings |
| **Reset** | Restore original charts and clear all filters |
| **Click a bar** | Instantly zoom into that year, month, or day |

Filters call the `/temporal_filter` endpoint, which re-aggregates the cached fine-grained data without re-reading the source file.

## Metrics Reference

All spatial metrics are computed in km² regardless of source CRS.

| Metric | Description |
|--------|-------------|
| `Score de complétude global` | Mean fill rate (%) and std across all columns |
| `Clés géographiques` | Columns identified as geographic join keys — patterns are country-specific |
| `Géotransformation` | Geometry representation detected (native, x/y, geocoding, spatial join) |
| `Score de complétude géographique` | % of geometries present and topologically valid |
| `CRS` | Detected coordinate reference system |
| `Types de géométrie` | Point, LineString, Polygon, etc. |
| `Emprise estimée (km2)` | Bounding envelope area in km² |
| `Densité (obj/km2)` | Objects per km² within estimated extent |
| `Taux de remplissage géométrique (%)` | Ratio of geometry area to bounding box area |
| `Complexité moyenne des géométries` | Average vertex count per geometry |
| `Part des geometries dupliquees (%)` | % of geometries with identical coordinates |
| `Couverture territoriale (%)` | Share of the configured reference territory covered |
| `Granularité` | Inferred spatial granularity (country-specific codes or geometry type) |

The web UI shows a hover tooltip (ⓘ) next to each spatial metric, available in FR and EN.

## Troubleshooting

### DuckDB Spatial Extension

```python
import duckdb
conn = duckdb.connect(':memory:')
conn.execute("INSTALL spatial; LOAD spatial;")
```

### Reference File Not Found

If the reference file for the configured country is missing from `reference_file/`, coverage metrics are disabled but all other metrics still compute.

### Shapefile Upload

Select all components together (`.shp`, `.shx`, `.dbf`, `.prj`) in the upload panel.

### CRS Detection

If CRS detection fails, area calculations fall back to EPSG:3857. For French data, Lambert 93 coordinates are detected automatically as EPSG:2154.

## Contributing

To add support for a new country:

1. Add a reference `.geojson` file to `reference_file/`
2. Add an entry to `LOCALISATIONS` in `web_app/config.py` with `reference_file`, `metric_crs`, `label`, `wgs84_bounds`, and `geo_keys`
3. Set `country: your_country` in `web_app/config.yaml`

## License

- DuckDB: MIT License
- GeoPandas: BSD License
- Flask: BSD License

## Credits

Developed for the GeoCancer project for efficient geodata quality assessment in cancer research data analysis.