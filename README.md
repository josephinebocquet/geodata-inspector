# Geodata Inspector

A high-performance geodata inspection tool using DuckDB for fast processing and comprehensive spatial analysis. Supports tabular, vector, and raster geospatial files. Available as a web application and a Python library.

## Features

- **Multi-format Support**: CSV, TXT, Excel (.xlsx), GeoJSON, Shapefile, GeoPackage, ZIP (vector/tabular) — GeoTIFF, ERDAS Imagine, JPEG 2000, Arc ASCII Grid, SRTM HGT, NetCDF (raster)
- **Automatic Geometry Detection**: Points, LineStrings, Polygons, WKT, GeoJSON, native geometry columns, lat/lon pairs
- **Raster Inspection**: Per-band statistics (min/max/mean/std/fill), cell resolution, WGS84 extent, thumbnail preview, NetCDF subdataset handling
- **CRS Detection**: Automatically detects coordinate reference systems; area and density always computed in km² regardless of source CRS; manual CRS override available on the map
- **Multi-country Support**: Built-in reference files and geographic key patterns for France, UK, Germany, Italy, Spain, USA, and Europe (NUTS)
- **Spatial Metrics**: Area, density, coverage, complexity, duplicates, fill rate
- **Temporal Analysis**: Automatic detection of date columns with interactive occupancy curves and histograms; filter by date range, column selection, and granularity (year/month/day)
- **Bilingual Interface**: French/English with hover tooltips (ⓘ) explaining each metric
- **Batch Processing**: Process directories or ZIP archives via the web UI or library; both ZIP upload and folder picker supported; stop button to interrupt
- **Advanced Export**: GeoJSON, GeoPackage, Shapefile, GeoParquet, CSV+WKT (vector) — GeoTIFF, NetCDF, JPEG 2000, ERDAS Imagine, ASCII Grid (raster); with column selection, value filters, temporal filter, and spatial filter (buffer or drawn polygon)
- **Data Preview**: 10-row data preview, with optional spatial filter applied

## Installation

### Option 1 — Docker (recommended for deployment)

```bash
git clone https://github.com/josephinebocquet/geodata-inspector.git
cd geodata-inspector
docker compose up --build
```

The `HOST` environment variable controls the bind address inside the container (defaults to `0.0.0.0` when set via Docker).

### Option 2 — Conda

```bash
git clone https://github.com/josephinebocquet/geodata-inspector.git
# Create and populate the environment from the lock file
conda env create -f environment.yml

# Activate it
conda activate geodata_env
```

To update an existing environment after changes to `environment.yml`:

```bash
conda env update -f environment.yml --prune
```

### Option 3 — pip only

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
├── MANIFEST.in
├── LICENSE
├── examples/
│   └── example_usage.py               # Usage examples
├── geodata_inspector/                 # Core Python package
│   ├── __init__.py
│   ├── core.py                        # Core inspection logic (DuckDB-optimized)
│   ├── metadata.py                    # MetadataExtractor library
│   ├── raster.py                      # Raster file inspection (rasterio-based)
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
  max_upload_size_mb: null   # null = no limit; set an integer to cap uploads
```

The `HOST` and `PORT` environment variables override the `server.host` and `server.port` config values at runtime (useful for Docker deployments).

### Supported Countries

The `country` key selects the reference file, metric CRS, geographic bounds, and geographic key patterns automatically.

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
python ./web_app/app.py
```

Open the URL shown in the terminal (default: `http://localhost:5050`).

#### Single file inspection

Upload one file via drag-and-drop or the Browse button. Supported formats:

- **Tabular / Vector**: CSV, TXT, XLSX, GeoJSON, JSON, SHP (+ sidecars .shx/.dbf/.prj), GPKG, ZIP
- **Raster**: GeoTIFF (.tif/.tiff), ERDAS Imagine (.img), JPEG 2000 (.jp2), Arc ASCII Grid (.asc), SRTM HGT (.hgt), NetCDF (.nc)

After inspection, the UI shows:
- Metadata and quality metrics
- Interactive map preview (up to 1 000 records; raster files show the extent rectangle with a thumbnail overlay)
- Temporal analysis section when date columns are detected
- 10-row data preview button
- CRS override widget if the map positioning looks wrong

#### Batch processing

Two modes are available:
- **ZIP upload**: upload a ZIP archive containing multiple data files; the inspector processes each file and generates a summary CSV/Excel
- **Folder picker**: select a local folder directly from the browser (uses `webkitdirectory`); all supported files are detected and processed automatically

Both modes show a live progress bar and a **Stop** button to interrupt processing early. Results can be downloaded as CSV or Excel once complete.

#### Export

The **Export** button opens a modal with the following options:

| Option | Description |
|---|---|
| **Format** | Vector: GeoJSON, GeoPackage, Shapefile (.zip), GeoParquet, CSV+WKT — Raster: GeoTIFF, NetCDF, JPEG 2000, ERDAS Imagine, ASCII Grid |
| **Columns** | Select which columns to include (vector only) |
| **Bands** | Select which bands to include (raster only) |
| **Value filter** | Filter rows by column value with operators: `=`, `≠`, `<`, `≤`, `>`, `≥`, `contains` |
| **Temporal filter** | Restrict rows to a date range on a chosen date column |
| **Spatial filter** | Clip to a buffer zone (click on map + radius) or a hand-drawn polygon |

> Note: Shapefile export automatically sanitises column names to DBF limits (10 chars, ASCII) and includes a `_field_names.csv` mapping file.

#### Spatial filter (preview)

A **Spatial filter** panel is available on the map: draw a buffer zone or a polygon to preview which rows fall within it before exporting.

#### CRS override

If the map shows data in the wrong location, use the **Reprojection** widget below the map to enter the correct EPSG code. For raster files, a dedicated `/raster_remap` endpoint re-inspects with the overridden CRS.

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

### 4. Raster inspection (direct API)

```python
from geodata_inspector.raster import inspect_raster
import geopandas as gpd

gdf_reference = gpd.read_file("reference_file/fr_regions.geojson").to_crs(epsg=2154)

result = inspect_raster(
    "path/to/your/file.tif",
    gdf_reference=gdf_reference,
    metric_crs=2154,
    wgs84_bounds=[-5.5, 41.0, 10.0, 51.5],
    inferred_label="inferred",
)

summary = result["summary"]
print(f"CRS: {summary['CRS']}")
print(f"Bands: {summary['Nb bandes']}")
print(f"Resolution: {summary['Résolution des cellules (m)']}")
print(f"Area: {summary['Emprise estimée (km2)']} km²")

# Per-band statistics
for band in summary["Bandes"]["data"]:
    print(f"  {band['Bande']}: fill={band['Remplissage (%)']}%, mean={band['Moyenne']}")
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

### Vector / Tabular metrics

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

### Raster metrics

| Metric | Description |
|--------|-------------|
| `CRS` | Detected coordinate reference system (may be inferred from bounds) |
| `Nb bandes` | Number of raster bands |
| `Nb colonnes (pixels)` / `Nb lignes (pixels)` | Pixel dimensions |
| `Bandes` | Per-band table: type, nodata value, min, max, mean, std, fill rate (%) |
| `Résolution des cellules (m)` | Pixel width × height in metres |
| `Étendue (WGS84)` | Four-corner coordinates (SW, NW, SE, NE) in WGS84 |
| `Emprise estimée (km2)` | Bounding-box area in km² (haversine approximation) |
| `Taux de remplissage (%)` | Mean fill rate across all bands (non-nodata pixels) |
| `Couverture territoriale (%)` | Share of the configured reference territory covered |
| `Variables NetCDF disponibles` | For NetCDF files: list of available data variables (skips coordinate axes) |

The web UI shows a hover tooltip (ⓘ) next to each spatial metric, available in FR and EN.

## Troubleshooting

### DuckDB Spatial Extension

```python
import duckdb
conn = duckdb.connect(':memory:')
conn.execute("INSTALL spatial; LOAD spatial;")
```

### Rasterio / PROJ conflict

If rasterio raises a PROJ database error alongside a PostgreSQL/PostGIS installation, the app automatically sets `PROJ_DATA` and `PROJ_LIB` to the conda environment's PROJ directory at startup.

### Reference File Not Found

If the reference file for the configured country is missing from `reference_file/`, coverage metrics are disabled but all other metrics still compute.

### Shapefile Upload

Select all components together (`.shp`, `.shx`, `.dbf`, `.prj`) in the upload panel.

### CRS Detection

If CRS detection fails, area calculations fall back to EPSG:3857. For French data, Lambert 93 coordinates are detected automatically as EPSG:2154. Use the **Reprojection** widget on the map to override the detected CRS manually.

### Upload Size Limit

Set `max_upload_size_mb` in `web_app/config.yaml` to cap file upload size. Set to `null` (the default) for no limit.

## Contributing

To add support for a new country:

1. Add a reference `.geojson` file to `reference_file/`
2. Add an entry to `LOCALISATIONS` in `web_app/config.py` with `reference_file`, `metric_crs`, `label`, `wgs84_bounds`, and `geo_keys`
3. Set `country: your_country` in `web_app/config.yaml`

## License

- DuckDB: MIT License
- GeoPandas: BSD License
- Flask: BSD License
- Rasterio: BSD License

## Credits

Developed for the GeoCancer project for efficient geodata quality assessment in cancer research data analysis.
