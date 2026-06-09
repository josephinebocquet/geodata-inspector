# Geodata Metadata Extraction Library

A comprehensive Python library for extracting metadata from geospatial and tabular files with automatic geometry detection, multi-country CRS handling, and spatial metrics calculation.

## Features

- **Multi-format Support**: CSV, Excel (.xlsx), GeoJSON, Shapefile, GeoPackage (tabular/vector) — GeoTIFF, ERDAS Imagine, JPEG 2000, Arc ASCII Grid, SRTM HGT, NetCDF (raster, via `raster.py`)
- **Automatic Geometry Detection**: Lat/lon columns, X/Y coordinates, LineString start/end pairs, WKT, GeoJSON geometries
- **Raster Inspection**: Per-band statistics, cell resolution, WGS84 extent, thumbnail generation, NetCDF subdataset handling (`geodata_inspector.raster.inspect_raster`)
- **Multi-country Support**: Built-in reference files and geographic key patterns for France, UK, Germany, Italy, Spain, USA, and Europe (NUTS)
- **CRS Detection**: Automatically detects coordinate reference systems; area and density always computed in km² regardless of source CRS
- **Bilingual**: French/English with hover tooltips explaining each metric
- **Batch Processing**: Process multiple files or entire directories
- **Multiple Export Formats**: Dict, DataFrame, JSON, CSV, Excel
- **Performance**: DuckDB-powered for fast processing of large files

## Installation

```bash
pip install -e .
# or
pip install -r requirements.txt
```

## Module Location

The library lives in `geodata_inspector/metadata.py`:

```python
from geodata_inspector.metadata import MetadataExtractor
```

## Configuration

The library reads `web_app/config.yaml` to determine the active country, which controls:
- which reference boundary file to load from `reference_file/`
- which metric CRS to use for area/density calculations
- which geographic key patterns to apply for join key detection

```yaml
# web_app/config.yaml
language: fr
localisation:
  country: france   # france | uk | germany | italy | spain | usa | europe | custom
```

### Supported Countries

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

## Quick Start

### Single File Extraction

```python
from geodata_inspector.metadata import MetadataExtractor

# Country and reference file resolved automatically from web_app/config.yaml
extractor = MetadataExtractor()

result = extractor.extract("data/your_file.csv")

if result.success:
    m = result.metadata
    print(f"File: {m['Nom du fichier']}")
    print(f"Rows: {m['Nb lignes']}")
    print(f"CRS: {m['CRS']}")
    print(f"Area: {m['Emprise estimée (km2)']} km²")
    print(f"Density: {m['Densité (obj/km2)']} obj/km²")
```

### Override Country at Runtime

```python
extractor = MetadataExtractor(country="uk")
```

### Batch Processing

```python
results = extractor.extract_batch(["file1.csv", "file2.geojson", "file3.xlsx"])

extractor.to_csv(results, "metadata.csv")
extractor.to_json(results, "metadata.json")
extractor.to_excel(results, "metadata.xlsx")
```

### Directory Processing

```python
results = extractor.extract_from_directory(
    directory="data",
    recursive=True,
    verbose=True
)

stats = extractor.get_summary_stats(results)
print(f"Processed {stats['total_files']} files in {stats['total_time']:.2f}s")
```

## API Reference

### MetadataExtractor Class

#### `__init__(country=None, reference_file=None, metric_crs=None, supported_extensions=None)`

**Parameters:**
- `country` (str, optional): Override the country from `config.yaml`. One of `france`, `uk`, `germany`, `italy`, `spain`, `usa`, `europe`, `custom`.
- `reference_file` (str, optional): Explicit path to a reference GeoJSON (overrides country lookup).
- `metric_crs` (int, optional): Explicit metric CRS EPSG code (overrides country lookup).
- `supported_extensions` (set, optional): Set of supported file extensions.

If none of these are provided, the country is read from `web_app/config.yaml` and the reference file and CRS are resolved from the `LOCALISATIONS` registry in `web_app/config.py`.

#### `extract(filepath, include_geodataframe=False)` → `MetadataResult`

#### `extract_batch(filepaths, verbose=True, stop_on_error=False)` → `List[MetadataResult]`

#### `extract_from_directory(directory, recursive=True, pattern="*", **kwargs)` → `List[MetadataResult]`

#### Static Methods

- `to_dataframe(results, flatten=True, include_errors=True)` → DataFrame
- `to_json(results, output_file=None, flatten=False, indent=2)` → JSON string or file
- `to_csv(results, output_file, flatten=True, **kwargs)` → CSV file
- `to_excel(results, output_file, flatten=True, **kwargs)` → Excel file
- `get_summary_stats(results)` → Dict with `success_rate`, `total_rows`, `total_time`, `successful`, `failed`

### MetadataResult Class

**Attributes:**
- `filepath` (str): Path to the processed file
- `metadata` (Dict): Extracted metadata (None if failed)
- `error` (str): Error message (None if successful)
- `elapsed_time` (float): Processing time in seconds
- `success` (bool): True if extraction succeeded

**Methods:**
- `to_dict(flatten=False)`: Convert result to dictionary

## Metadata Structure

### File Information

| Key | Description |
|-----|-------------|
| `Nom du fichier` | Filename |
| `Taille (Ko)` | File size in KB |
| `Date de création du fichier (Y-M-D)` | File creation date |
| `Type de fichier` | File type description |

### Data Structure

| Key | Description |
|-----|-------------|
| `Nb lignes` | Number of rows |
| `Nb colonnes` | Number of columns |
| `Colonnes` | Nested table: column name, sample value, type, missing values count |

### Data Quality

| Key | Structure | Description |
|-----|-----------|-------------|
| `Score de complétude global` | `{"_table": True, "data": [{"Score de complétude moyen (%)": float, "Score de complétude std (%)": float}]}` | Mean fill rate and std across all columns |
| `Score de complétude des clés géographique` | same `_table` structure | Fill rate restricted to detected geographic key columns |

### Geographic Information

| Key | Description |
|-----|-------------|
| `Clés géographiques` | Nested table: reference area type + identified column. Patterns are country-specific (see `web_app/config.py` `LOCALISATIONS`). |
| `Géotransformation` | Detected geometry method (see values below) |

**`Géotransformation` values:**
- `Aucune géométrie` — no geometry detected
- `Présence géométrie` — native geometry column (WKT, GeoJSON, geopoint)
- `Présence géométrie séparée (x,y)` — separate lat/lon or x/y columns
- `Présence géométrie multiples (x1,y1), (x2,y2)` — LineString from start/end coordinate pairs
- `Géocodage de l'adresse` — address column found, geocoding required
- `Jointure spatiale à l'aide de clés géographiques` — administrative codes for spatial join

### Spatial Metrics

All area-based metrics are computed in km² regardless of source CRS. Non-metric CRS (e.g. WGS84/EPSG:4326) are reprojected to the country's configured metric CRS for area calculations only — stored geometries are never modified.

| Key | Structure | Description |
|-----|-----------|-------------|
| `Score de complétude géographique` | `{"_table": True, "data": [{"Présentes (%)": float, "Valides (%)": float}]}` | Share of geometries present and topologically valid |
| `CRS` | string | Detected source CRS, e.g. `EPSG:4326` |
| `Types de géométrie` | string | Point, LineString, Polygon, etc. |
| `Emprise estimée (km2)` | float | Area of the bounding envelope in km² |
| `Densité (obj/km2)` | float | Objects per km² within estimated extent |
| `Taux de remplissage géométrique (%)` | float | Ratio of geometry area to bounding box area |
| `Complexité moyenne des géométries` | float or string | Average vertex count; `"None : POINT"` for point layers |
| `Part des geometries dupliquees (%)` | float | % of geometries with identical coordinates |
| `Couverture territoriale (%)` | float | Share of the configured reference territory covered |
| `Granularité` | string | Inferred spatial granularity (country-specific codes or geometry type) |

## Examples

### Example 1: Basic Usage

```python
from geodata_inspector.metadata import MetadataExtractor

extractor = MetadataExtractor()
result = extractor.extract("data/pesticides.csv")

if result.success:
    m = result.metadata
    print(f"Rows: {m['Nb lignes']:,}")
    print(f"Geometry: {m['Géotransformation']}")
    print(f"Area: {m['Emprise estimée (km2)']} km²")

    c = m['Score de complétude global']['data'][0]
    print(f"Mean completeness: {c['Score de complétude moyen (%)']}%")
```

### Example 2: Multi-country Batch

```python
from geodata_inspector.metadata import MetadataExtractor

fr_extractor = MetadataExtractor(country="france")
fr_results = fr_extractor.extract_from_directory("data/france")

uk_extractor = MetadataExtractor(country="uk")
uk_results = uk_extractor.extract_from_directory("data/uk")

fr_extractor.to_csv(fr_results, "france_metadata.csv")
uk_extractor.to_csv(uk_results, "uk_metadata.csv")
```

### Example 3: Accessing Nested Metrics

```python
result = extractor.extract("data/file.csv")
if result.success:
    m = result.metadata

    geo = m['Score de complétude géographique']['data'][0]
    print(f"Present: {geo['Présentes (%)']}%, Valid: {geo['Valides (%)']}%")

    keys = m['Score de complétude des clés géographique']['data'][0]
    print(f"Key fill rate: {keys['Score de complétude moyen (%)']}%")

    if isinstance(m['Clés géographiques'], dict):
        for row in m['Clés géographiques']['data']:
            print(f"  {row['Reference area']} → {row['Identified key']}")
```

### Example 4: Error Handling

```python
results = extractor.extract_batch(
    ["file1.csv", "nonexistent.csv", "file2.geojson"],
    stop_on_error=False
)

for r in results:
    if r.success:
        print(f"✓ {r.filepath}: {r.metadata['Nb lignes']} rows")
    else:
        print(f"✗ {r.filepath}: {r.error}")
```

## Metrics Glossary

| Metric | Definition |
|--------|------------|
| `Score de complétude global` | Mean fill rate and std computed across all columns |
| `Clés géographiques` | Columns identified as joinable geographic references — patterns are country-specific |
| `Géotransformation` | Type of geographic representation detected in the file |
| `CRS` | Coordinate Reference System detected from coordinate values |
| `Types de géométrie` | Geometry type(s): Point, LineString, Polygon, etc. |
| `Emprise estimée (km2)` | Bounding envelope area in km² — always metric regardless of source CRS |
| `Densité (obj/km2)` | Objects per km² within the estimated extent |
| `Taux de remplissage géométrique (%)` | Ratio of geometry area to bounding box area |
| `Complexité moyenne des géométries` | Average vertex count; "None : POINT" for point layers |
| `Part des geometries dupliquees (%)` | % of geometries with coordinates identical to another row |
| `Couverture territoriale (%)` | Share of the configured reference territory covered by the data extent |
| `Score de complétude géographique` | Share of geometries that are present and topologically valid |
| `Score de complétude des clés géographique` | Fill rate of columns identified as geographic join keys |

## Supported File Formats

### Tabular / Vector (`MetadataExtractor`)

| Format | Extensions | Notes |
|--------|-----------|-------|
| CSV | `.csv`, `.txt` | Auto-detects delimiter and encoding |
| Excel | `.xlsx` | Smart sampling for large files (Calamine engine) |
| GeoJSON | `.geojson`, `.json` | Native geometry support |
| Shapefile | `.shp` | Requires `.shx`, `.dbf`, `.prj` sidecar files |
| GeoPackage | `.gpkg` | SQLite-based format |

### Raster (`geodata_inspector.raster.inspect_raster`)

| Format | Extensions | Notes |
|--------|-----------|-------|
| GeoTIFF | `.tif`, `.tiff` | Most common raster format |
| ERDAS Imagine | `.img` | |
| JPEG 2000 | `.jp2` | |
| Arc ASCII Grid | `.asc` | |
| SRTM HGT | `.hgt` | Elevation data |
| NetCDF | `.nc` | Subdataset resolution; coordinate variables skipped automatically |

## Raster Inspection API

Raster files are handled by a separate module and are not processed by `MetadataExtractor`. Use `inspect_raster` directly:

```python
from geodata_inspector.raster import inspect_raster
import geopandas as gpd

gdf_reference = gpd.read_file("reference_file/fr_regions.geojson").to_crs(epsg=2154)

result = inspect_raster(
    "path/to/file.tif",
    gdf_reference=gdf_reference,
    metric_crs=2154,
    wgs84_bounds=[-5.5, 41.0, 10.0, 51.5],
    inferred_label="inferred",   # label appended when CRS is inferred
    crs_override=None,           # integer EPSG to force a specific CRS
)

summary = result["summary"]   # dict with all raster metrics
map_data = result["map"]      # {type, bounds, thumbnail (base64 PNG)}

print(f"CRS:        {summary['CRS']}")
print(f"Bands:      {summary['Nb bandes']}")
print(f"Dimensions: {summary['Nb colonnes (pixels)']} × {summary['Nb lignes (pixels)']} px")
print(f"Resolution: {summary['Résolution des cellules (m)']}")
print(f"Area:       {summary['Emprise estimée (km2)']} km²")
print(f"Fill rate:  {summary['Taux de remplissage (%)']}%")

for band in summary["Bandes"]["data"]:
    print(f"  {band['Bande']}: min={band['Min']}, max={band['Max']}, "
          f"mean={band['Moyenne']}, fill={band['Remplissage (%)']}%")

# NetCDF files list available data variables
if "Variables NetCDF disponibles" in summary:
    print(f"NetCDF vars: {summary['Variables NetCDF disponibles']}")
```

## CRS Handling

CRS is detected from median coordinate values. For area/density, non-metric CRS are reprojected to the country's configured metric CRS. Stored geometries are never modified.

## Troubleshooting

### DuckDB Spatial Extension

```bash
python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL spatial; LOAD spatial;')"
```

### Reference File Not Found

If the reference file for the configured country is missing from `reference_file/`, coverage metrics are disabled but all other metrics still compute.

### CRS Detection Issues

If detection returns `None`, area calculations fall back to EPSG:3857. For French data, Lambert 93 coordinates are detected automatically as EPSG:2154.

## See Also

- `LIBRARY_SUMMARY.md`: Quick start guide
- `examples/example_usage.py`: Comprehensive usage examples
- `README.md`: Web application documentation
- `web_app/config.py`: Full `LOCALISATIONS` registry with all country definitions