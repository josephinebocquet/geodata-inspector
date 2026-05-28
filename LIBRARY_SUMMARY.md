# Geodata Metadata Library - Summary

A Python library that extracts metadata from geospatial and tabular files and returns structured dictionaries or DataFrames.

## Project Structure

```
geodata_inspector/
├── README.md
├── README_LIBRARY.md
├── LIBRARY_SUMMARY.md
├── setup.py
├── requirements.txt
├── pyproject.toml
├── MANIFEST
├── LICENCE
├── examples/
│   └── example_usage.py
├── geodata_inspector/              ← Python package
│   ├── __init__.py
│   ├── core.py                     ← Core inspection logic (DuckDB)
│   ├── metadata.py                 ← MetadataExtractor library (this file)
│   └── spatial.py                  ← Spatial analysis functions
├── reference_file/                 ← Reference boundaries (one per country)
│   ├── fr_regions.geojson
│   ├── uk_regions.geojson
│   ├── germany_states.geojson
│   ├── italy_regions.geojson
│   ├── spain_regions.geojson
│   ├── usa_states.geojson
│   └── europe_geographic.geojson
└── web_app/                        ← Flask web application
    ├── app.py
    ├── config.py                   ← LOCALISATIONS registry + config loader
    ├── config.yaml                 ← User configuration
    └── templates/
        └── index.html
```

## Import

```python
from geodata_inspector.metadata import MetadataExtractor
```

## Configuration

The library reads `web_app/config.yaml` automatically. The `country` key controls which reference file, metric CRS, and geographic key patterns are used — no file path needs to be specified manually.

```yaml
# web_app/config.yaml
language: fr
localisation:
  country: france   # france | uk | germany | italy | spain | usa | europe | custom
```

| `country` | Reference file | Metric CRS |
|---|---|---|
| `france` | `fr_regions.geojson` | EPSG:2154 (Lambert 93) |
| `uk` | `uk_regions.geojson` | EPSG:27700 (BNG) |
| `germany` | `germany_states.geojson` | EPSG:25832 (UTM 32N) |
| `italy` | `italy_regions.geojson` | EPSG:25832 (UTM 32N) |
| `spain` | `spain_regions.geojson` | EPSG:2062 (Madrid) |
| `usa` | `usa_states.geojson` | EPSG:4326 (WGS84) |
| `europe` | `europe_geographic.geojson` | EPSG:3035 (ETRS89-LAEA) |
| `custom` | `custom_reference_path` | `custom_metric_crs` |

## Key Features

### Multiple Input Methods
```python
extractor = MetadataExtractor()            # reads country from config.yaml
extractor = MetadataExtractor(country="uk") # override at runtime

result  = extractor.extract("file.csv")
results = extractor.extract_batch(["file1.csv", "file2.geojson"])
results = extractor.extract_from_directory("data", recursive=True)
```

### Multiple Output Formats
```python
metadata = result.metadata
flat     = result.to_dict(flatten=True)
df       = extractor.to_dataframe(results)
extractor.to_json(results,  "output.json")
extractor.to_csv(results,   "output.csv")
extractor.to_excel(results, "output.xlsx")
```

### Error Handling
```python
result = extractor.extract("file.csv")
if result.success:
    print(result.metadata)
else:
    print(f"Error: {result.error}")
```

### Summary Statistics
```python
stats = extractor.get_summary_stats(results)
print(f"Success rate: {stats['success_rate']}%")
print(f"Total rows:   {stats['total_rows']:,}")
print(f"Total time:   {stats['total_time']:.2f}s")
```

## Metadata Structure

```python
{
    # File information
    "Nom du fichier": "file.csv",
    "Taille (Ko)": 1234.56,
    "Date de création du fichier (Y-M-D)": "2026-02-03",
    "Type de fichier": "CSV with Geometry (DuckDB Spatial)",

    # Data structure
    "Nb lignes": 10000,
    "Nb colonnes": 15,
    "Colonnes": {
        "_table": True,
        "data": [{"Colonne": "col_name", "Exemple": "val", "Type": "VARCHAR", "Valeurs manquantes": 0}]
    },

    # Data quality — all completeness scores use _table structure
    "Score de complétude global": {
        "_table": True,
        "data": [{"Score de complétude moyen (%)": 95.2, "Score de complétude std (%)": 3.1}]
    },

    # Geographic information
    # Keys detected using country-specific patterns (INSEE for France, postcode for UK, etc.)
    "Clés géographiques": {
        "_table": True,
        "data": [{"Reference area": "Code INSEE commune", "Identified key": "code_commune"}]
    },
    "Géotransformation": "Présence géométrie séparée (x,y)",
    "Score de complétude des clés géographique": {
        "_table": True,
        "data": [{"Score de complétude moyen (%)": 98.5, "Score de complétude std (%)": 1.2}]
    },

    # Spatial metrics — area/density always in km² regardless of source CRS
    "Score de complétude géographique": {
        "_table": True,
        "data": [{"Présentes (%)": 98.0, "Valides (%)": 97.4}]
    },
    "CRS": "EPSG:4326",
    "Types de géométrie": "Point",
    "Emprise estimée (km2)": 5432.10,
    "Densité (obj/km2)": 1.84,
    "Taux de remplissage géométrique (%)": 45.2,
    "Complexité moyenne des géométries": "None : POINT",
    "Part des geometries dupliquees (%)": 2.3,
    "Couverture territoriale (%)": 75.4,
    "Granularité": "Commune / INSEE + Ponctuelle (géométrie)",
}
```

## Accessing Nested `_table` Values

All completeness scores and geographic keys use a `_table` structure. Always access via `.data[0]`:

```python
m = result.metadata

# Global completeness
c = m['Score de complétude global']['data'][0]
print(f"Mean: {c['Score de complétude moyen (%)']}%, Std: {c['Score de complétude std (%)']}%")

# Geographic completeness
geo = m['Score de complétude géographique']['data'][0]
print(f"Present: {geo['Présentes (%)']}%, Valid: {geo['Valides (%)']}%")

# Geographic keys
if isinstance(m['Clés géographiques'], dict):
    for row in m['Clés géographiques']['data']:
        print(f"  {row['Reference area']} → {row['Identified key']}")
```

## CRS and Area Calculation

`Emprise estimée (km2)` and `Densité (obj/km2)` are always in km², regardless of source CRS. Non-metric CRS are reprojected to the country's configured metric CRS for area calculation only — stored geometries are never modified.

## Replacing batch_inspect_duckdb.py

```python
from geodata_inspector.metadata import MetadataExtractor

# Country resolved from web_app/config.yaml automatically
extractor = MetadataExtractor()
results = extractor.extract_from_directory("data-20260203T152655Z-3-001", recursive=True)

extractor.to_csv(results, "inspection_summary.csv")
extractor.to_excel(results, "inspection_summary.xlsx")

stats = extractor.get_summary_stats(results)
print(f"Processed: {stats['successful']} files, {stats['failed']} errors")
print(f"Total time: {stats['total_time']:.1f}s")
```