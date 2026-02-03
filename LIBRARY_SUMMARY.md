# Geodata Metadata Library - Summary

I've created a comprehensive Python library that extracts metadata from various file formats and returns it as dictionaries or DataFrames.

## Files Created

1. **`geodata_metadata.py`** - Main library file with:
   - `MetadataExtractor` class (main API)
   - `MetadataResult` class (result container)
   - Convenience functions for quick usage
   - Export methods (DataFrame, JSON, CSV, Excel)

2. **`example_usage.py`** - Comprehensive examples showing:
   - Single file extraction
   - Batch processing
   - Directory processing
   - Error handling
   - Custom analysis
   - Integration with existing code

3. **`README_LIBRARY.md`** - Complete documentation with:
   - Installation instructions
   - API reference
   - Metadata structure
   - Examples
   - Troubleshooting

4. **`test_library.py`** - Simple test script to verify everything works

## Key Features

### ✓ Multiple Input Methods
```python
# Single file
result = extractor.extract("file.csv")

# Multiple files
results = extractor.extract_batch(["file1.csv", "file2.geojson"])

# Entire directory
results = extractor.extract_from_directory("data", recursive=True)
```

### ✓ Multiple Output Formats
```python
# Dictionary
metadata = result.metadata

# Flattened dictionary
flat_dict = result.to_dict(flatten=True)

# DataFrame
df = extractor.to_dataframe(results)

# JSON
extractor.to_json(results, "output.json")

# CSV
extractor.to_csv(results, "output.csv")

# Excel
extractor.to_excel(results, "output.xlsx")
```

### ✓ Comprehensive Metadata

Each extraction returns a dictionary with:
- **File info**: name, size, type, creation date
- **Data structure**: rows, columns, column details
- **Data quality**: completeness scores
- **Geographic info**: CRS, geometry types, transformation method
- **Spatial metrics**: area, density, coverage, complexity, duplicates
- **Processing info**: elapsed time

### ✓ Error Handling
```python
result = extractor.extract("file.csv")
if result.success:
    print(result.metadata)
else:
    print(f"Error: {result.error}")
```

### ✓ Summary Statistics
```python
stats = extractor.get_summary_stats(results)
print(f"Success rate: {stats['success_rate']}%")
print(f"Total rows: {stats['total_rows']:,}")
print(f"Total time: {stats['total_time']:.2f}s")
```

## Quick Start

### Basic Usage

```python
from geodata_metadata import MetadataExtractor

# Initialize
extractor = MetadataExtractor(reference_file="data/regions.geojson")

# Extract metadata
result = extractor.extract("data/your_file.csv")

if result.success:
    m = result.metadata
    print(f"Rows: {m['Nb lignes']}")
    print(f"Columns: {m['Nb colonnes']}")
    print(f"CRS: {m['CRS']}")
    print(f"Area: {m['Emprise estimée (km2)']} km²")
```

### Batch Processing

```python
# Process multiple files
files = ["file1.csv", "file2.geojson", "file3.xlsx"]
results = extractor.extract_batch(files)

# Export to CSV
extractor.to_csv(results, "metadata_report.csv")

# Get statistics
stats = extractor.get_summary_stats(results)
print(f"Processed {stats['total_files']} files")
```

## Metadata Structure

The returned dictionary contains these keys:

```python
{
    # File information
    "Dossier": "data",
    "Nom du fichier": "file.csv",
    "Taille (Ko)": 1234.56,
    "Date de création du fichier (Y-M-D)": "2026-02-03",
    "Type de fichier": "CSV with Geometry (DuckDB Spatial)",

    # Data structure
    "Nb lignes": 10000,
    "Nb colonnes": 15,
    "Colonnes": {...},  # Detailed column info

    # Data quality
    "Score de complétude global": {
        "Score de complétude moyen": 0.95,
        "Score de complétude std": 0.12
    },

    # Geographic information
    "Géotransformation": "Présence géométrie séparée (x,y)",
    "Clés géographiques": "code_commune, code_region",
    "CRS": "EPSG:2154",
    "Types de géométrie": "Point",

    # Spatial metrics
    "Score de complétude géographique": "% présentes: 0.98, % valides: 0.97",
    "Emprise estimée (km2)": 5432.10,
    "Densité (obj/km2)": 1.84,
    "Taux de remplissage (%)": 45.2,
    "Complexite moyenne": "None : POINT",
    "Geometries dupliquees (%)": 2.3,
    "Couverture territoriale hexagonale (%)": 75.4,

    # Processing
    "Temps de traitement (s)": 2.45
}
```



