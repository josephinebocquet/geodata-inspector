# Geodata Metadata Extraction Library

A comprehensive Python library for extracting metadata from geospatial and tabular files with automatic geometry detection, CRS transformation, and spatial metrics calculation.

## Features

- **Multi-format Support**: CSV, Excel (.xlsx), GeoJSON, Shapefile, GeoPackage
- **Automatic Geometry Detection**: Detects lat/lon columns, X/Y coordinates, WKT, GeoJSON geometries
- **CRS Detection & Transformation**: Automatically detects and transforms coordinate reference systems
- **Spatial Metrics**: Area, density, coverage, complexity, duplicates, fill rate
- **Batch Processing**: Process multiple files or entire directories
- **Multiple Export Formats**: Dict, DataFrame, JSON, CSV, Excel
- **Performance**: DuckDB-powered for fast processing of large files
- **Error Handling**: Graceful error handling with detailed error messages

## Installation

The library requires the following dependencies:

```bash
pip install pandas geopandas duckdb openpyxl
```

## Quick Start

### Single File Extraction

```python
from geodata_metadata import MetadataExtractor

# Initialize extractor
extractor = MetadataExtractor(reference_file="data/regions.geojson")

# Extract metadata
result = extractor.extract("data/your_file.csv")

if result.success:
    metadata = result.metadata
    print(f"File: {metadata['Nom du fichier']}")
    print(f"Rows: {metadata['Nb lignes']}")
    print(f"Columns: {metadata['Nb colonnes']}")
    print(f"CRS: {metadata['CRS']}")
```

### Batch Processing

```python
# Process multiple files
files = ["file1.csv", "file2.geojson", "file3.xlsx"]
results = extractor.extract_batch(files)

# Export to various formats
extractor.to_csv(results, "metadata.csv")
extractor.to_json(results, "metadata.json")
extractor.to_excel(results, "metadata.xlsx")
```

### Directory Processing

```python
# Process all supported files in a directory
results = extractor.extract_from_directory(
    directory="data",
    recursive=True,
    verbose=True
)

# Get summary statistics
stats = extractor.get_summary_stats(results)
print(f"Processed {stats['total_files']} files in {stats['total_time']:.2f}s")
```

### Convenience Functions

```python
from geodata_metadata import extract_metadata, extract_metadata_batch

# Quick single file
metadata = extract_metadata("data/file.csv")

# Quick batch with export
extract_metadata_batch(
    ["file1.csv", "file2.geojson"],
    output_csv="metadata.csv",
    output_json="metadata.json"
)
```

## API Reference

### MetadataExtractor Class

#### `__init__(reference_file=None, supported_extensions=None)`

Initialize the metadata extractor.

**Parameters:**
- `reference_file` (str, optional): Path to reference GeoJSON for coverage calculations
- `supported_extensions` (set, optional): Set of supported file extensions

**Example:**
```python
extractor = MetadataExtractor(reference_file="data/regions.geojson")
```

#### `extract(filepath, include_geodataframe=False)`

Extract metadata from a single file.

**Parameters:**
- `filepath` (str): Path to the file to inspect
- `include_geodataframe` (bool): If True, include the GeoDataFrame in the result

**Returns:**
- `MetadataResult`: Result object with metadata or error information

**Example:**
```python
result = extractor.extract("data/file.csv")
if result.success:
    print(result.metadata)
else:
    print(result.error)
```

#### `extract_batch(filepaths, verbose=True, stop_on_error=False)`

Extract metadata from multiple files.

**Parameters:**
- `filepaths` (List[str]): List of file paths to process
- `verbose` (bool): If True, print progress information
- `stop_on_error` (bool): If True, stop processing on first error

**Returns:**
- `List[MetadataResult]`: List of results for each file

**Example:**
```python
results = extractor.extract_batch(
    ["file1.csv", "file2.geojson"],
    verbose=True,
    stop_on_error=False
)
```

#### `extract_from_directory(directory, recursive=True, pattern="*", **kwargs)`

Extract metadata from all supported files in a directory.

**Parameters:**
- `directory` (str): Path to directory
- `recursive` (bool): If True, search subdirectories
- `pattern` (str): Glob pattern to match files
- `**kwargs`: Additional arguments passed to extract_batch

**Returns:**
- `List[MetadataResult]`: List of results for each file

**Example:**
```python
results = extractor.extract_from_directory(
    directory="data",
    recursive=True,
    pattern="*"
)
```

#### Static Methods

##### `to_dataframe(results, flatten=True, include_errors=True)`

Convert results to pandas DataFrame.

**Example:**
```python
df = MetadataExtractor.to_dataframe(results, flatten=True)
print(df.head())
```

##### `to_json(results, output_file=None, flatten=False, indent=2)`

Convert results to JSON.

**Example:**
```python
# Write to file
MetadataExtractor.to_json(results, "output.json")

# Get JSON string
json_str = MetadataExtractor.to_json(results)
```

##### `to_csv(results, output_file, flatten=True, **kwargs)`

Convert results to CSV.

**Example:**
```python
MetadataExtractor.to_csv(results, "output.csv")
```

##### `to_excel(results, output_file, flatten=True, **kwargs)`

Convert results to Excel.

**Example:**
```python
MetadataExtractor.to_excel(results, "output.xlsx")
```

#### `get_summary_stats(results)`

Get summary statistics from batch results.

**Returns:**
- `Dict`: Dictionary with statistics

**Example:**
```python
stats = extractor.get_summary_stats(results)
print(f"Success rate: {stats['success_rate']:.1f}%")
print(f"Total rows: {stats['total_rows']:,}")
```

### MetadataResult Class

Container for metadata extraction results.

**Attributes:**
- `filepath` (str): Path to the processed file
- `metadata` (Dict): Extracted metadata (None if failed)
- `error` (str): Error message (None if successful)
- `elapsed_time` (float): Processing time in seconds
- `success` (bool): True if extraction succeeded

**Methods:**
- `to_dict(flatten=False)`: Convert result to dictionary

## Metadata Structure

Each successful extraction returns a dictionary with the following keys:

### File Information
- `Dossier`: Directory path
- `Nom du fichier`: Filename
- `Taille (Ko)`: File size in KB
- `Date de création du fichier (Y-M-D)`: File creation date
- `Type de fichier`: File type description

### Data Structure
- `Nb lignes`: Number of rows
- `Nb colonnes`: Number of columns
- `Colonnes`: Detailed column information (type, examples, missing values)

### Data Quality
- `Score de complétude global`: Overall completeness score
  - `Score de complétude moyen`: Mean completeness (0-1)
  - `Score de complétude std`: Standard deviation

### Geographic Information
- `Géotransformation`: Geometry transformation method
- `Clés géographiques`: Geographic join keys (INSEE codes, etc.)
- `Score de complétude géographique`: Geographic completeness
- `CRS`: Coordinate Reference System
- `Types de géométrie`: Geometry types (Point, Polygon, etc.)

### Spatial Metrics
- `Emprise estimée (km2)`: Estimated area in km²
- `Densité (obj/km2)`: Object density per km²
- `Taux de remplissage (%)`: Fill rate percentage
- `Complexite moyenne`: Average geometry complexity
- `Geometries dupliquees (%)`: Duplicate geometries percentage
- `Couverture territoriale hexagonale (%)`: Territorial coverage percentage

### Processing Information
- `Temps de traitement (s)`: Processing time in seconds

## Examples

### Example 1: Basic Usage

```python
from geodata_metadata import MetadataExtractor

# Initialize
extractor = MetadataExtractor(reference_file="data/regions.geojson")

# Extract metadata
result = extractor.extract("data/pesticides.csv")

if result.success:
    m = result.metadata
    print(f"File: {m['Nom du fichier']}")
    print(f"Rows: {m['Nb lignes']:,}")
    print(f"Columns: {m['Nb colonnes']}")
    print(f"Geometry: {m['Géotransformation']}")
    print(f"Area: {m['Emprise estimée (km2)']} km²")
```

### Example 2: Batch Processing with Analysis

```python
from geodata_metadata import MetadataExtractor

extractor = MetadataExtractor(reference_file="data/regions.geojson")

# Process directory
results = extractor.extract_from_directory("data", recursive=True)

# Convert to DataFrame for analysis
df = extractor.to_dataframe(results, flatten=True)

# Filter files with geometry
geo_files = df[df['Géotransformation'] != 'Aucune géométrie']
print(f"Files with geometry: {len(geo_files)}")

# Find largest files
largest = df.nlargest(5, 'Taille (Ko)')
print("\nLargest files:")
print(largest[['Nom du fichier', 'Taille (Ko)', 'Nb lignes']])

# Export results
extractor.to_csv(results, "metadata_report.csv")
```

### Example 3: Error Handling

```python
from geodata_metadata import MetadataExtractor

extractor = MetadataExtractor()

files = ["file1.csv", "nonexistent.csv", "file2.geojson"]
results = extractor.extract_batch(files, stop_on_error=False)

# Separate successful and failed
successful = [r for r in results if r.success]
failed = [r for r in results if not r.success]

print(f"Successful: {len(successful)}")
print(f"Failed: {len(failed)}")

for result in failed:
    print(f"  {result.filepath}: {result.error}")
```

### Example 4: Custom Analysis

```python
from geodata_metadata import MetadataExtractor
import pandas as pd

extractor = MetadataExtractor(reference_file="data/regions.geojson")
results = extractor.extract_from_directory("data")

# Convert to DataFrame
df = extractor.to_dataframe(results, flatten=True)

# Calculate statistics
total_rows = df['Nb lignes'].sum()
avg_completeness = df['Score de complétude global - Score de complétude moyen'].mean()

print(f"Total rows: {total_rows:,}")
print(f"Average completeness: {avg_completeness:.2%}")

# Group by file type
print("\nFiles by type:")
print(df.groupby('Type de fichier')['Nb lignes'].agg(['count', 'sum']))
```

## Supported File Formats

| Format | Extensions | Notes |
|--------|-----------|-------|
| CSV | `.csv`, `.txt` | Auto-detects delimiter and encoding |
| Excel | `.xlsx` | Smart sampling for large files |
| GeoJSON | `.geojson`, `.json` | Native geometry support |
| Shapefile | `.shp` | Requires .shx, .dbf, .prj files |
| GeoPackage | `.gpkg` | SQLite-based format |

## Geometry Detection Methods

The library automatically detects various geometry formats:

1. **Lat/Lon columns**: Columns named latitude/longitude, lat/lon, etc.
2. **X/Y coordinates**: Columns named X/Y (Lambert, Web Mercator)
3. **LineString coordinates**: Start/end coordinate pairs (xD, yD, xF, yF)
4. **WKT format**: Well-Known Text geometry strings
5. **GeoJSON objects**: GeoJSON geometry objects
6. **Geo_point format**: Comma-separated coordinates
7. **Address columns**: Columns containing addresses (requires geocoding)
8. **INSEE codes**: French administrative codes for spatial joins

## Performance

The library uses DuckDB for high-performance processing:

- **CSV files**: 10-100x faster than pandas for large files
- **SQL-based aggregations**: Efficient column statistics and metrics
- **Spatial operations**: DuckDB spatial extension for geometry processing
- **Smart sampling**: Intelligent sampling for very large Excel files

## Replacing batch_inspect_duckdb.py

The library can directly replace your batch script:

```python
from geodata_metadata import MetadataExtractor

# Original batch script equivalent
DATA_FOLDER = "data-20260203T152655Z-3-001"

extractor = MetadataExtractor(reference_file="data/regions.geojson")
results = extractor.extract_from_directory(DATA_FOLDER, recursive=True)

# Export results
extractor.to_csv(results, "inspection_summary_duckdb.csv")
extractor.to_excel(results, "inspection_summary_duckdb.xlsx")

# Print summary
stats = extractor.get_summary_stats(results)
print(f"Processed: {stats['successful']} files")
print(f"Errors: {stats['failed']}")
print(f"Total time: {stats['total_time']:.1f}s")
```

## Advanced Usage

### Custom Supported Extensions

```python
# Add support for custom extensions
extractor = MetadataExtractor(
    reference_file="data/regions.geojson",
    supported_extensions={'.csv', '.xlsx', '.geojson', '.parquet'}
)
```

### Pattern Matching

```python
# Process only CSV files
results = extractor.extract_from_directory(
    directory="data",
    pattern="*.csv"
)
```

### Include GeoDataFrame

```python
# Include GeoDataFrame for visualization
result = extractor.extract("data/file.geojson", include_geodataframe=True)

if result.success and "_geodataframe" in result.metadata:
    gdf = result.metadata["_geodataframe"]
    gdf.plot()
```

## Troubleshooting

### Missing Dependencies

If you get import errors, install required packages:

```bash
pip install pandas geopandas duckdb openpyxl calamine
```

### Large Files

For very large files (>1GB), the library automatically uses:
- DuckDB streaming for CSV files
- Smart sampling for Excel files
- Spatial indexing for geometry operations

### CRS Detection Issues

If CRS detection fails, the library defaults to Lambert 93 (EPSG:2154) for French data. You can manually set CRS in the underlying GeoDataFrame if needed.

## License

This library is based on the DuckDB-optimized geodata inspector.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- New features include examples
- Error handling is comprehensive

## See Also

- `example_usage.py`: Comprehensive examples
- `inspect_geodata_duckdb.py`: Underlying inspection module
- `batch_inspect_duckdb.py`: Original batch processing script
