"""
Geodata Metadata Extraction Library
====================================

A Python library for extracting comprehensive metadata from various file formats:
- CSV, Excel (.xlsx)
- GeoJSON, Shapefile, GeoPackage
- Automatic geometry detection and spatial metrics

Usage:
    from geodata_metadata import MetadataExtractor

    # Initialize extractor
    extractor = MetadataExtractor(reference_file="data/regions.geojson")

    # Extract metadata from a single file
    metadata = extractor.extract("data/file.csv")

    # Batch process multiple files
    results = extractor.extract_batch(["file1.csv", "file2.geojson"])

    # Export to various formats
    extractor.to_dataframe(results)
    extractor.to_json(results, "output.json")
    extractor.to_csv(results, "output.csv")
"""

import os
import json
import time
from typing import List, Dict, Optional, Any
from pathlib import Path
import pandas as pd
import geopandas as gpd

# Import the DuckDB inspector
from . import core as inspector
from dataclasses import dataclass
from typing import Dict, Any, Optional


class MetadataResult:
    """Container for metadata extraction results."""

    def __init__(self, filepath: str, metadata: Optional[Dict] = None,
                 error: Optional[str] = None, elapsed_time: float = 0):
        self.filepath = filepath
        self.metadata = metadata
        self.error = error
        self.elapsed_time = elapsed_time
        self.success = metadata is not None

    def __repr__(self):
        status = "✓" if self.success else "✗"
        return f"<MetadataResult {status} {os.path.basename(self.filepath)} ({self.elapsed_time:.2f}s)>"

    def to_dict(self, flatten: bool = False) -> Dict[str, Any]:
        """Convert result to dictionary."""
        if not self.success:
            return {
                "filepath": self.filepath,
                "success": False,
                "error": self.error,
                "elapsed_time": self.elapsed_time
            }

        result = {
            "filepath": self.filepath,
            "success": True,
            "elapsed_time": self.elapsed_time,
        }

        if flatten:
            result.update(self._flatten_metadata(self.metadata))
        else:
            result["metadata"] = self.metadata

        return result

    @staticmethod
    def _flatten_metadata(metadata: Dict, parent_key: str = '', sep: str = ' - ') -> Dict:
        """Flatten nested dictionaries in metadata."""
        items = []
        for key, value in metadata.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            # # Special handling for Colonnes table
            # if key == "Colonnes" and isinstance(value, dict) and "_table" in value:
            #     items.append((f"Nb colonnes détaillées", len(value.get("data", []))))

            # Special handling for Colonnes table
            if key == "Colonnes" and isinstance(value, dict) and "_table" in value:
                # Sérialiser la liste des colonnes en JSON
                import json
                items.append(("Colonnes (détails)", json.dumps(value.get("data", []), ensure_ascii=False)))
                items.append(("Nb colonnes détaillées", len(value.get("data", []))))
            elif isinstance(value, dict) and "_table" not in value:
                # Recursively flatten nested dicts
                items.extend(MetadataResult._flatten_metadata(value, new_key, sep=sep).items())
            else:
                items.append((new_key, value))

        return dict(items)


class MetadataExtractor:
    """
    Main class for extracting metadata from geospatial and tabular files.

    Supports:
        - CSV files with automatic geometry detection
        - Excel files (.xlsx)
        - GeoJSON, Shapefile, GeoPackage
        - Automatic CRS detection and transformation
        - Spatial metrics calculation

    Args:
        reference_file: Optional path to reference GeoJSON for coverage calculations
        supported_extensions: Set of supported file extensions (default: auto-detect)
    """

    DEFAULT_SUPPORTED_EXTENSIONS = {'.csv', '.xlsx', '.geojson', '.json', '.shp', '.gpkg', '.txt'}

    def __init__(self, reference_file: Optional[str] = None,
                 supported_extensions: Optional[set] = None):
        """
        Initialize the metadata extractor.

        Args:
            reference_file: Path to reference GeoJSON file (e.g., regions.geojson)
            supported_extensions: Set of file extensions to support
        """
        self.gdf_reference = None
        self.supported_extensions = supported_extensions or self.DEFAULT_SUPPORTED_EXTENSIONS

        # Load reference data if provided
        if reference_file and os.path.exists(reference_file):
            try:
                self.gdf_reference = gpd.read_file(reference_file).to_crs(epsg=2154)
                print(f"✓ Loaded reference data: {reference_file}")
            except Exception as e:
                print(f"⚠ Warning: Could not load reference file: {e}")
                self.gdf_reference = None
        elif reference_file:
            print(f"⚠ Warning: Reference file not found: {reference_file}")

    def extract(self, filepath: str, include_geodataframe: bool = False) -> MetadataResult:
        """
        Extract metadata from a single file.

        Args:
            filepath: Path to the file to inspect
            include_geodataframe: If True, include the GeoDataFrame in the result

        Returns:
            MetadataResult: Result object with metadata or error information
        """
        if not os.path.exists(filepath):
            return MetadataResult(
                filepath=filepath,
                error=f"File not found: {filepath}",
                elapsed_time=0
            )

        # Check if file extension is supported
        ext = os.path.splitext(filepath)[-1].lower()
        if ext not in self.supported_extensions:
            return MetadataResult(
                filepath=filepath,
                error=f"Unsupported file extension: {ext}",
                elapsed_time=0
            )

        # Clear previous results
        inspector.summary_rows.clear()
        inspector.last_gdf = None

        start_time = time.time()

        try:
            # Inspect the file
            inspector.inspect_file(filepath, self.gdf_reference)
            elapsed_time = time.time() - start_time

            # Get metadata
            if inspector.summary_rows:
                metadata = inspector.summary_rows[-1].copy()

                # Add processing time
                metadata["Temps de traitement (s)"] = round(elapsed_time, 2)

                # Optionally include GeoDataFrame
                if include_geodataframe and inspector.last_gdf is not None:
                    metadata["_geodataframe"] = inspector.last_gdf

                return MetadataResult(
                    filepath=filepath,
                    metadata=metadata,
                    elapsed_time=round(elapsed_time, 2)               
                )
            else:
                return MetadataResult(
                    filepath=filepath,
                    error="No metadata extracted",
                    elapsed_time=elapsed_time
                )

        except Exception as e:
            elapsed_time = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"

            return MetadataResult(
                filepath=filepath,
                error=error_msg,
                elapsed_time=round(elapsed_time, 2)
            )

    def extract_batch(self, filepaths: List[str],
                     verbose: bool = True,
                     stop_on_error: bool = False) -> List[MetadataResult]:
        """
        Extract metadata from multiple files.

        Args:
            filepaths: List of file paths to process
            verbose: If True, print progress information
            stop_on_error: If True, stop processing on first error

        Returns:
            List[MetadataResult]: List of results for each file
        """
        results = []
        total_files = len(filepaths)

        if verbose:
            print(f"\n{'='*70}")
            print(f"BATCH METADATA EXTRACTION")
            print(f"{'='*70}")
            print(f"Processing {total_files} files...\n")

        for i, filepath in enumerate(filepaths, 1):
            if verbose:
                rel_path = os.path.basename(filepath)
                print(f"[{i}/{total_files}] {rel_path}...", end=" ")

            result = self.extract(filepath)
            results.append(result)

            if verbose:
                if result.success:
                    print(f"✓ ({result.elapsed_time:.2f}s)")
                else:
                    print(f"✗ {result.error}")

            if not result.success and stop_on_error:
                if verbose:
                    print(f"\n⚠ Stopping due to error")
                break

        if verbose:
            success_count = sum(1 for r in results if r.success)
            error_count = total_files - success_count
            total_time = sum(r.elapsed_time for r in results)

            print(f"\n{'='*70}")
            print(f"SUMMARY")
            print(f"{'='*70}")
            print(f"Total files: {total_files}")
            print(f"Successful: {success_count}")
            print(f"Errors: {error_count}")
            print(f"Total time: {total_time:.2f}s")

        return results

    def extract_from_directory(self, directory: str,
                              recursive: bool = True,
                              pattern: str = "*",
                              **kwargs) -> List[MetadataResult]:
        """
        Extract metadata from all supported files in a directory.

        Args:
            directory: Path to directory
            recursive: If True, search subdirectories
            pattern: Glob pattern to match files (e.g., "*.csv")
            **kwargs: Additional arguments passed to extract_batch

        Returns:
            List[MetadataResult]: List of results for each file
        """
        path = Path(directory)

        if not path.exists() or not path.is_dir():
            raise ValueError(f"Directory not found: {directory}")

        # Find all matching files
        files = []
        if recursive:
            for ext in self.supported_extensions:
                files.extend(path.rglob(f"{pattern}{ext}"))
        else:
            for ext in self.supported_extensions:
                files.extend(path.glob(f"{pattern}{ext}"))

        # Convert to strings and sort
        filepaths = sorted([str(f) for f in files])

        return self.extract_batch(filepaths, **kwargs)

    @staticmethod
    def to_dataframe(results: List[MetadataResult],
                    flatten: bool = True,
                    include_errors: bool = True) -> pd.DataFrame:
        """
        Convert results to a pandas DataFrame.

        Args:
            results: List of MetadataResult objects
            flatten: If True, flatten nested dictionaries
            include_errors: If True, include failed extractions in the DataFrame

        Returns:
            pd.DataFrame: DataFrame with metadata
        """
        data = []

        for result in results:
            if result.success or include_errors:
                data.append(result.to_dict(flatten=flatten))

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Reorder columns for better readability (if flattened)
        if flatten and result.success:
            priority_cols = [
                "filepath", "success", "Dossier", "Nom du fichier",
                "Type de fichier", "Taille (Ko)", "Nb lignes", "Nb colonnes",
                "Temps de traitement (s)", "Géotransformation", "CRS",
                "Types de géométrie", "Emprise estimée (km2)",
                "Couverture territoriale hexagonale (%)"
            ]

            # Put priority columns first, then the rest
            ordered_cols = [c for c in priority_cols if c in df.columns]
            other_cols = [c for c in df.columns if c not in ordered_cols]
            df = df[ordered_cols + other_cols]

        return df

    @staticmethod
    def to_json(results: List[MetadataResult],
                output_file: Optional[str] = None,
                flatten: bool = False,
                indent: int = 2) -> Optional[str]:
        """
        Convert results to JSON format.

        Args:
            results: List of MetadataResult objects
            output_file: If provided, write to this file
            flatten: If True, flatten nested dictionaries
            indent: JSON indentation level

        Returns:
            str: JSON string (if output_file is None)
        """
        data = [result.to_dict(flatten=flatten) for result in results]
        json_str = json.dumps(data, indent=indent, default=str)

        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(json_str)
            print(f"✓ JSON exported to: {output_file}")
            return None
        else:
            return json_str

    @staticmethod
    def to_csv(results: List[MetadataResult],
               output_file: str,
               flatten: bool = True,
               **kwargs):
        """
        Convert results to CSV format.

        Args:
            results: List of MetadataResult objects
            output_file: Output CSV file path
            flatten: If True, flatten nested dictionaries
            **kwargs: Additional arguments passed to DataFrame.to_csv
        """
        df = MetadataExtractor.to_dataframe(results, flatten=flatten)

        # Default CSV parameters
        csv_params = {
            'index': False,
            'encoding': 'utf-8-sig'
        }
        csv_params.update(kwargs)

        df.to_csv(output_file, **csv_params)
        print(f"✓ CSV exported to: {output_file}")

    @staticmethod
    def to_excel(results: List[MetadataResult],
                 output_file: str,
                 flatten: bool = True,
                 **kwargs):
        """
        Convert results to Excel format.

        Args:
            results: List of MetadataResult objects
            output_file: Output Excel file path
            flatten: If True, flatten nested dictionaries
            **kwargs: Additional arguments passed to DataFrame.to_excel
        """
        df = MetadataExtractor.to_dataframe(results, flatten=flatten)

        # Default Excel parameters
        excel_params = {
            'index': False,
            'engine': 'openpyxl'
        }
        excel_params.update(kwargs)

        df.to_excel(output_file, **excel_params)
        print(f"✓ Excel exported to: {output_file}")

    def get_summary_stats(self, results: List[MetadataResult]) -> Dict[str, Any]:
        """
        Get summary statistics from a batch of results.

        Args:
            results: List of MetadataResult objects

        Returns:
            Dict with summary statistics
        """
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        stats = {
            "total_files": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": len(successful) / len(results) * 100 if results else 0,
            "total_time": sum(r.elapsed_time for r in results),
            "avg_time_per_file": sum(r.elapsed_time for r in results) / len(results) if results else 0,
        }

        # Aggregate file type statistics
        if successful:
            file_types = {}
            total_rows = 0
            total_size_kb = 0

            for result in successful:
                metadata = result.metadata
                file_type = metadata.get("Type de fichier", "Unknown")
                file_types[file_type] = file_types.get(file_type, 0) + 1

                if "Nb lignes" in metadata:
                    total_rows += metadata["Nb lignes"]

                if "Taille (Ko)" in metadata:
                    total_size_kb += metadata["Taille (Ko)"]

            stats["file_types"] = file_types
            stats["total_rows"] = total_rows
            stats["total_size_mb"] = round(total_size_kb / 1024, 2)

        # Error summary
        if failed:
            error_types = {}
            for result in failed:
                error = result.error.split(":")[0] if result.error else "Unknown"
                error_types[error] = error_types.get(error, 0) + 1
            stats["error_types"] = error_types

        return stats
        
@dataclass
class ExtractionResult:
    """Result of metadata extraction"""
    filename: str
    success: bool
    metadata: Dict[str, Any]
    processing_time: float
    error_message: Optional[str] = None

# Convenience functions for quick usage
def extract_metadata(filepath: str, reference_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick function to extract metadata from a single file.

    Args:
        filepath: Path to file
        reference_file: Optional reference GeoJSON file

    Returns:
        Dict with metadata or None if failed
    """
    extractor = MetadataExtractor(reference_file=reference_file)
    result = extractor.extract(filepath)
    return result.metadata if result.success else None


def extract_metadata_batch(filepaths: List[str],
                          reference_file: Optional[str] = None,
                          output_csv: Optional[str] = None,
                          output_json: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Quick function to extract metadata from multiple files and optionally export.

    Args:
        filepaths: List of file paths
        reference_file: Optional reference GeoJSON file
        output_csv: Optional output CSV file path
        output_json: Optional output JSON file path

    Returns:
        List of metadata dictionaries
    """
    extractor = MetadataExtractor(reference_file=reference_file)
    results = extractor.extract_batch(filepaths)

    # Export if requested
    if output_csv:
        extractor.to_csv(results, output_csv)

    if output_json:
        extractor.to_json(results, output_json)

    return [r.metadata for r in results if r.success]


# Example usage
if __name__ == "__main__":
    import sys
    import os
    
    print("""
Geodata Metadata Extraction Library
====================================""")
    
    if len(sys.argv) > 1:
        # Quick test with provided file
        filepath = sys.argv[1]
        reference = "data/regions.geojson" if os.path.exists("data/regions.geojson") else None
        print(f"\nTesting with: {filepath}\n")
        
        extractor = MetadataExtractor(reference_file=reference)
        result = extractor.extract(filepath)
        
        if result.success:
            print("Metadata extracted successfully:")
            for key, value in result.metadata.items():
                if key != "Colonnes":
                    print(f"  {key}: {value}")
        else:
            print(f"Failed to extract metadata: {result.error_message}")