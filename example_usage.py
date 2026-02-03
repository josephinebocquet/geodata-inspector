"""
Example usage of the Geodata Metadata Extraction Library
=========================================================

This file demonstrates various ways to use the geodata_metadata library.
"""

from geodata_metadata import MetadataExtractor, extract_metadata, extract_metadata_batch

# ==============================================================================
# Example 1: Extract metadata from a single file
# ==============================================================================
def example_single_file():
    """Extract metadata from a single file."""
    print("\n" + "="*70)
    print("EXAMPLE 1: Single File Extraction")
    print("="*70 + "\n")

    # Initialize extractor
    extractor = MetadataExtractor(reference_file="data/regions.geojson")

    # Extract metadata
    result = extractor.extract("data/your_file.csv")

    if result.success:
        print(f"✓ Successfully extracted metadata in {result.elapsed_time:.2f}s\n")

        # Access metadata
        metadata = result.metadata

        # Print key information
        print(f"File: {metadata.get('Nom du fichier')}")
        print(f"Type: {metadata.get('Type de fichier')}")
        print(f"Rows: {metadata.get('Nb lignes')}")
        print(f"Columns: {metadata.get('Nb colonnes')}")
        print(f"Size: {metadata.get('Taille (Ko)')} KB")
        print(f"Geometry: {metadata.get('Géotransformation')}")
        print(f"CRS: {metadata.get('CRS')}")

        # Access nested metadata
        completeness = metadata.get('Score de complétude global', {})
        print(f"\nCompleteness:")
        print(f"  Mean: {completeness.get('Score de complétude moyen')}")
        print(f"  Std: {completeness.get('Score de complétude std')}")

    else:
        print(f"✗ Error: {result.error}")


# ==============================================================================
# Example 2: Batch processing with export
# ==============================================================================
def example_batch_processing():
    """Process multiple files and export results."""
    print("\n" + "="*70)
    print("EXAMPLE 2: Batch Processing")
    print("="*70 + "\n")

    # Initialize extractor
    extractor = MetadataExtractor(reference_file="data/regions.geojson")

    # List of files to process
    files = [
        "data/file1.csv",
        "data/file2.geojson",
        "data/file3.xlsx",
    ]

    # Extract metadata from all files
    results = extractor.extract_batch(files, verbose=True)

    # Export to CSV
    extractor.to_csv(results, "metadata_output.csv")

    # Export to JSON
    extractor.to_json(results, "metadata_output.json")

    # Export to Excel
    extractor.to_excel(results, "metadata_output.xlsx")

    # Get summary statistics
    stats = extractor.get_summary_stats(results)
    print(f"\nSummary Statistics:")
    print(f"  Total files: {stats['total_files']}")
    print(f"  Successful: {stats['successful']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Success rate: {stats['success_rate']:.1f}%")
    print(f"  Total time: {stats['total_time']:.2f}s")
    print(f"  Avg time per file: {stats['avg_time_per_file']:.2f}s")


# ==============================================================================
# Example 3: Process entire directory
# ==============================================================================
def example_directory_processing():
    """Process all supported files in a directory."""
    print("\n" + "="*70)
    print("EXAMPLE 3: Directory Processing")
    print("="*70 + "\n")

    # Initialize extractor
    extractor = MetadataExtractor(reference_file="data/regions.geojson")

    # Process all files in directory (recursively)
    results = extractor.extract_from_directory(
        directory="data",
        recursive=True,
        verbose=True
    )

    # Convert to DataFrame for analysis
    df = extractor.to_dataframe(results, flatten=True)

    print("\nDataFrame preview:")
    print(df[['Nom du fichier', 'Type de fichier', 'Nb lignes', 'Nb colonnes']].head())

    # Export
    extractor.to_csv(results, "directory_metadata.csv")


# ==============================================================================
# Example 4: Quick convenience functions
# ==============================================================================
def example_convenience_functions():
    """Use quick convenience functions."""
    print("\n" + "="*70)
    print("EXAMPLE 4: Convenience Functions")
    print("="*70 + "\n")

    # Quick single file extraction
    metadata = extract_metadata("data/file.csv", reference_file="data/regions.geojson")

    if metadata:
        print("Metadata keys:", list(metadata.keys()))

    # Quick batch extraction with export
    files = ["data/file1.csv", "data/file2.geojson"]
    results = extract_metadata_batch(
        files,
        reference_file="data/regions.geojson",
        output_csv="quick_export.csv",
        output_json="quick_export.json"
    )

    print(f"\nProcessed {len(results)} files")


# ==============================================================================
# Example 5: Access detailed information
# ==============================================================================
def example_detailed_information():
    """Access detailed metadata information."""
    print("\n" + "="*70)
    print("EXAMPLE 5: Detailed Information Access")
    print("="*70 + "\n")

    extractor = MetadataExtractor(reference_file="data/regions.geojson")
    result = extractor.extract("data/file.csv")

    if result.success:
        metadata = result.metadata

        # File information
        print("File Information:")
        print(f"  Folder: {metadata.get('Dossier')}")
        print(f"  Filename: {metadata.get('Nom du fichier')}")
        print(f"  Size: {metadata.get('Taille (Ko)')} KB")
        print(f"  Created: {metadata.get('Date de création du fichier (Y-M-D)')}")

        # Data structure
        print("\nData Structure:")
        print(f"  Rows: {metadata.get('Nb lignes')}")
        print(f"  Columns: {metadata.get('Nb colonnes')}")
        print(f"  File type: {metadata.get('Type de fichier')}")

        # Geographic information
        print("\nGeographic Information:")
        print(f"  Transformation: {metadata.get('Géotransformation')}")
        print(f"  CRS: {metadata.get('CRS')}")
        print(f"  Geometry types: {metadata.get('Types de géométrie')}")
        print(f"  Geographic keys: {metadata.get('Clés géographiques')}")

        # Spatial metrics
        print("\nSpatial Metrics:")
        print(f"  Area (km²): {metadata.get('Emprise estimée (km2)')}")
        print(f"  Density (obj/km²): {metadata.get('Densité (obj/km2)')}")
        print(f"  Fill rate (%): {metadata.get('Taux de remplissage (%)')}")
        print(f"  Coverage (%): {metadata.get('Couverture territoriale hexagonale (%)')}")
        print(f"  Duplicates (%): {metadata.get('Geometries dupliquees (%)')}")
        print(f"  Complexity: {metadata.get('Complexite moyenne')}")

        # Column details (if available)
        columns_data = metadata.get('Colonnes')
        if columns_data and isinstance(columns_data, dict) and '_table' in columns_data:
            print(f"\nColumn Details: {len(columns_data.get('data', []))} columns")
            for col_info in columns_data.get('data', [])[:5]:  # Show first 5
                print(f"  - {col_info['Colonne']} ({col_info['Type']}): {col_info['Exemple'][:50]}")


# ==============================================================================
# Example 6: Custom filtering and analysis
# ==============================================================================
def example_custom_analysis():
    """Perform custom analysis on extracted metadata."""
    print("\n" + "="*70)
    print("EXAMPLE 6: Custom Analysis")
    print("="*70 + "\n")

    extractor = MetadataExtractor(reference_file="data/regions.geojson")

    # Process directory
    results = extractor.extract_from_directory("data", verbose=False)

    # Convert to DataFrame
    df = extractor.to_dataframe(results, flatten=True)

    # Filter only files with geometry
    geo_files = df[df['Géotransformation'] != 'Aucune géométrie']
    print(f"Files with geometry: {len(geo_files)}")

    # Find largest files
    if 'Taille (Ko)' in df.columns:
        largest = df.nlargest(5, 'Taille (Ko)')
        print("\nLargest files:")
        print(largest[['Nom du fichier', 'Taille (Ko)', 'Nb lignes']])

    # Calculate statistics
    if 'Nb lignes' in df.columns:
        print(f"\nTotal rows across all files: {df['Nb lignes'].sum():,}")
        print(f"Average rows per file: {df['Nb lignes'].mean():.0f}")

    # Group by file type
    if 'Type de fichier' in df.columns:
        print("\nFiles by type:")
        print(df['Type de fichier'].value_counts())


# ==============================================================================
# Example 7: Error handling and validation
# ==============================================================================
def example_error_handling():
    """Demonstrate error handling."""
    print("\n" + "="*70)
    print("EXAMPLE 7: Error Handling")
    print("="*70 + "\n")

    extractor = MetadataExtractor()

    # Try to process non-existent file
    result = extractor.extract("nonexistent_file.csv")
    print(f"Non-existent file: {result.success} - {result.error}")

    # Try to process unsupported file
    result = extractor.extract("file.txt")
    print(f"Unsupported file: {result.success} - {result.error}")

    # Batch processing with error handling
    files = [
        "data/valid_file.csv",
        "data/nonexistent.csv",
        "data/another_valid.geojson",
    ]

    results = extractor.extract_batch(files, verbose=False, stop_on_error=False)

    # Separate successful and failed results
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    print(f"\nSuccessful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    for result in failed:
        print(f"  - {result.filepath}: {result.error}")


# ==============================================================================
# Example 8: Integration with existing batch script
# ==============================================================================
def example_replace_batch_script():
    """Show how to replace the batch_inspect_duckdb.py script."""
    print("\n" + "="*70)
    print("EXAMPLE 8: Replace Batch Script")
    print("="*70 + "\n")

    # This is equivalent to batch_inspect_duckdb.py
    DATA_FOLDER = "data-20260203T152655Z-3-001"

    extractor = MetadataExtractor(reference_file="data/regions.geojson")

    # Process all files in folder
    results = extractor.extract_from_directory(
        directory=DATA_FOLDER,
        recursive=True,
        verbose=True
    )

    # Export results (equivalent to the batch script output)
    extractor.to_csv(results, "inspection_summary_duckdb.csv")
    extractor.to_excel(results, "inspection_summary_duckdb.xlsx")

    # Print summary
    stats = extractor.get_summary_stats(results)
    print("\nProcessing complete!")
    print(f"  Successful: {stats['successful']}")
    print(f"  Errors: {stats['failed']}")
    print(f"  Total time: {stats['total_time']:.1f}s")


# ==============================================================================
# Main
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "="*70)
    print("GEODATA METADATA EXTRACTION LIBRARY - EXAMPLES")
    print("="*70)

    print("\nAvailable examples:")
    print("  1. Single file extraction")
    print("  2. Batch processing with export")
    print("  3. Directory processing")
    print("  4. Convenience functions")
    print("  5. Detailed information access")
    print("  6. Custom analysis")
    print("  7. Error handling")
    print("  8. Replace batch script")

    print("\nTo run examples, uncomment the function calls below:")
    print()

    # Uncomment to run examples:
    # example_single_file()
    # example_batch_processing()
    # example_directory_processing()
    # example_convenience_functions()
    # example_detailed_information()
    # example_custom_analysis()
    # example_error_handling()
    # example_replace_batch_script()

    print("\nFor more information, see the docstrings in geodata_metadata.py")
