"""
Simple test script for the geodata_metadata library
====================================================

Run this to verify the library works correctly.
"""

from geodata_metadata import MetadataExtractor, extract_metadata
import os

def test_library():
    """Test the library with available files."""
    print("\n" + "="*70)
    print("TESTING GEODATA METADATA LIBRARY")
    print("="*70 + "\n")

    # Check if reference file exists
    reference_file = "data/regions.geojson"
    if not os.path.exists(reference_file):
        print(f"⚠ Warning: Reference file not found: {reference_file}")
        reference_file = None
    else:
        print(f"✓ Found reference file: {reference_file}\n")

    # Initialize extractor
    extractor = MetadataExtractor(reference_file=reference_file)

    # Find test files
    test_files = []
    for pattern in ["*.csv", "*.xlsx", "*.geojson", "*.shp", "*.gpkg"]:
        import glob
        test_files.extend(glob.glob(f"data/{pattern}"))
        test_files.extend(glob.glob(f"**/{pattern}", recursive=True))

    if not test_files:
        print("⚠ No test files found. Please add some data files to test.\n")
        print("Supported formats: CSV, Excel (.xlsx), GeoJSON, Shapefile, GeoPackage")
        return

    # Test single file extraction
    print(f"Found {len(test_files)} test files\n")
    test_file = test_files[0]

    print(f"Testing single file extraction: {test_file}")
    print("-" * 70)

    result = extractor.extract(test_file)

    if result.success:
        print(f"✓ SUCCESS! Extracted metadata in {result.elapsed_time:.2f}s\n")

        m = result.metadata
        print("Key information:")
        print(f"  Filename: {m.get('Nom du fichier')}")
        print(f"  Type: {m.get('Type de fichier')}")
        print(f"  Size: {m.get('Taille (Ko)')} KB")
        print(f"  Rows: {m.get('Nb lignes'):,}" if m.get('Nb lignes') else "  Rows: N/A")
        print(f"  Columns: {m.get('Nb colonnes')}")
        print(f"  Geometry: {m.get('Géotransformation')}")
        print(f"  CRS: {m.get('CRS')}")

        # Test flattened output
        flat = result.to_dict(flatten=True)
        print(f"\n  Flattened dict has {len(flat)} keys")

    else:
        print(f"✗ FAILED: {result.error}")

    # Test batch processing if multiple files
    if len(test_files) > 1:
        print(f"\n{'='*70}")
        print(f"Testing batch processing with {min(3, len(test_files))} files")
        print("-" * 70)

        batch_files = test_files[:3]
        results = extractor.extract_batch(batch_files, verbose=True)

        # Test DataFrame conversion
        print("\nTesting DataFrame conversion...")
        df = extractor.to_dataframe(results, flatten=True)
        print(f"✓ Created DataFrame with {len(df)} rows and {len(df.columns)} columns")

        # Test export
        print("\nTesting exports...")
        try:
            extractor.to_json(results, "test_output.json")
            extractor.to_csv(results, "test_output.csv")
            print("✓ JSON and CSV exports successful")

            # Clean up
            if os.path.exists("test_output.json"):
                os.remove("test_output.json")
            if os.path.exists("test_output.csv"):
                os.remove("test_output.csv")
            print("✓ Test files cleaned up")
        except Exception as e:
            print(f"⚠ Export error: {e}")

        # Test summary stats
        print("\nTesting summary statistics...")
        stats = extractor.get_summary_stats(results)
        print(f"✓ Summary stats:")
        print(f"  Total files: {stats['total_files']}")
        print(f"  Successful: {stats['successful']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Success rate: {stats['success_rate']:.1f}%")
        print(f"  Total time: {stats['total_time']:.2f}s")

    print("\n" + "="*70)
    print("LIBRARY TEST COMPLETE!")
    print("="*70 + "\n")


def test_convenience_functions():
    """Test convenience functions."""
    print("\n" + "="*70)
    print("TESTING CONVENIENCE FUNCTIONS")
    print("="*70 + "\n")

    # Find a test file
    import glob
    test_files = glob.glob("data/*.csv") or glob.glob("**/*.csv", recursive=True)

    if not test_files:
        print("⚠ No CSV files found for testing")
        return

    test_file = test_files[0]

    # Test quick extraction
    print(f"Testing extract_metadata() with: {test_file}")
    metadata = extract_metadata(test_file)

    if metadata:
        print(f"✓ SUCCESS! Got {len(metadata)} metadata fields")
        print(f"  File: {metadata.get('Nom du fichier')}")
        print(f"  Rows: {metadata.get('Nb lignes')}")
    else:
        print("✗ FAILED to extract metadata")

    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    # Run tests
    test_library()
    test_convenience_functions()

    print("\nFor more examples, see example_usage.py")
    print("For documentation, see README_LIBRARY.md")
