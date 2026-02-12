#!/usr/bin/env python3
"""
Test geodata-inspector on all files in a directory

Usage:
    python test_all_files.py /data/2024_jbt_work/collaboration_Noa/data/
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import traceback

# Import the library
try:
    from geodata_inspector import MetadataExtractor
    print("✓ geodata_inspector imported successfully")
except ImportError as e:
    print(f"❌ Error importing geodata_inspector: {e}")
    print("Make sure you've run: pip install -e .")
    sys.exit(1)


def find_all_data_files(directory, extensions=None):
    """
    Find all data files in a directory
    
    Args:
        directory: Path to search
        extensions: List of extensions to include (default: common geo formats)
    
    Returns:
        List of file paths
    """
    if extensions is None:
        extensions = ['.csv', '.xlsx', '.xls', '.geojson', '.shp', '.gpkg', '.zip', '.json']
    
    directory = Path(directory)
    files = []
    
    print(f"\n🔍 Searching for files in: {directory}")
    print(f"   Looking for extensions: {extensions}")
    print("-" * 70)
    
    for ext in extensions:
        found = list(directory.glob(f"**/*{ext}"))
        if found:
            print(f"   Found {len(found)} {ext} file(s)")
            files.extend(found)
    
    return sorted(files)


def test_all_files(data_dir, reference_file=None, output_dir=None):
    """
    Test geodata-inspector on all files in a directory
    
    Args:
        data_dir: Directory containing files to test
        reference_file: Optional reference file for coverage analysis
        output_dir: Directory to save results (default: current directory)
    """
    
    print("=" * 70)
    print("GEODATA INSPECTOR - BATCH TEST")
    print("=" * 70)
    print(f"Data directory: {data_dir}")
    print(f"Reference file: {reference_file or 'None'}")
    print(f"Output directory: {output_dir or 'current directory'}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Find all files
    files = find_all_data_files(data_dir)
    
    if not files:
        print("\n❌ No data files found!")
        return
    
    print(f"\n📁 Found {len(files)} files to process")
    print("=" * 70)
    
    # Initialize extractor
    print("\n🔧 Initializing MetadataExtractor...")
    try:
        extractor = MetadataExtractor(reference_file=reference_file)
        print("✓ MetadataExtractor initialized")
    except Exception as e:
        print(f"❌ Error initializing: {e}")
        traceback.print_exc()
        return
    
    print("\n" + "=" * 70)
    print("PROCESSING FILES")
    print("=" * 70)
    
    # Process files
    results = []
    success_count = 0
    failed_count = 0
    
    for i, filepath in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] Processing: {filepath.name}")
        print("-" * 70)
        
        try:
            result = extractor.extract(str(filepath))
            results.append(result)
            
            if result.success if hasattr(result, 'success') else result.metadata:
                success_count += 1
                # Get processing time if available
                proc_time = getattr(result, 'processing_time', 0.0)
                print(f"✓ Success ({proc_time:.2f}s)")
                
                # Print key metadata
                metadata = result.metadata if hasattr(result, 'metadata') else result
                if metadata:
                    rows = metadata.get('Nb lignes', 'N/A')
                    cols = metadata.get('Nb colonnes', 'N/A')
                    geom = metadata.get('Types de géométrie', 'None')
                    crs = metadata.get('CRS', 'N/A')
                    
                    print(f"  Rows: {rows}, Columns: {cols}")
                    print(f"  Geometry: {geom}")
                    print(f"  CRS: {crs}")
            else:
                failed_count += 1
                # Get error message if available
                error_msg = getattr(result, 'error_message', 'Unknown error')
                print(f"✗ Failed: {error_msg}")
        
        except KeyboardInterrupt:
            print("\n\n⚠ Processing interrupted by user")
            break
        
        except Exception as e:
            failed_count += 1
            print(f"✗ Error: {e}")
            traceback.print_exc()
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    stats = extractor.get_summary_stats(results)
    
    print(f"Total files processed: {stats['total_files']}")
    print(f"Successful: {success_count} ({stats['success_rate']:.1f}%)")
    print(f"Failed: {failed_count}")
    print(f"Total time: {stats['total_time']:.2f}s")
    avg_time = stats['total_time'] / stats['total_files'] if stats['total_files'] > 0 else 0
    print(f"Average time per file: {avg_time:.2f}s")
    
    # Export results
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = Path.cwd()
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Export to multiple formats
    print("\n" + "=" * 70)
    print("EXPORTING RESULTS")
    print("=" * 70)
    
    try:
        # CSV
        csv_file = output_path / f"geodata_inspection_{timestamp}.csv"
        extractor.to_csv(results, str(csv_file))
        print(f"✓ CSV exported: {csv_file}")
        
        # Excel
        xlsx_file = output_path / f"geodata_inspection_{timestamp}.xlsx"
        extractor.to_excel(results, str(xlsx_file))
        print(f"✓ Excel exported: {xlsx_file}")
        
        # JSON
        json_file = output_path / f"geodata_inspection_{timestamp}.json"
        extractor.to_json(results, str(json_file))
        print(f"✓ JSON exported: {json_file}")
        
    except Exception as e:
        print(f"⚠ Error exporting results: {e}")
        traceback.print_exc()
    
    print("\n" + "=" * 70)
    print("PROCESSING COMPLETE!")
    print("=" * 70)
    
    return results


def main():
    """Main entry point"""
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: python test_all_files.py <data_directory> [reference_file] [output_directory]")
        print("\nExample:")
        print("  python test_all_files.py /data/2024_jbt_work/collaboration_Noa/data/")
        print("  python test_all_files.py /data/files/ data/regions.geojson ./results/")
        sys.exit(1)
    
    data_dir = sys.argv[1]
    reference_file = sys.argv[2] if len(sys.argv) > 2 else None
    output_dir = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Validate data directory
    if not os.path.exists(data_dir):
        print(f"❌ Error: Directory not found: {data_dir}")
        sys.exit(1)
    
    if not os.path.isdir(data_dir):
        print(f"❌ Error: Not a directory: {data_dir}")
        sys.exit(1)
    
    # Run tests
    try:
        results = test_all_files(data_dir, reference_file, output_dir)
        
        if results:
            print(f"\n✅ Successfully processed {len(results)} files")
            sys.exit(0)
        else:
            print("\n⚠ No results generated")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
        sys.exit(130)
    
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
