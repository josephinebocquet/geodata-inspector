"""Geodata Inspector - Fast metadata extraction for geospatial files"""
__version__ = "0.1.0"

from .metadata import MetadataExtractor, ExtractionResult
from .core import inspect_file, summary_rows, last_gdf

__all__ = ["MetadataExtractor", "ExtractionResult", "inspect_file", "summary_rows", "last_gdf"]
