"""Geodata Inspector - Fast metadata extraction for geospatial files"""
__version__ = "0.1.0"

from .metadata import MetadataExtractor, ExtractionResult
from .core import inspect_file

try:
    from .core import summary_rows
except:
    summary_rows = []

__all__ = ["MetadataExtractor", "ExtractionResult", "inspect_file"]
