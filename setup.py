#!/usr/bin/env python
from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

install_requires = [
    "duckdb>=0.9.0",
    "geopandas>=0.14.0",
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "openpyxl>=3.1.0",
    "shapely>=2.0.0",
    "pyproj>=3.4.0",
]

extras_require = {
    "web": ["flask>=2.0.0", "werkzeug>=2.0.0"],
    "dev": ["pytest>=7.0.0", "pytest-cov>=4.0.0", "black>=22.0.0", "flake8>=5.0.0"],
}
extras_require["all"] = list(set(sum(extras_require.values(), [])))

setup(
    name="geodata-inspector",
    version="0.1.0",
    author="Josephine Bocquet",
    description="Fast metadata extraction for geospatial files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/josephinebocquet/geodata-inspector",
    packages=find_packages(exclude=["tests", "examples", "web_app"]),
    python_requires=">=3.8",
    install_requires=install_requires,
    extras_require=extras_require,
    include_package_data=True,
    package_data={"geodata_inspector": ["data/*.geojson"]},
)
