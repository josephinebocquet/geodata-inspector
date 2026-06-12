FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        gdal-bin libgdal-dev libgeos-dev libproj-dev libspatialindex-dev \
        libnetcdf-dev libhdf5-dev \
        gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-install the DuckDB spatial extension so the container works without
# outbound internet access at runtime.
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL spatial; LOAD spatial;')"

COPY geodata_inspector/ ./geodata_inspector/
COPY web_app/ ./web_app/
COPY reference_file/ ./reference_file/
COPY setup.py pyproject.toml MANIFEST.in ./
RUN pip install --no-cache-dir -e .

# Persistent directory for export/preview temp files (matches PREVIEW_DIR env var)
RUN mkdir -p /data/geodata_preview
ENV GEODATA_PREVIEW_DIR=/data/geodata_preview

EXPOSE 5050
CMD ["gunicorn", \
     "--bind", "0.0.0.0:5050", \
     "--workers", "1", \
     "--timeout", "300", \
     "--worker-class", "sync", \
     "web_app.app:app"]