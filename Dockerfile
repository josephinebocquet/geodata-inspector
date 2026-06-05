
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        gdal-bin libgdal-dev libgeos-dev libproj-dev libspatialindex-dev gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY geodata_inspector/ ./geodata_inspector/
COPY web_app/ ./web_app/
COPY reference_file/ ./reference_file/
COPY setup.py pyproject.toml MANIFEST.in ./
RUN pip install --no-cache-dir -e .
RUN mkdir -p /tmp/geodata_uploads
EXPOSE 5050
CMD ["python", "web_app/app.py"]