"""
config.py — Configuration loader for Geodata Inspector
=======================================================
Reads config.yaml from the web_app directory (or falls back to defaults).

Usage in app.py:
    from config import get_config, get_reference_info
    cfg = get_config()
"""

import os
import yaml

# ---------------------------------------------------------------------------
# Localisation registry
# Each entry: reference filename, metric CRS EPSG code, display name
# ---------------------------------------------------------------------------

LOCALISATIONS = {
    "france": {
        "reference_file": "fr_regions.geojson",
        "metric_crs": 2154,
        "label": "France (Lambert 93)",
        "wgs84_bounds": [-5.5, 41.0, 10.0, 51.5],
        "geo_keys": [
            {"label": "Code INSEE commune",   "patterns": ["code_insee", "insee_com", "codgeo",'insee','commune'],  "value_format": r"^\d{5}$"},
            {"label": "Code postal",          "patterns": ["code_postal", "codepostal", "cp", "postal"], "value_format": r"^\d{5}$"},
            {"label": "Code département",     "patterns": ["code_dep", "num_dep", "departement", "département","dep"], "value_format": r"^(\d{2,3}|2[AB])$"},
            {"label": "Code région",          "patterns": ["code_reg", "num_reg", "region",'reg'],       "value_format": r"^\d{2}$"},
            {"label": "Code IRIS",            "patterns": ["code_iris", "iris"],                         "value_format": r"^\d{9}$"},
            {"label": "Code EPCI",            "patterns": ["code_epci", "epci", "siren_epci"],           "value_format": r"^\d{9}$"},
        ],
    },
    "uk": {
        "reference_file": "uk_regions.geojson",
        "metric_crs": 27700,
        "label": "United Kingdom (British National Grid)",
        "wgs84_bounds": [-8.5, 49.5, 2.0, 61.0],
        "geo_keys": [
            {"label": "Postcode",             "patterns": ["postcode", "post_code", "pcd"],              "value_format": r"^[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$"},
            {"label": "ONS area code",        "patterns": ["ons_code", "area_code", "geo_code"],         "value_format": r"^[EWS]\d{8}$"},
            {"label": "Local Authority code", "patterns": ["lad_code", "lad21cd", "local_authority"],    "value_format": None},
            {"label": "LSOA code",            "patterns": ["lsoa_code", "lsoa11cd", "lsoa"],             "value_format": None},
            {"label": "MSOA code",            "patterns": ["msoa_code", "msoa11cd", "msoa"],             "value_format": None},
        ],
    },
    "germany": {
        "reference_file": "germany_states.geojson",
        "metric_crs": 25832,
        "label": "Germany (UTM Zone 32N)",
        "wgs84_bounds": [5.9, 47.3, 15.0, 55.1],
        "geo_keys": [
            {"label": "Postleitzahl (PLZ)",   "patterns": ["plz", "postleitzahl", "postcode","post"],           "value_format": r"^\d{5}$"},
            {"label": "Gemeindeschlüssel",    "patterns": ["ags", "gemeinde", "gemeindeschluessel"],     "value_format": r"^\d{8}$"},
            {"label": "Kreisschlüssel",       "patterns": ["kreis", "landkreis", "krs"],                 "value_format": r"^\d{5}$"},
            {"label": "Bundesland code",      "patterns": ["bundesland", "land_code", "bland"],          "value_format": r"^\d{2}$"},
            {"label": "NUTS code",            "patterns": ["nuts", "nuts_code", "nuts3"],                "value_format": r"^DE\w+$"},
        ],
    },
    "italy": {
        "reference_file": "italy_regions.geojson",
        "metric_crs": 25832,
        "label": "Italy (UTM Zone 32N)",
        "wgs84_bounds": [6.6, 36.6, 18.5, 47.1],
        "geo_keys": [
            {"label": "CAP (codice postale)", "patterns": ["cap", "codice_postale", "postal"],           "value_format": r"^\d{5}$"},
            {"label": "Codice ISTAT comune",  "patterns": ["istat", "cod_istat", "pro_com"],             "value_format": r"^\d{6}$"},
            {"label": "Codice provincia",     "patterns": ["cod_prov", "provincia", "sigla_prov"],       "value_format": r"^\d{3}$"},
            {"label": "Codice regione",       "patterns": ["cod_reg", "regione"],                        "value_format": r"^\d{2}$"},
            {"label": "NUTS code",            "patterns": ["nuts", "nuts_code"],                         "value_format": r"^IT\w+$"},
        ],
    },
    "spain": {
        "reference_file": "spain_regions.geojson",
        "metric_crs": 2062,
        "label": "Spain (Madrid 1870)",
        "wgs84_bounds": [-9.3, 35.9, 4.3, 43.8],
        "geo_keys": [
            {"label": "Código postal",        "patterns": ["cod_postal", "codigo_postal", "postal,""cp"],"value_format": r"^\d{5}$"},
            {"label": "Código INE municipio", "patterns": ["cod_ine", "codigo_ine", "cusec"],            "value_format": r"^\d{5}$"},
            {"label": "Código provincia",     "patterns": ["cod_prov", "provincia"],                     "value_format": r"^\d{2}$"},
            {"label": "Código comunidad",     "patterns": ["cod_ccaa", "comunidad", "ccaa"],             "value_format": r"^\d{2}$"},
            {"label": "NUTS code",            "patterns": ["nuts", "nuts_code"],                         "value_format": r"^ES\w+$"},
        ],
    },
    "usa": {
        "reference_file": "usa_states.geojson",
        "metric_crs": 4326,
        "label": "World Geodetic System 1984",
        "wgs84_bounds": [-125.0, 24.0, -66.0, 50.0],
        "geo_keys": [
            {"label": "ZIP code",             "patterns": ["zip", "zip_code", "zipcode", "postal"],      "value_format": r"^\d{5}(-\d{4})?$"},
            {"label": "FIPS state code",      "patterns": ["fips", "state_fips", "statefp"],             "value_format": r"^\d{2}$"},
            {"label": "FIPS county code",     "patterns": ["county_fips", "countyfp", "fips_county"],   "value_format": r"^\d{5}$"},
            {"label": "Census tract",         "patterns": ["tract", "census_tract", "tractce"],          "value_format": None},
            {"label": "State abbreviation",   "patterns": ["state_abbr", "state_code", "stusps"],        "value_format": r"^[A-Z]{2}$"},
        ],
    },
    "europe": {
        "reference_file": "europe_geographic.geojson",
        "metric_crs": 3035,
        "label": "Europe (ETRS89-LAEA)",
        "wgs84_bounds": [-25.0, 34.0, 45.0, 72.0],
        "geo_keys": [
            {"label": "NUTS 0 (country)",     "patterns": ["nuts0", "country_code", "cntr_code"],        "value_format": r"^[A-Z]{2}$"},
            {"label": "NUTS 1",               "patterns": ["nuts1", "nuts_1"],                           "value_format": r"^[A-Z]{2}\d$"},
            {"label": "NUTS 2",               "patterns": ["nuts2", "nuts_2"],                           "value_format": r"^[A-Z]{2}\d{2}$"},
            {"label": "NUTS 3",               "patterns": ["nuts3", "nuts_3"],                           "value_format": r"^[A-Z]{2}\d{3}$"},
            {"label": "LAU code",             "patterns": ["lau", "lau_code", "lau_id"],                 "value_format": None},
        ],
    },
}

SUPPORTED_LANGUAGES = {"fr", "en"}

# ---------------------------------------------------------------------------
# Defaults (used when config.yaml is absent or a key is missing)
# ---------------------------------------------------------------------------
DEFAULTS = {
    "server": {
        "host": "localhost",
        "port": 5050,
        "debug": False,
    },
    "localisation": {
        "country": "france",
        "custom_reference_path": "",
        "custom_metric_crs": 2154,
    },
    "language": "fr",
}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base, returning a new dict."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def get_config(config_path: str = None) -> dict:
    """
    Load and return the merged configuration.

    Args:
        config_path: Optional explicit path to config.yaml.
                     Defaults to web_app/config.yaml (same dir as this file).

    Returns:
        dict with keys: server, localisation, language
    """
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

    cfg = dict(DEFAULTS)

    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            user_cfg = yaml.safe_load(f) or {}
        cfg = _deep_merge(cfg, user_cfg)
        print(f"[Config] Loaded from {config_path}")
    else:
        print(f"[Config] config.yaml not found at {config_path}, using defaults.")

    # Validate language
    lang = cfg.get("language", "fr")
    if lang not in SUPPORTED_LANGUAGES:
        print(f"[Config] Unknown language '{lang}', falling back to 'fr'.")
        cfg["language"] = "fr"

    return cfg


def get_reference_info(cfg: dict, reference_dir: str) -> dict:
    """
    Resolve the reference file path and metric CRS from config.

    Args:
        cfg: config dict from get_config()
        reference_dir: absolute path to the reference_file/ directory

    Returns:
        dict with:
          - path: absolute path to the reference geojson
          - metric_crs: EPSG int for area calculations
          - label: human-readable country/region name
          - available: bool — whether the file actually exists on disk
    """
    loc = cfg.get("localisation", {})
    country = loc.get("country", "france").lower()

    if country == "custom":
        path = loc.get("custom_reference_path", "")
        crs  = int(loc.get("custom_metric_crs", 2154))
        label = f"Custom ({path})"
    else:
        if country not in LOCALISATIONS:
            print(f"[Config] Unknown country '{country}', falling back to 'france'.")
            country = "france"
        info  = LOCALISATIONS[country]
        path  = os.path.join(reference_dir, info["reference_file"])
        crs   = info["metric_crs"]
        label = info["label"]

    available = os.path.exists(path)
    if not available:
        print(f"[Config] WARNING: Reference file not found: {path}")

    return {
        "path":       path,
        "metric_crs": crs,
        "label":      label,
        "available":  available,
        "country":    country,
    }


def get_ui_labels(cfg):
    lang = cfg.get("language", "fr")
    labels = {
        "fr": {
            "page_title":              "Geodata Inspector",
            "html_lang":               "fr",
            "drop_prompt":             "Glissez-déposez un fichier ici",
            "browse_btn":              "Parcourir",
            "clear_btn":               "Effacer",
            "formats_single":          "Formats : CSV, TXT, XLSX, GeoJSON, JSON, SHP, GPKG, ZIP",
            "batch_desc":              "Analyse tous les fichiers d'un dossier et génère un tableur csv récapitulatif",
            "batch_zip_btn":           "📦 ZIP",
            "batch_dir_btn":           "📂 Dossier",
            "batch_waiting":           "En attente...",
            "batch_download_csv":      "⬇ Télécharger en CSV",
            "batch_download_excel":    "⬇ Télécharger en Excel",
            "formats_batch":           "Formats : dossier ou un ZIP de plusieurs fichiers",
            "show_logs":               "Afficher les logs",
            "hide_logs":               "Masquer les logs",
            "results_title":           "Résultats",
            "preview_btn":             "👁 Aperçu des données (10 lignes)",
            "preview_loading":         "⏳ Chargement...",
            "export_label":            "⬇ Exporter le dataset complet en données géographiques :",
            "export_loading":          "⏳ Re-lecture et conversion du fichier complet, merci de patienter...",
            "export_done":             "✓ Export {fmt} téléchargé.",
            "export_no_geo":           "Aucune donnée géographique détectée",
            "map_title":               "Carte",
            "no_geo_data":             "Pas de données géographiques",
            "map_note":                "ℹ️ Aperçu cartographique limité à 1 000 enregistrements. Utilisez l'export pour obtenir le dataset complet.",
            "status_analysing":        "Analyse en cours...",
            "status_large_file":       "Fichier volumineux ({size} MB) - analyse en cours, peut prendre jusqu'à une minute...",
            "log_file_selected":       "Fichier sélectionné: {name} ({size} Ko)",
            "log_sending":             "Envoi du fichier au serveur...",
            "log_receiving":           "Réception de la réponse...",
            "log_error":               "Erreur: {msg}",
            "log_success":             "Analyse terminée avec succès",
            "log_lines":               "Lignes: {rows}, Colonnes: {cols}",
            "log_map_generating":      "Génération de la carte...",
            "log_no_map":              "Pas de données géographiques pour la carte",
            "log_conn_error":          "Erreur de connexion: {msg}",
            "status_conn_error":       "Erreur de connexion : {msg}",
            "batch_sending":           "Envoi en cours...",
            "batch_started":           "Démarré : {total} fichiers à traiter...",
            "batch_error":             "Erreur : {msg}",
            "batch_processing":        "Traitement : {done}/{total} — {current}",
            "batch_finished":          "✓ Terminé : {done} fichiers traités",
            "batch_no_files":          "Aucun fichier compatible trouvé dans ce dossier.",
            "batch_detected":          "{count} fichier(s) détecté(s), envoi en cours...",
            "batch_poll_error":        "Erreur de polling : {msg}",
            "batch_errors_header":     "⚠ Erreurs :",
            "batch_nav_counter":       "{current} / {total}",
            "batch_nav_counter_live":  "{current} / {total} (en cours...)",
            "no_data":                 "Aucune donnée",
            "stop_btn":                "⏹ Arrêter",
            "clear_batch_btn":         "✕ Effacer",
            "glossary": {
                "CRS": "Système de référence de coordonnées détecté dans les données.",
                "Types de géométrie": "Type(s) de géométrie présents : Point, LineString, Polygon, etc.",
                "Emprise estimée (km2)": "Surface de l'enveloppe convexe des géométries, en km².",
                "Densité (obj/km2)": "Nombre d'objets géographiques par km² dans l'emprise estimée.",
                "Taux de remplissage géométrique (%)": "Ratio entre l'emprise réelle des géométries et leur boîte englobante.",
                "Complexité moyenne des géométries": "Nombre moyen de sommets par géométrie. 'None : POINT' pour les points.",
                "Part des geometries dupliquees (%)": "Pourcentage de géométries dont les coordonnées sont identiques à une autre.",
                "Couverture territoriale (%)": "Part du territoire de référence (ex: France hexagonale) couverte par les données.",
                "Score de complétude géographique": "Part des géométries présentes et valides topologiquement.",
                "Score de complétude des clés géographique": "Taux de remplissage moyen et écart-type calculés sur l'ensemble des colonnes identifiées comme clés géographiques.",
                "Score de complétude global": "Taux de remplissage moyen et écart-type calculés sur l'ensemble des colonnes du fichier.",
                "Clés géographiques": "Colonnes identifiées comme des référentiels géographiques joinables (ex: code INSEE, code département, code région).",
                "Géotransformation": "Type de transformation géographique détectée : géométrie native, coordonnées x/y, géocodage requis, ou jointure spatiale.",
            },
        },
        "en": {
            "page_title":              "Geodata Inspector",
            "html_lang":               "en",
            "drop_prompt":             "Drag and drop a file here",
            "browse_btn":              "Browse",
            "clear_btn":               "Clear",
            "formats_single":          "Formats: CSV, TXT, XLSX, GeoJSON, JSON, SHP, GPKG, ZIP",
            "batch_desc":              "Analyse all files in a folder and generate a summary CSV spreadsheet",
            "batch_zip_btn":           "📦 ZIP",
            "batch_dir_btn":           "📂 Folder",
            "batch_waiting":           "Waiting...",
            "batch_download_csv":      "⬇ Download as CSV",
            "batch_download_excel":    "⬇ Download as Excel",
            "formats_batch":           "Formats: folder or a multi-file ZIP",
            "show_logs":               "Show logs",
            "hide_logs":               "Hide logs",
            "results_title":           "Results",
            "preview_btn":             "👁 Data preview (10 rows)",
            "preview_loading":         "⏳ Loading...",
            "export_label":            "⬇ Export full dataset as geospatial file:",
            "export_loading":          "⏳ Reading and converting full file, please wait...",
            "export_done":             "✓ {fmt} export downloaded.",
            "export_no_geo":           "No geographic data detected",
            "map_title":               "Map",
            "no_geo_data":             "No geographic data",
            "map_note":                "ℹ️ Map preview limited to 1,000 records. Use export to get the full dataset.",
            "status_analysing":        "Analysing...",
            "status_large_file":       "Large file ({size} MB) - analysis in progress, may take up to a minute...",
            "log_file_selected":       "File selected: {name} ({size} KB)",
            "log_sending":             "Sending file to server...",
            "log_receiving":           "Receiving response...",
            "log_error":               "Error: {msg}",
            "log_success":             "Analysis completed successfully",
            "log_lines":               "Rows: {rows}, Columns: {cols}",
            "log_map_generating":      "Generating map...",
            "log_no_map":              "No geographic data for the map",
            "log_conn_error":          "Connection error: {msg}",
            "status_conn_error":       "Connection error: {msg}",
            "batch_sending":           "Sending...",
            "batch_started":           "Started: {total} files to process...",
            "batch_error":             "Error: {msg}",
            "batch_processing":        "Processing: {done}/{total} — {current}",
            "batch_finished":          "✓ Done: {done} files processed",
            "batch_no_files":          "No compatible files found in this folder.",
            "batch_detected":          "{count} file(s) detected, sending...",
            "batch_poll_error":        "Polling error: {msg}",
            "batch_errors_header":     "⚠ Errors:",
            "batch_nav_counter":       "{current} / {total}",
            "batch_nav_counter_live":  "{current} / {total} (in progress...)",
            "no_data":                 "No data",
            "stop_btn":                "⏹ Stop",
            "clear_batch_btn":         "✕ Clear",
            "glossary": {
                "CRS": "Coordinate Reference System detected in the data.",
                "Geometry types": "Geometry type(s) present: Point, LineString, Polygon, etc.",
                "Estimated extent (km2)": "Area of the convex hull of all geometries, in km².",
                "Density (obj/km2)": "Number of geographic objects per km² within the estimated extent.",
                "Geometric fill rate (%)": "Ratio between the actual geometry extent and their bounding box.",
                "Average geometry complexity": "Average number of vertices per geometry. 'None : POINT' for points.",
                "Duplicate geometries (%)": "Percentage of geometries whose coordinates are identical to another.",
                "Territorial coverage (%)": "Share of the reference territory (e.g. mainland France) covered by the data.",
                "Geographic completeness score": "Share of geometries that are present and topologically valid.",
                "Geographic key completeness score": "Fill rate of columns identified as geographic join keys.",
                "Geographic completeness score": "Average fill rate and standard deviation computed across all columns in the file.",
                "Geographic keys": "Columns identified as joinable geographic references (e.g. INSEE code, department code, region code).",
                "Geographic transformation": "Type of geographic transformation detected: native geometry, x/y coordinates, geocoding required, or spatial join.",
            },
        },
    }
    return labels.get(lang, labels["fr"])


def get_result_key_translations(cfg):
    """
    Returns a dict mapping French result keys -> translated keys
    for the configured language. If language is 'fr', returns identity mapping.
    """
    lang = cfg.get("language", "fr")

    if lang == "fr":
        return {}  # no translation needed

    translations = {
        "en": {
            # File metadata
            "Nom du fichier":                           "File name",
            "Taille (Ko)":                              "Size (KB)",
            "Date de création du fichier (Y-M-D)":     "File creation date (Y-M-D)",
            "Type de fichier":                          "File type",
            # Basic data info
            "Nb lignes":                                "Nb rows",
            "Nb colonnes":                              "Nb columns",
            "Colonnes":                                 "Columns",
            "Score de complétude global":               "Global completeness score",
            # Geographic info
            "Clés géographiques":                       "Geographic keys",
            "Géotransformation":                        "Geotransformation",
            "Score de complétude des clés géographique": "Geographic key completeness score",
            "Score de complétude géographique":         "Geographic completeness score",

            "Score de complétude moyen": "Mean completeness score" ,  
            "Score de complétude std": "Completeness std score",       
            "Score de complétude moyen (%)": "Mean completeness score (%)",
            "Score de complétude std (%)": "Completeness std score (%)",
            
            # Spatial metrics
            "CRS":                                      "CRS",
            "Types de géométrie":                       "Geometry types",
            "Emprise estimée (km2)":                    "Estimated extent (km2)",
            "Densité (obj/km2)":                        "Density (obj/km2)",
            "Taux de remplissage géométrique (%)":      "Geometric fill rate (%)",
            "Complexite moyenne":                       "Average complexity",
            "Geometries dupliquees (%)":                "Duplicate geometries (%)",
            "Couverture territoriale (%)":              "Territorial coverage (%)",
            "Clé identifiée":                           "Identified key",
            "Aire de référence" :                       "Reference area",
            "Présence géométrie séparée (x,y)":         "Separate geometry presence (x, y)",
            "Présence géométrie multiples (x1,y1), (x2,y2)": "Presence of multiple geometries (x1,y1), (x2,y2)", 
            "Jointure spatiale à l'aide de clés géographiques" :"Spatial join using geographic keys",
            "Présence géométrie" : "Presence geometry",
            "Géocodage de l'adresse": "Address geocoding",
            "Aucune géométrie" : "No geometry", 
            "Granularité" : "Granularity",
            "Complexité moyenne des géométries" : "Average complexity of geometries",
            "Part des geometries dupliquees (%)" : "Percentage of duplicate geometries (%)",
            
            # Nested column table headers
            "Colonne":                                  "Column",
            "Exemple":                                  "Example",
            "Type":                                     "Type",
            "Valeurs manquantes":                       "Missing values",
            "Présentes (%)" :                           "Presence (%)",
            "Valides (%)" :                             "Valid (%)",
            "No geometry":                              "No geometry",
            "Geometry present":                         "Geometry present",
            "Separate geometry (x,y)":                  "Separate geometry (x,y)",
            "Multiple geometry columns (x1,y1), (x2,y2)": "Multiple geometry columns (x1,y1), (x2,y2)",
            "Données géographiques" : "Geographic data"

        }
    }

    return translations.get(lang, {})

def get_geo_key_patterns(cfg):
    """Return the geo_keys list for the configured localisation."""
    loc = cfg.get("localisation", {})
    country = loc.get("country", "france").lower()
    if country == "custom":
        return []
    info = LOCALISATIONS.get(country, LOCALISATIONS["france"])
    return info.get("geo_keys", [])
    
def get_localisation_params(cfg):
    """Return wgs84_bounds and metric_crs for the configured localisation."""
    loc = cfg.get("localisation", {})
    country = loc.get("country", "france").lower()
    info = LOCALISATIONS.get(country, LOCALISATIONS["france"])
    return {
        "wgs84_bounds": info.get("wgs84_bounds", [-5.5, 41.0, 10.0, 51.5]),
        "metric_crs": info.get("metric_crs", 2154),
    }
    
# def get_wgs84_bounds(cfg):
#     """Return the WGS84 bounds for the configured localisation."""
#     loc = cfg.get("localisation", {})
#     country = loc.get("country", "france").lower()
#     info = LOCALISATIONS.get(country, LOCALISATIONS["france"])
#     return info.get("wgs84_bounds", [-5.5, 41.0, 10.0, 51.5])  # France fallback