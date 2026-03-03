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
    },
    "uk": {
        "reference_file": "uk_regions.geojson",
        "metric_crs": 27700,
        "label": "United Kingdom (British National Grid)",
        "wgs84_bounds": [-8.5, 49.5, 2.0, 61.0],
    },
    "germany": {
        "reference_file": "germany_states.geojson",
        "metric_crs": 25832,
        "label": "Germany (UTM Zone 32N)",
        "wgs84_bounds": [5.9, 47.3, 15.0, 55.1],
    },
    "italy": {
        "reference_file": "italy_regions.geojson",
        "metric_crs": 25832,
        "label": "Italy (UTM Zone 32N)",
        "wgs84_bounds": [6.6, 36.6, 18.5, 47.1],
    },
    "spain": {
        "reference_file": "spain_regions.geojson",
        "metric_crs": 2062,
        "label": "Spain (Madrid 1870)",
        "wgs84_bounds": [-9.3, 35.9, 4.3, 43.8],
    },
    "usa": {
        "reference_file": "usa_states.geojson",
        "metric_crs": 4326,
        "label": "World Geodetic System 1984",
        "wgs84_bounds": [-125.0, 24.0, -66.0, 50.0],
    },
    "europe": {
        "reference_file": "europe_geographic.geojson",
        "metric_crs": 3035,
        "label": "Europe (ETRS89-LAEA)",
        "wgs84_bounds": [-25.0, 34.0, 45.0, 72.0],
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
            # Spatial metrics
            "CRS":                                      "CRS",
            "Types de géométrie":                       "Geometry types",
            "Emprise estimée (km2)":                    "Estimated extent (km2)",
            "Densité (obj/km2)":                        "Density (obj/km2)",
            "Taux de remplissage (%)":                  "Fill rate (%)",
            "Complexite moyenne":                       "Average complexity",
            "Geometries dupliquees (%)":                "Duplicate geometries (%)",
            "Couverture territoriale hexagonale (%)":   "Hexagonal territorial coverage (%)",
            # Nested column table headers
            "Colonne":                                  "Column",
            "Exemple":                                  "Example",
            "Type":                                     "Type",
            "Valeurs manquantes":                       "Missing values",
        }
    }

    return translations.get(lang, {})