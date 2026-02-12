"""
Spatial Metrics Module
======================
Functions to calculate spatial quality metrics for geodata analysis.

Metrics included:
- couverture_territoriale: Territorial coverage percentage
- taux_de_remplissage: Fill rate (data extent vs bounding box)
- indice_moran: Moran's I spatial autocorrelation index
- coefficient_variation_spatiale: Spatial coefficient of variation
- complexite_moyenne: Average geometry complexity (vertices)
- pourcentage_geometries_dupliquees: Duplicate geometries percentage

Author: GeoCancer Project
"""

import geopandas as gpd
import numpy as np
from shapely.geometry import box
from shapely import get_num_coordinates
from shapely.geometry import box


# def couverture_territoriale(gdf, gdf_reference, crs_metric=2154):

#     # Check if data is empty
#     if len(gdf) == 0:
#         return {'Couverture territoriale (%)': 0}

#     # Project to metric CRS for accurate area calculation
#     if gdf.crs != crs_metric :
#         gdf = gdf.to_crs(epsg=crs_metric)

#     # Check again after projection
#     if len(gdf) == 0:
#         return {'Couverture territoriale (%)': 0}

#     # Dissolve reference to get total territory
#     ref_dissolved = gdf_reference.union_all()
#     ref_area = ref_dissolved.area

#     # Get geometry type
#     geom_type = gdf.geometry.iloc[0].geom_type

#     # Get data coverage based on geometry type
#     if geom_type == 'Point':
#         # For points: use convex hull
#         data_coverage = gdf.union_all().convex_hull

#     elif geom_type in ['LineString', 'MultiLineString']:
#         # For lines: union all lines, then buffer to create area
#         # Buffer size: 0.1% of reference diagonal
#         ref_bounds = gdf_reference.total_bounds
#         ref_width = ref_bounds[2] - ref_bounds[0]
#         ref_height = ref_bounds[3] - ref_bounds[1]
#         ref_diagonal = np.sqrt(ref_width**2 + ref_height**2)
#         buffer_size = ref_diagonal * 0.001  # 0.1% of diagonal

#         lines_union = gdf.union_all()
#         data_coverage = lines_union.buffer(buffer_size)

#     else:
#         # For polygons and others: union directly
#         data_coverage = gdf.union_all()

#     # Calculate intersection with reference
#     intersection = data_coverage.intersection(ref_dissolved)
#     intersection_area = intersection.area

#     # Calculate coverage percentage
#     coverage_pct = (intersection_area / ref_area) * 100 if ref_area > 0 else 0

#     return {'Couverture territoriale (%)': round(coverage_pct, 2)}

def taux_de_remplissage(gdf, gdf_reference, crs_metric=2154):
    """
    Calculate the fill rate: ratio of actual data coverage vs bounding box.

    ANALYTIC METHOD:
    ----------------
    For Points:
        - Bbox area: rectangular envelope around all points
        - Data area: convex hull of all points
        - Fill rate = (convex hull area / bbox area) × 100%

    For Polygons:
        - Bbox area: rectangular envelope around all geometries
        - Data area: union of all polygons
        - Fill rate = (union area / bbox area) × 100%

    For LineStrings:
        - Bbox area: rectangular envelope around all lines
        - Data coverage: buffer around union of lines (0.1% of diagonal)
        - Fill rate = (buffered lines area / bbox area) × 100%

    Args:
        gdf: GeoDataFrame to analyze (already cleaned)
        crs_metric: CRS for area calculations (default: EPSG:2154 for France)

    Returns:
        dict with:
        - Taux de remplissage (%): fill rate percentage
    """
    from shapely import unary_union, MultiPoint

    # Avoid projection if already in correct CRS
    if gdf.crs is not None and gdf.crs.to_epsg() == crs_metric:
        gdf_proj = gdf
    else:
        gdf_proj = gdf.to_crs(epsg=crs_metric)

    # Dissolve reference to get total territory
    ref_dissolved = gdf_reference.union_all()
    ref_area = ref_dissolved.area

    # Get bounding box
    minx, miny, maxx, maxy = gdf_proj.total_bounds

    # Validate bounds (check for NaN or infinite values)
    if not all(np.isfinite([minx, miny, maxx, maxy])):
        return {'Taux de remplissage (%)': 0}

    # Check for degenerate bbox (width or height is zero)
    bbox_width = maxx - minx
    bbox_height = maxy - miny

    if bbox_width < 1e-6 or bbox_height < 1e-6:  # Less than 1mm
        return {'Taux de remplissage (%)': 100}  # All at same location

    # Create bounding box (wrapped in try-except as final safeguard)
    try:
        bbox = box(minx, miny, maxx, maxy)
        bbox_area = bbox.area
    except:
        return {'Taux de remplissage (%)': 0}

    # Get geometry type
    geom_type = gdf_proj.geometry.iloc[0].geom_type

    # Calculate data coverage based on geometry type
    if geom_type == 'Point':
        # For points: create MultiPoint directly (faster than union_all)
        coords = [(g.x, g.y) for g in gdf_proj.geometry if g is not None]
        if len(coords) < 3:
            return {'Taux de remplissage (%)': 0}
        data_coverage = MultiPoint(coords).convex_hull
        data_area = data_coverage.area

    elif geom_type in ['LineString', 'MultiLineString']:
        # For lines: buffer each line FIRST, then union the polygons
        # This is ~15-20x faster than union_all().buffer()
        bbox_diagonal = np.sqrt(bbox_width**2 + bbox_height**2)
        buffer_size = bbox_diagonal * 0.001  # 0.1% of diagonal

        buffered = [g.buffer(buffer_size) for g in gdf_proj.geometry if g is not None]
        data_coverage = unary_union(buffered)
        data_area = data_coverage.area

    else:
        # For polygons and others: union directly
        geoms = [g for g in gdf_proj.geometry if g is not None and not g.is_empty]
        data_coverage = unary_union(geoms)
        data_area = data_coverage.area

    # Calculate fill rate
    fill_rate = (data_area / bbox_area) * 100 if bbox_area > 0 else 0

    # Calculate intersection with reference
    intersection = data_coverage.intersection(ref_dissolved)
    intersection_area = intersection.area

    # Calculate coverage percentage
    coverage_pct = (intersection_area / ref_area) * 100 if ref_area > 0 else 0
    return {'Taux de remplissage (%)': round(fill_rate, 2),
            'Couverture territoriale (%)': round(coverage_pct, 2)}

    
def indice_moran(gdf, attribute_col, crs_metric=2154):
    """
    Calculate Moran's I index for spatial autocorrelation.

    Moran's I ranges from -1 to +1:
    - +1: Perfect positive autocorrelation (similar values cluster together)
    - 0: Random distribution (no spatial pattern)
    - -1: Perfect negative autocorrelation (dissimilar values cluster)

    Args:
        gdf: GeoDataFrame with data
        attribute_col: Column name for the numeric attribute to analyze
        crs_metric: CRS for distance calculations

    Returns:
        dict with Moran's I metrics:
        - Indice de Moran (I): the Moran's I value
        - Valeur attendue (E[I]): expected value under null hypothesis
        - p-value: statistical significance
        - Z-score: standardized score
        - Interpretation: text interpretation of result
    """
    try:
        from libpysal.weights import KNN, Queen
        from esda.moran import Moran
    except ImportError:
        return {'Erreur': 'libpysal ou esda non installe. Installer avec: pip install libpysal esda'}

    gdf_proj = gdf.to_crs(epsg=crs_metric)

    # Get the attribute values
    y = gdf_proj[attribute_col].values

    # Create spatial weights
    try:
        if gdf_proj.geom_type.iloc[0] == 'Point':
            k = min(4, len(gdf_proj) - 1)
            w = KNN.from_dataframe(gdf_proj, k=k)
        else:
            w = Queen.from_dataframe(gdf_proj, use_index=False)
    except Exception as e:
        return {'Erreur': f'Impossible de creer les poids spatiaux: {e}'}

    # Row-standardize weights
    w.transform = 'r'

    # Calculate Moran's I
    moran = Moran(y, w)

    # Interpret the result
    if moran.p_sim < 0.05:
        if moran.I > 0:
            interpretation = 'Autocorrelation positive significative (clustering)'
        else:
            interpretation = 'Autocorrelation negative significative (dispersion)'
    else:
        interpretation = 'Pas d autocorrelation significative (distribution aleatoire)'

    return {
        'Indice de Moran (I)': round(moran.I, 4),
        'Valeur attendue (E[I])': round(moran.EI, 4),
        'p-value': round(moran.p_sim, 4),
        'Z-score': round(moran.z_sim, 4),
        'Interpretation': interpretation
    }


def coefficient_variation_spatiale(gdf, grid_size=50000, crs_metric=2154):
    """
    Calculate spatial coefficient of variation based on feature density per grid cell.
    CV = std / mean. High CV indicates uneven distribution, low CV indicates regularity.

    Args:
        gdf: GeoDataFrame to analyze
        grid_size: Grid cell size in meters (default 50km)
        crs_metric: CRS for calculations

    Returns:
        dict with CV metrics:
        - Coefficient de variation (%): the CV value
        - Moyenne par cellule: mean features per cell
        - Ecart-type: standard deviation
        - Cellules occupees (%): percentage of non-empty cells
        - Nb cellules total: total number of grid cells
        - Taille cellule (km): cell size in km
        - Interpretation: text interpretation
    """
    gdf_proj = gdf.to_crs(epsg=crs_metric)

    # Get bounding box
    minx, miny, maxx, maxy = gdf_proj.total_bounds

    # Create grid
    x_coords = np.arange(minx, maxx + grid_size, grid_size)
    y_coords = np.arange(miny, maxy + grid_size, grid_size)

    # Count features per grid cell
    counts = []
    for x in x_coords[:-1]:
        for y in y_coords[:-1]:
            cell = box(x, y, x + grid_size, y + grid_size)
            n = gdf_proj.intersects(cell).sum()
            counts.append(n)

    counts = np.array(counts)

    # Calculate statistics
    mean_count = counts.mean()
    std_count = counts.std()
    cv = (std_count / mean_count * 100) if mean_count > 0 else 0

    # Additional metrics
    empty_cells = (counts == 0).sum()
    total_cells = len(counts)
    occupied_pct = ((total_cells - empty_cells) / total_cells) * 100

    # Interpret CV
    if cv < 50:
        distribution = 'Distribution reguliere'
    elif cv < 100:
        distribution = 'Distribution moderement irreguliere'
    else:
        distribution = 'Distribution tres irreguliere (clustered)'

    return {
        'Coefficient de variation (%)': round(cv, 2),
        'Moyenne par cellule': round(mean_count, 2),
        'Ecart-type': round(std_count, 2),
        'Cellules occupees (%)': round(occupied_pct, 2),
        'Nb cellules total': total_cells,
        'Taille cellule (km)': grid_size / 1000,
        'Interpretation': distribution
    }


def complexite_moyenne(gdf):
    """
    Calculate average geometry complexity (number of vertices/coordinates).
    
    ANALYTIC METHOD:
    ----------------
    - Points: Not applicable (always 1 coordinate, no complexity)
    - LineStrings: number of points along the line (2 = straight, 100+ = complex)
    - Polygons: number of vertices (4-5 = rectangle, 1000+ = detailed)
    
    Args:
        gdf: GeoDataFrame to analyze
    
    Returns:
        dict with:
        - Complexite moyenne (sommets): average vertex count (or None for Points)
        - Ecart-type: standard deviation (or None for Points)
    """    
    # Check if GeoDataFrame is empty
    if len(gdf) == 0:
        return {
            'Complexite moyenne (sommets)': None,
            'Ecart-type': None
        }
    
    # Remove invalid geometries
    gdf_clean = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    
    if len(gdf_clean) == 0:
        return {
            'Complexite moyenne (sommets)': None,
            'Ecart-type': None
        }
    
    # Get geometry type
    geom_type = gdf_clean.geometry.iloc[0].geom_type
    
    # For Points: complexity is not applicable
    if geom_type == 'Point':
        return {
            'Complexite moyenne (sommets)': 'None : POINT',
            'Ecart-type': None,
            'Note': 'Points n\'ont pas de complexité (toujours 1 sommet)'
        }
    
    # For LineStrings and Polygons: calculate complexity
    vertex_counts = [get_num_coordinates(g) if g is not None else 0 for g in gdf_clean.geometry]
    vertex_counts = np.array(vertex_counts)
    
    mean_vertices = float(np.mean(vertex_counts))
    std_vertices = float(np.std(vertex_counts))
    
    return {
        'Complexite moyenne (sommets)': round(mean_vertices, 2),
        'Ecart-type': round(std_vertices, 2)
    }
    
def pourcentage_geometries_dupliquees(gdf, sample_size=1000):
    """
    Calculate percentage of duplicate geometries using WKT comparison.
    Efficient method suitable for large datasets (uses sampling if needed).

    Args:
        gdf: GeoDataFrame to analyze
        sample_size: Max features to compare for large datasets (default 1000)

    Returns:
        dict with duplicate metrics:
        - Geometries dupliquees (%): percentage of duplicates
        - Nb doublons: count of duplicates
        - Nb uniques: count of unique geometries
        - Nb total: total count analyzed
        - Echantillonne: whether sampling was used
    """
    # Sample for large datasets
    if len(gdf) > sample_size:
        gdf_sample = gdf.sample(n=sample_size, random_state=42)
        sampled = True
    else:
        gdf_sample = gdf
        sampled = False

    # Use WKT for fast comparison
    wkt_list = gdf_sample.geometry.apply(lambda g: g.wkt if g is not None else None).tolist()

    # Count duplicates using set
    unique_count = len(set(wkt_list))
    total_count = len(wkt_list)
    duplicate_count = total_count - unique_count
    duplicate_pct = (duplicate_count / total_count) * 100 if total_count > 0 else 0

    return {
        'Geometries dupliquees (%)': round(duplicate_pct, 2),
        'Nb doublons': duplicate_count,
        'Nb uniques': unique_count,
        'Nb total': total_count,
        'Echantillonne': 'Oui' if sampled else 'Non'
    }


def compute_all_metrics(gdf, gdf_reference=None, attribute_col=None, grid_size=50000):
    """
    Compute all available spatial metrics for a GeoDataFrame.

    Args:
        gdf: GeoDataFrame to analyze
        gdf_reference: Optional reference GeoDataFrame for coverage calculation
        attribute_col: Optional column name for Moran's I calculation
        grid_size: Grid cell size in meters for CV calculation

    Returns:
        dict with all metrics combined
    """
    results = {}

    # Always compute these
    results['taux_remplissage'] = taux_de_remplissage(gdf)
    results['complexite'] = complexite_moyenne(gdf)
    results['doublons'] = pourcentage_geometries_dupliquees(gdf)
    results['variation_spatiale'] = coefficient_variation_spatiale(gdf, grid_size=grid_size)

    # Conditional metrics
    if gdf_reference is not None:
        results['couverture'] = couverture_territoriale(gdf, gdf_reference)

    if attribute_col is not None and attribute_col in gdf.columns:
        results['moran'] = indice_moran(gdf, attribute_col)

    return results


