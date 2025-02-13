import geopandas as gpd
import numpy as np


def get_city(path, bbox):
    df = gpd.read_file(path)
    df = df.replace('', np.nan, regex=True)
    for col in [c for c in df.columns if c not in ['geom','geoid', 'do_date']]:
        try:
            df[col]=df[col].astype(float)
        except:
            df[col]=df[col].astype(str)

    df['geom'] = gpd.GeoSeries.from_wkt(df.geom)
    df = df.set_geometry('geom')
    return df.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]


def get_streets(path, bbox):
    '''
    Reads a csv in the unica_pedestrian folder, formats it appropiately as a geoDataFrame and restricts it to the bbox.
    Parameters:
        path (str): Path of the file to read.
        bbox (list): Array containing the bounding box, in format [x_min, y_min, x_max, y_max].
    Return:
        (geoDataFrame): Dataframe restricted to the bounding box.

    '''

    df = gpd.read_file(path)
    df = df.replace('', np.nan, regex=True)
    for col in ['imd','visita_tur_stica','trabajadores_estudiantes','residentes','compras_ocio','acceso_hosteler_a','acceso_tpte_p_blico','total']:
        df[col]=df[col].astype(float)

    df['geom'] = gpd.GeoSeries.from_wkt(df.geom)
    df = df.set_geometry('geom')
    return df.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]