import pandas as pd
import geopandas as gpd
import os
from joblib import Parallel, delayed
from pathlib import Path
from proxi_API.data.settings import N_CORES, H3_ZOOM
from proxi_API.model.data_processing import get_city, get_streets
from proxi_API.model import data_aggregation
from proxi_API.model import mobility_indices
import logging
import h3

def agg(col):
        if col.find("total")>=0:
            return "sum" # for columns indicating total quantities, we sum them
        else:
            return "mean" # for columns indicating ratios, we average them


def main(df, method = 'mean'):

    def get_h3_id(geometry, resolution):
        centroid = geometry.centroid
        return h3.latlng_to_cell(centroid.y, centroid.x, resolution)


    # Apply function to GeoDataFrame
    df = df.drop(['centroid_lon','centroid_lat','population'], axis = 1)
    df["h3_id"] = df["geometry"].apply(lambda x: get_h3_id(x, H3_ZOOM))
    
    dic = {}
    for col in df.columns:
        if col != 'geometry':
                dic[col] = method
    dic['h3_id'] = 'first'


    df_grouped = df.dissolve(by="h3_id", aggfunc=dic,)

    return df_grouped