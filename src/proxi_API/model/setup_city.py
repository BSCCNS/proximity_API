#setup_city.py

import pandas as pd
import geopandas as gpd
import os
from joblib import Parallel, delayed
from pathlib import Path
from proxi_API.data.settings import N_CORES, H3_ZOOM
from proxi_API.model.data_processing import get_city, get_streets
from proxi_API.model import data_aggregation
from proxi_API.model import mobility_indices
from proxi_API.model import h3_mapping 
import osmnx as ox


import logging

logger = logging.getLogger("uvicorn.error")
#Set data paths
data = Path(__file__).parents[1] / 'data'
out = Path(__file__).parents[1] / 'data' / 'cities'
cols = ['residentes', 'visita_tur_stica', 'trabajadores_estudiantes', 'compras_ocio',
         'acceso_hosteler_a','acceso_tpte_p_blico']

def main(CITY):
    if not Path(out / f'{CITY}_{H3_ZOOM}_agg.geojson').is_file():
        #Check folder and create it if not
        out.mkdir(parents=True, exist_ok=True)
        logger.info(f'Setting up model for {CITY}')
        boundary = ox.geocode_to_gdf(CITY)
        bbox = boundary.total_bounds


        logger.info('Reading proximity time data.')
        # Reading the proximity time files and using them to establish the bounding box
        proximity = gpd.read_file(data / 'proximity_time_spain' / f'{CITY}.geojson')
        bbox = proximity.total_bounds
        
        #If the file exists, we do not compute it
        if not Path(out / f'{CITY}_{H3_ZOOM}_pedestrian.geojson').is_file():
            path = data / 'unica_pedestrian'
            pathfiles = [name for name in os.listdir(path) if os.path.isfile(os.path.join(path, name)) and name.endswith('.csv')]
            pathfiles = [path / file for file in pathfiles]

            logger.info('Computing pedestrian data.')

            result = Parallel(n_jobs=N_CORES)(delayed(get_streets)(path, bbox) for path in pathfiles)
            pedestrian = pd.concat(result).reset_index(drop=True)
            

            for col in cols:
                pedestrian[col+'_total'] = pedestrian['imd']*pedestrian[col]

            pedestrian = pedestrian.set_crs(proximity.crs)
            pedestrian.to_file(out / f'{CITY}_{H3_ZOOM}_pedestrian.geojson', driver="GeoJSON")  
            logger.info('Pedestrian dataset saved to folder.')
        else:
            logger.info('Reading pedestrian data from disk.')

            pedestrian = gpd.read_file(out / f'{CITY}_{H3_ZOOM}_pedestrian.geojson')
        
        if not Path(out / f'{CITY}_{H3_ZOOM}_demo.geojson').is_file():
            path = data / 'unica_sociodemographics/2024/'
            pathfiles = [name for name in os.listdir(path) if os.path.isfile(os.path.join(path, name)) and name.endswith('.csv')]
            pathfiles = [path / file for file in pathfiles]

            logger.info('Computing demographics data.')

            result = Parallel(n_jobs=N_CORES)(delayed(get_city)(path, bbox) for path in pathfiles)
            sdemo = pd.concat(result)
            sdemo['do_date'] = pd.to_datetime(sdemo.do_date)
            sdemo = sdemo[sdemo['p_t'] != 0]
            sdemo = sdemo.set_crs(proximity.crs)
            sdemo.to_file(out / f'{CITY}_{H3_ZOOM}_demo.geojson', driver="GeoJSON")
            logger.info('Demographic dataset saved to temp folder.')
        else:
            logger.info('Reading demographic data from disk.')
            sdemo = gpd.read_file(out / f'{CITY}_{H3_ZOOM}_demo.geojson')
            
        

        logger.info('Aggregating data.')
        agg = data_aggregation.main(pedestrian, proximity, sdemo)
        logger.info('Computing mobility indices')
        agg = mobility_indices.main(agg)
        agg = agg.reset_index(drop=True)

        logger.info('Mapping H3 cells')
        agg = h3_mapping.main(agg)
        

        logger.info('Precomputing mobility indices')
        agg = mobility_indices.main(agg)

        agg = agg.reset_index(drop=True)
        agg.to_file(out / f'{CITY}_{H3_ZOOM}_agg.geojson', driver="GeoJSON")
        logger.info('Aggregated data saved to disk.')

    else:
        logger.info('Aggregated data already exists.')
    
    # Finally, we check the database file to store results 
    if not Path(out / f'{CITY}_{H3_ZOOM}_metrics.json').is_file():
        columnas = ['residentes', 
                    'turistas', 
                    'trabajadores',
                    'compras',
                    'hosteleria',
                    'transporte',
                    'value', 'h3_id'
                    ]
        df = gpd.GeoDataFrame(columns=columnas)

        df.to_file(out / f'{CITY}_{H3_ZOOM}_metrics.geojson',driver="GeoJSON")