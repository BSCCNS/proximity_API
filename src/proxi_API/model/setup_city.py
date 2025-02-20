# setup_city.py

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

# Set logger for logging info
logger = logging.getLogger("uvicorn.error") 

# Set data paths
data = Path(__file__).parents[1] / "data"
out = Path(__file__).parents[1] / "data" / "cities"

cols = data_aggregation.cols

def main(CITY):
    '''
    Setups the API for a specified city. It
    - Produces a bounding box for the city
    - Reads the proximity time data (provided by SONY)
    - Reads the pedestrian and socio-demographic data
    - Aggregates all data together
    - Dumps everything to disk

    If data is already present, it skips computation.

    
    '''
    if not Path(out / f"{CITY}_{H3_ZOOM}_agg.geojson").is_file():
        # Check folder and create it if not
        out.mkdir(parents=True, exist_ok=True)
        logger.info(f"Setting up model for {CITY}")
        boundary = ox.geocode_to_gdf(CITY)
        bbox = boundary.total_bounds

        logger.info("Reading proximity time data.")
        # Reading the proximity time files and using them to establish the bounding box
        proximity = gpd.read_file(data / "proximity_time_spain" / f"{CITY}.geojson")
        proximity = proximity.cx[bbox[0] : bbox[2], bbox[1] : bbox[3]]

        # If the file exists, we do not compute it
        if not Path(out / f"{CITY}_{H3_ZOOM}_pedestrian.geojson").is_file():
            path = data / "unica_pedestrian"
            pathfiles = [
                name
                for name in os.listdir(path)
                if os.path.isfile(os.path.join(path, name)) and name.endswith(".csv")
            ]
            pathfiles = [path / file for file in pathfiles]

            logger.info("Computing pedestrian data.")

            result = Parallel(n_jobs=N_CORES)(
                delayed(get_streets)(path, bbox) for path in pathfiles
            )
            pedestrian = pd.concat(result).reset_index(drop=True)

            for col in cols:
                pedestrian[col + "_total"] = pedestrian["imd"] * pedestrian[col]

            pedestrian = pedestrian.set_crs(proximity.crs)
            pedestrian.to_file(
                out / f"{CITY}_{H3_ZOOM}_pedestrian.geojson", driver="GeoJSON"
            )
            logger.info("Pedestrian dataset saved to folder.")
        else:
            logger.info("Reading pedestrian data from disk.")

            pedestrian = gpd.read_file(out / f"{CITY}_{H3_ZOOM}_pedestrian.geojson")

        if not Path(out / f"{CITY}_{H3_ZOOM}_demo.geojson").is_file():
            path = data / "unica_sociodemographics/2024/"
            pathfiles = [
                name
                for name in os.listdir(path)
                if os.path.isfile(os.path.join(path, name)) and name.endswith(".csv")
            ]
            pathfiles = [path / file for file in pathfiles]

            logger.info("Computing demographics data.")

            result = Parallel(n_jobs=N_CORES)(
                delayed(get_city)(path, bbox) for path in pathfiles
            )
            sdemo = pd.concat(result)
            sdemo["do_date"] = pd.to_datetime(sdemo.do_date)
            sdemo = sdemo[sdemo["p_t"] != 0]
            sdemo = sdemo.set_crs(proximity.crs)
            sdemo.to_file(out / f"{CITY}_{H3_ZOOM}_demo.geojson", driver="GeoJSON")
            logger.info("Demographic dataset saved to temp folder.")
        else:
            logger.info("Reading demographic data from disk.")
            sdemo = gpd.read_file(out / f"{CITY}_{H3_ZOOM}_demo.geojson")

        logger.info("Aggregating data.")
        agg = data_aggregation.main(pedestrian, proximity, sdemo)
        logger.info("Computing mobility indices")
        agg = mobility_indices.main(agg)
        agg = agg.reset_index(drop=True)

        agg.to_file(out / f"{CITY}_{H3_ZOOM}_prov.geojson", driver="GeoJSON")

        logger.info("Mapping H3 cells")
        agg = h3_mapping.main(agg)

        logger.info("Precomputing mobility indices")
        agg = mobility_indices.main(agg)

        agg = agg.reset_index(drop=True)
        agg.to_file(out / f"{CITY}_{H3_ZOOM}_agg.geojson", driver="GeoJSON")
        logger.info("Aggregated data saved to disk.")

        mp = agg[["geometry", "proximity_time_foot"]]
        mp.to_file(out / f"{CITY}_{H3_ZOOM}_map.geojson", driver="GeoJSON")

        logger.info("Map saved to disk")

    else:
        logger.info("Aggregated data already exists.")


