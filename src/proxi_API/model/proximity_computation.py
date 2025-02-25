import geopandas as gpd
import pandas as pd
from joblib import Parallel, delayed
import osmnx as ox
import h3
from shapely import Polygon
import json
import openrouteservice
from tqdm import tqdm

place = 'Barcelona, Spain'
client = openrouteservice.Client(base_url='http://localhost:8080/ors')
# Enable TQDM for joblib
tqdm.pandas()
# Function to apply proximity_time on a row
# Parallel apply using Joblib
num_cores = 6  # Use all available cores

#Now we load the category mapping
with open('categories.json', 'r') as file:
    categories = json.load(file)

    # Flatten the categories dictionary into a single mapping (amenity -> category)
amenity_to_category = {amenity: category for category, amenities in categories.items() for amenity in amenities}

def get_pois(start, dist, category):
    (lon, lat) = start
    try:
        df = ox.features_from_point((lat, lon), dist = dist, tags = {'amenity':True})
    except:
        df = gpd.GeoDataFrame([], columns = ['geometry','amenity'])
    df = df[['geometry','amenity']]
    df['category'] = df['amenity'].map(amenity_to_category)
    df = df[df['category'] == category]
    df = df.to_crs(epsg=3035)
    df['centroid'] = df.geometry.centroid
    df['lon'] = df.centroid.to_crs(epsg=4326).x
    df['lat'] = df.centroid.to_crs(epsg=4326).y


    return df[['lat','lon','category']].reset_index(drop = True)


def compute_time(start, end, client = client):
    # Format is (lon,lat)
    try:
        route = client.directions(
        coordinates=[start, end],
        profile = 'foot-walking',
        )
        return route['routes'][0]['summary']['duration']
    except: 
        return -1
    
def proximity_time(start, dist, category):
    # Format is (lon,lat)
    pois = get_pois(start, dist, category)
    pois['time'] = pois.apply(lambda row: compute_time(start, (row.lon,row.lat)), axis = 1)
    pois = pois[pois['time']!=-1]
    return (pois['time'].min())/60





if __name__ == "__main__":
    # First step is to produce the boundary of the town
    print('Loading city')
    boundary = ox.geocode_to_gdf(place).iloc[0].geometry
    print('Creating H3 cells')
    out = h3.geo_to_cells(boundary,8)
    h3_map = pd.DataFrame(out, columns = ['h3_index'])
    h3_map['lat'] = h3_map['h3_index'].apply(lambda x: h3.cell_to_latlng(x)[0])
    h3_map['lon'] = h3_map['h3_index'].apply(lambda x: h3.cell_to_latlng(x)[1])


    print(f'Using {num_cores} cores')

    for category in categories.keys():
        print(f'Computing for {category}')
        results = Parallel(n_jobs=num_cores)(
            delayed(lambda row: proximity_time((row.lon, row.lat), 4000, category))(row) for _, row in tqdm(h3_map.iterrows(), total=len(h3_map))
        )

        # Assign results back to the DataFrame
        h3_map['pt_'+category] = results


    def h3_to_polygon(h3_index):
        boundary = h3.cell_to_boundary(h3_index)  # Get hex boundary
        return Polygon(boundary)

    df = h3_map.copy()
    # Create a GeoDataFrame
    df['geometry'] = df['h3_index'].apply(h3_to_polygon)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # Set Coordinate Reference System (CRS)
    gdf.set_crs(epsg=4326, inplace=True)  # WGS 84

    gdf.to_file("map_prueba.geojson", driver="GeoJSON")

    print('Result saved to disk')
