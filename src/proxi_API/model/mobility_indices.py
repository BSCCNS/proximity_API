import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path
from proxi_API.data.settings import CITY

out = Path(__file__).parents[1] / 'data' / 'cities'

def main(agg):

    agg['mob_index'] = agg.proximity_time_foot*agg.p_t/agg.p_t.sum()
    agg['residentes_index'] = agg.proximity_time_foot*agg.residentes_total/agg.residentes_total.sum()
    agg['turistas_index'] = agg.proximity_time_foot*agg.visita_tur_stica_total/agg.visita_tur_stica_total.sum()
    agg['trabajadores_index'] = agg.proximity_time_foot*agg.compras_ocio_total/agg.compras_ocio_total.sum()
    agg['compras_index'] = agg.proximity_time_foot*agg.trabajadores_estudiantes_total/agg.trabajadores_estudiantes_total.sum()
    agg['hosteleria_index'] = agg.proximity_time_foot*agg.acceso_hosteler_a_total/agg.acceso_hosteler_a_total.sum()
    agg['transporte_index'] = agg.proximity_time_foot*agg.acceso_tpte_p_blico_total/agg.acceso_tpte_p_blico_total.sum()
    
    return agg


def metric_comp(sliders):
        sliders = np.array(sliders)
        sliders = sliders/sum(sliders)
        dataset = gpd.read_file(out / f'{CITY}_{H3_ZOOM}_agg.geojson')
        params = ['residentes', 
                    'turistas', 
                    'trabajadores',
                    'compras',
                    'hosteleria',
                    'transporte'
                    ]
   
        metrics = np.array([dataset[x+'_index'] for x in params]).T
        df = dataset[['h3_id']].copy()
        df['value'] = metrics@sliders
        for i, x in enumerate(params):
            df[x] = sliders[i]

        metrics = gpd.read_file(out / f'{CITY}_{H3_ZOOM}_metrics.geojson')
        metrics = gpd.GeoDataFrame( pd.concat([metrics, df], ignore_index=True) )
        metrics = metrics.drop_duplicates()
        metrics.to_file(out / f'{CITY}_{H3_ZOOM}_metrics.geojson',driver="GeoJSON")
