import pandas as pd
import geopandas as gpd
import numpy as np



def main(agg):

    agg['mob_index'] = agg.proximity_time_foot*agg.p_t/agg.p_t.sum()
    agg['residentes_index'] = agg.proximity_time_foot*agg.residentes_total/agg.residentes_total.sum()
    agg['turistas_index'] = agg.proximity_time_foot*agg.visita_tur_stica_total/agg.visita_tur_stica_total.sum()
    agg['trabajadores_index'] = agg.proximity_time_foot*agg.compras_ocio_total/agg.compras_ocio_total.sum()
    agg['compras_index'] = agg.proximity_time_foot*agg.trabajadores_estudiantes_total/agg.trabajadores_estudiantes_total.sum()
    agg['hosteleria_index'] = agg.proximity_time_foot*agg.acceso_hosteler_a_total/agg.acceso_hosteler_a_total.sum()
    agg['transporte_index'] = agg.proximity_time_foot*agg.acceso_tpte_p_blico_total/agg.acceso_tpte_p_blico_total.sum()
    
    return agg