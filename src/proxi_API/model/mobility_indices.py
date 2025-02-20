import geopandas as gpd
import numpy as np
from pathlib import Path
from proxi_API.data.settings import CITY, H3_ZOOM
import inequality

out = Path(__file__).parents[1] / "data" / "cities"


def main(agg):
    """
    Computes the mobility indices for the different pedestrian types.

    Parameters:
        agg (GeoDataFrame): Dataframe containing the information

    Returns:
        GeoDataFrame: Original dataframe with the new data added


    """
    agg["mob_index"] = agg.proximity_time_foot * agg.p_t / agg.p_t.sum()
    agg["residentes_index"] = (
        agg.proximity_time_foot * agg.residentes_total / agg.residentes_total.sum()
    )
    agg["turistas_index"] = (
        agg.proximity_time_foot
        * agg.visita_tur_stica_total
        / agg.visita_tur_stica_total.sum()
    )
    agg["trabajadores_index"] = (
        agg.proximity_time_foot * agg.compras_ocio_total / agg.compras_ocio_total.sum()
    )
    agg["compras_index"] = (
        agg.proximity_time_foot
        * agg.trabajadores_estudiantes_total
        / agg.trabajadores_estudiantes_total.sum()
    )
    agg["hosteleria_index"] = (
        agg.proximity_time_foot
        * agg.acceso_hosteler_a_total
        / agg.acceso_hosteler_a_total.sum()
    )
    agg["transporte_index"] = (
        agg.proximity_time_foot
        * agg.acceso_tpte_p_blico_total
        / agg.acceso_tpte_p_blico_total.sum()
    )

    return agg


def metric_comp(sliders):
    """
    Returns the proximity times and inequality metrics for the pedestrian categories, weighted by the input sliders.

    Parameters:
        sliders (array): Array of shape (6,) containing the numerical value for the weights of each pedestrian category.

    Returns:
        Dict: Dictionary containing the proximity and inequality metrics

    """

    sliders = np.array(sliders)
    sliders = sliders / sum(sliders)
    dataset = gpd.read_file(out / f"{CITY}_{H3_ZOOM}_agg.geojson")
    params = [
        "residentes",
        "turistas",
        "trabajadores",
        "compras",
        "hosteleria",
        "transporte",
    ]

    labels = [
        "residents",
        "tourists",
        "workers/students",
        "leisure",
        "access to hospitality",
        "access to public transport",
    ]

    metrics = np.array([dataset[x + "_index"] for x in params]).T
    df = dataset[["h3_id"]].copy()
    df["value"] = metrics @ sliders
    for i, x in enumerate(params):
        df[x] = sliders[i]

    total = df.value.sum()
    gini_index = 100 * inequality.gini.Gini(df["value"].values).g
    theil_index = inequality.theil.Theil(df["value"].values).T
    theil_percent = 100 * (1 - np.exp(-1 * theil_index))

    prox_times = [dataset[x + "_index"].sum() for x in ["mob"] + params]
    heads = [x + "_time" for x in ["mob"] + labels] + [
        "averaged_time",
        "gini (%)",
        "theil",
        "theil (%)",
    ]
    val = np.concatenate(
        (prox_times, np.array([total, gini_index, theil_index, theil_percent]))
    )

    result = {x: y for x, y in zip(heads, val)}

    return result
