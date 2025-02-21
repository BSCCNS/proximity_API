import numpy as np
from pathlib import Path

data = Path(__file__).parents[1] / "data" / "cities"  # Data path


# Labels for the aggregation of data

cols = [
    "residentes",
    "visita_tur_stica",
    "trabajadores_estudiantes",
    "compras_ocio",
    "acceso_hosteler_a",
    "acceso_tpte_p_blico",
]


categ = [
    "culture_foot",
    "education_foot",
    "healthcare_foot",
    "outdoor_foot",
    "physical_foot",
    "restaurant_foot",
    "services_foot",
    "supplies_foot",
    "transport_foot",
]


def agg(col):
    """
    Dictionary for aggregation of data.

    Parameters:
        col (str): Name of the column of the dataset.
    """
    if col.find("total") >= 0:
        return "sum"  # for columns indicating total quantities, we sum them

    else:
        return "mean"  # for columns indicating ratios, we average them


def main(pedestrian, proximity, sdemo):
    """
    Aggregates the datasets by using spatial joins. It projects the street info into the cells of the proximity dataset.

    Parameters:
        pedestrian (df): Dataset containing the information about pedestrian flow
        proximity (df): Dataset containing the information about proximity time
        sdemo (df): Dataset containing socio-demographic data

    Returns:
        df: Dataset with aggregated data
    """
    prox = proximity.set_crs(proximity.crs)
    proximity_expanded = prox.sjoin(
        pedestrian, how="inner", predicate="intersects"
    ).drop("index_right", axis="columns")

    proximity_expanded = proximity_expanded.sjoin(
        sdemo, how="inner", predicate="intersects"
    )


    drop_cols = [
        col
        for col in proximity_expanded.columns
        if type(proximity_expanded[col].values[0]) != np.float64
        and type(proximity_expanded[col].values[0]) != float
    ]
    aggdict = {
        col: agg(col)
        for col in proximity_expanded.columns
        if col not in drop_cols + ["geom"]
    }
    aggdict["geoid_left"] = "first"
    aggdict['p_t'] = 'sum'
    proximity_aggregated = proximity_expanded.dissolve(
        by="geoid_left",
        aggfunc=aggdict,
    )

    return proximity_aggregated
