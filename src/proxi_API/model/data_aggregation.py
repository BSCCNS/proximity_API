import geopandas as gpd
import numpy as np
import pandas as pd
from pathlib import Path
from proxi_API.data.settings import CUTOFF

data = Path(__file__).parents[1] / "data" / "cities"

cols = [
    "residentes",
    "visita_tur_stica",
    "trabajadores_estudiantes",
    "compras_ocio",
    "acceso_hosteler_a",
    "acceso_tpte_p_blico",
]

ages = ["00_14", "15_24", "25_44", "45_64", "65_79", "80_mas"]

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

bins = [0, 12450, 20200, 36000, 60000, 300000, 1000000]  # Bin edges


def agg(col):
    if col.find("total") >= 0:
        return "sum"  # for columns indicating total quantities, we sum them
    else:
        return "mean"  # for columns indicating ratios, we average them


def main(pedestrian, proximity, sdemo):

    sdemo["men"] = 0
    sdemo["women"] = 0
    for age in ages:
        sdemo["p_ed_" + age] = sdemo["p_ed_" + age + "_h"] + sdemo["p_ed_" + age + "_m"]
        sdemo["men"] += sdemo["p_ed_" + age + "_h"]
        sdemo["women"] += sdemo["p_ed_" + age + "_m"]

    # Let us also bin the data by income

    labels = [f"income_{x}" for x in bins[1:]]  # Labels for each bin

    sdemo["income_bin"] = pd.cut(
        sdemo["renta_hab_disp"], bins=bins, labels=labels, right=False
    )

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
    proximity_aggregated = proximity_expanded.dissolve(
        by="geoid_left",
        aggfunc=aggdict,
    )

    return proximity_aggregated
