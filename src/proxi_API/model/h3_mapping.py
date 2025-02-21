from proxi_API.data.settings import H3_ZOOM
import h3


def get_h3_id(geometry, resolution):
    """
    Computes the H3 cell centered at the centroid of a given geometry.

    Parameters:
        geometry (shapley object): Geometry object
        resolution (int): Resolution scale for the H3 hexagons

    Returns:
        str: H3 ID of the hexagon

    """
    centroid = geometry.centroid
    return h3.latlng_to_cell(centroid.x, centroid.y, resolution)


def main(df, method="mean"):
    """
    Computes the h3 cells covering a given dataset.

    Parameters:
        df (GeoDataFrame): Dataframe with geometry info
        method (optional, str): Method of aggregation for large resolutions

    Returns:
        GeoDataFrame: Dataframe converted to H3
    """

    # Apply function to GeoDataFrame

    df["h3_id"] = df["geometry"].apply(lambda x: get_h3_id(x, H3_ZOOM))

    dic = {}
    for col in df.columns:
        if col != "geometry":
            if df[col].dtype in ["int64", "float64"]:  # Numeric columns
                dic[col] = "sum"
            else:
                dic[col] = "first"  # Non-numeric columns
    dic["h3_id"] = "first"  # Ensure h3_id is retained
    dic['proximity_time_foot'] = 'mean'

    df_grouped = df.dissolve(by="h3_id", aggfunc=dic)

    return df_grouped
