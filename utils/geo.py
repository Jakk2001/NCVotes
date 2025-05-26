# viz/utils/geo.py
import geopandas as gpd

def load_county_geojson(path="data/geo/nc_counties.geojson"):
    return gpd.read_file(path)