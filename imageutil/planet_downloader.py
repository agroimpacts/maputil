# Import libraries
import os
import requests
import time
import geopandas as gpd
from shapely.geometry import box

class PlanetDownloader():
    def __init__(self) -> None:
        pass

    def get_basemap_grid(self, geom_path=None,  PLANET_API_KEY=None, API_URL=None, dates=None, aoi = None, bbox = None):
        """
        """
        if not os.path.exists(geom_path):
            print(f"{geom_path} does not exist. Creating the file...")

            ids = []
            geometries = []
            dts = []

            if PLANET_API_KEY is None or  API_URL is None:
                raise ValueError('Provide Planet API key and API URL to query mosaics')

            if bbox is None:
                if aoi is None:
                    raise ValueError('Provide at least an aoi or a bbox to query mosaics')
                bbox = aoi.total_bounds

            for date in dates:
                quads, _ = query_quads(PLANET_API_KEY, API_URL, date, bbox)
                for quad in quads['items']:
                    ids.append(quad['id'])
                    geometries.append(box(quad['bbox'][0], quad['bbox'][1], quad['bbox'][2], quad['bbox'][3]))
                    dts.append(date)

                # Create the catalog
                quads_gdf = gpd.GeoDataFrame({'grid': ids, "date": dts, 'geometry': geometries}, 
                                            crs="EPSG:4326")
                if aoi is not None:
                    quads_gdf = gpd.overlay(aoi, quads_gdf)
                    quads_gdf = gpd.sjoin(left_df=quads_gdf, right_df=aoi).drop(columns=['index_right'])
                if geom_path is not None:
                    quads_gdf.to_file(geom_path, driver='GeoJSON')
                    print(f"{geom_path} created")
        else:
            print(f"{geom_path} exists")
            quads_gdf = gpd.read_file(geom_path)
            print(f"{geom_path} read")
        return quads_gdf

def query_quads(PLANET_API_KEY, API_URL, date, bbox = None):
    """
    Helper function: actual function to query quads from the Planet API
    """
    session = setup_session(PLANET_API_KEY, API_URL )
    res = session.get(API_URL, params = {"name__contains" : date})
    mosaic = res.json()
    mosaic_id = mosaic['mosaics'][0]['id']
    mosaic_name =  mosaic['mosaics'][0]['name']
    if bbox is None:
        mosaic_bbox = mosaic['mosaics'][0]['bbox']
        bbox_str = ','.join(map(str, mosaic_bbox))
    else:
        bbox_str = ','.join(map(str, bbox))
    # List mosaics
    quads_url = f"{API_URL}/{mosaic_id}/quads"
    res = session.get(quads_url, params={'bbox': bbox_str,'minimal': True}, stream=True)
    quads = res.json()
    return quads, mosaic_name

def setup_session(API_KEY):
    session = requests.Session()
    session.auth = (API_KEY, "")
    return session