# Import libraries
import os
import time
import geopandas as gpd
from shapely.geometry import box
from planet.api import ClientV1, auth, utils


def planet_basemap_downloader(API_key, geom_path, dates, quads_path):
    """
    API_key (str): the Planet API key
    geom_path (str): the path to the nicfi catalog
    dates (arr): an array of strings containing the dates of quads to download
                with format 'yyyy-mm'
    quads_path (str): directory to output files
    """

    client = get_config(API_key)
    quads_gdf, bbox = get_catalog(client, geom_path, dates)
    quads_gdf = download_quads(client, quads_gdf, dates, bbox, quads_path)
    quads_gdf.to_file(geom_path, driver='GeoJSON')
    return

def get_config(API_key):
    if auth.find_api_key() is None:
        utils.write_planet_json({'key': API_key})
    client = ClientV1()
    return client

def get_catalog(client, geom_path, dates):
    if os.path.exists(geom_path):
        print(f'Quad grid {os.path.basename(geom_path)} exists')
        quads_gdf = gpd.read_file(geom_path)
        aoi = quads_gdf[['geometry']].dissolve()
        bbox = aoi.total_bounds
    else:
        # Create grid of quads to download
        tiles = []
        ids = []
        dts = []
        aoi = gpd.read_file(geom_path)[['geometry']].dissolve()
        bbox = aoi.total_bounds
        for date in dates:
            mosaic = client.get_mosaics(name_contains=date).get()['mosaics'][0]
            quads = client.get_quads(mosaic, bbox=bbox).items_iter(limit=10000)
            for quad in quads:
                tiles.append(box(quad['bbox'][0], quad['bbox'][1],
                                quad['bbox'][2], quad['bbox'][3]))
                ids.append(quad['id'])
                dts.append(date)

        quads_gdf = gpd.GeoDataFrame({'grid': ids, "date": dts, 'geometry': tiles}, 
                                crs="EPSG:4326")
        quads_gdf = gpd.overlay(aoi, quads_gdf)
        quads_gdf = gpd.sjoin(left_df=quads_gdf, 
                                right_df=aoi).drop(columns=['index_right'])
        quads_gdf.to_file(geom_path, driver='GeoJSON')
    return quads_gdf, bbox

def download_quads(client, quads_gdf, dates, bbox, quads_path):
    i = 0
    names = []
    for date in dates:
        print(date)
        mosaic = client.get_mosaics(name_contains=date).get()['mosaics'][0]
        quads = client.get_quads(mosaic, bbox=bbox).items_iter(limit=10000)
        for quad in quads:
            if quad['id'] in list(quads_gdf['grid']):
                
                print(quad['id'])
                
                # Output name
                fname = mosaic['name'] + '_' + quad['id'] + '.tif'
                names.append(fname)
                fname_full = os.path.join(quads_path, fname)

                if not os.path.exists(fname_full):
                    print(f'Downloading {fname_full}')  
                    client.download_quad(quad).get_body().write(fname_full)

                    # Increase counter
                    i += 1
                    if i % 25 == 0:
                        print(f'*** Download {i} tiles ***')
                    # Sleep to avoid connection reset
                    if i % 100 == 0:
                        time.sleep(5)
                else:
                    print(f'File {fname_full} already exists locally')  
    quads_gdf['file'] = names
    return quads_gdf
