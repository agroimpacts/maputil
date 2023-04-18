from maputil import *
import yaml
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd


def main(config_path):
    with open(config_path, "r") as config:
        config = yaml.safe_load(config)
    
    PLANET_API_KEY = config['key']
    geom_path = config['geom_path']
    catalog_path = config['catalog_path']
    catalog_temp_dir = config['catalog_temp_dir']
    list_quad_URL = config['list_quad_url']
    dates = config['dates']
    bbox = config['bbox']
    batch_size = config['batch_size']
    quad_dir = config['quad_dir']
    tile_dir = config['tile_dir']
    temp_dir = config['temp_dir']
    tilefile_path = config['tilefile_path']
    dst_width = config['dst_width']
    dst_height = config['dst_height']
    nbands = config['nbands']
    dst_crs = config['dst_crs']
    quad_name = config['quad_name']
    tile_name = config['tile_name']
    
    if os.path.isfile(geom_path):
        geom_gdf = gpd.read_file(geom_path)
        aoi = geom_gdf[['geometry']].dissolve()
    else:
        aoi = None

    downloader = PlanetDownloader()
    quads_url = None

    if config['doGetGrid']:
        if not os.path.isfile(catalog_path):
            if batch_size and batch_size > 0:
                assert len(dates) == 1, "Download 1 date at a time with batch'"
                if not os.path.isdir(catalog_temp_dir):
                    os.mkdir(catalog_temp_dir)

                for k,g in geom_gdf.groupby(np.arange(len(geom_gdf))//batch_size):
                    temp_file = Path(catalog_temp_dir) / f'temp_{k}.geojson'
                    if not os.path.isfile(temp_file):
                        bbox_aoi = g.total_bounds 
                        quads_gdf, quads_url = downloader.get_basemap_grid (
                            PLANET_API_KEY, list_quad_URL, temp_file, 
                            dates = dates, bbox = bbox_aoi
                        )
                        quads_gdf = gpd.overlay(aoi, quads_gdf)
                        quads_gdf = gpd.sjoin(left_df=quads_gdf, right_df=aoi).drop(columns=['index_right'])
                print(f"Merging temp catalogs")
                gdfs = [gpd.read_file(Path(catalog_temp_dir)/f) for f in os.listdir(catalog_temp_dir)]
                gdf = pd.concat(gdfs).pipe(gpd.GeoDataFrame)
                quads_gdf = gpd.overlay(aoi, gdf)
                quads_gdf = gpd.sjoin(left_df=quads_gdf, right_df=aoi).drop(columns=['index_right'])
        
                print(f"Saving catalog {catalog_path}")
                quads_gdf.to_file(catalog_path)
                print(len(gdf.index), gdf.crs)
            else:
                quads_gdf, quads_url = downloader.get_basemap_grid (
                    PLANET_API_KEY, list_quad_URL, catalog_path, 
                    dates = dates, aoi = aoi, bbox = bbox
                )
        
    if config['doDownload']:
        if not os.path.isdir(quad_dir):
            os.mkdir(quad_dir)
        if quads_url:
            quads_url = f"{quads_url}/<id>/full?api_key={PLANET_API_KEY}"
        if batch_size and batch_size > 0:
            for mini_cat in os.listdir(catalog_temp_dir):
                print(f"Downloading from {mini_cat}")
                mini_cat_path = Path(catalog_temp_dir) / mini_cat
                quads_gdf = gpd.read_file(mini_cat_path)
                downloader.download_tiles(
                    PLANET_API_KEY, quad_dir, quad_name, quads_gdf = quads_gdf, 
                    download_url = quads_url, list_quad_URL = list_quad_URL, dates = dates, bbox = bbox
                )
        else:
            quads_gdf = gpd.read_file(catalog_path)
            print(f"Downloading {len(quads_gdf.index)} quads")
            downloader.download_tiles(
                PLANET_API_KEY, quad_dir, quad_name, quads_gdf = quads_gdf, 
                download_url = quads_url, list_quad_URL = list_quad_URL, dates = dates, bbox = bbox
            )

    if config['doRetile']:
        if not os.path.isdir(tile_dir):
            os.mkdir(tile_dir)
        if not os.path.isdir(temp_dir):
            os.mkdir(temp_dir)
        errors = downloader.retiler(
            tile_dir, quad_dir, temp_dir, tilefile_path, dates, 
            dst_width, dst_height, nbands, dst_crs, tile_name, quads_gdf
        )
        print(f"errors: {errors}")

if __name__ =='__main__':
    main('config/config.yml')
