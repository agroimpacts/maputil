from maputil import *
import yaml


def main(config_path):
    with open(config_path, "r") as config:
        config = yaml.safe_load(config)
    
    geom_path = config['geom']
    PLANET_API_KEY = config['key']
    list_quad_URL = config['list_quad_url']
    dates = config['dates']
    bbox = config['bbox']
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
    try:
        aoi = gpd.read_file(config['geom'])[['geometry']].dissolve()
    except:
        aoi = None
    downloader = PlanetDownloader()
    if config['doGetGrid']:
        quads_gdf, quads_url = downloader.get_basemap_grid (
            PLANET_API_KEY, list_quad_URL,geom_path, dates = dates, aoi = aoi,bbox = bbox
        )
    if config['doDownload']:
        if quads_url is not None:
            quads_url = f"{quads_url}/<id>/full?api_key={PLANET_API_KEY}"
        downloader.download_tiles(
            PLANET_API_KEY, quad_dir, quad_name, quads_gdf = quads_gdf, 
            download_url = quads_url, list_quad_URL = list_quad_URL, dates = dates, bbox = bbox
        )
    if config['doRetile']:
        errors = downloader.retiler(
            tile_dir, quad_dir, temp_dir, tilefile_path, dates, 
            dst_width, dst_height, nbands, dst_crs, tile_name, quads_gdf
        )
        print(f"errors: {errors}")

if __name__ =='__main__':
    main('config/config.yml')
