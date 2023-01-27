from imageutil import *
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

    aoi = gpd.read_file(config['geom'])[['geometry']].dissolve()
    downloader = PlanetDownloader(geom_path = geom_path)
    quads_gdf, quads_url = downloader.get_basemap_grid (
                                            geom_path = geom_path, 
                                            PLANET_API_KEY = PLANET_API_KEY, 
                                            API_URL = list_quad_URL,
                                            dates = dates,
                                            aoi = aoi,
                                            bbox = bbox
                                        )
    if config['doDownload']:
        downloader.download_tiles(
            quad_dir, PLANET_API_KEY, quads_gdf = quads_gdf, 
            download_url = quads_url, list_quad_URL = list_quad_URL, dates = dates, bbox = bbox
        )
    if config['doRetile']:
        errors = downloader.retiler(tile_dir, quad_dir, temp_dir, tilefile_path, dates, quads_gdf)

if __name__ =='__main__':
    main('config/config.yml')