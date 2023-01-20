from imageutil import *
import yaml


def main(config_path):
    with open(config_path, "r") as config:
        config = yaml.safe_load(config)
    
    geom_path = config['downloader']['geom']
    PLANET_API_KEY = config['downloader']['key']
    list_quad_URL = config['downloader']['list_quad_url']
    dates = config['downloader']['dates']
    bbox = config['downloader']['bbox']
    quads_path = config['downloader']['quads_path']

    aoi = gpd.read_file(config['downloader']['geom'])[['geometry']].dissolve()
    downloader = PlanetDownloader()
    quads_gdf, quads_url = downloader.get_basemap_grid (
                                            geom_path = geom_path, 
                                            PLANET_API_KEY = PLANET_API_KEY, 
                                            API_URL = list_quad_URL,
                                            dates = dates,
                                            aoi = aoi,
                                            bbox = bbox
                                        )
    downloader.download_tiles(
        quads_path, PLANET_API_KEY, quads_gdf = quads_gdf, 
        download_url = quads_url, list_quad_URL = list_quad_URL, dates = dates, bbox = bbox
    )

if __name__ =='__main__':
    main('config/config.yml')