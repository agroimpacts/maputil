from imageutil import *
import yaml

def main(config_path):
    with open(config_path, "r") as config:
        config = yaml.safe_load(config)

    planet_basemap_downloader(config['downloader']['key'], config['downloader']['geom'], 
                              config['downloader']['dates'], config['downloader']['quads_path'])

if __name__ =='__main__':
    main('config/config.yml')