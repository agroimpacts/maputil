# Import libraries
import os
import re
import requests
import urllib
from joblib import Parallel, delayed
from subprocess import run
import time
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from geopandas.tools import sjoin
from shapely.geometry import box
import affine
import tempfile
import rasterio
from rasterio.merge import merge
from rasterio import fill
from rasterio.plot import show
from rasterio.io import MemoryFile
from rasterio.warp import reproject, Resampling
from .utils import *


class PlanetDownloader():
    def __init__(self) -> None:
        pass

    def get_basemap_grid(self, PLANET_API_KEY, API_URL, catalog_path=None, 
                         dates=None, aoi=None, bbox=None, _page_size=250):
        """
        Create a catalog of quads
        
        Parameters:
        ----------
        PLANET_API_KEY: str
            PlanetScope API key 
        API_URL: str
            The URL for HTTP GET request to list quads
        catalog_path: str
            File path to quad catalog
            If None, will not output a catalog file
        dates: list
            List of dates in string format
            Should be in format 'yyyy-dd' or 'yyyy-dd_yyyy-dd' for a time range
            Should match the date of downloaded quads
        aoi: geopandas
            Area of interest
        bbox: list
            Coordinates of the area to be queried
            Should be in format [xmin, ymin, xmax, ymax]
        _page_size: int
            Number of results to return per page
        
        Returns
        -------
        quads_gdf: geopandas
            Quad catalog
        quads_url: str
            URL to download quads
        """
        if catalog_path is None or not os.path.exists(catalog_path):
            print(f"{catalog_path} does not exist. Creating the catalog...")

            ids = []
            dts = []
            names = []
            geometries = []

            if PLANET_API_KEY is None or  API_URL is None:
                raise ValueError('Provide Planet API key and API URL to query \
                                 mosaics')

            if bbox is None:
                if aoi is None:
                    raise ValueError('Provide at least an aoi or a bbox to \
                                     query mosaics')
                bbox = aoi.total_bounds

            for date in dates:
                quads, mosaic_name, quads_url = list_quads(
                    PLANET_API_KEY, API_URL, date, bbox, _page_size
                )
                for quad in quads['items']:
                    ids.append(quad['id'])
                    geometries.append(box(quad['bbox'][0], quad['bbox'][1], 
                                          quad['bbox'][2], quad['bbox'][3]))
                    dts.append(date)
                    names.append(f"{mosaic_name}_{quad['id']}")

                # Create the catalog
                quads_gdf = gpd.GeoDataFrame(
                    {'tile': ids, "date": dts, 'geometry': geometries, 
                     'file':names}, crs="EPSG:4326"
                )
                if aoi is not None:
                    quads_gdf = gpd.overlay(aoi, quads_gdf)
                    quads_gdf = gpd.sjoin(left_df=quads_gdf, right_df=aoi)\
                        .drop(columns=['index_right'])
                if catalog_path is not None:
                    quads_gdf.to_file(catalog_path, driver='GeoJSON')
                    print(f"{catalog_path} created")
        else:
            print(f"Read {catalog_path}")
            quads_gdf = gpd.read_file(catalog_path)
            quads_url = None
        return quads_gdf, quads_url

    
    def download_tiles(
            self, PLANET_API_KEY, quad_dir, quad_name, quads_gdf=None, 
            catalog_path=None, download_url=None, list_quad_URL=None,  
            dates=None, bbox=None
        ):
        """
        Download basemaps from PlanetScope to local server
        
        Parameters:
        ----------
        PLANET_API_KEY: str
            PlanetScope API key
        quads_dir: str
            File path to quad direcroty
        quad_name: str
            Pattern of quad file path
        quads_gdf: geopandas
            a geopandas with quads ids and geometry
        catalog_path: str
            File path to quad catalog
        download_url: str
            URL to request and download quads
        list_quad_url: str
            URL to list quads from PlanetScope
        dates: list
            List of dates in string format
            Should be in format 'yyyy-dd' or 'yyyy-dd_yyyy-dd' for a time range
            Should match the date of downloaded quads
        bbox: list
            Coordinates of the area to be queried
            Should be in format [xmin, ymin, xmax, ymax]
        
        Returns
        -------
        """
        if download_url is not None:
            if quads_gdf is not None:
                pass
            elif catalog_path is not None:
                quads_gdf = gpd.read_file(catalog_path)

            for i, row in quads_gdf.iterrows():
                print(i)
                link = get_quad_download_url(download_url, row['tile'])
                filename = get_quad_path(quad_name, quad_dir, row['file'])#, 
                                        #  row['tile'])
                download_tiles_helper(link, filename)
            return

        else:
            if list_quad_URL is None:
                raise ValueError('Must supply URL to query quads')
            if dates is None:
                raise ValueError('Must supply dates to query quads')
            for date in dates:
                quads, mosaic_name, _ = list_quads(PLANET_API_KEY, 
                                                   list_quad_URL, date, bbox)
                for idx, i in enumerate(quads['items']):
                    print(idx)
                    if quads_gdf is not None:
                        if i['id'] not in list(quads_gdf['tile']):
                            continue
                    link = i['_links']['download']
                    filename = get_quad_path(quad_name, quad_dir, mosaic_name)#, 
                                             #i['id'])
                    download_tiles_helper(link, filename)
                return

    
    def retiler(
        self, tile_dir, quad_dir, temp_dir, tile_file, dates, dst_width, 
        dst_height, nbands, dst_crs, dst_img_pt, num_cores=1, verbose=True, 
        log=True, logger=None, quads_gdf=None, catalog_path=None
    ):
        """
        retile quads from quad_dir into smaller tiles and write tiles to 
        tile_dir
        
        Parameters:
        ----------
        tile_dir : str 
            Directory to store tiles
        quad_dir: str
            Directory storing quads
        temp_dir: str
            Directory to create temporary files
        tile_file: str or GeoDataFrame
            File path to the tile catalog or geopandas GeoDataFrame of tiles
        dates: list
            List of dates in string format
            Should be in format 'yyyy-dd' or 'yyyy-dd_yyyy-dd' for a time range
            Should match the date of downloaded quads
        dst_width : int 
            The pixel width of the output image
        dst_height : int
            The pixel height of the output image
        nbands : int
            Number of bands in input images
        dst_crs : str
            Code for output CRS, e.g "EPSG:4326"
        dst_img_pt : str
            Output file path and name pattern for output geotiff
        num_cores : int
            Number of cores used for processing. Defaults to 1 for serial mode
        verbose : bool
            Print messages to console or not
        log : bool
            Write messages to logger or not
        logger : logging.logger
            Logger object
        quads_gdf : geopandas
            geopandas of quads
        catalog_path : str
            File path to the quad catalog

        Returns
        -------
        errors: list
            A list of error
        """

        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)

        if type(tile_file) is str:
            tile_polys = gpd.read_file(tile_file).astype({"tile": "str"})
        else: 
            tile_polys = tile_file.astype({"tile": "str"})
            
        if quads_gdf is None:
            if catalog_path:
                quads_gdf = gpd.read_file(catalog_path)
            else:
                raise ValueError("Provide nicfi gdf or path to nicfi geojson")
        if 'file' not in quads_gdf.columns:
            try:
                quads_gdf['file'] = quads_gdf.apply(lambda
                    x: f"planet_medres_normalized_analytic_{x['date']}_mosaic_\
                        {x['tile']}.tif", 
                    axis=1
                )
            except KeyError:
                raise KeyError("Make sure the quads_gdf has 'tile' and\
                               date 'columns'")
        
        errors = []
        for date in dates:
            progress_reporter(f"Processing for date: {date}", verbose, log, 
                              logger)
            nicfi_tile_polys = quads_gdf[quads_gdf['date'] == date]
            tile_polys_prj = tile_polys.to_crs(nicfi_tile_polys.crs)

            # function to enable parallel processing
            def process_tile(i):
                tile = tile_polys_prj.iloc[[int(i)]]
                tiles_int = sjoin(tile, nicfi_tile_polys, how='left')

                # Name output file paths
                tile_id = int(float(tile['tile'].values.flatten()[0]))
                tile_id_str = f"{tile_id}"
                dst_img = re.sub('<tile_dir>', tile_dir, dst_img_pt)
                dst_img = re.sub('<tile_id>', tile_id_str, dst_img)
                dst_img = re.sub('<date>', date, dst_img)
                dst_cog = re.sub('.tif', '_cog.tif', dst_img)

                # Check if files already exist
                if os.path.exists(dst_img) and os.path.exists(dst_cog):
                    os.remove(dst_img)
                    return

                if os.path.exists(dst_cog):
                    progress_reporter(f"...{tile_id} exists, skipped", verbose, 
                                      log, logger)
                    return

                nicfi_int = nicfi_tile_polys[
                    nicfi_tile_polys['file'].isin(tiles_int['file'])
                ]
                if len(nicfi_int['file']) > 1:
                    image_list = [f'{quad_dir}/{file}' 
                                  for file in nicfi_int['file']]
                elif len(nicfi_int['file']) == 1: 
                    image_list = f"{quad_dir}/{nicfi_int['file'].values[0]}"
                else:
                    progress_reporter(f"{i}, empty nicfi_int['file']", verbose,
                                      log, logger)
                    errors.append((tile_id, 'empty'))
                    return

                dst_cog = re.sub('.tif', '_cog.tif', dst_img)
                poly = tile_polys[tile_polys['tile'].isin(tile['tile'])]
                transform = dst_transform(poly)

                # Retile
                progress_reporter(f"Processing tile {dst_img}", 
                                  verbose, log, logger)
                try:
                    reproject_retile_image(
                        image_list, transform, dst_width, dst_height, nbands, 
                        dst_crs, dst_img, temp_dir, inmemory=False, 
                        verbose=verbose, log=log, logger=logger
                    )
                except Exception as e:
                    progress_reporter(repr(e), verbose, log, logger)
                    errors.append(repr(e))

                # cogification
                cmd = ['rio', 'cogeo', 'create', '-b', '1,2,3,4', dst_img, 
                       dst_cog]
                p = run(cmd, capture_output=True)
                msg = p.stderr.decode().split('\n')
                # print(f'...{msg[-2]}')
                progress_reporter(f'...{msg[-2]}', verbose, log, logger)

                cmd = ['rio', 'cogeo', 'validate', dst_cog]
                p = run(cmd, capture_output = True)
                msg = p.stdout.decode().split('\n')
                progress_reporter(f'...{msg[0]}', verbose, log, logger)

                if os.path.exists(f"{dst_cog}"):
                    if os.path.exists(f"{dst_img}"):
                        os.remove(dst_img)

            # Parallelize the loop using joblib
            if num_cores > 1:
                progress_reporter(f'Processing job with {num_cores} cores', 
                                  verbose, log, logger)
                results = Parallel(n_jobs=num_cores)(
                    delayed(process_tile)(i) for i in range(len(tile_polys_prj))
                )
            else:
                progress_reporter("Processing serial", verbose, log, logger)
                results = [process_tile(i) for i in range(len(tile_polys_prj))]

            progress_reporter(f"Completed processing tiles for {date}", 
                              verbose, log, logger)   
             
        progress_reporter("All processed", verbose, log, logger)   
        
        return errors


def get_quad_download_url(url_pt, id):
    """
    Replace placeholder with values to get actuall url

    Parameters:
    ----------
    url_pt: str
        URL pattern
    id: str
        Quad id
    
    Returns
    -------
    Actuall url
    """
    return re.sub('<id>', id, url_pt)


# def get_quad_path(quad_name_pt, quad_dir, qname, id):
def get_quad_path(quad_name_pt, quad_dir, qname):
    """
    Replace placeholder with values to get actual file path

    Parameters:
    ----------
    quad_name_pt: str
        Pattern of quad file path
    quad_dir: str
        Quad directory
    qname: str
        quad name by PlanetScope

    Returns
    -------
    filename: str
        Actual file path
    """
    filename = re.sub('<quad_dir>', quad_dir, quad_name_pt)
    filename = re.sub('<qname>', qname, filename)
    # filename = re.sub('<id>', id, filename)
    return filename


def download_tiles_helper(url, filename):
    """
    A helper function to download file to local server
    
    Parameters:
    ----------
    url : str 
        The url to download file
    filename: str
        File path to where the file will be downloaded to    
    
    Returns
    -------
    """
    if not os.path.isfile(filename):
        urllib.request.urlretrieve(url, filename)
        print(f"Downloaded: {filename}")
    else:
        print(f"File already exists: {filename}")


def list_quads(PLANET_API_KEY, API_URL, date, bbox=None, _page_size=250):
    """
    Helper function: actual function to query quads from the Planet API
    
    Parameters:
    ----------
    PLANET_API_KEY : str 
        The API key from PlanetScope credential
    API_URL: str
        The URL for HTTP GET request to list quads
    date: str
        The date to query quads
        Should be in format 'yyyy-dd' or 'yyyy-dd_yyyy-dd' for a time range
    bbox: list
        Coordinates of the area to be queried
        Should be in format [xmin, ymin, xmax, ymax]
    
    Returns
    -------
    quads: dict
        A JSON response 
    mosaic_name: str
        The name of queried quads
    quads_url: str
        URL pattern to download quads
    """
    session = setup_session(PLANET_API_KEY)
    res = session.get(API_URL, params = {"name__contains" : date})
    mosaic = res.json()
    try:
        mosaic_id = mosaic['mosaics'][0]['id']
    except Exception as e:
        print(mosaic['message'])
        raise e
    mosaic_name =  mosaic['mosaics'][0]['name']
    if bbox is None:
        mosaic_bbox = mosaic['mosaics'][0]['bbox']
        bbox_str = ','.join(map(str, mosaic_bbox))
    else:
        bbox_str = ','.join(map(str, bbox))
    # List mosaics
    quads_url = f"{API_URL}/{mosaic_id}/quads"
    params = {'bbox': bbox_str,'minimal': True, '_page_size': _page_size}
    res = session.get(quads_url, params=params, stream=True)
    quads = res.json()
    return quads, mosaic_name, quads_url


def setup_session(API_KEY):
    """
    Set up a session to later query Planet API
    
    Parameters:
    ----------
    API_KEY : str 
        The API key from PlanetScope credential
    
    
    Returns
    -------
    A request session
    """
    session = requests.Session()
    session.auth = (API_KEY, "")
    return session

def get_tempfile_name(temp_dir, file_name='mosaic.tif'):
    """
    Create a temporary filename in the tmp directory
    
    Parameters:
    ----------
    temp_dir : str 
        File path of the tmp directory
    
    
    Returns
    -------
    The full file path of the temporary file
    """
    file_path = os.path.join(
        temp_dir, 
        next(tempfile._get_candidate_names()) + "_" + file_name
    )    
    return file_path

def dst_transform(poly, res = 0.005 / 200):
    """
    Create transform from boundaries of tiles
    
    Parameters
    ----------
    poly : GeoDataFrame
        Polygon containing dimensions of interest
    res : float
        Resolution desired for output transform
    
    Returns
    -------
    An Affine transform

    """
    bounds = poly['geometry'].bounds.values.flatten()
    minx = bounds[0]
    maxy = bounds[3]
    transform = affine.Affine(res, 0, minx, 0, -res, maxy)
    return(transform)


def reproject_retile_image(
        src_images, dst_transform, dst_width, dst_height, nbands, dst_crs,
        fileout, temp_dir, dst_dtype=np.int16, inmemory=True, cleanup=True, 
        verbose=True, log=False, logger=None
    ):
    """Takes an input images or list of images and merges (if several) and 
    reprojects and retiles it to align to the resolution and extent defined by
    an polygon and associated transform
    
    
    Parameters:
    ----------
    src_images : list 
        File path or list of file paths to input image(s). A list of images
        will be merged first.
    dst_transform : affine
        affine transformation object defining projection of output image
    dst_width : int 
        The pixel width of the output image
    dst_height : int
        The pixel height of the output image
    nbands : int
        Number of bands in input images
    dst_crs : str
        Code for output CRS, e.g "EPSG:4326"
    file_out : str
        Output file path and name for output geotiff
    dst_dtype : numpy data type
        (default is int16)
    inmemory : bool
        If a mosaic should be made in memory or not. Default is True. 
        If set to False then a mosaic with the mosaic will be 
        written to disk in a directory called ~/tmp and then removed
        upon completion
    cleanup : bool
        Whether to remove temporary mosaic (if made) or not
    verbose : bool
        Print messages to console or not
    log : bool
        Write messages to logger or not
    logger : logging.logger
        Logger object
    
    Returns
    -------
    geotiff of retiled image writen to disk 
    """
    
    def reproject_retile(src, nbands, dst_height, dst_width, fileout, temp_dir, 
                         dst_dtype, ): 
        src_kwargs = src.meta.copy()  # get metadata
        kwargs = src_kwargs
        kwargs.update({
            "width": dst_width,
            "height": dst_height,
            "count": nbands,
            "crs": dst_crs,
            "transform": dst_transform,
        })
        dst_canvas = np.zeros((nbands, dst_height, dst_width))
        for i in range(1, nbands + 1):
            dst_canvas[i-1,] = reproject(
                source = rasterio.band(src, i),
                destination = dst_canvas[i-1,],
                src_transform = src.transform,
                src_crs = src.crs,
                dst_transform = dst_transform,
                dst_crs = dst_crs,
                resampling = Resampling.cubic
            )[0]
        with rasterio.open(fileout, "w", **kwargs) as dst:
            dst.write(np.rint(dst_canvas).astype(dst_dtype))
            
    # mosaic if list
    if type(src_images) is list:
        progress_reporter(f"..mosaicking {len(src_images)} images", 
                          verbose, log, logger)
        
        images_to_mosaic = []
        for idx, image in enumerate(src_images):
            try:
              src = rasterio.open(image)
              images_to_mosaic.append(src)
            except:
              progress_reporter(f'..file not found: {image}', verbose, log, 
                                logger)
              continue
              # raise Exception('RasterioIOError: File not found')

        # perform mosaic
        mosaic, out_trans = merge(images_to_mosaic)

        out_meta = src.meta.copy()
        out_meta.update({
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
        })
        
        if inmemory:
            progress_reporter('....mosaicking in memory', verbose, log, 
                              logger)
            with MemoryFile() as memfile:
                with memfile.open(**out_meta) as dst:
                    dst.write(mosaic)

                msg = f"..reprojecting, retiling {os.path.basename(fileout)}"
                progress_reporter(msg, verbose, log, logger)
                reproject_retile(src, nbands, dst_height, dst_width, fileout, 
                                 temp_dir, dst_dtype)
        else: 
            temp_mosaic = get_tempfile_name(temp_dir, 'mosaic.tif')
            msg = f"....creating temporary mosaick {temp_mosaic}"
            progress_reporter(msg, verbose, log, logger)
            with rasterio.open(temp_mosaic, "w", **out_meta) as dst:
                  dst.write(mosaic)
            
            msg = f"..reprojecting, retiling {os.path.basename(fileout)}"
            progress_reporter(msg, verbose, log, logger)
            with rasterio.open(temp_mosaic, "r") as src:
                reproject_retile(src, nbands, dst_height, dst_width, fileout, 
                                 temp_dir, dst_dtype) 
            
            if cleanup: 
                progress_reporter(f"....removing temporary mosaick {fileout}", 
                                  verbose, log, logger)
                os.remove(temp_mosaic)
            
    else: 
        progress_reporter("..retiling from single image", verbose, log, logger)
        msg = f"..reprojecting, retiling {os.path.basename(fileout)}"
        progress_reporter(msg, verbose, log, logger)
        with rasterio.open(src_images, "r") as src:
            reproject_retile(src, nbands, dst_height, dst_width, fileout, 
                             temp_dir, dst_dtype) 
    
    msg = f"Retiling and reprojecting of {fileout} complete!"
    progress_reporter(msg, verbose, log, logger)


