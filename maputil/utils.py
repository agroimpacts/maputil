import urllib.parse as urlparse
import logging
import pandas as pd
from smart_open import smart_open
from datetime import datetime
import joblib
from filelock import FileLock

def reads3csv_with_credential(old_url, aws_key, aws_secret):
    parsed = urlparse.urlparse(old_url)
    new_url = urlparse.urlunparse(
        parsed._replace(
            netloc="{}:{}@{}".format(aws_key, aws_secret, parsed.netloc)
        )
    )
    df = pd.read_csv(smart_open(new_url))
    return df

def progress_reporter(msg, verbose, log, logger=None):
    """Helps control print statements and log writes

    Parameters
    ----------
    msg : str
        Message to write out
    verbose : bool
        Prints or not to console
    log : bool
        Whether to write to logs or not (requires logger to exist)
    logger : logging.logger
        logger (defaults to none)
      
    Returns:
    --------  
        Message to console and or log
    """
    
    if verbose:
        print(msg)

    if log and logger:
        logger.info(msg)

# def setup_logger(log_dir, log_name, num_cores=1, use_date=False):
#     """Create root logger, adapting from here: 
#     https://github.com/joblib/joblib/issues/1017#issuecomment-1535983689

#     Parameters
#     ----------
#     log_dir : str
#         Path to write log to
#     log_name : str
#         What to name the name
#     num_cores : int
#         Number of cores, to determine whether parallel logging is set up
#     use_date : bool
#         Use today's date and time in file name
      
#     Returns:
#     --------  
#         Message to console and or log
#     """
#     if use_date:
#         dt = datetime.now().strftime("%d%m%Y_%H%M")
#         log = "{}/{}_{}.log".format(log_dir, log_name, dt)
#     else: 
#         log = "{}/{}.log".format(log_dir, log_name)

#     logger = logging.getLogger()
#     console_handler = logging.StreamHandler()    
#     logging.basicConfig(filename=log, filemode='w')

#     log_format = (
#         f"%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s"
#     )
#     formatter = logging.Formatter()
#     console_handler.setFormatter(formatter)
#     logger.addHandler(console_handler)
#     logger.setLevel(logging.INFO)

#     return logger

# def configure_worker_logger(log_queue, log_level):
#     """Create worker logger for parallel jobs, following:
#     https://github.com/joblib/joblib/issues/1017#issuecomment-1535983689

#     """
#     worker_logger = logging.getLogger('worker')
#     if not worker_logger.hasHandlers():
#         h = QueueHandler(log_queue)
#         worker_logger.addHandler(h)
#     worker_logger.setLevel(log_level)

#     return worker_logger

def setup_logger(log_dir, log_name, use_date=False):
    """Create logger

    Parameters
    ----------
    log_dir : str
        Path to write log to
    log_name : str
        What to name the name
    use_date : bool
        Use today's date and time in file name
      
    Returns:
    --------  
        Message to console and or log
    """
    if use_date:
        dt = datetime.now().strftime("%d%m%Y_%H%M")
        log = "{}/{}_{}.log".format(log_dir, log_name, dt)
    else: 
        log = "{}/{}.log".format(log_dir, log_name)
        
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_format = (
        f"%(asctime)s::%(levelname)s::%(name)s::%(filename)s::"
        f"%(lineno)d::%(message)s"
    )
    logging.basicConfig(filename=log, filemode='w',
                        level=logging.INFO, format=log_format)
    
    return logging.getLogger()

# def setup_logger(log_dir, log_name, num_cores=1, use_date=False):
#     """Create logger

#     Parameters
#     ----------
#     log_dir : str
#         Path to write log to
#     log_name : str
#         Name of the log file
#     num_cores : int
#         Number of cores, to determine whether parallel logging is set up
#     use_date : bool
#         Use today's date and time in file name

#     Returns
#     -------
#     logger : logging.Logger
#         Logger object for logging messages
#     """

#     # Create log file name
#     if use_date:
#         dt = datetime.now().strftime("%d%m%Y_%H%M")
#         log = "{}/{}_{}.log".format(log_dir, log_name, dt)
#     else:
#         log = "{}/{}.log".format(log_dir, log_name)

#     # Configure log format
#     log_format = (
#         "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::"
#         "%(lineno)d::%(message)s"
#     )

#     # Set up a file lock for the log file
#     lock_file = log + ".lock"
#     file_lock = FileLock(lock_file)

#     # Clear existing log handlers
#     for handler in logging.root.handlers[:]:
#         logging.root.removeHandler(handler)

#     # Configure log handler
#     file_handler = logging.FileHandler(log, mode='w')
#     file_handler.setLevel(logging.INFO)
#     file_handler.setFormatter(logging.Formatter(log_format))

#     # Apply file lock to log file handler
#     file_handler.addFilter(
#         lambda record: file_lock.acquire() or file_lock.release()
#     )

#     # Add log handler to logger
#     logger = logging.getLogger()
#     logger.setLevel(logging.INFO)
#     logger.addHandler(file_handler)

#     # Configure logger based on number of cores
#     if num_cores > 2:
#         joblib.logger = logger

#     return logger