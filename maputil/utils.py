import urllib.parse as urlparse
import logging
import pandas as pd
from smart_open import smart_open

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