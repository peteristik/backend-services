import logging
from datetime import datetime
from pytz import timezone

logger = None

if not logger:
    logger = logging.getLogger('custom_logger')
    
    # create a streaming handler
    ch = logging.StreamHandler()
    # create a formatter and add add it to the handler
    ch_formatter = logging.Formatter("[%(asctime)s][%(filename)s][%(levelname)s]:%(message)s", "%Y-%m-%d %I:%M:%S %p SGT")
    ch_formatter.converter = lambda *args: datetime.now(tz=timezone('Asia/Singapore')).timetuple()
    ch.setFormatter(ch_formatter)
    # set logging levels
    logger.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)
    # add handler to the logger
    logger.addHandler(ch)
    # prevent double printing
    logger.propagate = False