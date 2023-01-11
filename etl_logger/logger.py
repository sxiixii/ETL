import logging.config
from datetime import datetime
from os import path

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logger.conf')
logging.config.fileConfig(log_file_path)
log = logging.getLogger('MainLogger')

file_handler = logging.FileHandler('{:%Y-%m-%d}.log'.format(datetime.now()))
formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(funcName)-40s | %(message)s')
file_handler.setFormatter(formatter)

log.addHandler(file_handler)
