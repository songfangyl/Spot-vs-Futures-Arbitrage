# logger_config.py
import logging
import os
from datetime import datetime

def setup_logger(name):
    # Create a custom logger
    logger = logging.getLogger(name)

    # Set the default log level
    logger.setLevel(logging.DEBUG)  # Debug will capture everything, adjust as needed

    # Create handlers (console and file)
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(f'logs/{name}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log')
    os.makedirs('logs', exist_ok=True)  # Create logs directory if it doesn't exist

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Set levels for handlers
    c_handler.setLevel(logging.WARNING)  # Console handler for warning and above
    f_handler.setLevel(logging.DEBUG)  # File handler for debug and above

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return logger
