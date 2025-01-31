import os
from datetime import date
from dotenv import load_dotenv
import logging.config

load_dotenv()

# postgres schema to ingest into
SCHEMA = "dima_prod"

# Dockerfile for the platform agnostic extractor
DOCKERFILE_DIR = "./_1_dima_extract"

# DB credentials, modify .env file
# PROD or DEV available
DATABASE_CONFIG = {
    "dbname": os.getenv('PROD_DBNAME'),
    "user": os.getenv('PROD_DBUSER'),
    "password": os.getenv('PROD_DBPASSWORD'),
    "host": os.getenv('PROD_DBHOST'),
    "port": os.getenv('PROD_DBPORT'),
}

# Variables for column additions / modifications
TODAYS_DATE = date.today().isoformat()

# CLI variables
DATA_DIR = "./_1_dima_extract/extracted"


# Configuration options for logs
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': './_2_dima_loadingest/logs/app.log',
            'formatter': 'detailed',
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
    },
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'simple': {
            'format': '%(levelname)s - %(message)s'
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'DEBUG',
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
