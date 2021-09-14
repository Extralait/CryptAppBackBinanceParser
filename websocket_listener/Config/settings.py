import os
import dotenv

from pathlib import Path
from logging.config import dictConfig

BASE_DIR = Path(__file__).resolve().parent

dotenv_file = os.path.join(BASE_DIR, "../.env.binance")

if os.path.isfile(dotenv_file):
    dotenv.load_dotenv(dotenv_file)

dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(levelname)-8s %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        },
    }
})

CLICKHOUSE_TRAD_TABLE_NAME = os.getenv('CLICKHOUSE_TRAD_TABLE_NAME')

SERVER_HOST = os.getenv('SERVER_HOST')

CLICKHOUSE_CLIENT_SETTINGS = {
    'host': os.getenv('CLICKHOUSE_HOST'),
    'database': os.getenv('CLICKHOUSE_DB'),
    'user': os.getenv('CLICKHOUSE_USER'),
    'password': os.getenv('CLICKHOUSE_PASSWORD'),
    'compression': True,
    'settings': {
        'send_logs_level': os.getenv('LOGS_LEVEL'),
    }
}
