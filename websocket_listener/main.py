import threading

from clickhouse_driver import Client

from Config.parser import BinanceParser
from Config.settings import *


def main():
    client = Client(**CLICKHOUSE_CLIENT_SETTINGS)
    client.execute(
        f"CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TRAD_TABLE_NAME} "
        "("
        "coin_pair_name String, "
        "take_time DateTime, "
        "volume Decimal(20,7), "
        "price Decimal(20,7)"
        ") Engine = Memory"
    )
    BinanceParser(client)

if __name__ == '__main__':
    main()
