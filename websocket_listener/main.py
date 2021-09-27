import threading

from clickhouse_driver import Client

from Config.parser import BinanceParser
from Config.settings import *


def main():
    client = Client(**CLICKHOUSE_CLIENT_SETTINGS)
    client.execute(
        "CREATE TABLE IF NOT EXISTS trades "
        "("
            "coin_pair_name String, "
            "volume Decimal(20,7), "
            "price Decimal(20,7),"
            "take_time DateTime"
        ") Engine = Memory"
    )
    client.execute(
        f"CREATE TABLE IF NOT EXISTS indicators "
        "("
            "coin_pair_name String, "
            "indicator_name String, "
            "indicator_kline_type String, "
            "indicator_type String, "
            "values_data Array(Decimal(20,7)), "
            "take_time Array(DateTime)"
        ") Engine = Memory"
    )
    client.execute(
        f"CREATE TABLE IF NOT EXISTS klines "
        "("
            "coin_pair_name String, "
            "stock_exchange String, "
            "mean Decimal(20,7), "
            "median Decimal(20,7),"
            "volatility Decimal(20,7),"
            "slope Decimal(20,7),"
            "open_price Decimal(20,7),"
            "close_price Decimal(20,7),"
            "high Decimal(20,7),"
            "low Decimal(20,7),"
            "volume Decimal(20,7),"
            "kline_type String,"
            "take_time DateTime"
        ") Engine = Memory"
    )
    BinanceParser(client)

if __name__ == '__main__':
    main()
