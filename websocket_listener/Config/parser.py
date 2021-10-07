import asyncio
import copy
import json
import threading

import clickhouse_driver

from datetime import datetime
from decimal import Decimal
from binance import AsyncClient, BinanceSocketManager
from bottle import run, post, request
from pytz import timezone


class BinanceParser:

    def __init__(self, clickhouse_client: clickhouse_driver.Client, actual_crypto_pairs: list = None):
        default_crypto_pairs = ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'DOTUSDT',
                                'SOLUSDT', 'UNIUSDT', 'LINKUSDT', 'BCHUSDT', 'LTCUSDT', 'LUNAUSDT', 'MATICUSDT',
                                'ICPUSDT', 'WBTCUSDT', 'XLMUSDT', 'ETCUSDT', 'VETUSDT', 'AVAXUSDT', 'FILUSDT',
                                'THETAUSDT', 'TRXUSDT', 'XMRUSDT', 'CAKEUSDT', 'AAVEUSDT', 'EOSUSDT', 'GRTUSDT',
                                'FTTUSDT', 'ATOMUSDT', 'AXSUSDT', 'KLAYUSDT', 'NEOUSDT', 'CROUSDT', 'ALGOUSDT',
                                'BTCBUSDT', 'MKRUSDT', 'XTZUSDT', 'SHIBUSDT', 'MIOTAUSDT', 'BSVUSDT', 'BTTUSDT',
                                'EGLDUSDT', 'LEOUSDT', 'AMPUSDT', 'DASHUSDT', 'KSMUSDT', 'WAVESUSDT',
                                'RUNEUSDT', 'COMPUSDT', 'HNTUSDT', 'HTUSDT', 'NEARUSDT', 'HBARUSDT', 'CHZUSDT',
                                'DCRUSDT', 'QNTUSDT', 'XDCUSDT', 'ZECUSDT', 'HOTUSDT', 'TFUELUSDT', 'STXUSDT',
                                'ENJUSDT', 'SUSHIUSDT', 'MANAUSDT', 'SNXUSDT', 'TELUSDT', 'YFIUSDT', 'CELUSDT',
                                'FTMUSDT', 'RVNUSDT', 'FLOWUSDT', 'QTUMUSDT', 'ZILUSDT', 'RARIUSDT', 'DOCKUSDT']

        self.actual_crypto_pairs = self._crypto_pairs_validate(actual_crypto_pairs, default_crypto_pairs)
        self.agg_trades = []
        self.wait_updating = False
        self.clickhouse_client = clickhouse_client
        self.reload = False

        @post('/update_coins')
        def update_coins_route():
            """Обновляет список валютных пар пост запросом"""
            self.update_coins(json.loads(request.forms.get('coins')))
            self.reload = True

        asyncio.get_event_loop().run_until_complete(self.__async__main())

    @staticmethod
    def _crypto_pairs_validate(new_crypto_pairs: list, old_crypto_pairs: list, use_valid_values: bool = False) -> list:
        """Возвращает new_crypto_pairs с элементами в верхнем регистре, если массив валиден,
        в противном случае, если установлен, use_valid_values = True вернет массив только
        с валидными криптовалютными парами, иначе вернет old_crypto_pairs"""

        if not new_crypto_pairs or type(new_crypto_pairs) != list:
            return old_crypto_pairs
        elif not all(type(pair) == str for pair in new_crypto_pairs):
            if use_valid_values:
                return [pair.upper() for pair in new_crypto_pairs if type(pair) == str]
            return old_crypto_pairs
        return list(map(lambda x: x.upper(), new_crypto_pairs))

    def update_coins(self, new_crypto_pairs: list, use_valid_values: bool = False) -> None:
        """Обновляет actual_crypto_pairs экземпляра класса результатом вызова _crypto_pairs_validate"""

        prev_crypto_pairs = self.actual_crypto_pairs
        self.actual_crypto_pairs = self._crypto_pairs_validate(new_crypto_pairs,
                                                               self.actual_crypto_pairs,
                                                               use_valid_values)
        if prev_crypto_pairs == self.actual_crypto_pairs:
            self.wait_updating = True


    def __handle_socket_message(self, trades: json):
        """Парсит информацию о сделках"""
        agg_trades = self.agg_trades
        trades_data = trades['data']
        # print(trades_data)
        agg_trades.append({
            'coin_pair_name': trades_data['s'],
            'take_time': datetime. fromtimestamp(trades_data['T'] / 1000, tz=timezone('Europe/Moscow')),
            'volume': Decimal(str(trades_data['q'])),
            'price': Decimal(str(trades_data['p']))
        })

    async def __async__main(self):
        """Запускает цикл получения сделок на binance"""
        print('run main')
        loop = asyncio.get_event_loop()
        reset_event = asyncio.Event()
        loop.create_task(self.__async__monitor(reset_event))
        loop.create_task(self.__async__wright_trades())
        # loop.create_task(self.__async__run_server())
        while True:
            workers = []
            workers.append(loop.create_task(self.__async__take_streams()))
            # workers.append(loop.create_task(self.__async__run_server()))
            await reset_event.wait()
            reset_event.clear()
            for t in workers:
                t.cancel()

    # @staticmethod
    # async def __async__run_server():
    #     """Запускает HTTP сервер"""
    #     print('run server')
    #     run(host='localhost', port=8080, debug=True)

    async def __async__monitor(self,reset_event):
        """Отслеживает прекращение стрима с binance"""
        await asyncio.sleep(5)
        while True:
            first_check = self.agg_trades
            await asyncio.sleep(2)
            second_check = self.agg_trades
            if self.reload or (not (first_check+second_check)):
                self.reload = False
                print('reset!')
                await self.binance_client.close_connection()
                reset_event.set()
            await asyncio.sleep(1)

    async def __async__wright_trades(self):
        """Записывает информацию о сделках в базу данных"""
        while True:
            await asyncio.sleep(60)
            using_agg_trades = copy.deepcopy(self.agg_trades)
            self.agg_trades = []

            self.clickhouse_client.execute(
                f'INSERT INTO trades VALUES',
                using_agg_trades
            )

    async def __async__take_streams(self):
        """Подписывается на websocket стримы сделок по валютным парам"""
        print('run stream')
        self.binance_client = await AsyncClient.create()
        socket_manager = BinanceSocketManager(self.binance_client)

        while True:
            streams = []
            for currency in self.actual_crypto_pairs:
                currency = currency.lower()
                streams.append(f'{currency}@aggTrade')
            ts = socket_manager.multiplex_socket(streams=streams)
            async with ts as tscm:
                while True:
                    res = await tscm.recv()
                    if res.get('e') == 'error':
                        break
                    elif self.wait_updating:
                        self.wait_updating = False
                        break
                    else:
                        self.__handle_socket_message(res)
            await self.binance_client.close_connection()
