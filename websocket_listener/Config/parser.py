import asyncio
import copy
import json
import traceback
import logging

import clickhouse_driver

from datetime import datetime
from decimal import Decimal
from pytz import timezone

from binance import AsyncClient, BinanceSocketManager


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

        asyncio.get_event_loop().run_until_complete(self.__async__main())

    @staticmethod
    def _crypto_pairs_validate(new_crypto_pairs: list, old_crypto_pairs: list, use_valid_values: bool = False) -> list:
        """???????????????????? new_crypto_pairs ?? ???????????????????? ?? ?????????????? ????????????????, ???????? ???????????? ??????????????,
        ?? ?????????????????? ????????????, ???????? ????????????????????, use_valid_values = True ???????????? ???????????? ????????????
        ?? ?????????????????? ?????????????????????????????? ????????????, ?????????? ???????????? old_crypto_pairs"""

        if not new_crypto_pairs or type(new_crypto_pairs) != list:
            return old_crypto_pairs
        elif not all(type(pair) == str for pair in new_crypto_pairs):
            if use_valid_values:
                return [pair.upper() for pair in new_crypto_pairs if type(pair) == str]
            return old_crypto_pairs
        return list(map(lambda x: x.upper(), new_crypto_pairs))

    def update_coins(self, new_crypto_pairs: list, use_valid_values: bool = False) -> None:
        """?????????????????? actual_crypto_pairs ???????????????????? ???????????? ?????????????????????? ???????????? _crypto_pairs_validate"""

        prev_crypto_pairs = self.actual_crypto_pairs
        self.actual_crypto_pairs = self._crypto_pairs_validate(new_crypto_pairs,
                                                               self.actual_crypto_pairs,
                                                               use_valid_values)
        if prev_crypto_pairs == self.actual_crypto_pairs:
            self.wait_updating = True

    def __handle_socket_message(self, trades: json):
        """???????????? ???????????????????? ?? ??????????????"""
        agg_trades = self.agg_trades
        trades_data = trades['data']
        agg_trades.append({
            'coin_pair_name': trades_data['s'],
            'take_time': datetime.fromtimestamp(trades_data['T'] / 1000, tz=timezone('Europe/Moscow')),
            'volume': Decimal(str(trades_data['q'])),
            'price': Decimal(str(trades_data['p']))
        })

    async def __async__main(self):
        """?????????????????? ???????? ?????????????????? ???????????? ???? binance"""
        while True:
            logging.info(f"[Main coroutine] - Start main coroutine")
            try:
                loop = asyncio.get_event_loop()
                reset_event = asyncio.Event()
                loop.create_task(self.__async__monitor(reset_event))
                loop.create_task(self.__async__write_trades())
                while True:
                    logging.info(f"[Main coroutine] - Adding all workers")
                    workers = []
                    workers.append(loop.create_task(self.__async__take_streams()))
                    await reset_event.wait()
                    logging.info(f"[Main coroutine] - Restarting all workers")
                    reset_event.clear()
                    for t in workers:
                        t.cancel()
            except Exception:
                logging.error(f"[Main coroutine] - Fatal error")
                logging.error(f"[Main coroutine] - {traceback.print_exc()}")


    async def __async__monitor(self, reset_event):
        """?????????????????????? ?????????????????????? ???????????? ?? binance"""
        await asyncio.sleep(5)
        logging.info(f"[Monitor coroutine] - Start monitor coroutine")
        while True:
            try:
                logging.info(f"[Monitor coroutine] - Check data in agg_trades {len(self.agg_trades)}")
                first_check = self.agg_trades
                await asyncio.sleep(2)
                second_check = self.agg_trades
                if self.reload or (not (first_check + second_check)):
                    logging.error(f"[Monitor coroutine] - No data in agg_trades, restarting")
                    self.reload = False
                    logging.info(f"[Monitor coroutine] - Close binance connection")
                    await self.binance_client.close_connection()
                    reset_event.set()
                await asyncio.sleep(1)
            except Exception:
                logging.error(f"[Monitor coroutine] - Fatal error")
                logging.error(f"[Monitor coroutine] - {traceback.print_exc()}")

    async def __async__write_trades(self):
        """???????????????????? ???????????????????? ?? ?????????????? ?? ???????? ????????????"""
        while True:
            logging.info(f"[Trade writer coroutine] - Start trade writer coroutine")
            try:
                await asyncio.sleep(60)
                using_agg_trades = copy.deepcopy(self.agg_trades)
                self.agg_trades = []
    
                self.clickhouse_client.execute(
                    f'INSERT INTO trades VALUES',
                    using_agg_trades
                )
            except Exception:
                logging.error(f"[Trade writer coroutine] - Fatal error")
                logging.error(f"[Trade writer coroutine] - {traceback.print_exc()}")

    async def __async__take_streams(self):
        """?????????????????????????? ???? websocket ???????????? ???????????? ???? ???????????????? ??????????"""
        logging.info(f"[Stream coroutine] - Start stream coroutine")
        logging.info(f"[Stream coroutine] - Connection to binance")
        self.binance_client = await AsyncClient.create()
        socket_manager = BinanceSocketManager(self.binance_client)

        while True:
            try:
                streams = []
                for currency in self.actual_crypto_pairs:
                    currency = currency.lower()
                    streams.append(f'{currency}@aggTrade')
                logging.info(f"[Stream coroutine] - Create streams")
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
                logging.info(f"[Stream coroutine] - Close binance connection")
                await self.binance_client.close_connection()
            except Exception:
                logging.error(f"[Stream coroutine] - Fatal error")
                logging.error(f"[Stream coroutine] - {traceback.print_exc()}")
