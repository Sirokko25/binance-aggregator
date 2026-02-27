from binance.client import Client
import asyncio
import signal
import time
from app.config import Config
import logging
from app.writer import EventWriter
from binance.exceptions import BinanceAPIException
from app.check_database import DatabaseChecker
from app.constants import (
    MAX_DATABASE_RETRIES,
    LOG_FORMAT,
    LOG_LEVEL,
)


class Collector:
    """
    High‑level orchestrator:
    pulls data from Binance futures API and pushes it into ClickHouse.
    """

    def __init__(self, conf: Config):
        self.conf = conf
        self.logger = logging.getLogger("Collector")
        self.logger.setLevel(getattr(logging, LOG_LEVEL))
        if not self.logger.handlers:
            logging.basicConfig(
                format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL), force=True
            )
        # Used to stop long‑running loops gracefully from signal handlers
        self.stop_event = asyncio.Event()
        self.event_writer = None
        self.clients = {}
        self.symbols = {}
        self._register_signal_handlers()

    def _init_clients(self):
        """
        Create one Binance futures client per account in the config.
        """
        self.clients = {}
        for cred in self.conf.credentials:
            self.clients[cred.name] = Client(
                api_key=cred.api_key, api_secret=cred.api_secret,
            )
        self.logger.info(f"Initialized {len(self.clients)} clients")

    def _init_database_checker(self) -> bool:
        """
        Make sure ClickHouse is reachable and target tables exist.
        Uses simple exponential backoff to handle temporary outages.
        """
        retry_count = 0
        while retry_count < MAX_DATABASE_RETRIES and not self.stop_event.is_set():
            try:
                with DatabaseChecker(self.conf) as db_checker:
                    if db_checker.check_database():
                        self.logger.info("Database checked and created successfully")
                        return True
                    else:
                        self.logger.error(
                            f"Database check failed (attempt {retry_count + 1}/{MAX_DATABASE_RETRIES})"
                        )
                        sleep_seconds = 1 if retry_count == 0 else pow(2, retry_count)
                        time.sleep(sleep_seconds)
                retry_count += 1
            except Exception as e:
                self.logger.error(
                    f"Database initialization error (attempt {retry_count + 1}/{MAX_DATABASE_RETRIES}): {e}"
                )
                sleep_seconds = 1 if retry_count == 0 else pow(2, retry_count)
                retry_count += 1
                time.sleep(sleep_seconds)

        self.logger.error("Exceeded maximum database initialization retries")
        return False

    def get_exchange_info(self):
        """
        Discover active futures symbols from Binance.
        Only symbols with status=TRADING are used for historical backfill.
        """
        while not self.stop_event.is_set():
            try:
                base_set_account_symbols = self.conf.credentials[0].name
                exchange_info = self.clients[
                    base_set_account_symbols
                ].futures_exchange_info()
                active_symbols = [
                    s["symbol"]
                    for s in exchange_info["symbols"]
                    if s["status"] == "TRADING"
                ]
                if active_symbols:
                    self.symbols = active_symbols
                    self.logger.info("Symbols successfully received")
                    return self.symbols
            except BinanceAPIException as e:
                self.logger.error(f"Binance API error: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Unknown error: {e}")
                raise

    async def get_futures_trade_history(self):
        """
        Backfill trades or orders for all configured accounts and symbols.
        Resumes from the last id stored in ClickHouse to avoid duplicates.
        """
        try:
            last_from_id = 0
            last_order_id = 0
            for cred in self.conf.credentials:
                for symbol in self.symbols:
                    try:
                        if self.conf.collection_config.flag == "trades":
                            last_from_id = await self.event_writer.get_last_from_id(
                                    symbol, cred.name
                                )
                            self.logger.info(
                                f"Fetching historical trades for {symbol} from {cred.name} from id: {last_from_id}"
                            )

                        elif self.conf.collection_config.flag == "orders":
                            last_order_id = await self.event_writer.get_last_order_id(
                                symbol, cred.name
                            )
                            self.logger.info(
                                f"Fetching historical orders for {symbol} from {cred.name} from orderId: {last_order_id}"
                            )

                        # Fetch and store data in 1000‑item pages until the API returns nothing
                        while not self.stop_event.is_set():
                            if self.conf.collection_config.flag == "trades":
                                data_batch = self.clients[cred.name].futures_account_trades(
                                    symbol=symbol, limit=1000, fromId=last_from_id
                                )
                            elif self.conf.collection_config.flag == "orders":
                                data_batch = self.clients[cred.name].futures_get_all_orders(
                                    symbol=symbol, limit=1000, orderId=last_order_id
                                )

                            if not data_batch:
                                self.logger.info(
                                    f"No more data {symbol}, for account {cred.name}"
                                )
                                break

                            sz = len(data_batch)
                            
                            if self.conf.collection_config.flag == "trades":
                                await self.event_writer.write_trades_batch(
                                    data_batch, symbol, cred.name
                                )
                            elif self.conf.collection_config.flag == "orders":
                                await self.event_writer.write_orders_batch(
                                    data_batch, symbol, cred.name
                                )

                            # Binance returns at most 1000 items per call.
                            # If we got a full page, there might be more data.
                            if sz >= 1000:
                                if self.conf.collection_config.flag == "trades":
                                    self.logger.info(
                                        f"Wrote batch for symbol {symbol} account {cred.name} size {sz}"
                                    )
                                    last_from_id = data_batch[-1]["id"] + 1

                                elif self.conf.collection_config.flag == "orders":
                                    self.logger.info(
                                        f"Wrote batch for symbol {symbol} account {cred.name} size {sz}"
                                    )
                                    last_order_id = data_batch[-1]["orderId"] + 1
                            else:
                                self.logger.info(
                                    f"Got batch for symbol {symbol} account {cred.name} size {sz}, exiting"
                                )
                                break

                            await asyncio.sleep(1)
                        if self.stop_event.is_set():
                            return
                    except BinanceAPIException as e:
                        self.logger.error(
                            f"Binance API error {symbol}: {e}, for account name: {cred.name}"
                        )
                        await asyncio.sleep(1)

                    except Exception as e:
                        self.logger.error(
                            f"Unexpected error processing {symbol}: {e}, for account name: {cred.name}"
                        )
                        await asyncio.sleep(1)
        except Exception as e:
            self.logger.error(f"Error processing symbol {symbol}: {e}")
            await asyncio.sleep(1)


    async def start_async(self):
        """
        Public entry point for the whole collection flow.
        """

        self.logger.warning("Starting collector")

        self._init_clients()
        if not self._init_database_checker():
            self.stop()
            self.logger.info("Collector stopped due to database initialization failure")
            return

        self.event_writer = EventWriter(self.conf)

        # Discover the list of tradable symbols once before backfill
        self.get_exchange_info()

        await self._async_cycle()
        if not self.stop_event.is_set():
            self.stop()
        self.logger.info("Collector stopped")

    async def _async_cycle(self):
        """
        Wrap the main long‑running tasks into asyncio primitives.
        If more tasks appear in the future, they should be added here.
        """
        tasks = []
        try:
            tasks.append(
                asyncio.create_task(
                    self.get_futures_trade_history(), name=f"Receiving trades"
                )
            )

            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            self.logger.error(f"Error collecting data: {e}")
            raise

        self.logger.info("Trades collection cycle stopped")

    def stop(self):
        """
        Cooperative shutdown hook used from signal handlers and error paths.
        """
        self.logger.info("Stopping collector...")
        if self.stop_event:
            self.stop_event.set()
        if hasattr(self, "event_writer") and self.event_writer:
            self.event_writer.close()

    def _register_signal_handlers(self):
        """
        Register basic OS signal handlers to allow Ctrl+C / SIGTERM shutdown.
        """
        try:
            signal.signal(signal.SIGINT, lambda *_: self.stop())
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, lambda *_: self.stop())
            self.logger.info("Registered signal handlers")
        except Exception as e:
            self.logger.warning(f"Failed to register signal handlers: {e}")
            raise RuntimeError(f"Failed to register signal handlers: {e}")
