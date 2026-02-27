import clickhouse_connect
import logging
from typing import Optional
from app.config import Config
from app.constants import (
    CREATE_TRADES_TABLE_QUERY,
    CREATE_ORDERS_TABLE_QUERY,
    TABLE_NAME_TRADES,
    TABLE_NAME_ORDERS,
    LOG_FORMAT,
    LOG_LEVEL,
)


class DatabaseChecker:
    """
    Small helper responsible for talking to ClickHouse during startup.
    Makes sure the connection works and the target tables exist.
    """

    def __init__(self, config: Config) -> None:

        self.config = config
        self._client: Optional[clickhouse_connect.driver.client.Client] = None
        self.logger = logging.getLogger("DatabaseChecker")
        self.logger.setLevel(getattr(logging, LOG_LEVEL))
        if not self.logger.handlers:
            logging.basicConfig(
                format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL), force=True
            )

    def _get_client(self) -> clickhouse_connect.driver.client.Client:
        """
        Lazily create a ClickHouse client using settings from the config.
        """

        if self._client is None:
            clickhouse_config = self.config.clickhouse[0]

            # Extra validation so we fail fast if config is broken
            if not all(
                [
                    clickhouse_config.ClickHouse_Host,
                    clickhouse_config.ClickHouse_Port,
                    clickhouse_config.ClickHouse_User,
                    clickhouse_config.ClickHouse_Password,
                ]
            ):
                raise ValueError("Incomplete ClickHouse configuration")

            self._client = clickhouse_connect.get_client(
                host=clickhouse_config.ClickHouse_Host,
                port=clickhouse_config.ClickHouse_Port,
                username=clickhouse_config.ClickHouse_User,
                password=clickhouse_config.ClickHouse_Password,
            )

        return self._client

    def close_connection(self) -> None:
        """
        Close the ClickHouse client if it has been created.
        """

        if self._client is not None:
            self._client.close()
            self._client = None

    def test_database_connection(self) -> bool:
        """
        Simple health‑check query to verify that ClickHouse is reachable.
        """

        try:

            client = self._get_client()
            result = client.query("SELECT 1")
            self.logger.info("Connection to ClickHouse has been successfully established")
            return True

        except Exception as e:

            self.logger.error(f"Error connecting to ClickHouse: {e}")
            return False

    def check_and_create_trades_table(self) -> bool:
        """
        Create target tables if they do not exist yet.
        Chooses between trades and orders based on the collection flag.
        """

        try:
            client = self._get_client()
            if self.config.collection_config.flag == "trades":
                client.command(CREATE_TRADES_TABLE_QUERY)
                self.logger.info(f"The {TABLE_NAME_TRADES} table has been successfully created")
            elif self.config.collection_config.flag == "orders":
                client.command(CREATE_ORDERS_TABLE_QUERY)
                self.logger.info(f"The {TABLE_NAME_ORDERS} table has been successfully created")
            return True
        except Exception as e:
            self.logger.error(f"Error when working with the trades table: {e}")
            return False

    def check_database(self) -> bool:
        """
        High‑level helper combining connection check and schema check.
        """
        try:
            if not self.test_database_connection():
                return False

            if not self.check_and_create_trades_table():
                return False

            return True

        finally:

            self.close_connection()

    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        self.close_connection()
