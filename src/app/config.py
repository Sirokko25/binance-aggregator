from typing import Dict
from dataclasses import dataclass
import logging

@dataclass
class Credentials:
    """
    Single Binance account used for collection.
    """
    api_key: str
    api_secret: str
    name: str


@dataclass
class ClickHouse:
    """
    Minimal ClickHouse connection settings.
    """
    ClickHouse_Host: str
    ClickHouse_Port: int
    ClickHouse_User: str
    ClickHouse_Password: str


@dataclass
class CollectionConfig:
    """
    Which kind of data we collect in this run: 'trades' or 'orders'.
    """
    flag: str


class Config:
    """
    Thin wrapper around raw config dict with some validation and helpers.
    """

    def __init__(self, conf: Dict[str, any], collection_type: str):
        self.conf = conf

        # Parse and validate credentials section
        credentials_data = conf.get("credentials_accounts", [])
        if not credentials_data:
            raise ValueError("Не найдены credentials в конфигурации")

        self.credentials = [
            Credentials(
                api_key=cred.get("api_key", ""),
                api_secret=cred.get("api_secret", ""),
                name=cred.get("name", ""),
            )
            for cred in credentials_data
        ]

        # Basic sanity check for each account
        for cred in self.credentials:
            if not all([cred.api_key, cred.api_secret, cred.name]):
                raise ValueError(f"Неполные данные для credentials: {cred.name}")

        # Map flat dict into a tiny dataclass so the rest of the code
        # does not depend on raw JSON structure
        clickhouse_section = conf.get("clickhouse", {})
        self.clickhouse = [
            ClickHouse(
                ClickHouse_Host=clickhouse_section.get("host", "localhost"),
                ClickHouse_Port=int(clickhouse_section.get("port", 8123)),
                ClickHouse_User=clickhouse_section.get("user", "default"),
                ClickHouse_Password=clickhouse_section.get("password", "password123"),
            )
        ]
        logging.warning(f"Got host {self.clickhouse[0].ClickHouse_Host}")
        
        self.collection_config = CollectionConfig(flag=collection_type)