import clickhouse_connect
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from app.config import Config
from app.constants import TABLE_NAME_TRADES, TABLE_NAME_ORDERS, LOG_FORMAT, LOG_LEVEL


class EventWriter:
    """
    Writes trades and orders into ClickHouse in bulk.
    Keeps a single client instance and exposes small async helpers for the collector.
    """
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("EventWriter")
        self.logger.setLevel(getattr(logging, LOG_LEVEL))
        if not self.logger.handlers:
            logging.basicConfig(
                format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL), force=True
            )
        # ClickHouse client is created once and reused across all write calls
        self.client = None
        self._init_clickhouse_client()

    def _init_clickhouse_client(self):
        try:
            # At the moment we expect a single ClickHouse target and take the first one
            clickhouse_config = self.config.clickhouse[0]
            self.client = clickhouse_connect.get_client(
                host=clickhouse_config.ClickHouse_Host,
                port=clickhouse_config.ClickHouse_Port,
                username=clickhouse_config.ClickHouse_User,
                password=clickhouse_config.ClickHouse_Password,
            )
            self.logger.info("ClickHouse client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize ClickHouse client: {e}")
            raise e

    async def write_trades_batch(
        self, trades_data: List[Dict[str, Any]], symbol: str, account_name: str
    ):

        # Nothing to do if the exchange returned an empty page
        if not trades_data:
            return

        try:
            rows = []
            # Transform raw Binance trades into a flat structure expected by ClickHouse
            for trade in trades_data:

                row = {
                    "buyer": trade.get("buyer", False),
                    "commission": float(trade.get("commission", 0.0)),
                    "commissionAsset": trade.get("commissionAsset", ""),
                    "id": int(trade.get("id", 0)),
                    "price": float(trade.get("price", 0.0)),
                    "qty": float(trade.get("qty", 0.0)),
                    "quoteQTY": float(trade.get("quoteQty", 0.0)),
                    "realizedPnl": float(trade.get("realizedPnl", 0.0)),
                    "positionSide": trade.get("positionSide", ""),
                    "symbol": symbol,
                    "name": account_name,
                    "time": int(trade.get("time", 0)),
                    # Store a human‑readable timestamp alongside the raw millisecond value
                    "date": datetime.fromtimestamp(
                        int(trade.get("time", 0)) / 1000
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                }
                rows.append(row)

            await self._async_write_trades(rows)

            self.logger.info(
                f"Successfully wrote {len(rows)} trades for {symbol} from {account_name}"
            )

        except Exception as e:
            self.logger.error(f"Error writing trades batch for {symbol}: {e}")

    async def _async_write_trades(self, rows: List[Dict[str, Any]]):
        try:
            # We build a single INSERT statement instead of one query per row
            if self.client and rows:
                values_parts = []
                for row in rows:
                    value_part = f"({str(row['buyer']).lower()}, {row['commission']}, '{row['commissionAsset']}', {row['id']}, {row['price']}, {row['qty']}, {row['quoteQTY']}, {row['realizedPnl']}, '{row['positionSide']}', '{row['symbol']}', '{row['name']}', {row['time']}, '{row['date']}')"
                    values_parts.append(value_part)

                insert_sql = f"""
                INSERT INTO {TABLE_NAME_TRADES} (
                    buyer, commission, commissionAsset, id, price, qty, quoteQTY, 
                    realizedPnl, positionSide, symbol, name, time, date
                ) VALUES {', '.join(values_parts)}
                """

                self.logger.info(f"Executing batch insert for {len(rows)} rows")
                _ = self.client.command(insert_sql)
                self.logger.info(f"Successful batch insert. Processed {len(rows)} rows.")

        except Exception as e:
            self.logger.error(f"Error in async write trades: {e}")
            if hasattr(e, "message"):
                self.logger.error(f"ClickHouse error message: {e.message}")
            if hasattr(e, "args"):
                self.logger.error(f"Error args: {e.args}")
            raise
    
    async def write_orders_batch(
        self, orders_data: List[Dict[str, Any]], symbol: str, account_name: str
    ):

        # Nothing to do if the exchange returned an empty page
        if not orders_data :
            return

        try:
            rows = []
            # Transform raw Binance orders into a flat structure expected by ClickHouse
            for order in orders_data:

                row = {
                    "avgPrice": float(order.get("avgPrice", 0.0)),
                    "clientOrderId": order.get("clientOrderId", ""),
                    "cumQuote": float(order.get("cumQuote", 0.0)),
                    "executedQty": float(order.get("executedQty", 0.0)),
                    "orderId": int(order.get("orderId", 0)),
                    "origQty": float(order.get("origQty", 0.0)),
                    "origType": order.get("origType", ""),
                    "price": float(order.get("price", 0.0)),
                    "reduceOnly": order.get("reduceOnly", False),
                    "side": order.get("side", ""),
                    "positionSide": order.get("positionSide", ""),
                    "status": order.get("status", ""),
                    "stopPrice": float(order.get("stopPrice", 0.0)),
                    "closePosition": order.get("closePosition", False),
                    "symbol": symbol,
                    "time": int(order.get("time", 0)),
                    "timeInForce": order.get("timeInForce", ""),
                    "type": order.get("type", ""),
                    "activatePrice": float(order.get("activatePrice", 0.0)),
                    "priceRate": float(order.get("priceRate", 0.0)),
                    "updateTime": int(order.get("updateTime", 0)),
                    "workingType": order.get("workingType", ""),
                    "priceProtect": order.get("priceProtect", False),
                    "priceMatch": order.get("priceMatch", ""),
                    "selfTradePreventionMode": order.get("selfTradePreventionMode", ""),
                    "goodTillDate": int(order.get("goodTillDate", 0)),
                    "name": account_name,
                    # Same convention as for trades: raw time in ms plus a formatted datetime
                    "date": datetime.fromtimestamp(
                        int(order.get("time", 0)) / 1000
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                }
                rows.append(row)

            await self._async_write_orders(rows)

            self.logger.info(
                f"Successfully wrote {len(rows)} orders for {symbol} from {account_name}"
            )

        except Exception as e:
            self.logger.error(f"Error writing orders batch for {symbol}: {e}")

    async def _async_write_orders(self, rows: List[Dict[str, Any]]):
        try:
            # Again, we batch everything into a single INSERT statement for performance
            if self.client and rows:
                values_parts = []
                for row in rows:
                    value_part = f"({row['avgPrice']}, '{row['clientOrderId']}', {row['cumQuote']}, {row['executedQty']}, {row['orderId']}, {row['origQty']}, '{row['origType']}', {row['price']}, {str(row['reduceOnly']).lower()}, '{row['side']}', '{row['positionSide']}', '{row['status']}', {row['stopPrice']}, {str(row['closePosition']).lower()}, '{row['symbol']}', {row['time']}, '{row['timeInForce']}', '{row['type']}', {row['activatePrice']}, {row['priceRate']}, {row['updateTime']}, '{row['workingType']}', {str(row['priceProtect']).lower()}, '{row['priceMatch']}', '{row['selfTradePreventionMode']}', {row['goodTillDate']}, '{row['name']}', '{row['date']}')"
                    values_parts.append(value_part)

                insert_sql = f"""
                INSERT INTO {TABLE_NAME_ORDERS} (
                    avgPrice, clientOrderId, cumQuote, executedQty, orderId, origQty, origType, 
                    price, reduceOnly, side, positionSide, status, stopPrice, closePosition, 
                    symbol, time, timeInForce, type, activatePrice, priceRate, updateTime, 
                    workingType, priceProtect, priceMatch, selfTradePreventionMode, goodTillDate, 
                    name, date
                ) VALUES {', '.join(values_parts)}
                """

                self.logger.info(f"Executing batch insert for {len(rows)} rows")
                _ = self.client.command(insert_sql)
                self.logger.info(f"Successful batch insert. Processed {len(rows)} rows.")

        except Exception as e:
            self.logger.error(f"Error in async write orders: {e}")
            if hasattr(e, "message"):
                self.logger.error(f"ClickHouse error message: {e.message}")
            if hasattr(e, "args"):
                self.logger.error(f"Error args: {e.args}")
            raise
    
    async def get_last_from_id(self, symbol: str, account_name: str) -> int:

        try:
            # Query the last stored trade id (per symbol / account) so that the collector can resume
            query = f"""
            SELECT MAX(id) as last_from_id
            FROM {TABLE_NAME_TRADES}
            WHERE symbol = '{symbol}' AND name = '{account_name}'
            """

            result = await self._async_query(query)

            if result and result[0][0]:
                return int(result[0][0]) + 1
            else:
                return 0

        except Exception as e:
            self.logger.error(f"Error getting last from id for {symbol}: {e}")
            return 0

    async def get_last_order_id(self, symbol: str, account_name: str) -> int:

        try:
            # Same idea as get_last_from_id, but for order ids
            query = f"""
            SELECT MAX(orderId) as last_order_id
            FROM {TABLE_NAME_ORDERS}
            WHERE symbol = '{symbol}' AND name = '{account_name}'
            """

            result = await self._async_query(query)

            if result and result[0][0]:
                return int(result[0][0]) + 1
            else:
                return 0

        except Exception as e:
            self.logger.error(f"Error getting last orderId for {symbol}: {e}")
            return 0

    async def _async_query(self, query: str):
        try:
            if self.client:
                result = self.client.query(query)
                return result.result_rows
        except Exception as e:
            self.logger.error(f"Error in async query: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            self.logger.info("ClickHouse connection closed")
