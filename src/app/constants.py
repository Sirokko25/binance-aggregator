"""
Constants used across the trades collector project.
"""

# Retry settings
MAX_DATABASE_RETRIES = 5
DATABASE_RETRY_DELAY = 5  # seconds

# Logging settings
LOG_FORMAT = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s"
LOG_LEVEL = "INFO"

# API settings
API_RETRY_DELAY = 5

# DB connection settings
MAX_CLIENTS = 1

# Table names
TABLE_NAME_TRADES = "binance.trades"
TABLE_NAME_ORDERS = "binance.orders"

CREATE_ORDERS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS binance.orders on cluster "default"(
    avgPrice Float64,
    clientOrderId String,
    cumQuote Float64,
    executedQty Float64,
    orderId UInt64,
    origQty Float64,
    origType LowCardinality(String),
    price Float64,
    reduceOnly Boolean,
    side LowCardinality(String),
    positionSide LowCardinality(String),
    status LowCardinality(String),
    stopPrice Float64,
    closePosition Boolean,
    symbol LowCardinality(String),
    time UInt64,
    timeInForce LowCardinality(String),
    type LowCardinality(String),
    activatePrice Float64,
    priceRate Float64,
    updateTime UInt64,
    workingType LowCardinality(String),
    priceProtect Boolean,
    priceMatch LowCardinality(String),
    selfTradePreventionMode LowCardinality(String),
    goodTillDate UInt64,
    name LowCardinality(String),
    date DateTime
) ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY date
"""

CREATE_TRADES_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS binance.trades on cluster "default" (
    buyer Boolean,
    commission Float64,
    commissionAsset LowCardinality(String),
    id UInt64,
    price Float64,
    qty Float64,
    quoteQTY Float64,
    realizedPnl Float64,
    positionSide LowCardinality(String),
    symbol LowCardinality(String),
    name LowCardinality(String),
    time UInt128,
    date DateTime
) ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY date
"""
