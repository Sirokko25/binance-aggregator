import json
import asyncio
import sys
from pathlib import Path
from app.collector import Collector
from app.config import Config
import logging


async def main_async(collection_type: str):
    """
    Entry point used by CLI and Docker.
    Loads config, constructs the collector and runs the async loop.
    """

    path = Path(__file__).parent.parent / "config.json"

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            conf = Config(json.load(f), collection_type)

        logging.info(f"Starting collector with type: {collection_type}")
        
        collector = Collector(conf)

        await collector.start_async()

    except FileNotFoundError:

        logging.error(f"Configuration file not found: {path}")
        return

    except json.JSONDecodeError as e:

        logging.error(f"JSON convert error: {e}")
        return
    except Exception as e:
        logging.error(f"Starting error: {e}")
        return


def main():
    """
    Small CLI wrapper:
    python -m src.main trades|orders
    """
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "trades":
            collection_type = "trades"
        elif arg == "orders":
            collection_type = "orders"
        else:
            logging.error(f"Invalid collection type: {arg}. Use 'trades' or 'orders'")
            sys.exit(1)
    else:
        logging.error("Missing collection type. Use 'trades' or 'orders'")
        sys.exit(1)
    
    asyncio.run(main_async(collection_type))


if __name__ == "__main__":
    main()
