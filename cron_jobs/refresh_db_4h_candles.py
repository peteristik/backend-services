import os
import traceback
from typing import Callable, Dict, Any, List
from utils.coinalyze_rest_adapter import CoinalyzeRestAdapter
from utils.db_util import refresh_data, get_db_path
from utils.logging import logger

# --- Configuration ---
BINANCE_PERP_CONFIG = {
    'table_name': 'binance_perp_ohlcv',
    'interval': '4hour',
    'filter': lambda m: (
        m.get('exchange') == 'A' and
        m['is_perpetual'] is True and
        m['margined'] == 'STABLE'
    )
}

HYPERLIQUID_PERP_CONFIG = {
    'table_name': 'hyperliquid_perp_ohlcv',
    'interval': '4hour',
    'filter': lambda m: (
        m.get('exchange') == 'H' and # Using .get() for safety
        m['is_perpetual'] is True and
        m['margined'] == 'STABLE'
    )
}


def _refresh_market_data(
    db_path: str,
    table_name: str,
    interval: str,
    market_filter: Callable[[Dict[str, Any]], bool]
):
    """
    Generic function to fetch and refresh market data based on a filter.

    Args:
        db_path: The absolute path to the SQLite database.
        table_name: The name of the table to update.
        interval: The data interval (e.g., '1min', '15min').
        market_filter: A function that returns True for markets to include.
    """
    try:
        # Initialize CoinalyzeRestAdapter
        ca = CoinalyzeRestAdapter()

        # Get all supported future markets
        logger.info("Fetching all supported future markets...")
        all_future_markets = ca.get_supported_future_markets()

        # Filter for the desired tickers
        logger.info(f"Filtering markets for table '{table_name}'...")
        filtered_tickers = [
            market['symbol'] for market in all_future_markets if market_filter(market)
        ]

        if not filtered_tickers:
            logger.warning(f"No tickers found for table '{table_name}'. The refresh will be skipped.")
            return

        logger.info(f"Found {len(filtered_tickers)} tickers for '{table_name}'.")

        # Refresh data for the filtered tickers
        logger.info(f"Refreshing data (interval={interval}) for table '{table_name}'...")
        refresh_data(
            db_path=db_path,
            table_name=table_name,
            symbols=filtered_tickers,
            interval=interval
        )
        logger.info(f"Data refresh for '{table_name}' completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the refresh for '{table_name}': {e}")
        logger.error(traceback.format_exc())


def refresh_binance_perp():
    """
    Refreshes Binance perpetual futures data.
    """
    logger.info("--- Starting Binance Perp Refresh Task ---")
    db_path = get_db_path()
    _refresh_market_data(
        db_path=db_path,
        table_name=BINANCE_PERP_CONFIG['table_name'],
        interval=BINANCE_PERP_CONFIG['interval'],
        market_filter=BINANCE_PERP_CONFIG['filter']
    )
    logger.info("--- Finished Binance Perp Refresh Task ---")


def refresh_hyperliquid_perp():
    """
    Refreshes Hyperliquid perpetual futures data.
    """
    logger.info("--- Starting Hyperliquid Perp Refresh Task ---")
    db_path = get_db_path()
    _refresh_market_data(
        db_path=db_path,
        table_name=HYPERLIQUID_PERP_CONFIG['table_name'],
        interval=HYPERLIQUID_PERP_CONFIG['interval'],
        market_filter=HYPERLIQUID_PERP_CONFIG['filter']
    )
    logger.info("--- Finished Hyperliquid Perp Refresh Task ---")


if __name__ == "__main__":
    # You can now run both refresh tasks sequentially
    refresh_binance_perp()
    print("\n" + "="*50 + "\n") # Separator for clarity in logs
    refresh_hyperliquid_perp()
