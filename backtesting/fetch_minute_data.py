import logging
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from config import API_KEY, API_SECRET

# Configure logging with thread safety
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Thread-safe print lock
print_lock = Lock()

# Stocks that had splits during our data period (March 2024 - Present)
SKIP_SYMBOLS = {
    'NVDA',  # 8:1 split on 6/10/2024
    'AVGO',  # 10:1 split on 7/15/2024
}

def fetch_symbol_data(symbol, client, start_date, end_date):
    """Fetch minute data for a single symbol."""
    try:
        with print_lock:
            logger.info(f"Fetching minute data for {symbol}")
        
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Minute,
            start=start_date,
            end=end_date
        )
        bars = client.get_stock_bars(request_params)
        
        if bars and not bars.df.empty:
            with print_lock:
                logger.info(f"Successfully fetched {len(bars.df)} minute bars for {symbol}")
                logger.info(f"Data range: {bars.df.index.get_level_values(1).min()} to {bars.df.index.get_level_values(1).max()}")
            return symbol, bars.df
        else:
            with print_lock:
                logger.warning(f"No data returned for {symbol}")
            return symbol, None
            
    except Exception as e:
        with print_lock:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return symbol, None

def save_symbol_data(symbol, data, output_dir):
    """Save data for a single symbol."""
    try:
        file_name = f'{output_dir}/{symbol}_historical_data_minute.csv'
        data.to_csv(file_name)
        
        with print_lock:
            logger.info(f"\n{symbol} Summary:")
            logger.info(f"Start Date: {data.index.get_level_values(1).min()}")
            logger.info(f"End Date: {data.index.get_level_values(1).max()}")
            logger.info(f"Total Minutes: {len(data)}")
            logger.info(f"Trading Days: {len(data) / 390:.1f}")  # ~390 minutes per trading day
            
    except Exception as e:
        with print_lock:
            logger.error(f"Error saving data for {symbol}: {str(e)}")

def main():
    # Full list of S&P 100 symbols
    sp100_symbols = [
        'AAPL', 'ABBV', 'ABT', 'ACN', 'ADBE', 'AIG', 'AMD', 'AMGN', 'AMT', 'AMZN',
        'AXP', 'BA', 'BAC', 'BK', 'BKNG', 'BLK', 'BMY', 'C',
        'CAT', 'CHTR', 'CL', 'CMCSA', 'COF', 'COP', 'COST', 'CRM', 'CSCO', 'CVS',
        'CVX', 'DE', 'DHR', 'DIS', 'DOW', 'DUK', 'EMR', 'EXC', 'F', 'FDX',
        'GD', 'GE', 'GILD', 'GM', 'GOOGL', 'GS', 'HD', 'HON', 'IBM',
        'INTC', 'JNJ', 'JPM', 'KHC', 'KO', 'LIN', 'LLY', 'LMT', 'LOW', 'MA',
        'MCD', 'MDLZ', 'MDT', 'MET', 'META', 'MMM', 'MO', 'MRK', 'MS', 'MSFT',
        'NEE', 'NFLX', 'NKE', 'ORCL', 'PEP', 'PFE', 'PG', 'PM', 'PYPL',
        'QCOM', 'RTX', 'SBUX', 'SCHW', 'SO', 'SPG', 'T', 'TGT', 'TMO', 'TMUS',
        'TSLA', 'TXN', 'UNH', 'UNP', 'UPS', 'USB', 'V', 'VZ', 'WBA', 'WFC',
        'WMT', 'XOM'
    ]
    
    # Filter out symbols to skip
    symbols = [s for s in sp100_symbols if s not in SKIP_SYMBOLS]
    
    # Create output directory
    output_dir = 'backtesting/historical_data_minute'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Initialize client
    client = StockHistoricalDataClient(API_KEY, API_SECRET)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=270)  # 9 months back
    
    logger.info("Starting parallel minute data download...")
    logger.info("=" * 50)
    
    # Number of worker threads (adjust based on your system)
    max_workers = 10
    
    # Track progress
    total_symbols = len(symbols)
    completed = 0
    failed = []
    
    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all fetch tasks
        future_to_symbol = {
            executor.submit(fetch_symbol_data, symbol, client, start_date, end_date): symbol 
            for symbol in symbols
        }
        
        # Process completed tasks
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                symbol, data = future.result()
                completed += 1
                
                if data is not None:
                    # Save the data
                    save_symbol_data(symbol, data, output_dir)
                else:
                    failed.append(symbol)
                
                # Print progress
                with print_lock:
                    logger.info(f"Progress: {completed}/{total_symbols} symbols processed")
                    
            except Exception as e:
                with print_lock:
                    logger.error(f"Error processing {symbol}: {str(e)}")
                failed.append(symbol)
    
    # Print summary
    logger.info("\nDownload Summary:")
    logger.info("=" * 50)
    logger.info(f"Total symbols processed: {completed}")
    logger.info(f"Successfully downloaded: {completed - len(failed)}")
    logger.info(f"Failed downloads: {len(failed)}")
    if failed:
        logger.info(f"Failed symbols: {failed}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
