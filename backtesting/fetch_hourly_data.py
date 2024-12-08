import logging
import pandas as pd
import os
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
API_KEY = "PKNTZ0OC8WFDUG2BIP8U"
API_SECRET = "xeQ5ehe0CTmi9HVcCV1NKKRj9gcdJytRft8L2ewF"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def fetch_historical_data(symbols):
    """Fetch historical hourly data for the given symbols."""
    client = StockHistoricalDataClient(API_KEY, API_SECRET)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=270)  # 6 months back for hourly data

    historical_data = {}

    for symbol in symbols:
        logger.info(f"Fetching hourly data for {symbol}")
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Hour,
            start=start_date,
            end=end_date
        )
        try:
            bars = client.get_stock_bars(request_params)
            if bars:
                historical_data[symbol] = bars.df
                logger.info(f"Successfully fetched {len(bars.df)} hourly bars for {symbol}")
            else:
                logger.warning(f"No data returned for {symbol}")
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            continue

    return historical_data

def save_historical_data(historical_data):
    """Save the historical data to CSV files in the historical_data_hourly directory."""
    directory = 'backtesting/historical_data_hourly'
    if not os.path.exists(directory):
        os.makedirs(directory)

    for symbol, data in historical_data.items():
        file_name = f'{directory}/{symbol}_historical_data_hourly.csv'
        data.to_csv(file_name)
        logger.info(f"Saved hourly data for {symbol} to {file_name}")
        logger.info(f"Data range: {data.index[0]} to {data.index[-1]}")
        logger.info(f"Number of hourly bars: {len(data)}")

def main():
    # Start with our test symbols
    test_symbols = ['AMD', 'AAPL', 'JPM', 'NVDA', 'XOM']
    
    logger.info("Starting hourly data download...")
    logger.info("=" * 50)
    
    historical_data = fetch_historical_data(test_symbols)
    save_historical_data(historical_data)
    
    logger.info("Download complete!")
    logger.info("=" * 50)
    
    # Print summary
    for symbol in test_symbols:
        if symbol in historical_data:
            data = historical_data[symbol]
            logger.info(f"\n{symbol} Summary:")
            logger.info(f"Start Date: {data.index[0]}")
            logger.info(f"End Date: {data.index[-1]}")
            logger.info(f"Total Hours: {len(data)}")
            logger.info(f"Trading Days: {len(data) / 6.5:.1f}")  # Approx 6.5 trading hours per day

if __name__ == "__main__":
    main()
