import pandas as pd
import numpy as np
import talib
import json
import os
from datetime import datetime, timedelta
from tqdm import tqdm

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def analyze_bbands(symbol, initial_capital=50000):
    """Analyze BBANDS values for a given symbol."""
    print(f"\nAnalyzing BBANDS for {symbol}")
    print("=" * 50)
    
    # Load and prepare daily data first
    daily_path = f'backtesting/historical_data_daily/{symbol}_historical_data.csv'
    daily_data = pd.read_csv(daily_path)
    daily_data['timestamp'] = pd.to_datetime(daily_data['timestamp']).dt.tz_localize(None)
    # Set index to date only
    daily_data['date'] = daily_data['timestamp'].dt.date
    daily_data.set_index('date', inplace=True)
    
    # Ensure column names are uppercase
    column_map = {'close': 'Close', 'high': 'High', 'low': 'Low', 'open': 'Open', 'volume': 'Volume'}
    daily_data = daily_data.rename(columns=column_map)
    
    # Calculate BBANDS on daily data
    upper, middle, lower = talib.BBANDS(
        daily_data['Close'].values, 
        timeperiod=15,
        nbdevup=1.8,
        nbdevdn=1.8,
        matype=1
    )
    daily_data['BB_upper'] = upper
    daily_data['BB_middle'] = middle
    daily_data['BB_lower'] = lower
    
    # Get last 6 months of daily data
    end_date = daily_data.index.max()
    start_date = end_date - pd.DateOffset(months=6)
    start_date = start_date.date()  # Convert to date
    daily_data = daily_data[daily_data.index >= start_date]
    
    print(f"Daily data period: {start_date} to {end_date}")
    print(f"Daily data points: {len(daily_data)}")
    
    # Print first few daily bands
    print("\nFirst few daily bands:")
    print(daily_data[['Close', 'BB_upper', 'BB_middle', 'BB_lower']].head())
    
    # Initialize tracking
    capital = initial_capital
    min_cash_buffer = 15000
    trades = []
    current_position = None
    time_delta = 1.0
    points_tally = 0
    successful_trades = 0
    failed_trades = 0
    upper_crosses = 0
    lower_crosses = 0
    total_minutes = 0
    
    # Process minute data day by day
    minute_path = f'backtesting/historical_data_minute/{symbol}_historical_data_minute.csv'
    
    # Read minute data
    minute_data = pd.read_csv(minute_path)
    minute_data['timestamp'] = pd.to_datetime(minute_data['timestamp']).dt.tz_localize(None)
    minute_data['date'] = minute_data['timestamp'].dt.date
    minute_data = minute_data.rename(columns=column_map)
    
    # Filter to last 6 months
    minute_data = minute_data[minute_data['date'] >= start_date]
    minute_data = minute_data[minute_data['date'] <= end_date]
    print(f"\nMinute data points after filtering: {len(minute_data)}")
    
    # Print first few minute prices
    print("\nFirst few minute prices:")
    print(minute_data[['timestamp', 'Close']].head())
    
    # Group by day for processing
    daily_groups = minute_data.groupby('date')
    print(f"Number of trading days: {len(daily_groups)}")
    
    print("\nProcessing minute data...")
    for day, day_data in tqdm(daily_groups):
        # Get daily bands for this day
        if day not in daily_data.index:
            print(f"Warning: No daily data for {day}")
            continue
            
        day_bands = daily_data.loc[day]
        
        # Skip days with invalid bands
        if pd.isna(day_bands['BB_upper']) or pd.isna(day_bands['BB_lower']) or pd.isna(day_bands['BB_middle']):
            print(f"Warning: Invalid bands for {day}")
            continue
        
        # Create copy of day_data to avoid SettingWithCopyWarning
        day_data = day_data.copy()
        
        # Forward fill bands to all minutes in this day
        day_data['BB_upper'] = day_bands['BB_upper']
        day_data['BB_middle'] = day_bands['BB_middle']
        day_data['BB_lower'] = day_bands['BB_lower']
        
        # Update statistics
        day_upper_crosses = len(day_data[day_data['Close'] > day_data['BB_upper']])
        day_lower_crosses = len(day_data[day_data['Close'] < day_data['BB_lower']])
        day_minutes = len(day_data)
        
        if day_minutes > 0:
            upper_crosses += day_upper_crosses
            lower_crosses += day_lower_crosses
            total_minutes += day_minutes
            
            # Print first day's data
            if total_minutes <= day_minutes:
                print(f"\nFirst day ({day}) data:")
                print(f"Minutes: {day_minutes}")
                print(f"Upper crosses: {day_upper_crosses}")
                print(f"Lower crosses: {day_lower_crosses}")
                print(f"Bands: Upper={day_bands['BB_upper']:.2f}, Middle={day_bands['BB_middle']:.2f}, Lower={day_bands['BB_lower']:.2f}")
                print("\nFirst few minutes of the day:")
                print(day_data[['timestamp', 'Close', 'BB_upper', 'BB_middle', 'BB_lower']].head())
            
            # Process each minute
            for _, row in day_data.iterrows():
                # Check for buy signal
                trend_up = row['Close'] > row['BB_middle']
                if row['Close'] < row['BB_lower'] and trend_up and current_position is None:
                    max_position_value = capital * 0.1  # 10% max position size
                    shares = int(max_position_value / row['Close'])
                    position_cost = shares * row['Close']
                    
                    if capital - position_cost >= min_cash_buffer:
                        current_position = {
                            'entry_date': row['timestamp'],
                            'entry_price': row['Close'],
                            'shares': shares,
                            'cost': position_cost
                        }
                        capital -= position_cost
                        print(f"\nBUY {symbol}: {shares} shares @ ${row['Close']:.2f}")
                        print(f"Lower Band: ${row['BB_lower']:.2f}")
                        print(f"Cost: ${position_cost:.2f}")
                
                # Check for sell signal
                elif row['Close'] > row['BB_upper'] and not trend_up and current_position is not None:
                    position_value = current_position['shares'] * row['Close']
                    profit = position_value - current_position['cost']
                    price_change_ratio = row['Close'] / current_position['entry_price']
                    
                    if row['Close'] > current_position['entry_price']:
                        successful_trades += 1
                        if price_change_ratio < 1.05:
                            points = time_delta * 1
                        elif price_change_ratio < 1.1:
                            points = time_delta * 1.5
                        else:
                            points = time_delta * 2
                    else:
                        failed_trades += 1
                        if price_change_ratio > 0.975:
                            points = -time_delta * 1
                        elif price_change_ratio > 0.95:
                            points = -time_delta * 1.5
                        else:
                            points = -time_delta * 2
                            
                    points_tally += points
                    capital += position_value
                    
                    print(f"\nSELL {symbol}: {current_position['shares']} shares @ ${row['Close']:.2f}")
                    print(f"Upper Band: ${row['BB_upper']:.2f}")
                    print(f"Entry: ${current_position['entry_price']:.2f}")
                    print(f"Exit: ${row['Close']:.2f}")
                    print(f"Profit: ${profit:.2f} ({((row['Close']/current_position['entry_price'])-1)*100:.1f}%)")
                    print(f"Points: {points:.2f}")
                    
                    trades.append({
                        'entry_date': current_position['entry_date'],
                        'exit_date': row['timestamp'],
                        'entry_price': current_position['entry_price'],
                        'exit_price': row['Close'],
                        'shares': current_position['shares'],
                        'profit': profit,
                        'points': points,
                        'ratio': price_change_ratio,
                        'time_delta': time_delta
                    })
                    
                    current_position = None
        
        # Increment time_delta at end of day
        time_delta += 0.01
    
    if total_minutes == 0:
        print("\nError: No valid minute data found!")
        return None
    
    # Calculate final portfolio value including open position
    portfolio_value = capital
    if current_position:
        final_price = minute_data['Close'].iloc[-1]
        position_value = current_position['shares'] * final_price
        profit = position_value - current_position['cost']
        portfolio_value = capital + position_value
        
        print(f"\nOpen Position:")
        print(f"Entry: {current_position['entry_date']}, Price: ${current_position['entry_price']:.2f}")
        print(f"Current: {minute_data['timestamp'].iloc[-1]}, Price: ${final_price:.2f}")
        print(f"Shares: {current_position['shares']}")
        print(f"Cost Basis: ${current_position['cost']:.2f}")
        print(f"Current Value: ${position_value:.2f}")
        print(f"Unrealized P&L: ${profit:.2f} ({(profit/current_position['cost'])*100:.1f}%)")
    
    print("\nBBANDS Statistics:")
    print(f"Minutes above upper band: {upper_crosses} ({upper_crosses/total_minutes*100:.2f}%)")
    print(f"Minutes below lower band: {lower_crosses} ({lower_crosses/total_minutes*100:.2f}%)")
    print(f"Total minutes analyzed: {total_minutes}")
    
    print("\nStrategy Performance:")
    print(f"Initial Capital: ${initial_capital:,.2f}")
    print(f"Final Capital: ${capital:,.2f}")
    if current_position:
        print(f"Open Position: {current_position['shares']} shares @ ${current_position['entry_price']:.2f}")
        print(f"Current Value: ${position_value:,.2f}")
        print(f"Unrealized P&L: ${profit:.2f}")
    print(f"Total Portfolio Value: ${portfolio_value:,.2f}")
    print(f"Total Return: {((portfolio_value/initial_capital)-1)*100:.2f}%")
    print(f"Completed Trades: {len(trades)} (Success: {successful_trades}, Failed: {failed_trades})")
    print(f"Points (from completed trades): {points_tally:.2f}")
    
    return {
        'symbol': symbol,
        'portfolio_value': portfolio_value,
        'total_return': ((portfolio_value/initial_capital)-1)*100,
        'points': points_tally,
        'trades': len(trades),
        'successful_trades': successful_trades,
        'failed_trades': failed_trades,
        'open_position': current_position is not None,
        'bbands_stats': {
            'upper_crosses': upper_crosses,
            'lower_crosses': lower_crosses,
            'total_minutes': total_minutes
        }
    }

def main():
    # Test with AMD and TSLA
    symbols = ['AMD', 'TSLA']
    results = []
    
    print("\nTesting BBANDS Strategy")
    print("=" * 50)
    
    for symbol in symbols:
        result = analyze_bbands(symbol)
        if result:
            results.append(result)
    
    if not results:
        print("No valid results to display")
        return
        
    print("\nStrategy Summary:")
    print("=" * 100)
    print(f"{'Symbol':<6} {'Initial':<12} {'Final':<12} {'Return %':<10} {'Points':<8} {'Trades':<6}")
    print("-" * 100)
    
    for r in results:
        print(f"{r['symbol']:<6} ${50000:<11,.2f} ${r['portfolio_value']:<11,.2f} "
              f"{r['total_return']:>9.2f}% {r['points']:>7.2f} {r['trades']:>6d}")
    
    print("-" * 100)
    
    # Calculate averages
    avg_portfolio = sum(r['portfolio_value'] for r in results) / len(results)
    avg_return = sum(r['total_return'] for r in results) / len(results)
    avg_points = sum(r['points'] for r in results) / len(results)
    total_trades = sum(r['trades'] for r in results)
    
    print(f"AVERAGE: ${50000:<11,.2f} ${avg_portfolio:<11,.2f} "
          f"{avg_return:>9.2f}% {avg_points:>7.2f} {total_trades:>6d}")

if __name__ == "__main__":
    main()
