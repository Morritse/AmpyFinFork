import pandas as pd
import numpy as np
import talib
import json
import os
from datetime import datetime, timedelta
from scoring import StrategyScorer

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def analyze_rsi(symbol, initial_capital=50000):
    """Analyze RSI values for a given symbol."""
    print(f"\nAnalyzing RSI for {symbol}")
    print("=" * 50)
    
    # Load daily data for RSI calculation
    daily_path = f'backtesting/historical_data_daily/{symbol}_historical_data.csv'
    daily_data = pd.read_csv(daily_path)
    daily_data.set_index('timestamp', inplace=True)
    daily_data.index = pd.to_datetime(daily_data.index)
    
    # Ensure column names are uppercase
    column_map = {'close': 'Close', 'high': 'High', 'low': 'Low', 'open': 'Open', 'volume': 'Volume'}
    daily_data = daily_data.rename(columns=column_map)
    
    # Calculate RSI on daily data
    daily_data['RSI'] = talib.RSI(daily_data['Close'].values, timeperiod=14)
    
    # Load minute data
    data_path = f'backtesting/historical_data_minute/{symbol}_historical_data_minute.csv'
    data = pd.read_csv(data_path)
    data.set_index('timestamp', inplace=True)
    data.index = pd.to_datetime(data.index)
    
    # Ensure column names are uppercase
    data = data.rename(columns=column_map)
    
    # Map daily RSI to minute data
    data['RSI'] = data.index.map(lambda x: daily_data['RSI'].asof(x))
    
    # Get last 6 months of data for backtesting
    end_date = data.index.max()
    start_date = end_date - pd.DateOffset(months=6)
    backtest_data = data[start_date:]
    
    print(f"Period: {start_date} to {end_date}")
    
    # Track RSI statistics
    rsi_stats = {
        'min': backtest_data['RSI'].min(),
        'max': backtest_data['RSI'].max(),
        'avg': backtest_data['RSI'].mean(),
        'buy_signals': len([x for x in backtest_data['RSI'] if x < 30]),
        'sell_signals': len([x for x in backtest_data['RSI'] if x > 70])
    }
    
    print("\nRSI Statistics:")
    print(f"Min RSI: {rsi_stats['min']:.2f}")
    print(f"Max RSI: {rsi_stats['max']:.2f}")
    print(f"Avg RSI: {rsi_stats['avg']:.2f}")
    print(f"Buy Signals (RSI < 30): {rsi_stats['buy_signals']}")
    print(f"Sell Signals (RSI > 70): {rsi_stats['sell_signals']}")
    
    # Initialize tracking
    capital = initial_capital
    min_cash_buffer = 15000
    trades = []
    current_position = None
    time_delta = 1.0
    points_tally = 0
    successful_trades = 0
    failed_trades = 0
    
    # Group data by trading day
    daily_groups = backtest_data.groupby(backtest_data.index.date)
    
    # Run backtest
    for day, day_data in daily_groups:
        # Process all minutes in the day
        for timestamp, row in day_data.iterrows():
            # Skip if RSI not available
            if pd.isna(row['RSI']):
                continue
                
            # Check for buy signal
            if row['RSI'] < 30 and current_position is None:
                max_position_value = capital * 0.1  # 10% max position size
                shares = int(max_position_value / row['Close'])
                position_cost = shares * row['Close']
                
                if capital - position_cost >= min_cash_buffer:
                    current_position = {
                        'entry_date': timestamp,
                        'entry_price': row['Close'],
                        'entry_rsi': row['RSI'],
                        'shares': shares,
                        'cost': position_cost
                    }
                    capital -= position_cost
                    print(f"\nBUY {symbol}: {shares} shares @ ${row['Close']:.2f}")
                    print(f"RSI: {row['RSI']:.2f}")
                    print(f"Cost: ${position_cost:.2f}")
                    print(f"Capital: ${capital:.2f}")
            
            # Check for sell signal
            elif row['RSI'] > 70 and current_position is not None:
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
                print(f"RSI: {row['RSI']:.2f}")
                print(f"Entry: ${current_position['entry_price']:.2f}")
                print(f"Exit: ${row['Close']:.2f}")
                print(f"Profit: ${profit:.2f} ({((row['Close']/current_position['entry_price'])-1)*100:.1f}%)")
                print(f"Points: {points:.2f}")
                print(f"Capital: ${capital:.2f}")
                
                trades.append({
                    'entry_date': current_position['entry_date'],
                    'exit_date': timestamp,
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
    
    # Calculate final portfolio value including open position
    portfolio_value = capital
    if current_position:
        final_price = backtest_data['Close'].iloc[-1]
        position_value = current_position['shares'] * final_price
        profit = position_value - current_position['cost']
        portfolio_value = capital + position_value
        
        print(f"\nOpen Position:")
        print(f"Entry: {current_position['entry_date']}, Price: ${current_position['entry_price']:.2f}, RSI: {current_position['entry_rsi']:.2f}")
        print(f"Current: {backtest_data.index[-1]}, Price: ${final_price:.2f}, RSI: {backtest_data['RSI'].iloc[-1]:.2f}")
        print(f"Shares: {current_position['shares']}")
        print(f"Cost Basis: ${current_position['cost']:.2f}")
        print(f"Current Value: ${position_value:.2f}")
        print(f"Unrealized P&L: ${profit:.2f} ({(profit/current_position['cost'])*100:.1f}%)")
    
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
        'rsi_stats': rsi_stats
    }

def main():
    symbols = [
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
    results = []
    
    print("\nTesting RSI Strategy Across Multiple Symbols")
    print("=" * 50)
    
    for symbol in symbols:
        result = analyze_rsi(symbol)
        results.append(result)
    
    print("\nStrategy Summary:")
    print("=" * 100)
    print(f"{'Symbol':<6} {'Initial':<12} {'Final':<12} {'Change':<12} {'Return %':<10} {'Points':<8} {'Trades':<6}")
    print("-" * 100)
    
    for r in results:
        initial = 50000
        final = r['portfolio_value']
        change = final - initial
        ret_pct = r['total_return']
        points = r['points']
        trades = r['trades']
        
        print(f"{r['symbol']:<6} ${initial:<11,.2f} ${final:<11,.2f} ${change:<11,.2f} "
              f"{ret_pct:>9.2f}% {points:>7.2f} {trades:>6d}")
    
    print("-" * 100)
    
    # Calculate averages
    avg_portfolio = sum(r['portfolio_value'] for r in results) / len(results)
    avg_return = sum(r['total_return'] for r in results) / len(results)
    avg_points = sum(r['points'] for r in results) / len(results)
    total_trades = sum(r['trades'] for r in results)
    
    print(f"AVERAGE: ${50000:<11,.2f} ${avg_portfolio:<11,.2f} ${avg_portfolio-50000:<11,.2f} "
          f"{avg_return:>9.2f}% {avg_points:>7.2f} {total_trades:>6d}")

if __name__ == "__main__":
    main()
