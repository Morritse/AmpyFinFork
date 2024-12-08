import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class Backtester:
    def __init__(self, initial_capital=50000, min_cash_buffer=15000, max_position_pct=0.1):
        self.initial_capital = initial_capital
        self.min_cash_buffer = min_cash_buffer
        self.max_position_pct = max_position_pct
        self.reset()
    
    def reset(self):
        """Reset backtester state for new run"""
        self.capital = self.initial_capital
        self.trades = []
        self.current_position = None
        self.time_delta = 1.0
        self.points_tally = 0
        self.successful_trades = 0
        self.failed_trades = 0
    
    def load_data(self, symbol):
        """Load both daily data for indicators and minute data for prices"""
        # Load daily data for indicator calculation (matches live client's get_data())
        daily_path = f'backtesting/historical_data_daily/{symbol}_historical_data.csv'
        daily_data = pd.read_csv(daily_path)
        daily_data.set_index('timestamp', inplace=True)
        daily_data.index = pd.to_datetime(daily_data.index)
        
        # Load minute data for price checks (matches live client's get_latest_price())
        minute_path = f'backtesting/historical_data_minute/{symbol}_historical_data_minute.csv'
        minute_data = pd.read_csv(minute_path)
        minute_data.set_index('timestamp', inplace=True)
        minute_data.index = pd.to_datetime(minute_data.index)
        
        # Ensure column names are uppercase
        column_map = {'close': 'Close', 'high': 'High', 'low': 'Low', 'open': 'Open', 'volume': 'Volume'}
        daily_data = daily_data.rename(columns=column_map)
        minute_data = minute_data.rename(columns=column_map)
        
        return daily_data, minute_data
    
    def prepare_data(self, daily_data, minute_data, strategy_func, months=6):
        """Prepare data for backtesting period"""
        # Get last 6 months for backtest period
        end_date = min(minute_data.index.max(), daily_data.index.max())
        backtest_start = end_date - pd.DateOffset(months=months)
        
        # Get 3 months before backtest period for indicator calculation
        indicator_start = backtest_start - pd.DateOffset(months=3)
        
        # Filter daily data to include indicator calculation period
        daily_backtest = daily_data[daily_data.index <= end_date]
        daily_backtest = daily_backtest[daily_backtest.index >= indicator_start]
        
        # Filter minute data to just the backtest period
        minute_backtest = minute_data[minute_data.index <= end_date]
        minute_backtest = minute_backtest[minute_backtest.index >= backtest_start]
        
        # Get required lookback period from strategy name
        if 'MACD' in strategy_func.__name__:
            required_days = 35  # 26 + 9
        elif 'RSI' in strategy_func.__name__:
            required_days = 14
        elif 'EMA' in strategy_func.__name__:
            required_days = 30
        elif 'BBANDS' in strategy_func.__name__:
            required_days = 20
        else:
            required_days = 35  # Default to maximum needed
        
        # Ensure we have enough daily data for the strategy
        if len(daily_backtest) < required_days:
            raise ValueError(f"Insufficient data for {strategy_func.__name__} (needs {required_days} days)")
        
        print(f"Indicator calculation period: {indicator_start} to {end_date}")
        print(f"Backtest period: {backtest_start} to {end_date}")
        print(f"Daily data points: {len(daily_backtest)}")
        print(f"Minute data points: {len(minute_backtest)}")
        
        return daily_backtest, minute_backtest, backtest_start, end_date

    def calculate_points(self, price_change_ratio):
        """Calculate points based on price change ratio"""
        if price_change_ratio > 1:  # Profitable trade
            if price_change_ratio < 1.05:      # 0-5% gain
                points = self.time_delta * 1
            elif price_change_ratio < 1.1:     # 5-10% gain
                points = self.time_delta * 1.5
            else:                              # >10% gain
                points = self.time_delta * 2
        else:  # Losing trade
            if price_change_ratio > 0.975:     # 0-2.5% loss
                points = -self.time_delta * 1
            elif price_change_ratio > 0.95:    # 2.5-5% loss
                points = -self.time_delta * 1.5
            else:                              # >5% loss
                points = -self.time_delta * 2
        return points
    
    def execute_buy(self, timestamp, price):
        """Execute buy order if conditions met"""
        max_position_value = self.capital * self.max_position_pct  # Use configurable position size
        shares = int(max_position_value / price)
        position_cost = shares * price
        
        if self.capital - position_cost >= self.min_cash_buffer:
            self.current_position = {
                'entry_date': timestamp,
                'entry_price': price,
                'shares': shares,
                'cost': position_cost
            }
            self.capital -= position_cost
            return True
        return False
    
    def execute_sell(self, timestamp, price):
        """Execute sell order and calculate points"""
        if not self.current_position:
            return False
            
        position_value = self.current_position['shares'] * price
        profit = position_value - self.current_position['cost']
        price_change_ratio = price / self.current_position['entry_price']
        
        # Calculate points
        points = self.calculate_points(price_change_ratio)
        self.points_tally += points
        
        # Update trade statistics
        if profit > 0:
            self.successful_trades += 1
        else:
            self.failed_trades += 1
        
        # Record trade
        self.trades.append({
            'entry_date': self.current_position['entry_date'],
            'exit_date': timestamp,
            'entry_price': self.current_position['entry_price'],
            'exit_price': price,
            'shares': self.current_position['shares'],
            'profit': profit,
            'points': points,
            'ratio': price_change_ratio,
            'time_delta': self.time_delta
        })
        
        # Update capital
        self.capital += position_value
        self.current_position = None
        return True
    
    def get_portfolio_value(self, current_price):
        """Calculate current portfolio value including any open position"""
        value = self.capital
        if self.current_position:
            position_value = self.current_position['shares'] * current_price
            value += position_value
        return value
    
    def get_results(self):
        """Get backtest results"""
        return {
            'portfolio_value': self.get_portfolio_value(self.last_price),
            'total_return': ((self.get_portfolio_value(self.last_price)/self.initial_capital)-1)*100,
            'points': self.points_tally,
            'trades': len(self.trades),
            'successful_trades': self.successful_trades,
            'failed_trades': self.failed_trades,
            'open_position': self.current_position is not None
        }

    def run(self, symbol, strategy_func):
        """Run backtest for a symbol using given strategy"""
        self.reset()
        
        try:
            # Load and prepare data
            daily_data, minute_data = self.load_data(symbol)
            daily_backtest, minute_backtest, start_date, end_date = self.prepare_data(daily_data, minute_data, strategy_func)
            
            print(f"\nBacktesting {symbol} with {strategy_func.__name__}")
            print(f"Period: {start_date} to {end_date}")
            print(f"Daily data points: {len(daily_backtest)}")
            print(f"Max position size: {self.max_position_pct*100}% of capital")
            
            # Group minute data by trading day
            daily_groups = minute_backtest.groupby(minute_backtest.index.date)
            
            # Run backtest
            for day, day_data in daily_groups:
                # Get daily data up to this day
                daily_slice = daily_backtest[daily_backtest.index.date <= day]
                
                # Calculate signal once for the day using daily data
                signal = strategy_func(symbol, daily_slice)
                
                if signal != 'Hold':
                    print(f"Signal: {signal}")
                
                # Apply signal to minute data throughout the day
                for timestamp, row in day_data.iterrows():
                    if signal == 'Buy' and not self.current_position:
                        if self.execute_buy(timestamp, row['Close']):
                            print(f"BUY: {self.current_position['shares']} shares @ ${row['Close']:.2f}")
                            print(f"Cost: ${self.current_position['cost']:.2f}")
                    
                    elif signal == 'Sell' and self.current_position:
                        if self.execute_sell(timestamp, row['Close']):
                            profit = self.trades[-1]['profit']
                            points = self.trades[-1]['points']
                            print(f"SELL: {self.trades[-1]['shares']} shares @ ${row['Close']:.2f}")
                            print(f"Profit: ${profit:.2f} ({((row['Close']/self.trades[-1]['entry_price'])-1)*100:.1f}%)")
                            print(f"Points: {points:.2f}")
                
                # Increment time_delta at end of day
                self.time_delta += 0.01
            
            # Store last price for portfolio value calculation
            self.last_price = minute_backtest['Close'].iloc[-1]
            
            # Print final position details if any
            if self.current_position:
                position_value = self.current_position['shares'] * self.last_price
                profit = position_value - self.current_position['cost']
                price_change_ratio = self.last_price / self.current_position['entry_price']
                unrealized_points = self.calculate_points(price_change_ratio)
                print(f"\nStill holding {self.current_position['shares']} shares at end of test period")
                print(f"Entry price: ${self.current_position['entry_price']:.2f}")
                print(f"Final price: ${self.last_price:.2f}")
                print(f"Final RSI: {daily_slice['RSI'].iloc[-1]:.2f}" if 'RSI' in daily_slice else "")
                print(f"Unrealized P&L: ${profit:.2f}")
                # Add unrealized points to total
                self.points_tally += unrealized_points
            
            return self.get_results()
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {str(e)}")
            raise
