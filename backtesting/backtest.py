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

class Backtester:
    def __init__(self, symbol, timeframe='daily', initial_capital=50000):
        self.symbol = symbol
        self.timeframe = timeframe
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.scorer = StrategyScorer(initial_capital)
        self.trades = []
        self.current_position = None
        self.min_cash_buffer = 15000  # Same as live trading
        
    def run_backtest(self, data, strategy_func):
        """Run backtest using daily EMA for signals but trading at specified timeframe."""
        logger.info(f"\nStarting backtest for {strategy_func.__name__}")
        logger.info(f"Full data period: {data.index.min()} to {data.index.max()}")
        
        # Calculate daily EMA first
        daily_data = data.resample('D').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
        
        daily_data['EMA'] = talib.EMA(daily_data['Close'].values, timeperiod=30)
        
        # Map daily EMA to trading timeframe data
        data['EMA'] = data.index.map(lambda x: daily_data['EMA'].asof(x))
        
        # Get last 6 months for backtesting
        end_date = data.index.max()
        start_date = end_date - pd.DateOffset(months=6)
        backtest_data = data[start_date:]
        
        logger.info(f"Backtest period: {start_date} to {end_date}")
        
        # Track trading days for time_delta
        current_day = None
        
        for timestamp, row in backtest_data.iterrows():
            # Check if this is a new trading day
            trading_day = timestamp.date()
            if current_day != trading_day:
                current_day = trading_day
                self.scorer.increment_day()
            
            # Skip if EMA not available
            if pd.isna(row['EMA']):
                continue
                
            # Get strategy signal
            signal = strategy_func(self.symbol, pd.DataFrame({'Close': [row['Close']], 'EMA': [row['EMA']]}))
            
            # Process signal
            if signal == 'Buy' and self.current_position is None:
                # Calculate position size (max 10% of portfolio)
                max_position_value = self.capital * 0.1
                shares = int(max_position_value / row['Close'])
                position_cost = shares * row['Close']
                
                # Check minimum cash buffer
                if self.capital - position_cost >= self.min_cash_buffer:
                    self.current_position = {
                        'entry_date': timestamp,
                        'entry_price': row['Close'],
                        'entry_ema': row['EMA'],
                        'shares': shares,
                        'cost': position_cost
                    }
                    self.capital -= position_cost
                    logger.info(f"\nBUY: {shares} shares at ${row['Close']:.2f} (EMA: {row['EMA']:.2f})")
                    
            elif signal == 'Sell' and self.current_position is not None:
                # Calculate trade results
                position_value = self.current_position['shares'] * row['Close']
                profit = position_value - self.current_position['cost']
                
                # Calculate points
                points, ratio = self.scorer.calculate_trade_points(
                    self.current_position['entry_price'],
                    row['Close'],
                    self.current_position['entry_date'],
                    timestamp
                )
                
                # Record trade
                self.trades.append({
                    'entry_date': self.current_position['entry_date'],
                    'exit_date': timestamp,
                    'entry_price': self.current_position['entry_price'],
                    'exit_price': row['Close'],
                    'shares': self.current_position['shares'],
                    'profit': profit,
                    'points': points,
                    'ratio': ratio
                })
                
                logger.info(f"\nSELL: {self.current_position['shares']} shares at ${row['Close']:.2f} (EMA: {row['EMA']:.2f}, Points: {points:.2f})")
                
                self.capital += position_value
                self.current_position = None
        
        # Calculate results including open position
        return self.get_results(backtest_data)
    
    def get_results(self, data):
        """Calculate backtest results using same scoring as ranking client."""
        if not self.trades and self.current_position is None:
            return {
                'initial_capital': self.initial_capital,
                'final_capital': self.capital,
                'total_return': 0,
                'total_trades': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'win_rate': 0,
                'total_points': 0,
                'open_position': False
            }
        
        # Calculate points from completed trades
        points_tally = sum(t['points'] for t in self.trades)
        
        # Add points from open position if exists
        if self.current_position:
            current_value = self.current_position['shares'] * data['Close'].iloc[-1]
            open_profit = current_value - self.current_position['cost']
            open_points, _ = self.scorer.calculate_open_position_points(
                self.current_position['entry_price'],
                data['Close'].iloc[-1],
                self.current_position['entry_date'],
                data.index[-1]
            )
            points_tally += open_points
            portfolio_value = self.capital + current_value
            
            # Print open position details
            logger.info(f"\nStill holding {self.current_position['shares']} shares at end of test period")
            logger.info(f"Entry price: ${self.current_position['entry_price']:.2f}")
            logger.info(f"Final price: ${data['Close'].iloc[-1]:.2f}")
            logger.info(f"Final EMA: {data['EMA'].iloc[-1]:.2f}")
            logger.info(f"Unrealized P&L: ${open_profit:.2f}")
            
        else:
            portfolio_value = self.capital
        
        # Calculate trade statistics
        successful_trades = len([t for t in self.trades if t['profit'] > 0])
        failed_trades = len([t for t in self.trades if t['profit'] <= 0])
        
        return {
            'initial_capital': self.initial_capital,
            'final_capital': portfolio_value,
            'total_return': ((portfolio_value / self.initial_capital) - 1) * 100,
            'total_trades': len(self.trades),
            'successful_trades': successful_trades,
            'failed_trades': failed_trades,
            'win_rate': (successful_trades / len(self.trades) * 100) if self.trades else 0,
            'total_points': points_tally,
            'open_position': self.current_position is not None
        }

def main():
    # Test EMA strategy on AMD
    symbol = 'AMD'
    timeframe = 'daily'  # Start with daily to verify scoring
    
    # Load data
    data_path = f'backtesting/historical_data_daily/{symbol}_historical_data.csv'
    
    try:
        # Load and prepare data
        data = pd.read_csv(data_path)
        data.set_index('timestamp', inplace=True)
        data.index = pd.to_datetime(data.index)
        
        # Ensure column names are uppercase
        column_map = {'close': 'Close', 'high': 'High', 'low': 'Low', 'open': 'Open', 'volume': 'Volume'}
        data = data.rename(columns=column_map)
        
        # Run backtest
        backtester = Backtester(symbol, timeframe)
        results = backtester.run_backtest(data, EMA_indicator)
        
        # Save results
        output_dir = 'backtesting/results'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        output_file = f'{output_dir}/EMA_{symbol}_{timeframe}_results.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=4)
        
        logger.info(f"Results saved to {output_file}")
        
        # Print summary
        print("\nBacktest Results:")
        print(f"Period: {data.index.min()} to {data.index.max()}")
        print(f"Initial Capital: ${results['initial_capital']:,.2f}")
        print(f"Final Portfolio Value: ${results['final_capital']:,.2f}")
        print(f"Total Return: {results['total_return']:.2f}%")
        print(f"Total Trades: {results['total_trades']}")
        print(f"Successful Trades: {results['successful_trades']}")
        print(f"Failed Trades: {results['failed_trades']}")
        print(f"Win Rate: {results['win_rate']:.1f}%")
        print(f"Total Points: {results['total_points']:.2f}")
        print(f"Open Position: {'Yes' if results['open_position'] else 'No'}")
        
    except Exception as e:
        logger.error(f"Error running backtest: {str(e)}")

if __name__ == "__main__":
    from strategies.talib_indicators import EMA_indicator
    main()
