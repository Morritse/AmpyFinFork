from strategies.talib_indicators import KAMA_indicator as strategy
from backtester import Backtester
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    # Use all S&P 100 symbols
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
    ][1:4]
    backtester = Backtester()
    
    strategy_name = strategy.__name__.replace('_indicator', '')
    print(f"\nTesting {strategy_name} Strategy")
    print("=" * 50)
    
    results = []
    total_points = 0
    total_trades = 0
    total_success = 0
    total_failed = 0
    total_minute_decisions = 0  # Add counter for total decisions
    
    for symbol in symbols:
        try:
            # Run backtest
            result = backtester.run(symbol, strategy)
            results.append({'symbol': symbol, **result})
            
            total_points += result['points']
            total_trades += result['trades']
            total_success += result['successful_trades']
            total_failed += result['failed_trades']
            
            # Count minute decisions
            minute_decisions = 0
            daily_data, minute_data = backtester.load_data(symbol)
            daily_backtest, minute_backtest, start_date, end_date = backtester.prepare_data(daily_data, minute_data, strategy)
            daily_groups = minute_backtest.groupby(minute_backtest.index.date)
            for day, day_data in daily_groups:
                minute_decisions += len(day_data)
            total_minute_decisions += minute_decisions
            
            # Print detailed analysis for this symbol
            print(f"\n{strategy_name} Analysis for {symbol}:")
            print(f"Total Trades: {result['trades']}")
            print(f"Total Minute Decisions: {minute_decisions}")
            if result['trades'] > 0:
                print(f"Win Rate: {(result['successful_trades']/result['trades']*100):.1f}%")
            print(f"Points: {result['points']:.2f}")
            print(f"Return: {result['total_return']:.2f}%")
            if result['open_position']:
                print("Has open position")
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {str(e)}")
            continue
    
    # Calculate overall metrics
    if results:
        avg_return = sum(r['total_return'] for r in results) / len(results)
        avg_points = total_points / len(results)
        win_rate = (total_success/total_trades*100) if total_trades > 0 else 0
        
        # Save results
        try:
            with open('backtesting/strategy_scores.json', 'r') as f:
                scores = json.load(f)
        except FileNotFoundError:
            scores = {}
            
        scores[f'{strategy_name}_indicator'] = {
            'total_points': total_points,
            'average_points': avg_points,
            'win_rate': win_rate,
            'total_trades': total_trades,
            'average_return': avg_return,
            'total_minute_decisions': total_minute_decisions
        }
        
        with open('backtesting/strategy_scores.json', 'w') as f:
            json.dump(scores, f, indent=4)
            
        # Print summary
        print("\nStrategy Summary:")
        print("=" * 100)
        print(f"{'Symbol':<6} {'Initial':<12} {'Final':<12} {'Return %':<10} {'Points':<8} {'Trades':<6} {'Success':<8} {'Failed':<6}")
        print("-" * 100)
        
        for r in results:
            print(f"{r['symbol']:<6} ${50000:<11,.2f} ${r['portfolio_value']:<11,.2f} "
                  f"{r['total_return']:>9.2f}% {r['points']:>7.2f} {r['trades']:>6d} "
                  f"{r['successful_trades']:>8d} {r['failed_trades']:>6d}")
        
        print("-" * 100)
        print(f"\nOverall Performance:")
        print(f"Total Points: {total_points:.2f}")
        print(f"Average Points per Stock: {avg_points:.2f}")
        print(f"Total Trades: {total_trades}")
        print(f"Win Rate: {win_rate:.1f}% ({total_success}/{total_trades})")
        print(f"Average Return: {avg_return:.2f}%")
        print(f"Total Minute Decisions: {total_minute_decisions}")
    else:
        print("\nNo valid results to display")

if __name__ == "__main__":
    main()
