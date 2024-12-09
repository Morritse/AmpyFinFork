import json
from pymongo import MongoClient
from config import mongo_url
from datetime import datetime

def update_strategy_scores():
    try:
        # Load backtest results
        with open('backtesting/strategy_scores_unified.json', 'r') as f:
            backtest = json.load(f)

        # Connect to MongoDB
        client = MongoClient(mongo_url)
        db = client.trading_simulator
        points_tally = db.points_tally

        # Clear existing points
        points_tally.delete_many({})

        # Prepare new documents
        now = datetime.now()
        new_docs = [
            {
                'strategy': strategy,
                'total_points': float(data['total_points']),
                'initialized_date': now,
                'last_updated': now
            }
            for strategy, data in backtest.items()
        ]

        # Insert new documents
        points_tally.insert_many(new_docs)

        # Print verification
        print('Updated points_tally with backtest scores\n')
        print('All Strategies Ranked by Points:')
        print('-' * 60)
        print(f"{'Strategy':<40} {'Points':>15} {'Win Rate':>10}")
        print('-' * 60)

        # Sort and print all strategies
        sorted_strategies = sorted(backtest.items(), key=lambda x: x[1]['total_points'], reverse=True)
        for strategy, data in sorted_strategies:
            points = data['total_points']
            win_rate = data.get('win_rate', 0)  # Some strategies might not have win_rate
            print(f"{strategy:<40} {points:>15.2f} {win_rate:>10.2f}")

        # Print summary stats
        total_strategies = len(new_docs)
        positive_strategies = sum(1 for doc in new_docs if doc['total_points'] > 0)
        negative_strategies = sum(1 for doc in new_docs if doc['total_points'] < 0)
        zero_strategies = sum(1 for doc in new_docs if doc['total_points'] == 0)

        print('\nSummary:')
        print(f'Total Strategies: {total_strategies}')
        print(f'Positive Performing: {positive_strategies}')
        print(f'Negative Performing: {negative_strategies}')
        print(f'Zero Performing: {zero_strategies}')

    except Exception as e:
        print(f"Error updating scores: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    update_strategy_scores()
