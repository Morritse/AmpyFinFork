from scoring import StrategyScorer
from datetime import datetime, timedelta

def test_scoring():
    print("\nTesting Strategy Scoring Logic")
    print("=" * 50)
    
    # Initialize scorer
    scorer = StrategyScorer(initial_capital=50000)
    
    print("\nTesting Point Calculations:")
    print("-" * 30)
    
    # Test cases from ranking_client.py logic:
    test_cases = [
        # Profitable trades
        (1.03, "3% gain"),    # < 5% gain: 1 point * time_delta
        (1.07, "7% gain"),    # 5-10% gain: 1.5 points * time_delta
        (1.15, "15% gain"),   # > 10% gain: 2 points * time_delta
        
        # Losing trades
        (0.98, "2% loss"),    # < 2.5% loss: -1 point * time_delta
        (0.96, "4% loss"),    # 2.5-5% loss: -1.5 points * time_delta
        (0.93, "7% loss"),    # > 5% loss: -2 points * time_delta
    ]
    
    for ratio, desc in test_cases:
        points = scorer.calculate_points(ratio)
        print(f"\n{desc} (ratio={ratio:.3f}):")
        print(f"Points: {points:.2f} (time_delta={scorer.time_delta:.2f})")
        print(f"Expected multiplier: {get_expected_multiplier(ratio):.1f}")
    
    # Test time_delta progression
    print("\nTesting Time Delta Progression:")
    print("-" * 30)
    
    # Track points for same trade over multiple days
    ratio = 1.12  # 12% gain
    days = 5
    
    for day in range(days):
        points = scorer.calculate_points(ratio)
        print(f"\nDay {day + 1}:")
        print(f"time_delta: {scorer.time_delta:.2f}")
        print(f"12% gain points: {points:.2f}")
        scorer.increment_day()
    
    # Test final score calculation
    print("\nTesting Final Score Calculation:")
    print("-" * 30)
    
    test_portfolios = [
        (50000, 10.0),  # Break even with 10 points
        (55000, 10.0),  # 10% gain with 10 points
        (45000, 10.0),  # 10% loss with 10 points
        (50000, -5.0),  # Break even with -5 points
    ]
    
    for portfolio_value, points_tally in test_portfolios:
        score = scorer.calculate_strategy_score(portfolio_value, points_tally)
        pnl = ((portfolio_value / 50000) - 1) * 100
        print(f"\nPortfolio: ${portfolio_value:,.2f} ({pnl:+.1f}%)")
        print(f"Points Tally: {points_tally:.1f}")
        print(f"Score: {score:.2f}")
        print(f"Components:")
        print(f"  - Points component: {points_tally/10:.2f}")
        print(f"  - Portfolio component: {(portfolio_value/50000 * 2):.2f}")

def get_expected_multiplier(ratio):
    """Return the expected point multiplier based on ranking_client.py logic."""
    if ratio > 1:  # Profitable trade
        if ratio < 1.05:      return 1.0   # 0-5% gain
        elif ratio < 1.1:     return 1.5   # 5-10% gain
        else:                 return 2.0   # >10% gain
    else:  # Losing trade
        if ratio > 0.975:     return -1.0  # 0-2.5% loss
        elif ratio > 0.95:    return -1.5  # 2.5-5% loss
        else:                 return -2.0  # >5% loss

if __name__ == "__main__":
    test_scoring()
