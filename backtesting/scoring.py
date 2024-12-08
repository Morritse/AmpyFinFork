class StrategyScorer:
    def __init__(self, initial_capital=50000):
        self.initial_capital = initial_capital
        self.time_delta = 1.0  # Starts at 1.0 and increases by 0.01 each day
        
    def calculate_points(self, price_change_ratio):
        """
        Calculate points based on price change ratio and time delta.
        From ranking_client.py scoring logic.
        """
        if price_change_ratio > 1:  # Profitable trade
            if price_change_ratio < 1.05:      # 0-5% gain: 1 point * time_delta
                points = self.time_delta * 1
            elif price_change_ratio < 1.1:     # 5-10% gain: 1.5 points * time_delta
                points = self.time_delta * 1.5
            else:                              # >10% gain: 2 points * time_delta
                points = self.time_delta * 2
        else:  # Losing trade
            if price_change_ratio > 0.975:     # 0-2.5% loss: -1 point * time_delta
                points = -self.time_delta * 1
            elif price_change_ratio > 0.95:    # 2.5-5% loss: -1.5 points * time_delta
                points = -self.time_delta * 1.5
            else:                              # >5% loss: -2 points * time_delta
                points = -self.time_delta * 2
        return points
    
    def increment_day(self):
        """Increment time_delta by 0.01 for each trading day."""
        self.time_delta += 0.01
    
    def calculate_trade_points(self, entry_price, exit_price, entry_date, exit_date):
        """
        Calculate points for a completed trade.
        
        Args:
            entry_price: Buy price
            exit_price: Sell price
            entry_date: Buy date (datetime)
            exit_date: Sell date (datetime)
            
        Returns:
            tuple: (points, price_change_ratio)
        """
        price_change_ratio = exit_price / entry_price
        points = self.calculate_points(price_change_ratio)
        return points, price_change_ratio
    
    def calculate_strategy_score(self, portfolio_value, points_tally):
        """
        Calculate final strategy score using ranking formula.
        From ranking_client.py scoring logic.
        
        Score = (points_tally/10 + (portfolio_value/50000 * 2))
        """
        return (points_tally/10 + (portfolio_value/self.initial_capital * 2))
    
    def calculate_open_position_points(self, entry_price, current_price, entry_date, current_date):
        """
        Calculate points for an open position.
        Same logic as closed trades, but using current price as exit price.
        """
        return self.calculate_trade_points(entry_price, current_price, entry_date, current_date)

# Example usage:
if __name__ == "__main__":
    # Example trades
    scorer = StrategyScorer(initial_capital=50000)
    points_tally = 0
    
    # Trade 1: 5% gain after 3 days
    scorer.increment_day()  # Day 1
    scorer.increment_day()  # Day 2
    scorer.increment_day()  # Day 3
    points, ratio = scorer.calculate_trade_points(100, 105, None, None)
    points_tally += points
    print(f"Trade 1 (5% gain): {points:.2f} points (ratio: {ratio:.3f}, time_delta: {scorer.time_delta:.2f})")
    
    # Trade 2: 15% gain after 5 more days
    for _ in range(5):
        scorer.increment_day()
    points, ratio = scorer.calculate_trade_points(100, 115, None, None)
    points_tally += points
    print(f"Trade 2 (15% gain): {points:.2f} points (ratio: {ratio:.3f}, time_delta: {scorer.time_delta:.2f})")
    
    # Open position: 7% gain so far
    for _ in range(3):
        scorer.increment_day()
    points, ratio = scorer.calculate_open_position_points(100, 107, None, None)
    points_tally += points
    print(f"Open Position (7% gain): {points:.2f} points (ratio: {ratio:.3f}, time_delta: {scorer.time_delta:.2f})")
    
    # Calculate final score
    portfolio_value = 55000  # 10% return
    final_score = scorer.calculate_strategy_score(portfolio_value, points_tally)
    print(f"\nFinal Results:")
    print(f"Points Tally: {points_tally:.2f}")
    print(f"Portfolio Value: ${portfolio_value:,.2f}")
    print(f"Final Score: {final_score:.2f}")
