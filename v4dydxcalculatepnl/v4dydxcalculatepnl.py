import pandas as pd
from collections import deque
import sys

filename = sys.argv[1]
latest_price = float(sys.argv[2])
# Load the CSV data from transactions.csv without headers
df = pd.read_csv(filename, header=None)

# Assign column names
df.columns = ['createdAt', 'market', 'side', 'size', 'price', 'fee', 'id']

# Ensure correct data types
df['createdAt'] = pd.to_datetime(df['createdAt'])
df['size'] = df['size'].astype(float)
df['price'] = df['price'].astype(float)
df['fee'] = df['fee'].astype(float)

# Initialize variables
buy_queue = deque()  # To store buy transactions (quantity, price) for FIFO
total_fees = 0
realized_pnl = 0
open_position = 0

# Process transactions in chronological order
for _, row in df.sort_values('createdAt').iterrows():
    action = row['side']
    qty = row['size']
    price = row['price']
    fee = row['fee']

    # Add fee to total
    total_fees += fee

    if action == 'BUY':
        # Add buy to queue (FIFO: track quantity and price)
        buy_queue.append((qty, price))
        open_position += qty
    elif action == 'SELL':
        # Process sell using FIFO
        sell_qty = qty
        open_position -= qty

        while sell_qty > 0 and buy_queue:
            buy_qty, buy_price = buy_queue[0]
            if buy_qty <= sell_qty:
                # Use up entire buy lot
                realized_pnl += buy_qty * (price - buy_price)
                sell_qty -= buy_qty
                buy_queue.popleft()
            else:
                # Partially use buy lot
                realized_pnl += sell_qty * (price - buy_price)
                buy_queue[0] = (buy_qty - sell_qty, buy_price)
                sell_qty = 0

# Calculate unrealized PnL and average open price for remaining open position
# Use the latest price in the dataset for current market price
unrealized_pnl = 0
total_cost = 0
for qty, buy_price in buy_queue:
    unrealized_pnl += qty * (latest_price - buy_price)
    total_cost += qty * buy_price

# Calculate average open price (weighted by quantity)
average_open_price = total_cost / open_position if open_position > 0 else 0

# Output results
print(f"Open Position Remaining: {open_position:.8f} BTC")
print(f"Average Open Price: ${average_open_price:.2f}")
print(f"Realized PnL (FIFO): ${realized_pnl:.2f}")
print(f"Unrealized PnL (FIFO, using latest price ${latest_price:.2f}): ${unrealized_pnl:.2f}")
print(f"Total Fees: ${total_fees:.2f}")
