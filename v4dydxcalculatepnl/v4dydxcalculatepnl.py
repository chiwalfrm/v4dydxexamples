import sys
from decimal import Decimal, getcontext
from collections import deque
from datetime import datetime

getcontext().prec = 28

def main():
    if len(sys.argv) != 3:
        print("Usage: python pnl_calculator.py transactions.csv current_price")
        sys.exit(1)

    filename = sys.argv[1]
    current_price = Decimal(sys.argv[2])

    trades = []
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) != 7:
                continue
            ts, market, side, size_str, price_str, fee_str, order_id = parts
            # Parse timestamp (handle Z as UTC)
            timestamp = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            size = Decimal(size_str)
            price = Decimal(price_str)
            fee = Decimal(fee_str)
            trades.append((timestamp, market, side.upper(), size, price, fee, order_id))

    if not trades:
        open_position = Decimal('0')
        average_open_price = Decimal('0')
        realized_pnl = Decimal('0')
        unrealized_pnl = Decimal('0')
        total_fees = Decimal('0')
        market = 'BTC-USD'
    else:
        # Sort by timestamp ascending (oldest first for FIFO)
        trades.sort(key=lambda t: t[0])
        market = trades[0][1]  # Assume all rows have same market

        open_lots = deque()  # deque of (signed_qty, entry_price)
        realized_pnl = Decimal('0')
        total_fees = Decimal('0')

        for trade in trades:
            _, _, side, size, price, fee, _ = trade
            total_fees += fee

            signed_qty = size if side == 'BUY' else -size
            remaining = signed_qty

            while open_lots and remaining != 0:
                oldest_qty, oldest_price = open_lots[0]
                # Same direction? Stop closing
                same_direction = (remaining > 0 and oldest_qty > 0) or (remaining < 0 and oldest_qty < 0)
                if same_direction:
                    break

                # Opposite direction → close
                close_amt = min(abs(remaining), abs(oldest_qty))

                if oldest_qty > 0:  # Closing long
                    pnl = (price - oldest_price) * close_amt
                else:  # Closing short
                    pnl = (oldest_price - price) * close_amt

                realized_pnl += pnl

                # Update oldest lot
                if oldest_qty > 0:
                    oldest_qty -= close_amt
                else:
                    oldest_qty += close_amt

                if oldest_qty == 0:
                    open_lots.popleft()
                else:
                    open_lots[0] = (oldest_qty, oldest_price)

                # Update remaining trade quantity
                if remaining > 0:
                    remaining -= close_amt
                else:
                    remaining += close_amt

            # If anything left, open new lot in this direction
            if remaining != 0:
                open_lots.append((remaining, price))

        # Compute final position (signed)
        open_position = sum(q for q, _ in open_lots) if open_lots else Decimal('0')

        if open_position == 0:
            average_open_price = Decimal('0')
            unrealized_pnl = Decimal('0')
        else:
            abs_total_qty = abs(open_position)
            # Weighted average entry price (all lots same direction)
            average_open_price = sum(abs(q) * p for q, p in open_lots) / abs_total_qty
            # Unified unrealized P&L formula works for both long and short
            unrealized_pnl = open_position * (current_price - average_open_price)

    # Exact output format required
    print(f"Open Position Remaining: {float(open_position):.8f} {market}")
    print(f"Average Open Price: ${float(average_open_price):.2f}")
    print(f"Realized PnL (FIFO): ${float(realized_pnl):.2f}")
    print(f"Unrealized PnL (FIFO, using latest price ${float(current_price):.2f}): ${float(unrealized_pnl):.2f}")
    print(f"Total Fees: ${float(total_fees):.2f}")

if __name__ == "__main__":
    main()
