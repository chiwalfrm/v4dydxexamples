**Instructions**

1. Run ```v4dydxget_all_fills3.py``` with the dYdXChain address, the market, and the starting block number to get all fills for that market from that block number to now.  Send the output to a file.

```python3 v4dydxget_all_fills3.py dydx1c280kjc2m5384mlzgf32wm4qxkmuqak9td0290 BTC-USD 58412676 > /tmp/transactions.csv```

2. Then run ```v4dydxcalculatepnl.py``` with that file, and the current price for the market, and it will report the realized and unrealized PnL as well as the total fees paid using FIFO (First In, First Out) methodology.  The current price is needed to calculate the unrealized PnL (for the open Position).

```python3 v4dydxcalculatepnl.py /tmp/transactions.csv 90000```

**Demo**

```$ python3 v4dydxget_all_fills3.py dydx1c280kjc2m5384mlzgf32wm4qxkmuqak9td0290 BTC-USD 58412676 > /tmp/transactions.csv
$ python3 v4dydxcalculatepnl.py /tmp/transactions.csv 90000
Open Position Remaining: 0.00000000 BTC
Average Open Price: $129194835313394843648.00
Realized PnL (FIFO): $-9021.27
Unrealized PnL (FIFO, using latest price $90000.00): $-1532.22
Total Fees: $278.98```

***End***
