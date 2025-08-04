**These scripts create the json file to upgrade markets (e.g. from ISOLATED to CROSS, and/or change the liquidity tier)**<br>
Put all files in a directory and then run ```upgradecrossmarkets.sh```.
<br>
<br>
See ```liquiditytiers.txt``` for a complete list of all liquidity tiers.

Syntax: ```./delistmarkets.sh <markettuple1> <markettuple2> ...```
where ```<markettuple>``` is a colon-separated tuple consisting of: ```market:liquiditytier:cross_y/n```

For example: ```ZORA-USD:2:y``` means
- ZORA-USD market
- 2 is the new liquidity tier for Long-Tail (currently 7 for IML 5x)
- y means upgrade it from ISOLATED to CROSS

and ```BONK-USD:1:n``` means:
- BONK-USD market
- 1 is the new liquidity tier for Small-Cap (currently 2 for Long-Tail)
- n means don't upgrade it (because it is already CROSS)

Example: ```./upgradecrossmarkets.sh ZORA-USD:2:y BONK-USD:1:n```

https://github.com/user-attachments/assets/b5068f47-882c-4780-906f-4ba168db3a0b

