import requests


order_book = requests.get("https://fapi.binance.com/fapi/v1/depth?symbol=btcusdt&limit=10").json()
bids = dict(order_book["bids"])
bids = dict([a, float(x)] for a,x in bids.items())


asks = dict(order_book["asks"])
asks = dict([a, float(x)] for a,x in asks.items())
bids["5000"] = 20
res = {}
for a in bids:
    try:
        res[a] = bids[a] - asks[a]
    except:
        res[a] = bids[a]

for a in asks:
    try:
        if bids[a]:
            pass
    except:
        res[a] = -asks[a]


print(bids)
print(asks)
print(res)