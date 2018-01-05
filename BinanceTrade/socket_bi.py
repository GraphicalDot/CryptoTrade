#!/usr/bin/python

from binance.client import Client
import os
import pymongo
from binance.websockets import BinanceSocketManager

connection = pymongo.MongoClient()
db = connection["crypto_trade"]




client = Client(os.environ['binance_api_key'], os.environ['binance_secret_key'])

prices = client.get_all_tickers()
streams = []
for e in prices:
    if e["symbol"].endswith("BTC"):
        f = (e["symbol"].lower()+ "@aggTrade")
        streams.append(f)
print (streams)

def process_message(msg):
    print("message type:{}".format(msg['e']))
    print(msg)
    
def process_message_multiplex(msg):
    print("stream:{}data:{}".format(msg['stream'], msg['data']))
    collection = db[msg['data']['s']]
    collection.insert(msg['data'])
    print (msg['data'])
# do something
bm = BinanceSocketManager(client)
#bm.start_aggtrade_socket(symbol='BNBBTC', callback=process_message)
bm.start_multiplex_socket(streams, process_message_multiplex)
#conn_key = bm.start_kline_socket(streams, process_message)
bm.start()


if __name__ == "__main__":
    pass


