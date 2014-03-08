#!/usr/bin/python3.2

import pymongo
from datetime import datetime
import json
from os import listdir, remove
from os.path import isdir, isfile, join
from pymongo import MongoClient
import pprint

class ExchangeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def importCryptsy(bid_currency, quote_currency, book_data, content, file_time):
    if (content['success'] != 1):
        raise ExchangeError('Unsuccessful response from Cryptsy')

    if (not isinstance(content['return'], dict)):
        raise ExchangeError('No object in response from Cryptsy')

    market_data = content['return'][bid_currency]
    asks = []
    bids = []
    
    if (isinstance(market_data['sellorders'], list)):
        for order in market_data['sellorders']:
    	    asks.append([order['price'], order['quantity']])
            
    if (isinstance(market_data['buyorders'], list)):
        for order in market_data['buyorders']:
        	bids.append([order['price'], order['quantity']])
    
    book = {"bid_currency": bid_currency,
	"quote_currency": quote_currency,
        "exchange": "Cryptsy",
        "time": file_time,
        "asks": asks,
        "bids": bids}
    book_data.insert(book)

def importVircurex(bid_currency, quote_currency, book_data, content, file_time):
    book = {"bid_currency": bid_currency,
        "quote_currency": quote_currency,
        "exchange": "Vircurex",
        "time": file_time,
        "asks": content["asks"],
        "bids": content["bids"]}
    book_data.insert(book)

client = MongoClient()

market_data_db = client.market_data

imported_files = market_data_db.imported_files
book_data = market_data_db.book

base_dir = "/home/jrn/cryptocurrency_data/vtc_book_data"

pp = pprint.PrettyPrinter(indent=4)

for hour in [ d for d in listdir(base_dir) if isdir(join(base_dir, d)) ]:
    hour_dir = join(base_dir, hour)

    for exchange in [ d for d in listdir(hour_dir) if isdir(join(hour_dir, d)) ]:
        exchange_dir = join(hour_dir, exchange)

        for data_file in [ f for f in listdir(exchange_dir) if isfile(join(exchange_dir, f)) ]:
            file_path = join(exchange_dir, data_file)
            file_time = datetime.strptime(data_file, "%Y-%m-%dT%H:%M+0000.json")

            existing_file = imported_files.find_one({"market": "VTC/BTC",
                "exchange": exchange,
                "filename": data_file})

            if (existing_file):
                print("File " + file_path + " already imported.")
                continue

            imported_file = {"market": "VTC/BTC",
                "exchange": exchange,
                "filename": data_file}

            try:
                with open(file_path, 'r') as f:
                    content = json.load(f)
            except ValueError:
               print ("File " + file_path + " contains is not valid JSON.")
               remove(file_path)
               continue

            try:
                object_id = imported_files.insert(imported_file)
                if (exchange == "Cryptsy"):
                    importCryptsy("VTC", "BTC", book_data, content, file_time)
                elif (exchange == "Vircurex"):
                    importVircurex("VTC", "BTC", book_data, content, file_time)
            except KeyError as e:
                print ("File " + file_path + " is invalid, missing key: " + str(e))
                continue
            except ExchangeError:
                print ("File " + file_path + " is not a valid dataset.")
                continue
