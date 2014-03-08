#!/usr/bin/python3.2

import pymongo
from datetime import datetime
from decimal import *
import json
from pymongo import MongoClient
import pprint
import math

# Slices the given list down to the top n% only
def confidence_interval_top(data, interval):
    new_length = math.ceil(len(data) * interval)
    
    if (new_length == len(data)):
        return data
    
    return data[: new_length]

# Calculates weighted mean from a set of market depth data
def weighted_mean(depth):
    total = 0
    weight = 0
    
    for record in depth:
        total += (record[0] * record[1])
        weight += record[1]
    
    if (weight == Decimal(0)):
        return weight
    else:
        return total / weight
    
def clean_book(book_data, market_price, bid_currency, quote_currency):
    start_time = None

    for record in market_price.find({
        "bid_currency": bid_currency,
        "quote_currency": quote_currency
        }).sort("time", -1):
        start_time = record['time']
        break

    if start_time is None:
        for record in book_data.find({
        "bid_currency": bid_currency,
        "quote_currency": quote_currency
        }).sort("time", 1):
            start_time = record['time']
            break

    if start_time is None:
        print ("No data to clean.")
        exit
        
    time_points = []
    
    for time_point in book_data.find(
        {
            "bid_currency": bid_currency,
            "quote_currency": quote_currency,
            "time": {"$gte": start_time}
        },
        ["time"]
    ):
        time_points.append(time_point['time'])
        
    pp = pprint.PrettyPrinter(indent=4)
    
    for time_point in set(time_points):
        collated_asks = []
        collated_bids = []
    
        for record in book_data.find({
            "bid_currency": bid_currency,
            "quote_currency": quote_currency,
            "time": time_point}, ["bids", "asks"]):
            if (len(record['bids']) > 0
                and len(record['asks']) > 0):
                max_bid = record['bids'][0][0]
                min_ask = record['asks'][0][0]
                if (max_bid > min_ask):
                    # Warn the user about mangled data?
                    continue
            
            for bid in record['bids']:
                collated_bids.append([Decimal(bid[0]), Decimal(bid[1])])
                
            for ask in record['asks']:
                collated_asks.append([Decimal(ask[0]), Decimal(ask[1])])
            
                
        # Get naive bid, ask, mid
        collated_asks = sorted(collated_asks, key=lambda ask: ask[0])
        collated_bids = sorted(collated_bids, key=lambda bid: bid[0], reverse=True)
        
        if (len(collated_asks) > 0):
            simple_ask = collated_asks[0][0]
        else:
            simple_ask = None
            
        if (len(collated_bids) > 0):
            simple_bid = collated_bids[0][0]
        else:
            simple_bid = None
            
        if (simple_ask is None or simple_bid is None):
            simple_mid = None
        else:
            simple_mid = ((simple_ask + simple_bid) / 2).quantize(Decimal('.00000001'), rounding=ROUND_HALF_UP)
            
        # Cut very low volumes
        collated_asks = sorted(collated_asks, key=lambda ask: ask[1])
        collated_bids = sorted(collated_bids, key=lambda bid: bid[1])
        
        collated_asks = confidence_interval_top(collated_asks, 0.95)
        collated_bids = confidence_interval_top(collated_bids, 0.95)
            
        collated_asks = sorted(collated_asks, key=lambda ask: ask[0])
        collated_bids = sorted(collated_bids, key=lambda bid: bid[0], reverse=True)
        
        if (len(collated_asks) > 0):
            filtered_ask = collated_asks[0][0]
        else:
            filtered_ask = None
            
        if (len(collated_bids) > 0):
            filtered_bid = collated_bids[0][0]
        else:
            filtered_bid = None
        
        # Calculate mid from bid/ask if available
        if (filtered_ask is None or filtered_bid is None):
            filtered_mid = None
        else:
            filtered_mid = ((filtered_ask + filtered_bid) / 2).quantize(Decimal('.00000001'), rounding=ROUND_HALF_UP)        
        
        # Assemble the price record to be inserted into the database
        price = {"bid_currency": bid_currency,
	    "quote_currency": quote_currency,
            "time": time_point,
            "ask": str(simple_ask),
            "bid": str(simple_bid),
            "mid": str(simple_mid),
            "filtered_ask": str(filtered_ask),
            "filtered_bid": str(filtered_bid)}
            
        if (simple_mid is not None):
            price["simple_mid"] = str(simple_mid)
            price["filtered_mid"] = str(filtered_mid)
            
        market_price.insert(price)

client = MongoClient()

market_data_db = client.market_data
book_data = market_data_db.book
market_price = market_data_db.market_price


book_data.ensure_index(
    [
        ("bid_currency", pymongo.DESCENDING),
        ("quote_currency", pymongo.DESCENDING)
    ])
book_data.ensure_index("time")
market_price.ensure_index(
    [
        ("bid_currency", pymongo.DESCENDING),
        ("quote_currency", pymongo.DESCENDING)
    ])
market_price.ensure_index("time")

clean_book(book_data, market_price, "DOGE", "BTC")
clean_book(book_data, market_price, "DOGE", "USD")
clean_book(book_data, market_price, "VTC", "BTC")

