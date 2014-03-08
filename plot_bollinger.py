#!/usr/bin/python3.2

import pymongo
from datetime import datetime, timedelta
from decimal import *
from pymongo import MongoClient
import pprint
import math
import pygal

def daterange(start_time, end_time, bar_period_minutes):
    interval = end_time - start_time
    interval_minutes = interval.days * 24 * 60 + math.floor(interval.seconds / 60)

    for n in range(int(interval_minutes / bar_period_minutes)):
        yield start_time + timedelta(0, 0, 0, 0, n * bar_period_minutes)

def get_periodic_price_data(market_price, bid_currency, quote_currency,
    start_time, end_time, bar_period_minutes):
    bar_period = timedelta(0, 0, 0, 0, bar_period_minutes)
    prices = []
    period_start = start_time
    period_end = start_time + bar_period
    
    for price in market_price.find(
        {
            "bid_currency": bid_currency,
            "quote_currency": quote_currency,
            "time": {"$gte": start_time},
            "time": {"$lte": end_time}
        },
        {
            "time": 1,
            "filtered_mid": 1
        }
    ):
        prices.append(float(price["filtered_mid"]))
            
    return prices
    
def plot_bollinger_bands(market_price, bid_currency, quote_currency,
    bar_period_minutes, sample_size_bars, graph_length_bars):
    end_time = datetime.utcnow()
    end_time = datetime(end_time.year, end_time.month, end_time.day,
        end_time.hour, end_time.minute - (end_time.minute % bar_period_minutes))
    start_time = end_time - timedelta(0, 0, 0, 0, bar_period_minutes * graph_length_bars)
    
    prices = get_periodic_price_data(market_price, bid_currency, quote_currency,
        start_time, end_time, bar_period_minutes)
    
    line_chart = pygal.Line()
    line_chart.title = bid_currency + '/' + quote_currency + ' Rate'
    line_chart.x_labels = map(str, daterange(start_time, end_time, bar_period_minutes))
    line_chart.add(bid_currency + '/' + quote_currency, prices)
    line_chart.render_to_file('bar_chart.svg')


client = MongoClient()

market_data_db = client.market_data
market_price = market_data_db.market_price

# Plot DOGE/BTC in 4 minute data, sampling Bollinger Bands from 40 data points, 
# across a half-day (12 hours) range
plot_bollinger_bands(market_price, "DOGE", "BTC", 4, 40, (int)(12 * 60 / 4))


