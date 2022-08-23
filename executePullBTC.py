import pullMarketData
import pandas as pd
from dydx3 import Client
import schedule
import numpy as np
import time
import datetime as dt

# Run the global parameter insurance fund balance from here
market = 'BTC-USD'

# only need for ETH, BTC markets
def highFreqRun(mkt=market):
    global tradesBTC
    tradesBTC = pd.concat([tradesBTC, pullMarketData.pullTrades(mkt)]).drop_duplicates()

def mediumFreqRun(mkt=market):
    global insurFund
    global openInterestBTC
    global pricesBTC
    global marketsInterest
    insurFund = pd.concat([insurFund, pullMarketData.pullInsurance()])
    openInterestBTC = pd.concat([openInterestBTC, pullMarketData.pullOI(mkt)])
    pricesBTC = pd.concat([pricesBTC, pullMarketData.pullPrices(mkt)])

def lowFreqRun(mkt=market):
    global tradesBTC
    global insurFund
    global openInterestBTC
    global bidsBTC
    global asksBTC
    global pricesBTC
    global fundingBTC
    fundingBTC = pd.concat([fundingBTC, pullMarketData.pullFunding(mkt)])
    bidsBTC = pd.concat([bidsBTC, pullMarketData.pullOrderBookDepth(mkt, 'bids')])
    asksBTC = pd.concat([asksBTC, pullMarketData.pullOrderBookDepth(mkt, 'asks')])
    openInterestBTC.to_csv('openInterestBTC.csv')
    pricesBTC.to_csv('PricesBTC.csv')
    tradesBTC.to_csv('tradesBTC.csv')
    insurFund.to_csv('insurFund.csv')
    bidsBTC.to_csv('bidsBTC.csv')
    asksBTC.to_csv('asksBTC.csv')
    fundingBTC.to_csv('fundingBTC.csv')

#initialise dataframes
pricesBTC = pd.DataFrame()
tradesBTC = pd.DataFrame()
insurFund = pd.DataFrame()
openInterestBTC = pd.DataFrame()
bidsBTC = pd.DataFrame()
asksBTC = pd.DataFrame()
fundingBTC = pd.DataFrame()

#edit this in ETH, BTC vs others
schedule.every(30).seconds.do(highFreqRun)
schedule.every(5).minutes.do(mediumFreqRun)
schedule.every(30).minutes.do(lowFreqRun)

while True:
    schedule.run_pending()
    time.sleep(1)