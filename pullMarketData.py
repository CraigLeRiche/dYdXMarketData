from dydx3 import Client
from dydx3.constants import *
from web3 import Web3
import pandas as pd
import numpy as np
import datetime as dt

client = Client(
    host='https://api.dydx.exchange'
)

def pullPrices():
    #Returns a dataframe with rows indexPrice, oraclePrice and columns trading pairs
    markets = client.public.get_markets()
    mkts = pd.DataFrame(markets.data['markets'])
    mkts.insert(0, 'TimeStampGMT', pd.to_datetime('now', utc=True).replace(microsecond=0))
    return pd.DataFrame([mkts.loc['indexPrice'], mkts.loc['oraclePrice']])

def pullOI(mkt):
    #Returns the open interest in each trading pair
    markets = client.public.get_markets()
    mkts = pd.DataFrame({'openInterest':markets.data["markets"][mkt]["openInterest"]}, index=[pd.to_datetime(dt.datetime.now())])
    return mkts

def pullOrderBookDepth(mkt, side):
    orderbook = client.public.get_orderbook(
        market=mkt,
    )
    odbk = np.transpose(pd.DataFrame(orderbook.data[side]))
    ord = pd.DataFrame(odbk)
    ord.insert(0, 'TimeStampGMT', pd.to_datetime('now', utc=True).replace(microsecond=0))
    return ord

def pullTrades(mkt):
    all_trades = client.public.get_trades(
        market=mkt,
    )
    trades = pd.DataFrame(all_trades.data['trades'])
    return trades

def pullInsurance():
    balance = client.public.get_insurance_fund_balance()
    bal_dict = balance.data
    bal = pd.DataFrame(bal_dict, index=[pd.to_datetime(dt.datetime.now())])
    return bal

def pullFunding(mkt):
    markets = client.public.get_markets()
    funding = pd.DataFrame({'nextFundingRate':markets.data["markets"][mkt]["nextFundingRate"]}, index=[pd.to_datetime(dt.datetime.now())])
    return funding

def highFreqRun():
    global trades_BTC
    global trades_ETH
    trades_BTC = pd.concat([trades_BTC, pullTrades('BTC-USD')]).drop_duplicates()
    trades_ETH = pd.concat([trades_ETH, pullTrades('ETH-USD')]).drop_duplicates()

def pullPrices(mkt):
    #Returns a dataframe with rows indexPrice, oraclePrice and columns trading pairs
    markets = client.public.get_markets()
    mkts = pd.DataFrame({'indexPrice':markets.data["markets"][mkt]["indexPrice"], 'oraclePrice': markets.data["markets"][mkt]["oraclePrice"]}, index=[pd.to_datetime(dt.datetime.now())])
    return mkts