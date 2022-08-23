import pullMarketData
import pandas as pd
from dydx3 import Client
import schedule
import numpy as np
import time
import datetime as dt

def mediumFreqRun():
    global tradesCELO
    global tradesLINK
    global tradesDOGE
    global trades1INCH
    global tradesXMR
    global tradesFIL
    global tradesAAVE
    global tradesATOM
    global tradesMKR
    global tradesEOS
    global tradesCOMP
    global tradesALGO
    global tradesXTZ
    global tradesUNI
    global tradesADA
    global tradesZRX
    global tradesYFI
    global tradesMATIC
    global tradesETC
    global tradesAVAX
    global tradesLTC
    global tradesENJ
    global tradesDOT
    global tradesSNX
    global tradesRUNE
    global tradesXLM
    global tradesBCH
    global tradesTRX
    global tradesUMA
    global tradesNEAR
    global tradesZEC
    global tradesSOL
    global tradesSUSHI
    global tradesICP
    global tradesCRV

    global openInterestCELO
    global openInterestLINK
    global openInterestDOGE
    global openInterest1INCH
    global openInterestXMR
    global openInterestFIL
    global openInterestAAVE
    global openInterestATOM
    global openInterestMKR
    global openInterestEOS
    global openInterestCOMP
    global openInterestALGO
    global openInterestXTZ
    global openInterestUNI
    global openInterestADA
    global openInterestZRX
    global openInterestYFI
    global openInterestMATIC
    global openInterestETC
    global openInterestAVAX
    global openInterestLTC
    global openInterestENJ
    global openInterestDOT
    global openInterestSNX
    global openInterestRUNE
    global openInterestXLM
    global openInterestBCH
    global openInterestTRX
    global openInterestUMA
    global openInterestNEAR
    global openInterestZEC
    global openInterestSOL
    global openInterestSUSHI
    global openInterestICP
    global openInterestCRV

    global pricesCELO
    global pricesLINK
    global pricesDOGE
    global prices1INCH
    global pricesXMR
    global pricesFIL
    global pricesAAVE
    global pricesATOM
    global pricesMKR
    global pricesEOS
    global pricesCOMP
    global pricesALGO
    global pricesXTZ
    global pricesUNI
    global pricesADA
    global pricesZRX
    global pricesYFI
    global pricesMATIC
    global pricesETC
    global pricesAVAX
    global pricesLTC
    global pricesENJ
    global pricesDOT
    global pricesSNX
    global pricesRUNE
    global pricesXLM
    global pricesBCH
    global pricesTRX
    global pricesUMA
    global pricesNEAR
    global pricesZEC
    global pricesSOL
    global pricesSUSHI
    global pricesICP
    global pricesCRV

    global fundingCELO
    global fundingLINK
    global fundingDOGE
    global funding1INCH
    global fundingXMR
    global fundingFIL
    global fundingAAVE
    global fundingATOM
    global fundingMKR
    global fundingEOS
    global fundingCOMP
    global fundingALGO
    global fundingXTZ
    global fundingUNI
    global fundingADA
    global fundingZRX
    global fundingYFI
    global fundingMATIC
    global fundingETC
    global fundingAVAX
    global fundingLTC
    global fundingENJ
    global fundingDOT
    global fundingSNX
    global fundingRUNE
    global fundingXLM
    global fundingBCH
    global fundingTRX
    global fundingUMA
    global fundingNEAR
    global fundingZEC
    global fundingSOL
    global fundingSUSHI
    global fundingICP
    global fundingCRV

    global marketsInterest
    for mkt in marketsInterest:
        globals()[f"openInterest{marketsDict[mkt]}"] = pd.concat([openInterest, pullMarketData.pullOI(mkt)])
        globals()[f"funding{marketsDict[mkt]}"] = pd.concat([funding, pullMarketData.pullFunding(mkt)])
        globals()[f"prices{marketsDict[mkt]}"] = pd.concat([prices, pullMarketData.pullPrices(mkt)])

#update filenames in each new market
def lowFreqRun(mkt=market):
    global tradesCELO
    global tradesLINK
    global tradesDOGE
    global trades1INCH
    global tradesXMR
    global tradesFIL
    global tradesAAVE
    global tradesATOM
    global tradesMKR
    global tradesEOS
    global tradesCOMP
    global tradesALGO
    global tradesXTZ
    global tradesUNI
    global tradesADA
    global tradesZRX
    global tradesYFI
    global tradesMATIC
    global tradesETC
    global tradesAVAX
    global tradesLTC
    global tradesENJ
    global tradesDOT
    global tradesSNX
    global tradesRUNE
    global tradesXLM
    global tradesBCH
    global tradesTRX
    global tradesUMA
    global tradesNEAR
    global tradesZEC
    global tradesSOL
    global tradesSUSHI
    global tradesICP
    global tradesCRV

    global openInterestCELO
    global openInterestLINK
    global openInterestDOGE
    global openInterest1INCH
    global openInterestXMR
    global openInterestFIL
    global openInterestAAVE
    global openInterestATOM
    global openInterestMKR
    global openInterestEOS
    global openInterestCOMP
    global openInterestALGO
    global openInterestXTZ
    global openInterestUNI
    global openInterestADA
    global openInterestZRX
    global openInterestYFI
    global openInterestMATIC
    global openInterestETC
    global openInterestAVAX
    global openInterestLTC
    global openInterestENJ
    global openInterestDOT
    global openInterestSNX
    global openInterestRUNE
    global openInterestXLM
    global openInterestBCH
    global openInterestTRX
    global openInterestUMA
    global openInterestNEAR
    global openInterestZEC
    global openInterestSOL
    global openInterestSUSHI
    global openInterestICP
    global openInterestCRV

    global pricesCELO
    global pricesLINK
    global pricesDOGE
    global prices1INCH
    global pricesXMR
    global pricesFIL
    global pricesAAVE
    global pricesATOM
    global pricesMKR
    global pricesEOS
    global pricesCOMP
    global pricesALGO
    global pricesXTZ
    global pricesUNI
    global pricesADA
    global pricesZRX
    global pricesYFI
    global pricesMATIC
    global pricesETC
    global pricesAVAX
    global pricesLTC
    global pricesENJ
    global pricesDOT
    global pricesSNX
    global pricesRUNE
    global pricesXLM
    global pricesBCH
    global pricesTRX
    global pricesUMA
    global pricesNEAR
    global pricesZEC
    global pricesSOL
    global pricesSUSHI
    global pricesICP
    global pricesCRV

    global fundingCELO
    global fundingLINK
    global fundingDOGE
    global funding1INCH
    global fundingXMR
    global fundingFIL
    global fundingAAVE
    global fundingATOM
    global fundingMKR
    global fundingEOS
    global fundingCOMP
    global fundingALGO
    global fundingXTZ
    global fundingUNI
    global fundingADA
    global fundingZRX
    global fundingYFI
    global fundingMATIC
    global fundingETC
    global fundingAVAX
    global fundingLTC
    global fundingENJ
    global fundingDOT
    global fundingSNX
    global fundingRUNE
    global fundingXLM
    global fundingBCH
    global fundingTRX
    global fundingUMA
    global fundingNEAR
    global fundingZEC
    global fundingSOL
    global fundingSUSHI
    global fundingICP
    global fundingCRV

    global bidsCELO
    global bidsLINK
    global bidsDOGE
    global bids1INCH
    global bidsXMR
    global bidsFIL
    global bidsAAVE
    global bidsATOM
    global bidsMKR
    global bidsEOS
    global bidsCOMP
    global bidsALGO
    global bidsXTZ
    global bidsUNI
    global bidsADA
    global bidsZRX
    global bidsYFI
    global bidsMATIC
    global bidsETC
    global bidsAVAX
    global bidsLTC
    global bidsENJ
    global bidsDOT
    global bidsSNX
    global bidsRUNE
    global bidsXLM
    global bidsBCH
    global bidsTRX
    global bidsUMA
    global bidsNEAR
    global bidsZEC
    global bidsSOL
    global bidsSUSHI
    global bidsICP
    global bidsCRV

    global asksCELO
    global asksLINK
    global asksDOGE
    global asks1INCH
    global asksXMR
    global asksFIL
    global asksAAVE
    global asksATOM
    global asksMKR
    global asksEOS
    global asksCOMP
    global asksALGO
    global asksXTZ
    global asksUNI
    global asksADA
    global asksZRX
    global asksYFI
    global asksMATIC
    global asksETC
    global asksAVAX
    global asksLTC
    global asksENJ
    global asksDOT
    global asksSNX
    global asksRUNE
    global asksXLM
    global asksBCH
    global asksTRX
    global asksUMA
    global asksNEAR
    global asksZEC
    global asksSOL
    global asksSUSHI
    global asksICP
    global asksCRV

    for mkt in marketsInterest:
        globals()[f"bids{marketsDict[mkt]}"] = pd.concat([bids, pullMarketData.pullOrderBookDepth(mkt, 'bids')])
        globals()[f"funding{marketsDict[mkt]}"] = pd.concat([funding, pullMarketData.pullFunding(mkt)])
        globals()[f"prices{marketsDict[mkt]}"] = pd.concat([prices, pullMarketData.pullPrices(mkt)])


    bids = pd.concat([bids, pullMarketData.pullOrderBookDepth(mkt, 'bids')])
    asks = pd.concat([asks, pullMarketData.pullOrderBookDepth(mkt, 'asks')])
    openInterest.to_csv('openInterestBTC.csv')
    prices.to_csv('PricesBTC.csv')
    trades.to_csv('tradesBTC.csv')
    insurFund.to_csv('insurFund.csv')
    bids.to_csv('bidsBTC.csv')
    asks.to_csv('asksBTC.csv')
    funding.to_csv('fundingBTC.csv')

#initialise dataframes - need to do all
prices = pd.DataFrame()
trades = pd.DataFrame()
insurFund = pd.DataFrame()
openInterest = pd.DataFrame()
bids = pd.DataFrame()
asks = pd.DataFrame()
funding = pd.DataFrame()

marketsInterest = ['ETH-USD', 'BTC-USD', 'CELO-USD', 'LINK-USD', 'DOGE-USD', '1INCH-USD', 'XMR-USD', 'FIL-USD', 'AAVE-USD', 'ATOM-USD', 'MKR-USD', 'EOS-USD', 'COMP-USD', 'ALGO-USD', 'XTZ-USD', 'UNI-USD', 'ADA-USD', 'ZRX-USD', 'YFI-USD', 'MATIC-USD', 'ETC-USD', 'AVAX-USD', 'LTC-USD', 'ENJ-USD', 'DOT-USD', 'SNX-USD', 'RUNE-USD', 'XLM-USD', 'BCH-USD', 'TRX-USD', 'UMA-USD', 'NEAR-USD', 'ZEC-USD', 'SOL-USD', 'SUSHI-USD', 'ICP-USD', 'CRV-USD']
marketsDict = {'ETH-USD':'ETH', 'BTC-USD':'BTC', 'CELO-USD':'CELO', 'LINK-USD':'LINK', 'DOGE-USD':'DOGE', '1INCH-USD':'1INCH', 'XMR-USD':'XMR', 'FIL-USD':'FIL', 'AAVE-USD':'AAVE', 'ATOM-USD':'ATOM', 'MKR-USD':'MKR', 'EOS-USD':'EOS', 'COMP-USD':'COMP', 'ALGO-USD':'ALGO', 'XTZ-USD':'XTZ', 'UNI-USD':'UNI', 'ADA-USD':'ADA', 'ZRX-USD':'ZRX', 'YFI-USD':'YFI', 'MATIC-USD':'MATIC', 'ETC-USD':'ETC', 'AVAX-USD':'AVAX', 'LTC-USD':'LTC', 'ENJ-USD':'ENJ', 'DOT-USD':'DOT', 'SNX-USD':'SNX', 'RUNE-USD':'RUNE', 'XLM-USD':'XLM', 'BCH-USD':'BCH', 'TRX-USD':'TRX', 'UMA-USD':'UMA', 'NEAR-USD':'NEAR', 'ZEC-USD':'ZEC', 'SOL-USD':'SOL', 'SUSHI-USD':'SUSHI', 'ICP-USD':'ICP', 'CRV-USD':'CRV'}


#edit this in ETH, BTC vs others
schedule.every(30).seconds.do(highFreqRun)
schedule.every(5).minutes.do(mediumFreqRun)
schedule.every(30).minutes.do(lowFreqRun)

while True:
    schedule.run_pending()
    time.sleep(1)