import pullMarketData
import pandas as pd
from dydx3 import Client
import schedule
import numpy as np
import time
import datetime as dt
import smtplib
import ssl
from email.message import EmailMessage


# All filenames I output and want ot email to myself
filenames = ['openInterestETH.csv', 'pricesETH.csv', 'tradesETH.csv', 'bidsETH.csv', 'asksETH.csv', 'fundingETH.csv']

# Define email sender and receiver
email_sender = 'consideredfinancedev@gmail.com'
email_password = 'ekrsfkjvtcsijkpc'
email_receiver = 'consideredfinancedev@gmail.com'

# Set the subject and body of the email
subject = 'Latest data files'
body = """
Latest data files
"""

em = EmailMessage()
em['From'] = email_sender
em['To'] = email_receiver
em['Subject'] = subject
em.set_content(body)

# Add SSL (layer of security)
context = ssl.create_default_context()

# Email from here
market = 'ETH-USD'

# only need for ETH, BTC markets
def highFreqRun(mkt=market):
    global tradesETH
    tradesETH = pd.concat([tradesETH, pullMarketData.pullTrades(mkt)]).drop_duplicates()

def mediumFreqRun(mkt=market):
    global openInterestETH
    global pricesETH
    openInterestETH = pd.concat([openInterestETH, pullMarketData.pullOI(mkt)])
    pricesETH = pd.concat([pricesETH, pullMarketData.pullPrices(mkt)])

#update filenames in each new market
def lowFreqRun(mkt=market):
    global tradesETH
    global openInterestETH
    global bidsETH
    global asksETH
    global pricesETH
    global fundingETH
    fundingETH = pd.concat([fundingETH, pullMarketData.pullFunding(mkt)])
    bidsETH = pd.concat([bidsETH, pullMarketData.pullOrderBookDepth(mkt, 'bids')])
    asksETH = pd.concat([asksETH, pullMarketData.pullOrderBookDepth(mkt, 'asks')])
    fundingETH = pd.concat([fundingETH, pullMarketData.pullFunding(mkt)])
    openInterestETH.to_csv('openInterestETH.csv')
    pricesETH.to_csv('pricesETH.csv')
    tradesETH.to_csv('tradesETH.csv')
    bidsETH.to_csv('bidsETH.csv')
    asksETH.to_csv('asksETH.csv')
    fundingETH.to_csv('fundingETH.csv')

def mailingFn():
    # Log in and send the email

    for file_name in filenames:
        with open(file_name, 'r') as f:
            file_data = f.read()
            #file_name = f.name()
            em.add_attachment(file_data, maintype="application", subtype="csv", filename=file_name)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.sendmail(email_sender, email_receiver, em.as_string())
    print('Email sent')

#initialise dataframes
pricesETH = pd.DataFrame()
tradesETH = pd.DataFrame()
openInterestETH = pd.DataFrame()
bidsETH = pd.DataFrame()
asksETH = pd.DataFrame()
fundingETH = pd.DataFrame()

#edit this in ETH, BTC vs others
schedule.every(30).seconds.do(highFreqRun)
schedule.every(5).minutes.do(mediumFreqRun)
schedule.every(30).minutes.do(lowFreqRun)
#schedule.every(1).hours.do(mailingFn)

while True:
    schedule.run_pending()
    time.sleep(1)