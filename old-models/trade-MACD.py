# MACD trading bot
# by evil3sc

from os import symlink
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
from pandas.core.frame import DataFrame
import ta
from time import sleep
from keys import api_key, api_secret
import sqlite3
import sqlalchemy

def get_minute_data(symbol):
    try:
        df = pd.DataFrame(client.get_historical_klines(symbol, '1m', '40m UTC'))
    except BinanceAPIException as e:
        print(e)
        sleep(60)
        df = pd.DataFrame(client.get_historical_klines(symbol, '1m', '40m UTC'))
    df = df.iloc[:,:6]
    df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = df.set_index('Time')
    df.index = pd.to_datetime(df.index, unit='ms')
    df = df.astype(float)

    return df

def trade_strat(pair, qty, open_position=False):
    while True:
        df = get_minute_data(pair)
        if not open_position:
            if ta.trend.macd_diff(df.Close).iloc[-1] > 0 and \
                ta.trend.macd_diff(df.Close).iloc[-2] < 0:
                order = client.create_order(
                    symbol = pair,
                    side = 'BUY',
                    type = 'MARKET',
                    quantity = qty
                )
                frame = clean_order(order)
                frame.to_sql('BTCUSDTorders', engine, if_exists='append', index=False)
                print(frame)
                open_position = True
                buyprice = float(order['fills'][0]['price'])
                break

    if open_position:
        while True:
            df = get_minute_data(pair)
            if ta.trend.macd_diff(df.Close).iloc[-1] < 0 and \
                ta.trend.macd_diff(df.Close).iloc[-2] > 0:
                order = client.create_order(
                    symbol = pair,
                    side = 'SELL',
                    type = 'MARKET',
                    quantity = qty
                )
                frame = clean_order(order)
                frame.to_sql('BTCUSDTorders', engine, if_exists='append', index=False)
                print(frame)
                sellprice = float(order['fills'][0]['price'])
                print(f'Profit = {(sellprice - buyprice)/buyprice}')
                open_position= False
                break

def clean_order(order):
    relev_info = {
        'OrderId':order['clientOrderId'],
        'Time':pd.to_datetime(order['transactTime'], unit='ms'),
        'Side':order['side'],
        'Qty':float(order['fills'][0]['qty']),
        'Commission':float(order['fills'][0]['commission']),
        'Price':float(order['fills'][0]['price'])
    }
    df = pd.DataFrame([relev_info])
    return df

def get_main_balances():
    for item in client.get_account()['balances']:
        if item['asset'] == 'BTC':
            print('BTC:\tFree: {}, Locked: {}'.format(item['free'], item['locked']))
        elif item['asset'] == 'USDT':
            print('USDT:\tFree: {}, Locked: {}'.format(item['free'], item['locked']))

def retrade():
    order = client.create_order(
        symbol = 'BTCUSDT',
        side = 'SELL',
        type = 'MARKET',
        quantity = 0.00034
    )

def main():
    get_main_balances()
    #trade_strat('BTCUSDT', 0.00034)
    retrade()
    get_main_balances()

if __name__ == '__main__':
    client = Client(api_key, api_secret)
    connection = sqlite3.connect('BTCUSDTMACDorders.db')
    engine = sqlalchemy.create_engine('sqlite:///BTCUSDTMACDorders.db')
    main()