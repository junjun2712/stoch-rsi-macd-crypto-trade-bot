# Trading bot https://youtu.be/X50-c54BWV8
    # Strats: Stochastics Slow, RSI, MACD, Target Profit and Stop loss

from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
import ta
from time import sleep
from keys import api_key, api_secret
import numpy as np
import sqlite3
import sqlalchemy 

def get_minute_data(pair, interval, lookback):
    frame = pd.DataFrame(client.get_historical_klines(pair, interval, lookback + ' min ago UTC'))
    frame = frame.iloc[:,:6]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame

def apply_technicals(df):
    df['%K'] = ta.momentum.stoch(
        df.High, 
        df.Low, 
        df.Close, 
        window=14, 
        smooth_window=3
    )
    df['%D'] = df['%K'].rolling(3).mean()
    df['rsi'] = ta.momentum.rsi(df.Close, window=14)
    df['macd'] = ta.trend.macd_diff(df.Close)
    df.dropna(inplace=True)
    return df

class Signals:
    def __init__(self, df, lags):
        self.df = df
        self.lags = lags
    
    def get_trigger(self):
        dfx = pd.DataFrame()
        for i in range(self.lags + 1):
            mask = (self.df['%K'].shift(i) < 20) & (self.df['%D'].shift(i) < 20)
            dfx = dfx.append(mask, ignore_index=True)
        return dfx.sum(axis=0)
    
    def decide(self):
        self.df['Trigger'] = np.where(self.get_trigger(), 1, 0)
        self.df['Buy'] = np.where(
            (self.df.Trigger) & 
            (self.df['%K'].between(20, 80)) & (self.df['%D'].between(20, 80)) &
            (self.df.rsi > 50) &
            (self.df.macd > 0),
            1, 0
        )
        return self.df

def strat(pair, qty, open_position=False):
    mindata = get_minute_data(pair, '5m', '1500')
    techdata = apply_technicals(mindata)
    inst = Signals(techdata, 20)
    data = inst.decide()
    #print(data.iloc[-1])
    #print(f'current Close is '+str(data.Close.iloc[-1]))
    if data.Buy.iloc[-1]:
        # placing order
        try:
            buyorder = client.create_order(
                symbol=pair,
                side='BUY',
                type='MARKET',
                quantity= qty
            )
            buyprice = float(buyorder['fills'][0]['price'])
            #print(order)
            #frame = clean_order(buyorder)
            #print(frame.iloc[0][['Time', 'Side', 'Price']])
            #frame.to_sql('BTCUSDTStoch-RSI-MACDorders', engine, if_exists='append', index=False)
            print('Buy at price: {}, stop: {}, target price: {}'.format(buyprice, buyprice * 0.996, buyprice * 1.03))
            print(get_main_free_balances())
            open_position = True
        except BinanceAPIException as e:
            print('Error: {} ({})'.format(e.message, e.status_code))
            sleep(5)

    while open_position:
        sleep(0.1)
        mindata = get_minute_data(pair, '1m', '2')
        #print(f'Current Close '+str(mindata.Close.iloc[-1]))
        #print(f'Current Target '+str(buyprice * 1.05))
        #print(f'Current Stop is '+str(buyprice * 0.995))
        #mindata.Close[-1] <= buyprice * 0.995 or 
        if mindata.Close[-1] <= buyprice * 0.95 or mindata.Close[-1] >= buyprice * 1.03:
            # removing order
            sellorder = client.create_order(
                symbol=pair,
                side='SELL',
                type='MARKET',
                quantity= qty
            )
            print('Sell at stop: {}, target: {}'.format(buyprice * 0.995, buyprice * 1.02))
            print(get_main_free_balances())
            print('Win/loss: {}%'.format(round((float(sellorder['fills'][0]['price']) / buyprice - 1) * 100, 3)))
            #print(order)
            #frame = clean_order(sellorder)
            #print(frame.iloc[0][['Time', 'Side', 'Price']])
            #frame.to_sql('BTCUSDTStoch-RSI-MACDorders', engine, if_exists='append', index=False)
            open_position = False
            sleep(5)
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

def get_main_free_balances():
    btc = 0
    usdt = 0
    for item in client.get_account()['balances']:
        if item['asset'] == 'BTC':
            btc = item['free']
        elif item['asset'] == 'USDT':
            usdt = item['free']
    return 'Free BTC: {}, USDT: {}'.format(btc, usdt)

def retrade():
    retradeorder = client.create_order(
        symbol = 'BTCUSDT',
        side = 'SELL',
        type = 'MARKET',
        quantity = 0.00030
    )

def main(args=None):
    #retrade()
    print(get_main_free_balances())

    while True:
        sleep(0.5)
        strat('BTCUSDT', 0.00030)

    '''
    while True:
        sleep(1)
        mindata = get_minute_data('BTCUSDT', '1m', '70')
        df = apply_technicals(mindata)
        inst = Signals(df, 20)
        final = inst.decide()
        print(final)
        #if inst.Buy.iloc[-1]:
            #print('Order placed paps')
    '''
if __name__ == '__main__':
    print('on run') 
    client = Client(api_key, api_secret)
    #connection = sqlite3.connect('db/BTCUSDTStoch-RSI-MACDorders.db')
    #engine = sqlalchemy.create_engine('sqlite:///db/BTCUSDTStoch-RSI-MACDorders.db')
    main()
