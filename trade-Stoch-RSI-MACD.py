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
    mindata = get_minute_data(pair, '1m', '100')
    techdata = apply_technicals(mindata)
    inst = Signals(techdata, 10)
    data = inst.decide()
    #print(f'current Close is '+str(data.Close.iloc[-1]))
    if data.Buy.iloc[-1]:
        # placing order
        order = client.create_order(
            symbol=pair,
            side='BUY',
            type='MARKET',
            quantity= qty
        )
        buyprice = float(order['fills'][0]['price'])
        #print(order)
        frame = clean_order(order)
        print(frame)
        frame.to_sql('BTCUSDTStoch-RSI-MACDorders', engine, if_exists='append', index=False)
    while open_position:
        sleep(0.1)
        mindata = get_minute_data(pair, '1m', '2')
        #print(f'Current Close '+str(mindata.Close.iloc[-1]))
        #print(f'Current Target '+str(buyprice * 1.05))
        #print(f'Current Stop is '+str(buyprice * 0.995))
        if mindata.Close[-1] <= buyprice * 0.995 or mindata.Close[-1] >= 1.05 * buyprice:
            # removing order
            order = client.create_order(
                symbol=pair,
                side='SELL',
                type='MARKET',
                quantity= qty
            )
            #print(order)
            frame = clean_order(order)
            print(frame)
            frame.to_sql('BTCUSDTStoch-RSI-MACDorders', engine, if_exists='append', index=False)
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

def main(args=None):
    print(get_main_balances())
    while True:
        sleep(0.5)
        strat('BTCUSDT', 0.00034)
    
    '''  
    while True:
        sleep(1)
        df = get_minute_data('BTCUSDT', '1m', '100')
        df = apply_technicals(df)
        inst = Signals(df, 25)
        print(inst.decide())
        if df.Buy.iloc[-1]:
            print('Order placed paps')
    '''

if __name__ == '__main__':
    print('on run') 
    client = Client(api_key, api_secret)
    connection = sqlite3.connect('db/BTCUSDTStoch-RSI-MACDorders.db')
    engine = sqlalchemy.create_engine('sqlite:///db/BTCUSDTStoch-RSI-MACDorders.db')
    main()