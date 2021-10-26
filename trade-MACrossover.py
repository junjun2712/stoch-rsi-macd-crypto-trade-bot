# trade bot
#   strat: real-time moving average crossover (hour span)

from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
from keys import api_key, api_secret
import ta
from time import sleep
from decimal import *
from math import floor

def MAstrat(pair, amt, stop_loss, open_position = False):
    ST = 7
    LT = 25

    while True:
        historicals = gethistoricals(pair, ST, LT)
        timer = 3600
        qty = (floor(amt / historicals['Close'] * 100000)) / 100000
        buyprice = 0;
        if not open_position:
            if historicals['ST'] > historicals['LT'] and historicals['rsi'] > 60:
                #print('buy')
                try:
                    buyorder = client.create_order(
                        symbol=pair,
                        side='BUY',
                        type='MARKET',
                        quantity= qty
                    )
                    buyprice = float(buyorder['fills'][0]['price'])
                    print('Buy at price: {}, stop: {}, target price: {}'.format(buyprice, buyprice * stop_loss, buyprice * 1.01))
                    print(get_main_free_balances())
                    open_position = True
                except BinanceAPIException as e:
                    print('Error: {} ({})'.format(e.message, e.status_code))
                break

        if open_position:
            if (historicals['LT'] > historicals['ST'] and historicals['rsi'] < 50 and historicals['Close'] > buyprice * 1.01) or (buyprice <= historicals['Close'] * stop_loss):
                #print('sell')
                try:
                    sellorder = client.create_order(
                        symbol=pair,
                        side='SELL',
                        type='MARKET',
                        quantity= qty
                    )
                    print('Sell at stop: {}, target: {}'.format(buyprice * 0.96, buyprice * 1.02))
                    print('Win/loss: {}%'.format(round((float(sellorder['fills'][0]['price']) / buyprice - 1) * 100, 3)))
                    print(get_main_free_balances())
                    open_position = False
                    sleep(5)
                except BinanceAPIException as e:
                    print('Error: {} ({})'.format(e.message, e.status_code))
                break
        
        while (timer > 0):
            sleep(600)
            #print info data
            timer -= 600

def gethistoricals(pair, ST, LT):
    df = pd.DataFrame(client.get_historical_klines(pair, '1h', str(LT) + 'days ago UTC', '1h ago UTC'))
    closes = pd.DataFrame(df[4])
    closes.columns = ['Close']
    closes = closes.astype(float)
    closes['rsi'] = ta.momentum.rsi(closes.Close, window=14)
    closes['ST'] = closes.Close.rolling(window=ST).mean()
    closes['LT'] = closes.Close.rolling(window=LT).mean()
    #closes.dropna(inplace=True)
    closes = closes.iloc[-1]
    print(closes)
    return closes

def get_main_free_balances():
    btc = 0
    usdt = 0
    for item in client.get_account()['balances']:
        if item['asset'] == 'BTC':
            btc = item['free']
        elif item['asset'] == 'USDT':
            usdt = item['free']
    return 'Free BTC: {}, USDT: {}'.format(btc, usdt)

def createlivepriceframe(msg):
    df = pd.DataFrame(msg)
    df = pd.DataFrame(df[4])
    df = pd.DataFrame(df.iloc[0])
    df.columns = ['Price']
    df = df.astype(float)
    #print(df)
    return df

def liveSMA(hist, live, ST, LT):
    liveST = (hist['ST'].values + live.Price.values) / (ST)
    liveLT = (hist['LT'].values + live.Price.values) / (LT)
    print('ST: {}, LT: {}'.format(liveST, liveLT))
    if liveST > liveLT:
        return True


#def on_message(wsapp, message):
    #print(message)

def main(args=None):
    '''
    while True:
        timer = 3600
        while (timer > 0):
            MAstrat('BTCUSDT', 15, 0.96)
            sleep(600)
            timer -= 600
    '''
    MAstrat('BTCUSDT', 15, 0.95)


if __name__ == '__main__':
    client = Client(api_key, api_secret)
    #wsapp = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/btcusdt@trade", on_message=on_message)
    #wsapp.run_forever()
    #print(pd.DataFrame(client.get_historical_klines('BTCUSDT', '1h', '1h ago UTC')))
    main()