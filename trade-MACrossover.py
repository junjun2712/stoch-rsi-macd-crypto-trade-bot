# trade bot
#   strat: real-time moving average crossover (hour span)

import pandas as pd
import sqlalchemy
import ta
from binance.client import Client
from binance.exceptions import BinanceAPIException
from sqlalchemy import exc
from time import sleep
from math import floor
from keys import api_key, api_secret

def MAstrat(pair, amt, stop_loss, open_position = False):
    ST = 7
    LT = 20

    buyprice = 0
    prehis = gethistoricals(pair, ST, LT)['Close']

    qty = floor((amt / prehis)*100)/100
    
    try:
        dbres = pd.read_sql('Orders', engine)
        if dbres.empty != True:
            if dbres.iloc[-1].completed == False and dbres.iloc[-1].symbol == pair:
                print('Oper order founded!')
                buyprice = dbres.iloc[-1].price
                qty = dbres.iloc[-1].qty
                open_position = True
    except exc.SQLAlchemyError:
        print('SQLAlchemy error!')

    while True:
        historicals = gethistoricals(pair, ST, LT)
        timer = 1800
        
        if not open_position:
            print('Price: {}, rsi: {}, ST/LT mov diff: {}'.format(historicals['Close'], round(historicals['rsi'], 3), round(historicals['ST'] - historicals['LT'], 3)))
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
                    print('Buy at price: {}, stop: {}, min target: {}'.format(buyprice, round(buyprice * stop_loss, 2), round(buyprice * 1.01, 2)))
                    print(get_main_free_balances())
                    frame = createorderframe(buyorder)
                    try:
                        frame.to_sql('Orders', engine, if_exists='append', index=False)
                    except exc.SQLAlchemyError:
                        print('SQLAlchemy error!')
                    open_position = True
                except BinanceAPIException as e:
                    print('Error: {} ({})'.format(e.message, e.status_code))
                
        if open_position:
            print('Actual win/loss: {}%, rsi: {}, mov diff: {}'.format(round((((historicals['Close'] - buyprice)/buyprice)*100), 3), round(historicals['rsi'], 3), round(historicals['ST'] - historicals['LT'], 3)))
            if (historicals['LT'] > historicals['ST'] and historicals['rsi'] < 55 and historicals['Close'] > buyprice * 1.01) or (buyprice <= historicals['Close'] * stop_loss):
                #print('sell')
                try:
                    sellorder = client.create_order(
                        symbol=pair,
                        side='SELL',
                        type='MARKET',
                        quantity= qty
                    )
                    print('Sell at price: {}, stop: {}, target: {}'.format(buyprice, buyprice * stop_loss, buyprice * 1.01))
                    if buyprice != 0:
                        print('Win/loss: {}%'.format(round((float(sellorder['fills'][0]['price']) / buyprice - 1) * 100, 3)))
                    print(get_main_free_balances())
                    # actualizar orden de base de datos
                    try:
                        dbres = pd.read_sql('Orders', engine)
                        if dbres.iloc[-1].symbol == pair:
                            sql = 'update Orders set completed = 1 where id = (select max(id) from Orders)'
                            engine.execute(sql)
                    except exc.SQLAlchemyError:
                        print('SQLAlchemy error!')
                    open_position = False
                    sleep(5)
                except BinanceAPIException as e:
                    print('Error: {} ({})'.format(e.message, e.status_code))
            else:
                open_position = True
        while (timer > 0):
            sleep(600)
            #print info data
            timer -= 600

def gethistoricals(pair, ST, LT):
    df = pd.DataFrame(client.get_historical_klines(pair, '30m', str(LT) + 'days ago UTC', '30m ago UTC'))
    closes = pd.DataFrame(df[4])
    closes.columns = ['Close']
    closes = closes.astype(float)
    closes['rsi'] = ta.momentum.rsi(closes.Close, window=14)
    closes['ST'] = closes.Close.rolling(window=ST).mean()
    closes['LT'] = closes.Close.rolling(window=LT).mean()
    #closes.dropna(inplace=True)
    closes = closes.iloc[-1]
    #print('Price: {}, rsi: {}, ST: {}, LT: {}'.format(closes['Close'], closes['rsi'], closes['ST'], closes['LT']))
    return closes

def createorderframe(msg):
    lastsell = msg['fills'][0]
    data = [{ "symbol": msg['symbol'], "qty": lastsell['qty'], "price": lastsell['price'], "commission": lastsell['commission'], "completed": False }]
    df = pd.DataFrame(data)
    df.qty = df.qty.astype(float)
    df.price = df.price.astype(float)
    df.commission = df.commission.astype(float)
    return df

def get_main_free_balances():
    btc = 0
    sol = 0
    busd = 0
    for item in client.get_account()['balances']:
        if item['asset'] == 'BTC':
            btc = item['free']
        elif item['asset'] == 'SOL':
            sol = item['free']
        elif item['asset'] == 'BUSD':
            usdt = item['free']
    return 'Balance BTC: {}, USDT: {}, BUSD: {}'.format(btc, sol, busd)

def main(args=None):
    MAstrat('SOLBUSD', 12, 0.95)
    #print(gethistoricals('SHIBBUSD', 7, 20))

if __name__ == '__main__':
    client = Client(api_key, api_secret)
    engine = sqlalchemy.create_engine('sqlite:///db/MACross.db')
    main()