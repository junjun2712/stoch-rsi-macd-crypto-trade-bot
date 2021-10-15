import sqlite3
import pandas as pd
import sqlalchemy
from binance import Client
from keys import api_key, api_secret

# Trendfollowing
# if the crypto was rising by x % -> Buy
# exit when profit is above 0.15% or loss is crossing -0.15%

def strategy(entry, lookback, qty, open_position=False):
    while True:
        df = pd.read_sql('BTCUSDT', engine)
        lookbackperiod = df.iloc[-lookback:]
        cumret = (lookbackperiod.Price.pct_change() + 1).cumprod() - 1
        if not open_position:
            #print(cumret[cumret.last_valid_index()])
            if cumret[cumret.last_valid_index()] > entry:
                print('Buying, return {}'.format(cumret[cumret.last_valid_index()]))

                order = client.create_order(
                    symbol='BTCUSDT',
                    side= 'BUY',
                    type='MARKET',
                    quantity=qty
                )

                ordertosave = {
                    'OrderId':order['clientOrderId'],
                    'Time':order['transactTime'], 
                    'Cummulative':cumret[cumret.last_valid_index()], 
                    'Quantity':order['fills'][0]['qty'],
                    'Commission':order['fills'][0]['commission'],
                    'Completed':False
                }

                frame = create_order_frame(ordertosave)
                frame.to_sql('BTCUSDTorders', engine, if_exists='append', index=False)
                print(frame)
                open_position = True
                break

    if open_position:
        while True:
            df = pd.read_sql('BTCUSDT', engine)
            sincebuy = df.loc[
                df.Time > pd.to_datetime(order['transactTime'], unit='ms')
            ]

            if len(sincebuy) > 1:
                sincebuyret = (sincebuy.Price.pct_change() + 1).cumprod() - 1
                last_entry = sincebuyret[sincebuyret.last_valid_index()]
                #print(last_entry * 100)
                if last_entry > 0.0015:
                    print('Selling, return: {}'.format(last_entry))
                    order = client.create_order(
                        symbol='BTCUSDT',
                        side='SELL',
                        type='MARKET',
                        quantity=qty
                    )
                    print(order)
                    break

def create_order_frame(msg):
    df = pd.DataFrame([msg])
    df.columns = ['OrderId', 'Time','Cummulative','Quantity', 'Commission', 'Completed']
    df.Time = pd.to_datetime(df.Time, unit='ms')
    df.Quantity = df.Quantity.astype(float)
    df.Cummulative = df.Cummulative.astype(float)
    df.Commission = df.Commission.astype(float)
    df.Completed = df.Completed.astype(bool)
    return df

def get_currency_acount_balance(symbol):
    acc = client.get_account()
    #print(acc['balances'][0])
    for item in acc['balances']:
        if item['asset'] == symbol:
            return '{}: Free: {}, Locked: {}'.format(symbol, item['free'], item['locked'])

def get_main_balances():
    for item in client.get_account()['balances']:
        if item['asset'] == 'BTC':
            print('BTC:\tFree: {}, Locked: {}'.format(item['free'], item['locked']))
        elif item['asset'] == 'USDT':
            print('USDT:\tFree: {}, Locked: {}'.format(item['free'], item['locked']))

def save_order():
    ordertosave = {
        'OrderId':'12ad31fa4',
        'Time':'1634261156105', 
        'Cummulative':'0.1231', 
        'Quantity':'1231',
        'Commission':'0.123',
        'Completed':False
    }
    print(ordertosave)
    frame = create_order_frame(ordertosave)
    print(frame)
    frame.to_sql('BTCUSDTorders', engine, if_exists='append', index=False)

def complete_last_order():
    df = pd.read_sql('BTCUSDTorders', engine)
    lastorder = df.iloc[-1]
    if lastorder['Completed'] == False:
        print('Completing order:\n{}'.format(lastorder))
        while True:
            df = pd.read_sql('BTCUSDT', engine)
            sincebuy = df.loc[
                df.Time > lastorder.Time
            ]

            if len(sincebuy) > 1:
                sincebuyret = (sincebuy.Price.pct_change() + 1).cumprod() - 1
                last_entry = sincebuyret[sincebuyret.last_valid_index()]
                #print(last_entry * 100)
                if last_entry > 0.0015:
                    print('Selling, return: {}'.format(last_entry))
                    order = client.create_order(
                        symbol='BTCUSDT',
                        side='SELL',
                        type='MARKET',
                        quantity=lastorder.Quantity
                    )
                    
                    print(order)
                    break
    else:
        print('Last order was completed!')

def main():
    get_main_balances()
    strategy(0.001, 60, 0.00034)
    get_main_balances()
    #save_order()
    #complete_last_order()

if __name__ == '__main__':
    print('On run')
    connection = sqlite3.connect('BTCUSDTstream.db')
    client = Client(api_key, api_secret)
    engine = sqlalchemy.create_engine('sqlite:///BTCUSDTstream.db')
    main()