import asyncio
from binance import AsyncClient, BinanceSocketManager
import pandas as pd
import sqlalchemy
from keys import api_key, api_secret

engine = sqlalchemy.create_engine('sqlite:///BTCUSDTstream.db')

async def main():
    lastsecond = 0
    client = await AsyncClient.create(api_key, api_secret)
    bm = BinanceSocketManager(client)
    ts = bm.trade_socket('BTCUSDT')
    async with ts as tscm:
        while True:
            res = await tscm.recv()
            if res['e'] != 'error':
                frame = createframe(res)
                if frame.Time.dt.second[0] > lastsecond :
                    print('Time: {}\tValue: {}'.format(frame.iloc[0].Time, frame.iloc[0].Price))
                    frame.to_sql('BTCUSDT', engine, if_exists='append', index=False)
                    lastsecond = frame.Time.dt.second[0]
                else:
                    if lastsecond == 59 and frame.Time.dt.microsecond[0] > 900000:
                        lastsecond = 0
            else:
                print('Error: {}'.format(res['m']))
            
    await client.close_connection()

def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:,['s','E','p']]
    df.columns = ['symbol','Time','Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())