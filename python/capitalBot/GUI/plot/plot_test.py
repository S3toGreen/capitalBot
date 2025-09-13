from clickhouse_connect import get_client
from .FootprintChart import PlotWidget
from dotenv import load_dotenv
from core.DBEngine.Receiver import DataReceiver
from core.DBEngine.AsyncWorker import AsyncWorker
import msgspec
import os
load_dotenv()
import sys

client = get_client(host='localhost', username='default', password=os.getenv("CLIENT_PASS"), compression=True)

symbol='MTX00'
query_fp = f"""
    SELECT time, price, agg_buy, agg_sell, agg_buy-agg_sell as delta, vol
    FROM fpDM
    WHERE symbol = '{symbol}' and time>today()
    ORDER BY time
"""
query_ohlcv = f"""select time, open, high, low, close, vol, delta
    FROM (
    SELECT time,open,high,low,close,vol,delta_close as delta
    FROM ohlcvDM
    WHERE symbol = '{symbol}' 
    ORDER BY time desc limit 3000
    ) ORDER BY time
"""
fp = client.query_df(query_fp)
ohlcv = client.query_df(query_ohlcv)

if __name__=='__main__':
    from PySide6.QtWidgets import QApplication, QMainWindow
    app = QApplication([])
    main = QMainWindow()
    worker = AsyncWorker()
    datareceiver = DataReceiver.create(worker,channels=[f'snap:DM:{symbol}'])

    if ohlcv.empty:
        # xread from redis if no data signal ticker to fetch data
        history = datareceiver.redis.xrange(f"histroy:DM:{symbol}")
        if history:
            for id, data in history:
                msg = msgspec.msgpack.decode(data,strict=False)
                print(msg)
        else:
            datareceiver.redis.publish('request:DM', symbol)


    ohlcv[['open','high','low','close']] = ohlcv[['open','high','low','close']].astype(float)
    if not fp.empty:
        fp['price'] = fp['price'].astype(float)

    chart = PlotWidget(title='fp test',ohlcv=ohlcv,fp=fp)#,fp=fp, symbol=symbol)
    datareceiver.message_received.connect(chart._msg_handler)

    main.setCentralWidget(chart)
    main.show()
    sys.exit(app.exec())
    # chart.update_data()