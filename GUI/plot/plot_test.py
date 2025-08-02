from clickhouse_connect import get_client
from .FootprintChart import FootprintChart
from redisworker.Config import pw
import sys
import numpy as np
import pandas as pd

client = get_client(host='localhost', username='client', password=pw, compression=True)

symbol='MTX00'
query_fp = f"""
    SELECT time, price, neutral, agg_buy, agg_sell, vol
    FROM fpDM
    WHERE symbol = '{symbol}' and time>today()
    ORDER BY time
"""
query_ohlcv = f"""
    SELECT time,open,high,low,close,vol
    FROM ohlcvDM
    WHERE symbol = '{symbol}' and time>today()
    ORDER BY time
"""
fp = client.query_df(query_fp).set_index(['time','price'])
ohlcv = client.query_df(query_ohlcv).set_index('time')
print("----------- Original DataFrame -----------")
print(fp,ohlcv)

# 2. Select relevant columns and explode the 'orderflow' list
# This creates a new row for each price level
# exploded_df = df[['time', 'price_map']].explode('price_map', ignore_index=True)

# # 3. Unpack the tuples into separate columns
# # .apply(pd.Series) is a very efficient way to do this
# orderflow_cols = exploded_df['price_map'].apply(pd.Series)
# orderflow_cols.columns = ['price', 'vol', 'bid', 'ask'] # Name the new columns

# # 4. Combine with the timestamp and set the MultiIndex
# footprint_df = pd.concat([exploded_df['time'], orderflow_cols], axis=1)
# footprint_df = footprint_df.set_index(['time', 'price'])

# # 5. Ensure data types are correct (optional but good practice)
# footprint_df = footprint_df.astype({'bid': np.int64, 'ask': np.int64})

# print("\n\n----------- Transformed Footprint DataFrame -----------")
# print(footprint_df)

if __name__=='__main__':
    from PySide6.QtWidgets import QApplication, QMainWindow
    app = QApplication([])
    main = QMainWindow()

    # chart = FootprintChart(title='fp test',data=bars)
    # main.setCentralWidget(chart)
    main.show()
    sys.exit(app.exec())
    # chart.update_data()