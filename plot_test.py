from clickhouse_connect import get_client
from Config import passwd
import pyqtgraph as pg
from pyqtgraph.Qt import QtWidgets
import sys

client = get_client(host='localhost', username='admin', password=passwd, compression=True)

query_MTX = """
    SELECT *
    FROM orderflowDM
    WHERE symbol = 'MTX00'
    ORDER BY time
"""
df= client.query(query_MTX).set_index('time')

