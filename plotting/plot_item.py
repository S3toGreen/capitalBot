import sys
import numpy as np
import pyqtgraph as pg
from pyqtgraph import DateAxisItem
from PySide6 import QtGui, QtCore, QtWidgets
import pandas as pd
from clickhouse_connect import get_client

pg.setConfigOptions(useOpenGL=True)
pg.setConfigOption('background', 'k')
pg.setConfigOption('foreground', 'w')
pg.setConfigOption('antialias', True)

class CandlestickItem(pg.GraphicsObject):
    def __init__(self, data):
        super().__init__()
        # self.db = get_client(host='localhost', username='admin', password='Seto927098', compression=True)
        # q = f"""
        #     SELECT time,open,high,low,close,volume
        #     FROM orderflowDM
        #     WHERE symbol = '{symbol}'
        #     ORDER BY time
        # """
        # self.data= self.db.query_np(q)
        self.data = np.array(data)
        self.wick_pen = pg.mkPen('w', width=3)
        self.bull_brush = pg.mkBrush(color='#00b060')
        self.bear_brush = pg.mkBrush(color='#d40000')
        # self.setFlag(self.ItemUsesExtendedPainters)
        self.generatePicture()
    
    def generatePicture(self):
        """
        Pre-generates the QPicture object that draws the candles.
        This is called when the data changes.
        """
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)
        
        w = 0.4 # Width of the candle body relative to the bar spacing

        for (t, o, h, l, c, v) in self.data:
            p.setPen(self.wick_pen)
            # Draw high-low wick
            p.drawLine(QtCore.QPointF(t, l), QtCore.QPointF(t, h))

            # Set brush color based on open/close
            # p.setPen(pg.mkPen(None))
            p.setBrush(self.bull_brush if c >= o else self.bear_brush)
            # Draw open-close body
            p.drawRect(QtCore.QRectF(t - w*6, o, w * 12, c - o))


    def paint(self, painter, *args):
        """
        This method is called by the graphics framework to draw the item.
        It simply replays the pre-generated QPicture.
        """
        painter.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        """
        Returns the bounding rectangle of all data points.
        This is required for PyQtGraph to manage item updates and view autoscaling.
        """
        if self.data is None or len(self.data) == 0:
            return QtCore.QRectF()
        
        t_min, t_max = self.data[0, 0], self.data[-1, 0]
        p_min = self.data[:, 3].min()  # Minimum of all 'low' prices
        p_max = self.data[:, 2].max()  # Maximum of all 'high' prices
        
        # Add some padding to the timestamp bounds
        bar_width = self.data[1, 0] - self.data[0, 0] if len(self.data) > 1 else 60
        return QtCore.QRectF(t_min - bar_width, p_min, (t_max - t_min) + 2 * bar_width, p_max - p_min)

if __name__=='__main__':
    app = QtWidgets.QApplication([])
    win = QtWidgets.QMainWindow()
    win.setWindowTitle('Candlestick Chart')
    # win.resize(800, 600)
    glw = pg.GraphicsLayoutWidget()
    win.setCentralWidget(glw)
    plot = glw.addPlot(axisItems={'bottom': DateAxisItem()})
    plot.setLabel('left', 'Price')
    plot.setMouseEnabled(x=True,y=False)
    plot.showGrid(x=True,y=True,alpha=.15)
    plot.setClipToView(True)
    plot.setDownsampling(mode='peak')
    # t = CandlestickItem("MTX00")

    #dummy data
    ts=pd.to_datetime('20251010').timestamp()
    data=[
        (ts,500,512,490,509,300),
        (ts+60,509.5,515,504,513,150),
        (ts+120,513,530,510,525,750),
        (ts+180,525,525,485,505,1500)
    ]
    t = CandlestickItem(data)
    plot.addItem(t)
    win.show()
    sys.exit(app.exec())
