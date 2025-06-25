import pandas as pd
import pyqtgraph as pg
from PySide6 import QtWidgets, QtCore
import numpy as np
import sys

# 步驟 1: 建立自訂的 K 線圖形項目 (CandlestickItem)
# 這個類別負責接收 OHLC 數據並高效地繪製 K 線。
class CandlestickItem(pg.GraphicsObject):
    def __init__(self, data):
        """
        參數:
        data (list): 一個包含元組 (tuple) 的列表，格式為 (timestamp, open, high, low, close)。
        """
        pg.GraphicsObject.__init__(self)
        self.data = data
        self.generatePicture()

    def generatePicture(self):
        """
        這是效能優化的核心。
        我們不逐一繪製每個物件，而是將所有 K 線一次性畫在一個 QPicture 上。
        之後的重繪操作只需要顯示這個 picture 即可，速度非常快。
        """
        self.picture = pg.QtGui.QPicture()
        # QPainter 是用來執行繪圖操作的工具
        p = pg.QtGui.QPainter(self.picture)
        
        # 設定 K 線的寬度，0.4 是基於時間戳單位
        w = 0.4 
        
        # 遍歷所有數據點來繪製
        for (timestamp, open_val, high_val, low_val, close_val) in self.data:
            # 設置畫筆顏色為白色，用來畫影線
            p.setPen(pg.mkPen('w'))
            # 繪製從最高價到最低價的垂直線 (影線)
            p.drawLine(QtCore.QPointF(timestamp, low_val), QtCore.QPointF(timestamp, high_val))
            
            # 根據漲跌決定 K 棒的顏色
            # 收盤價 > 開盤價，是上漲 K 棒，設置為綠色
            if close_val > open_val:
                p.setBrush(pg.mkBrush('g'))
                p.setPen(pg.mkPen('g')) # 邊框也設為綠色
            # 收盤價 < 開盤價，是下跌 K 棒，設置為紅色
            else:
                p.setBrush(pg.mkBrush('r'))
                p.setPen(pg.mkPen('r')) # 邊框也設為紅色

            # 繪製 K 棒的實體部分
            # QRectF(左上角 x, 左上角 y, 寬度, 高度)
            p.drawRect(QtCore.QRectF(timestamp - w/2, open_val, w, close_val - open_val))
        
        # 結束繪圖
        p.end()

    def paint(self, painter, *args):
        """這個方法會在需要重繪圖形時被 pyqtgraph 自動呼叫"""
        painter.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        """
        這個方法必須實作，它告訴 pyqtgraph 這個圖形物件所佔據的矩形範圍，
        pyqtgraph 會根據這個範圍來自動調整視圖。
        """
        return QtCore.QRectF(self.picture.boundingRect())

# 步驟 2: 建立自訂的時間軸標籤 (DateAxis)
class DateAxis(pg.AxisItem):
    """這個類別讓 X 軸的刻度顯示為可讀的 HH:MM 格式，而不是一長串數字。"""
    def tickStrings(self, values, scale, spacing):
        # 將 pyqtgraph 傳入的 timestamp 數值轉換為日期時間字串
        return [pd.to_datetime(value, unit='s').strftime('%H:%M') for value in values]

# 步驟 3: 建立主應用程式視窗 (OrderFlowChart)
class OrderFlowChart(QtWidgets.QMainWindow):
    def __init__(self, data):
        super().__init__()
        self.setWindowTitle("訂單流圖表 - 主面板")
        self.resize(800, 600)
        
        # 建立一個 GraphicsLayoutWidget，這是 pyqtgraph 中組織圖表佈局的強力工具
        self.graph_widget = pg.GraphicsLayoutWidget()
        self.setCentralWidget(self.graph_widget)

        # 在佈局中新增一個圖表 (PlotItem)，並指定 X 軸使用我們自訂的 DateAxis
        self.plot = self.graph_widget.addPlot(row=0, col=0, axisItems={'bottom': DateAxis(orientation='bottom')})
        
        # 設定圖表的標籤和網格線
        self.plot.setLabel('left', "價格 (Price)")
        self.plot.setLabel('bottom', "時間 (Time)")
        self.plot.showGrid(x=True, y=True, alpha=0.3)
        
        # 呼叫繪圖函式
        self.plot_candlestick(data)

    def plot_candlestick(self, data):
        """此函式負責處理數據並將 K 線圖加入到圖表中"""
        df = pd.DataFrame(data)
        
        # pyqtgraph 的時間軸是基於 Unix timestamp (從 1970 年至今的秒數)
        # 我們需要將 pandas 的 datetime 物件轉換成 timestamp
        timestamps = df['time'].astype(np.int64) // 10**9

        # 將數據整理成 CandlestickItem 需要的格式: (timestamp, open, high, low, close)
        candlestick_data = []
        for i in range(len(df)):
            candlestick_data.append((
                timestamps[i],
                df['open'][i],
                df['high'][i],
                df['low'][i],
                df['close'][i]
            ))
            
        # 建立我們自訂的 CandlestickItem 實例
        candlestick_item = CandlestickItem(candlestick_data)
        
        # 將 K 線圖形項目加入到圖表中
        self.plot.addItem(candlestick_item)


def main():
    # 這是您提供的範例數據
    sample_data = {
        'time': pd.to_datetime(['2025-06-24 13:00', '2025-06-24 13:01', '2025-06-24 13:02', '2025-06-24 13:03']),
        'open': [100, 102, 101, 103],
        'high': [103, 104, 103.5, 105],
        'low': [99, 101.5, 100.5, 102.5],
        'close': [102, 103, 103, 104.5],
        'delta_hlc': [50, -30, 80, -20],
        'trades_delta': [120, -80, 150, -50],
        'price_level': [
            [(100, 100, 10, 50), (101, 150, -5, -20), (102, 200, 25, 100)],
            [(102, 120, 15, 60), (103, 180, -10, -40), (104, 220, 30, 120)],
            [(101, 110, 5, 30), (102, 160, -15, -60), (103, 210, 20, 80)],
            [(103, 130, 20, 70), (104, 190, -20, -80), (105, 230, 35, 140)]
        ]
    }
    
    # 建立 Qt 應用程式
    app = QtWidgets.QApplication(sys.argv)
    # 建立我們的 OrderFlowChart 視窗，並傳入數據
    window = OrderFlowChart(sample_data)
    # 顯示視窗
    window.show()
    # 執行應用程式
    sys.exit(app.exec())

if __name__ == '__main__':
    main()