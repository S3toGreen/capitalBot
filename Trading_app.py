import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QDockWidget, QWidget,
    QVBoxLayout, QTableView, QTabWidget, QPushButton,
    QToolBar, QStatusBar, QLineEdit, QLabel,QSplitter
)
from PySide6.QtGui import QAction, QIcon
from PySide6.QtCore import Qt, Slot, Signal
from DataCenter import DataCenter
from GUI.watchlist import WatchList

class TradingMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Trading App")
        self.setMinimumSize(1500, 900) # 設定最小尺寸

        self.data_center = DataCenter.create()
        self.watchlist = WatchList(self)

        self._create_widgets()
        self._create_layouts()
        self._setup_dock_widgets()

        # Connect data signals
        self.data_center.quote_sig.connect(self._handle_quote)

        # self.data_center.sub_ticker('DM','TX00',None)
        # self.data_center.sub_ticker('OS','NQ0000',None)


    def _create_widgets(self):
        """初始化主要功能區的 Widget"""
        # 這些將是您自定義的組件
        self.quote_watchlist = WatchList() # 暫時用 QWidget 佔位
        self.market_data_detail = QWidget() # 暫時用 QWidget 佔位
        self.order_entry = QWidget() # 暫時用 QWidget 佔位
        self.open_positions = QWidget() # 暫時用 QWidget 佔位
        self.strategy_monitor = QWidget() # 暫時用 QWidget 佔位

        # 創建一個 QTabWidget 來容納市場數據和訂單輸入
        self.market_order_tabs = QTabWidget()
        self.market_order_tabs.addTab(self.market_data_detail, "行情詳情")
        self.market_order_tabs.addTab(self.order_entry, "下單")
    def _create_layouts(self):
        """設定主窗口的佈局"""
        # 主中央Widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout(central_widget)
        # 使用 QSplitter 讓用戶可以調整面板大小
        top_splitter = QSplitter(Qt.Orientation.Horizontal)
        top_splitter.addWidget(self.quote_watchlist)
        top_splitter.addWidget(self.market_order_tabs)
        # top_splitter.setStretchFactor(0, 1) # 報價列表佔用較少空間
        # top_splitter.setStretchFactor(1, 3) # 行情/下單佔用較多空間

        bottom_splitter = QSplitter(Qt.Orientation.Horizontal)
        bottom_splitter.addWidget(self.open_positions)
        bottom_splitter.addWidget(self.strategy_monitor)
        # bottom_splitter.setStretchFactor(0, 1)
        # bottom_splitter.setStretchFactor(1, 1)


        # 將 top_splitter 和 bottom_splitter 垂直堆疊
        overall_splitter = QSplitter(Qt.Orientation.Vertical)
        overall_splitter.addWidget(top_splitter)
        overall_splitter.addWidget(bottom_splitter)
        # overall_splitter.setStretchFactor(0, 2) # 上半部分佔用較多空間
        # overall_splitter.setStretchFactor(1, 1) # 下半部分佔用較少空間

        main_layout.addWidget(overall_splitter)
    def _setup_dock_widgets(self):
        """
        設定 Dock Widget (可拖曳和停靠的面板)。
        這讓用戶可以自由安排介面佈局。
        """
        quote_dock = QDockWidget("Quote List", self)
        quote_dock.setWidget(self.quote_watchlist)
        self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, quote_dock)

        market_data_dock = QDockWidget("行情詳情/下單", self)
        market_data_dock.setWidget(self.market_order_tabs)
        self.addDockWidget(Qt.DockWidgetArea.RightDockWidgetArea, market_data_dock)

        positions_dock = QDockWidget("Positions", self)
        positions_dock.setWidget(self.open_positions)
        self.addDockWidget(Qt.DockWidgetArea.BottomDockWidgetArea, positions_dock)
        
        strategy_dock = QDockWidget("Strategy Monitor", self)
        strategy_dock.setWidget(self.strategy_monitor)
        self.addDockWidget(Qt.DockWidgetArea.BottomDockWidgetArea, strategy_dock)
        self.tabifyDockWidget(positions_dock, strategy_dock) # 讓它們可以以標籤頁形式顯示

        # 注意：使用 QSplitter 和 QDockWidget 可以實現非常靈活的佈局。
        # 對於初次設計，從 QSplitter 開始可能更容易理解。
        # 如果需要更高級的佈局管理，QDockWidget 是更好的選擇。
        pass # 目前我們使用 QSplitter 進行佈局
    @Slot(str,dict)
    def _handle_quote(self,channel,data):
        market = channel[6:]
        # print(market, data)
        self.quote_watchlist.quote_update(market,data)
        pass

    def closeEvent(self, event):
        self.data_center.stop()
        return super().closeEvent(event)

if __name__=='__main__':
    import ctypes
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(u'com.S3toGreen.TickersApp')
    app = QApplication([])
    app.setStyle('Fusion')
    icon = QIcon('icon.ico')
    app.setWindowIcon(icon)

    main = TradingMainWindow()
    main.show()
    
    sys.exit(app.exec())