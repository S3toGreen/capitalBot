import sys
import numpy as np
import pyqtgraph as pg
from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtCore import QTimer, QRectF, Qt
from PySide6.QtGui import QColor, QFont, QPen

pg.setConfigOptions(antialias=True)

class DOMItem(pg.GraphicsObject):
    def __init__(self):
        super().__init__()
        self.bids = np.array([])
        self.asks = np.array([])
        self.max_size = 1 # For scaling the bars
        self.tick_size = 0.25
        self.price_format = "{:.2f}"

        # --- Define Colors and Fonts ---
        self.font = QFont("Arial", 10)
        self.color_bid = QColor("#EF5350") # Red
        self.color_ask = QColor("#26A69A") # Green
        self.color_price = QColor(220, 220, 220)
        self.color_spread_bg = QColor(60, 60, 80, 150)

    def setData(self, data):
        self.bids = np.array(data.get('bids', []))
        self.asks = np.array(data.get('asks', []))
        
        if self.bids.size > 0 and self.asks.size > 0:
            # Update max size for scaling bars (use 99th percentile for stability)
            all_sizes = np.concatenate([self.bids[:, 1], self.asks[:, 1]])
            self.max_size = np.percentile(all_sizes, 99) if len(all_sizes) > 1 else all_sizes.max()
        
        self.picture = pg.QtGui.QPicture()
        self.generatePicture()
        self.informViewBoundsChanged() # Notify the view to update its range
        self.update()

    def generatePicture(self):
        painter = pg.QtGui.QPainter(self.picture)
        if self.bids.size == 0 or self.asks.size == 0:
            painter.end()
            return

        painter.setFont(self.font)

        # Define our virtual column positions
        BID_X, PRICE_X, ASK_X = -1, 0, 1
        
        # --- Highlight the bid/ask spread ---
        best_bid_price = self.bids[0, 0]
        best_ask_price = self.asks[0, 0]
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(self.color_spread_bg)
        painter.drawRect(QRectF(-2, best_bid_price, 4, best_ask_price - best_bid_price))

        # --- Draw Bids ---
        painter.setPen(self.color_bid)
        for price, size in self.bids:
            # Draw size text, right-aligned
            painter.drawText(QRectF(BID_X - 1, price, 1, 0), Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, str(int(size)))
            # Draw bar, scaled by max size
            bar_width = (size / self.max_size) * 0.8
            painter.fillRect(QRectF(BID_X - bar_width, price - self.tick_size*0.4, bar_width, self.tick_size*0.8), self.color_bid)

        # --- Draw Asks ---
        painter.setPen(self.color_ask)
        for price, size in self.asks:
            # Draw size text, left-aligned
            painter.drawText(QRectF(ASK_X, price, 1, 0), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, str(int(size)))
            # Draw bar
            bar_width = (size / self.max_size) * 0.8
            painter.fillRect(QRectF(ASK_X, price - self.tick_size*0.4, bar_width, self.tick_size*0.8), self.color_ask)

        # --- Draw Prices ---
        painter.setPen(self.color_price)
        all_prices = np.unique(np.concatenate([self.bids[:, 0], self.asks[:, 0]]))
        for price in all_prices:
            painter.drawText(QRectF(PRICE_X, price, 0, 0), Qt.AlignmentFlag.AlignCenter, self.price_format.format(price))

        painter.end()

    def paint(self, painter, option, widget):
        if hasattr(self, 'picture'):
            self.picture.play(painter)

    def boundingRect(self):
        # Define the bounding rectangle for the item
        if self.bids.size == 0 or self.asks.size == 0: return QRectF()
        y_min = self.asks[:, 0].min()
        y_max = self.bids[:, 0].max()
        return QRectF(-2, y_min, 4, y_max - y_min)

# --- Example Usage ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Clean DOM Ladder")
        
        # Use a PlotWidget as a container
        plot_widget = pg.PlotWidget()
        self.setCentralWidget(plot_widget)
        
        # Get the PlotItem and configure it
        self.plot_item = plot_widget.getPlotItem()
        self.plot_item.setMouseEnabled(x=False, y=True)
        self.plot_item.getAxis('bottom').hide()
        self.plot_item.getAxis('left').hide()

        # Create and add our custom DOMItem
        self.dom_item = DOMItem()
        self.plot_item.addItem(self.dom_item)

        self.timer = QTimer()
        self.timer.setInterval(250) # Faster updates
        self.timer.timeout.connect(self.generate_fake_data)
        self.timer.start()
        self.base_price = 20000.00

    def generate_fake_data(self):
        # Simulate price movement
        self.base_price += np.random.choice([-0.25, 0, 0.25]) * np.random.randint(0, 2)
        
        # Create fake bid/ask data
        bids = [[np.round(self.base_price - i*0.25, 2), np.random.randint(1, 200)] for i in range(50)]
        asks = [[np.round(self.base_price + 0.25 + i*0.25, 2), np.random.randint(1, 200)] for i in range(50)]
        
        data = {'bids': bids, 'asks': asks}
        self.dom_item.setData(data)

        # Auto-center the view
        center_price = (bids[0][0] + asks[0][0]) / 2.0
        self.plot_item.setYRange(center_price - 12, center_price + 12, padding=0)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = MainWindow()
    win.resize(350, 800)
    win.show()
    sys.exit(app.exec())