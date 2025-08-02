import pyqtgraph as pg
from PySide6 import QtGui, QtCore
import numpy as np
from datetime import datetime

pg.setConfigOptions(imageAxisOrder='row-major',
                    useOpenGL=True,useCupy=True,useNumba=True)

class MinuteAxisItem(pg.DateAxisItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zoomLevels.popitem()
        self.zoomLevels.popitem()

class FixedScaleViewBox(pg.ViewBox):
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        self.last_width = 0
        self.setLimits(minXRange=300,yMin=0)
        self.setMouseEnabled(x=True,y=False)
        self.setAutoVisible(y=True)
        self.enableAutoRange(self.YAxis)
        # self.setMouseMode(self.RectMode)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        current_width = self.width()
        if self.last_width == 0 or current_width == 0:
            self.last_width = current_width
            return
        x_min, x_max = self.viewRange()[0]
        scale = (x_max - x_min) / self.last_width
        new_range_width = scale * current_width
        self.setXRange(x_min, x_min + new_range_width, padding=0)
        self.last_width = current_width

class FootprintItem(pg.GraphicsObject):
    def __init__(self, data):
        super().__init__()
        self.data = data
        self.bar_width_seconds = 60 # Default to 1-minute bars
        self.tick_size = 0.5
        self.imbalance_threshold = 2.0  # e.g., Ask is 2x Bid
        self.generatePicture()

    def setData(self, data):
        """Updates the data and redraws the item."""
        self.data = data
        self.generatePicture()
        self.update()

    def generatePicture(self):
        """Pre-calculates boundaries and prepares for painting."""
        if self.data.empty:
            self.picture = None
            return
            
        # Get unique timestamps to determine bar spacing
        self.timestamps = self.data.index.get_level_values(0).unique()
        if len(self.timestamps) > 1:
            # Calculate bar width from the first two timestamps
            self.bar_width_seconds = (self.timestamps[1] - self.timestamps[0]).total_seconds() * 0.8

        self._bounds = self.boundingRect()
        self.picture = self.paint # For pyqtgraph's internal caching

    def boundingRect(self):
        """Returns the total bounds of the data."""
        if self.data.empty:
            return QtCore.QRectF()
            
        min_time = self.data.index.get_level_values(0).min().timestamp()
        max_time = self.data.index.get_level_values(0).max().timestamp()
        min_price = self.data.index.get_level_values(1).min()
        max_price = self.data.index.get_level_values(1).max()

        # Add padding for the last bar's width
        return QtCore.QRectF(min_time, max_price, (max_time - min_time) + self.bar_width_seconds, (max_price - min_price))

    def paint(self, painter, option, widget):
        if self.data.empty:
            return

        # painter.setRenderHint(painter.RenderHint.Antialiasing, False)
        
        # Get the visible range from the view
        view_range = self.getViewBox().viewRect()
        
        # Group data by timestamp for bar-by-bar processing
        grouped = self.data.groupby(level=0)
        
        for timestamp, bar_data in grouped:
            ts_posix = timestamp.timestamp()
            
            # Culling: Don't draw bars outside the visible range
            if ts_posix + self.bar_width_seconds < view_range.left() or ts_posix > view_range.right():
                continue

            # Find POC (Point of Control) and max volume for this bar
            bar_data['total_volume'] = bar_data['bid'] + bar_data['ask']
            poc_price = bar_data['total_volume'].idxmax()[1]
            max_volume_in_bar = bar_data['total_volume'].max()
            if max_volume_in_bar == 0: continue

            # --- Set up painter properties ---
            painter.setPen(QtGui.QPen(QtGui.QColor(100, 100, 100)))
            font = QtGui.QFont()
            font.setPointSize(8)
            painter.setFont(font)

            # Draw each price level cell within the bar
            for (ts, price), row in bar_data.iterrows():
                bid_vol, ask_vol = row['bid'], row['ask']
                
                # --- Cell Rectangle ---
                # The cell is split in half for bid and ask
                cell_rect = QtCore.QRectF(ts_posix, price - self.tick_size / 2, self.bar_width_seconds, self.tick_size)
                bid_rect = QtCore.QRectF(cell_rect.left(), cell_rect.top(), cell_rect.width() / 2, cell_rect.height())
                ask_rect = QtCore.QRectF(cell_rect.center().x(), cell_rect.top(), cell_rect.width() / 2, cell_rect.height())
                
                # --- Background Coloring (Volume Heatmap) ---
                # Opacity is proportional to volume
                bg_alpha = int(255 * (row['total_volume'] / max_volume_in_bar))
                painter.fillRect(bid_rect, QtGui.QBrush(QtGui.QColor(255, 0, 0, bg_alpha // 4)))
                painter.fillRect(ask_rect, QtGui.QBrush(QtGui.QColor(0, 255, 0, bg_alpha // 4)))
                
                # --- Imbalance Highlighting ---
                if ask_vol > bid_vol * self.imbalance_threshold and bid_vol > 0:
                     painter.fillRect(ask_rect, QtGui.QBrush(QtGui.QColor(0, 255, 0, 150)))
                elif bid_vol > ask_vol * self.imbalance_threshold and ask_vol > 0:
                     painter.fillRect(bid_rect, QtGui.QBrush(QtGui.QColor(255, 0, 0, 150)))

                # --- Text Drawing ---
                text = f"{int(bid_vol)} x {int(ask_vol)}"
                painter.drawText(cell_rect, QtCore.Qt.AlignmentFlag.AlignCenter, text)

                # --- POC Border ---
                if price == poc_price:
                    painter.setBrush(Qt.BrushStyle.NoBrush)
                    poc_pen = QtGui.QPen(QtGui.QColor("yellow"), 2)
                    painter.setPen(poc_pen)
                    painter.drawRect(cell_rect)
                    painter.setPen(QtGui.QPen(QtGui.QColor(100, 100, 100))) # Reset pen

class FootprintChart(pg.GraphicsLayoutWidget):
    def __init__(self,data=None, parent=None, show=False, size=None, title=None, **kargs):
        super().__init__(parent, show, size, title, **kargs)
        self.ci.layout.setSpacing(0)
        self.ci.setContentsMargins(0,0,0,0)

        date_axis = MinuteAxisItem()
        view_box = FixedScaleViewBox()
        self.plot = self.addPlot(row=0, col=0, axisItems={'bottom':date_axis}, viewBox=view_box)
        self.plot.showGrid(x=True, y=True, alpha=0.3)
        self.plot.setLabel('left', 'Price')
        self.plot.setLabel('bottom', 'Time')

        self.fp = FootprintItem()
        self.plot.addItem(self.fp)