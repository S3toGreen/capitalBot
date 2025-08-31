import pyqtgraph as pg
from PySide6 import QtGui, QtCore
import numpy as np
import pandas as pd
from redisworker.Receiver import DataReceiver
from redisworker.AsyncWorker import AsyncWorker
import msgpack

pg.setConfigOptions(imageAxisOrder='row-major',
                    useOpenGL=False, antialias=False)

class MinuteAxisItem(pg.AxisItem):
    def __init__(self, time, orientation='bottom', **kwargs):
        super().__init__(orientation, **kwargs)
        self.setStyle(showValues=True, autoExpandTextSpace=True, autoReduceTextSpace=True)
        self.enableAutoSIPrefix()
        self.time = time.to_numpy()
        self.tf = 1  # timeframe in minute
    def tickStrings(self, values, scale, spacing):
        """Formats the tick labels to show minutes"""
        if not values: return []
        if self.tf*spacing >= 1440: # ≥ 1 day
            fmt = "%Y-%m-%d"
        elif self.tf*spacing >= 60:  # ≥ 1 hour
            fmt = "%m-%d %H:%M"
        else:    # ≥ 1 minute
            fmt = "%H:%M"

        return [self.time[int(v)].strftime(fmt) if 0 <= int(v) < len(self.time) else '' for v in values]

    def append_ts(self,ts):
        self.time=np.append(self.time, pd.Timestamp(ts,unit='s',tz='Asia/Taipei'))
    
class FixedScaleViewBox(pg.ViewBox):
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        self.last_width = 0
        self.setLimits(minXRange=30,yMin=0,xMin=-3)
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

class CandleStickItem(pg.GraphicsObject):
    def __init__(self, data, **kwargs):
        super().__init__()
        self.tdata = data[['open', 'high', 'low', 'close']].astype(float)

        self.x_indices = self.tdata.index.to_numpy()
        self.opens = self.tdata['open'].to_numpy()
        self.highs = self.tdata['high'].to_numpy()
        self.lows = self.tdata['low'].to_numpy()
        self.closes = self.tdata['close'].to_numpy()

        self.pen_wick = pg.mkPen(color=(150, 150, 150)) # Neutral gray for all wicks
        self.pen_green = pg.mkPen('g')
        self.pen_red = pg.mkPen('r')
        self.brush_green = pg.mkBrush('g')
        self.brush_red = pg.mkBrush('r')

        self.prepareGeometryChange()
        self.informViewBoundsChanged()

    def paint(self, p, *args):
        view_box = self.getViewBox()
        if view_box is None:
            return

        x_min, x_max = view_box.viewRange()[0]
        
        # Find the start and end indices of the visible data
        start_index = max(0, int(x_min) - 1)
        end_index = min(len(self.x_indices), int(x_max) + 2)

        # No need to draw if the range is invalid
        if start_index >= end_index:
            return

        # Use array slicing for maximum performance
        visible_x = self.x_indices[start_index:end_index]
        visible_opens = self.opens[start_index:end_index]
        visible_highs = self.highs[start_index:end_index]
        visible_lows = self.lows[start_index:end_index]
        visible_closes = self.closes[start_index:end_index]
        
        
        # Set a dynamic candle width based on zoom
        # This gives a good appearance at different zoom levels
        candle_width = 0.8 * (visible_x[1] - visible_x[0]) if len(visible_x) > 1 else 0.8
        half_width = candle_width / 2

        # --- CORRECTED WICK DRAWING using QPainterPath ---
        # 1. Create a QPainterPath object
        # path = QtGui.QPainterPath()
        
        # 2. Build coordinate arrays for lines (low to high)
        #    Format is [x0, x0, x1, x1, ...], [low0, high0, low1, high1, ...]
        x_coords = np.repeat(visible_x, 2)
        y_coords = np.empty(len(visible_x) * 2)
        y_coords[0::2] = visible_lows
        y_coords[1::2] = visible_highs
        # print(y_coords)
        # 3. Use arrayToQPath to build the path from numpy arrays.
        #    The 'connect' array tells it to start a new line for each wick.
        #    0 = moveTo, 1 = lineTo
        connect = np.zeros(len(x_coords), dtype=np.ubyte)
        connect[0::2] = 1  # Start a new line at the beginning of each wick
        
        # This is the magic function call
        path = pg.functions.arrayToQPath(x_coords, y_coords, connect, False)
        
        # 4. Draw the entire path in a single, fast call
        p.setPen(self.pen_wick)
        p.drawPath(path)
        # --- End of wick drawing ---

        # Draw the candle bodies
        is_green = visible_opens <= visible_closes
        is_red = ~is_green
        
        # Draw green bodies
        green_indices = np.where(is_green)[0]
        if len(green_indices) > 0:
            p.setPen(self.pen_green)
            p.setBrush(self.brush_green)
            # Create a list of QRectF objects for all green candles
            rects = [
                QtCore.QRectF(
                    visible_x[i] - half_width, 
                    visible_opens[i], 
                    candle_width, 
                    visible_closes[i] - visible_opens[i]
                ) for i in green_indices
            ]
            p.drawRects(rects)
        # Draw red bodies
        red_indices = np.where(is_red)[0]
        if len(red_indices) > 0:
            p.setPen(self.pen_red)
            p.setBrush(self.brush_red)
            # Create a list of QRectF objects for all red candles
            rects = [
                QtCore.QRectF(
                    visible_x[i] - half_width, 
                    visible_opens[i], 
                    candle_width, 
                    visible_closes[i] - visible_opens[i]
                ) for i in red_indices
            ]
            p.drawRects(rects)
    
    def boundingRect(self):
        # The bounding rectangle must encompass the entire dataset
        # so pyqtgraph knows the item's total extent.
        if self.x_indices.size == 0:
            return QtCore.QRectF()
        x_min = self.x_indices[0]-1
        x_max = self.x_indices[-1]+1
        y_min = self.lows.min()-1
        y_max = self.highs.max()+1
        return QtCore.QRectF(x_min, y_min, x_max - x_min, y_max - y_min)
    
    def update_bar(self, o,h,l,c,append=False):
        if append:
            self.opens = np.append(self.opens, o/100)
            self.highs = np.append(self.highs, h/100)
            self.lows = np.append(self.lows, l/100)
            self.closes = np.append(self.closes, c/100)
            self.x_indices = np.append(self.x_indices, self.x_indices[-1]+1)
            self.prepareGeometryChange()
        else:
            self.highs[-1] = h/100
            self.lows[-1] = l/100
            self.closes[-1] = c/100
        self.update() # Triggers a repaint of this item

class FootPrintItem(pg.GraphicsObject):
    def __init__(self, data, *args):
        super().__init__(*args)


class MainChart(pg.PlotItem):
    def __init__(self, data, parent=None, name=None, labels=None, title=None, enableMenu=True, **kargs):
        self.xAxis = MinuteAxisItem(data['time'])
        self.view = FixedScaleViewBox()
        super().__init__(parent, name, labels, title, self.view, {'bottom':self.xAxis}, enableMenu, **kargs)
        self.showGrid(x=True, y=True, alpha=0.3)
        self.setLabel('left','Price')
        #add candlestick
        self.candle_item = CandleStickItem(data)
        self.addItem(self.candle_item)
        #add footprint
        # self.fp_item = FootPrintItem(data)
        # self.addItem(self.fp_item)

        self.view.sigXRangeChanged.connect(self.update_y_range)
        self.update_x_limits()
    def update_y_range(self):
        """This function is called when the user pans or zooms."""
        x_min, x_max = self.view.viewRange()[0]
        
        # Convert float range to integer indices
        start_idx = max(0, int(x_min))
        end_idx = min(len(self.candle_item.x_indices), int(x_max) + 1)
        
        # # Slice the DataFrame to get only the visible data
        visible_data = self.candle_item.x_indices[start_idx:end_idx]
        
        if not visible_data.size:
            return # Do nothing if no data is visible
            
        # Find the min and max of the 'low' and 'high' columns for the visible data
        y_min = self.candle_item.lows[start_idx:end_idx].min()
        y_max = self.candle_item.highs[start_idx:end_idx].max()
        
        # Add some padding to the top and bottom
        padding = (y_max - y_min) * 0.1
        self.view.setYRange(y_min - padding, y_max + padding, padding=0)

    def update_x_limits(self):
        self.view.setLimits(xMax=len(self.xAxis.time)+6)

    def update_data(self, data, append=False):
        if append:
            self.xAxis.append_ts(data['ts'])
            self.update_x_limits()
        self.candle_item.update_bar(data['o'],data['h'],data['l'],data['c'],append)
        

class Chart(pg.GraphicsLayout):
    def __init__(self, data, parent=None, border=None):
        super().__init__(parent, border)
        self.last_bar_time= int(data['time'].iloc[-1].timestamp())
        self.setSpacing(0)
        self.setContentsMargins(0,0,0,0)
        self.main = MainChart(data)
        self.addItem(self.main,0,0)
        self.main.sigXRangeChanged.connect(self.update_volume_range)

        self.vol_plt = self.addPlot(1,0)
        view = self.vol_plt.getViewBox()
        view.setLimits(yMin=0)
        view.enableAutoRange(y=True)
        view.setAutoVisible(y=True)
        view.setMouseEnabled(y=False)
        self.vol_plt.setXLink(self.main)
        self.vol_plt.setLabel('left', 'Volume')
        self.vol_item = pg.BarGraphItem(x=data.index.to_numpy(),
                                          height=data['vol'].to_numpy(),
                                          width=0.8,brush=pg.mkBrush('lightgray'))
        self.vol_plt.addItem(self.vol_item)

        self.layout.setRowStretchFactor(0,3)
        # update autoscroll flag
        # x_range = self.view.viewRange()[0]
        # self.auto_scroll = x_range[1] > (len(self.ohlcv) - 3)
    def update_data(self, data):
        if data['ts']!= self.last_bar_time:
            # print(data['ts'],self.last_bar_time,"append")
            self.last_bar_time=data['ts']
            self.main.update_data(data,True)
        else:
            self.main.update_data(data) 
    def update_volume_range(self):
        # get currently visible X range in main chart
        x_min, x_max = self.main.viewRange()[0]  # (xmin, xmax)
        start_idx = max(0, int(x_min))
        end_idx = min(len(self.vol_item.opts.get('x')), int(x_max) + 1)
        # find indices within this range
        visible_vol = self.vol_item.opts.get('height')[start_idx:end_idx]
        # print(type(visible_vol))
        if visible_vol.size:
            y_max = visible_vol.max()
            self.vol_plt.getViewBox().setYRange(0, y_max * 1.05, padding=0)
            
class PlotWidget(pg.GraphicsLayoutWidget):
    def __init__(self,ohlcv=None, parent=None, show=False, size=None, title=None, **kargs):
        super().__init__(parent, show, size, title, **kargs)
        self.worker = AsyncWorker()
        self.datareceiver = DataReceiver.create(self.worker,channels=['snap:DM:MTX00'])
        self.datareceiver.message_received.connect(self._msg_handler)

        self.ci.layout.setSpacing(0)
        self.ci.setContentsMargins(0,0,0,0)

        self.MTX = Chart(ohlcv)
        self.addItem(self.MTX,row=0,col=0)    

    @QtCore.Slot(str,str,bytes)
    def _msg_handler(self, pattern, channel, data):
        """Handles incoming messages from Redis."""
        if not pattern:
            # not quote
            channel, market, symbol = channel.split(':')
            # if channel == 'snap':
            #     ts= pd.Timestamp(data['time'])
            data = msgpack.unpackb(data, strict_map_key=False)
            # print(symbol, data)
            self.MTX.update_data(data)
        else:
            pass
            # print('pattern',data)
    