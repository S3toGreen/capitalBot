import pyqtgraph as pg
from PySide6.QtCore import Slot, QRectF, Qt, QPointF
from PySide6.QtGui import QColor,QPen,QBrush,QPicture,QPainter, QPainterPath
# from fpimage import FPHeatmap
import numpy as np
import pandas as pd
import msgspec
import math

pg.setConfigOptions(imageAxisOrder='row-major',
                    useOpenGL=False, antialias=False)
class ChartScheme:
    pen_wick = pg.mkPen('#9E9E9E') # Neutral gray for all wicks
    pen_green = pg.mkPen('#26A69A')
    pen_red = pg.mkPen('#EF5350')
    brush_green = pg.mkBrush('g')
    brush_red = pg.mkBrush('r')

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
        self.setLimits(minXRange=30,yMin=0,xMin=-6,maxXRange=2400)
        self.setMouseEnabled(x=True, y=False)
        self.setAutoVisible(x=False, y=True)
        self.enableAutoRange(x=False, y=True)
        self.setAspectLocked(False)

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

# class CandleStickItem(pg.GraphicsObject):
#     def __init__(self, data, **kwargs):
#         super().__init__()
#         tdata = data[['open', 'high', 'low', 'close']]

#         self.x_indices = tdata.index.to_numpy()
#         self.opens = tdata['open'].to_numpy()
#         self.highs = tdata['high'].to_numpy()
#         self.lows = tdata['low'].to_numpy()
#         self.closes = tdata['close'].to_numpy()
#         self.pen_wick = pg.mkPen('#9E9E9E') # Neutral gray for all wicks
#         self.pen_green = pg.mkPen('#26A69A')
#         self.pen_red = pg.mkPen('#EF5350')
#         self.brush_green = pg.mkBrush('g')
#         self.brush_red = pg.mkBrush('r')

#         self.prepareGeometryChange()
#         self.informViewBoundsChanged()

#     def paint(self, p, *args):
#         view_box = self.getViewBox()
#         if view_box is None:
#             return

#         x_min, x_max = view_box.viewRange()[0]
        
#         # Find the start and end indices of the visible data
#         start_index = max(0, int(x_min) - 1)
#         end_index = min(len(self.x_indices), int(x_max) + 2)

#         # No need to draw if the range is invalid
#         if start_index >= end_index:
#             return

#         # Use array slicing for maximum performance
#         visible_x = self.x_indices[start_index:end_index]
#         visible_opens = self.opens[start_index:end_index]
#         visible_highs = self.highs[start_index:end_index]
#         visible_lows = self.lows[start_index:end_index]
#         visible_closes = self.closes[start_index:end_index]
        
        
#         # Set a dynamic candle width based on zoom
#         # This gives a good appearance at different zoom levels
#         candle_width = 0.8 * (visible_x[1] - visible_x[0]) if len(visible_x) > 1 else 0.8
#         half_width = candle_width / 2

#         # --- CORRECTED WICK DRAWING using QPainterPath ---
#         # 1. Create a QPainterPath object
#         # path = QtGui.QPainterPath()
        
#         # 2. Build coordinate arrays for lines (low to high)
#         #    Format is [x0, x0, x1, x1, ...], [low0, high0, low1, high1, ...]
#         x_coords = np.repeat(visible_x, 2)
#         y_coords = np.empty(len(visible_x) * 2)
#         y_coords[0::2] = visible_lows
#         y_coords[1::2] = visible_highs
#         # 3. Use arrayToQPath to build the path from numpy arrays.
#         #    The 'connect' array tells it to start a new line for each wick.
#         #    0 = moveTo, 1 = lineTo
#         connect = np.zeros(len(x_coords), dtype=np.ubyte)
#         connect[0::2] = 1  # Start a new line at the beginning of each wick
        
#         # This is the magic function call
#         path = pg.functions.arrayToQPath(x_coords, y_coords, connect, False)
        
#         # 4. Draw the entire path in a single, fast call
#         p.setPen(self.pen_wick)
#         p.drawPath(path)
#         # --- End of wick drawing ---

#         # Draw the candle bodies
#         is_green = visible_opens <= visible_closes
#         is_red = ~is_green
        
#         # Draw green bodies
#         green_indices = np.where(is_green)[0]
#         if len(green_indices) > 0:
#             p.setPen(self.pen_green)
#             p.setBrush(self.brush_green)
#             # Create a list of QRectF objects for all green candles
#             rects = [
#                 QRectF(
#                     visible_x[i] - half_width, 
#                     visible_opens[i], 
#                     candle_width, 
#                     visible_closes[i] - visible_opens[i]
#                 ) for i in green_indices
#             ]
#             p.drawRects(rects)
#         # Draw red bodies
#         red_indices = np.where(is_red)[0]
#         if len(red_indices) > 0:
#             p.setPen(self.pen_red)
#             p.setBrush(self.brush_red)
#             # Create a list of QRectF objects for all red candles
#             rects = [
#                 QRectF(
#                     visible_x[i] - half_width, 
#                     visible_opens[i], 
#                     candle_width, 
#                     visible_closes[i] - visible_opens[i]
#                 ) for i in red_indices
#             ]
#             p.drawRects(rects)
    
#     def boundingRect(self):
#         if self.x_indices.size == 0:
#             return QRectF()
#         x_min = self.x_indices[0]
#         x_max = self.x_indices[-1]+1
#         y_min = self.lows.min()
#         y_max = self.highs.max()+1
#         return QRectF(x_min, y_min, x_max - x_min, y_max - y_min)
#     def dataBounds(self, axis, frac, orthoRange):
#         if not orthoRange:
#             return (None,None)
#         if axis == 1:
#             if orthoRange is not None and orthoRange[0] is not None:
#                 x0, x1 = orthoRange
#                 x0 = max(0, int(x0))
#                 x1 = min(self.x_indices[-1], int(x1))
#                 return (self.lows[x0:x1+1].min(),self.highs[x0:x1+1].max())
#         else:
#             return (None, None)

#     def update_bar(self, o,h,l,c,append=False):
#         if append:
#             self.opens = np.append(self.opens, o/100)
#             self.highs = np.append(self.highs, h/100)
#             self.lows = np.append(self.lows, l/100)
#             self.closes = np.append(self.closes, c/100)
#             self.x_indices = np.append(self.x_indices, self.x_indices[-1]+1)
#             self.prepareGeometryChange()
#         else:
#             self.highs[-1] = h/100
#             self.lows[-1] = l/100
#             self.closes[-1] = c/100
#         self.update() # Triggers a repaint of this item

# class FootPrintItem(pg.ImageItem):
#     def __init__(self, data=None, **kargs):
#         # preprocess data 
#         image = data
#         super().__init__(image, autoLevels=False, **kargs)
#         self.setRect()
#     def update_data(self):
#         self.updateImage()

# class FootPrintTile(pg.GraphicsObject):
#     """
#     single footprint graphic object to do both text and heatmap through QPicture
#     """
#     def __init__(self, data, *args):
#         super().__init__(*args)
#         self.time_to_index = {t: i for i, t in enumerate(data['time'].drop_duplicates())}


class CandleStickTile(pg.GraphicsObject, ChartScheme):
    def __init__(self, data, tile_size=500):
        super().__init__()
        self.tile_size = tile_size
        self.setCacheMode(self.CacheMode.DeviceCoordinateCache)
        self.setData(data)

    def setData(self, data: pd.DataFrame):
        """初始化資料 + 預先分 tile"""
        self.x = data.index.to_numpy()
        self.o = data['open'].to_numpy(dtype=float)
        self.h = data['high'].to_numpy(dtype=float)
        self.l = data['low'].to_numpy(dtype=float)
        self.c = data['close'].to_numpy(dtype=float)
        self._bounding_rect = QRectF(0,self.l.min(),self.x[-1]+1,self.h.max()-self.l.min())
        self.tiles = []
        self._generate_tiles()
        self.prepareGeometryChange()
        self.informViewBoundsChanged()

    def _generate_tiles(self):
        """依 tile_size 建立快取 QPicture"""
        self.tiles.clear()
        n = len(self.x)
        for start in range(0, n, self.tile_size):
            stop = min(start + self.tile_size, n)
            pic = self._make_tile(start, stop)
            self.tiles.append((start, stop, pic))

    def _make_tile(self, start, stop):
        """單個 tile → QPicture"""
        picture = QPicture()
        p = QPainter(picture)
        w = 0.75

        xx = np.repeat(self.x[start:stop], 2)
        yy = np.empty(len(xx), dtype=float)
        yy[0::2] = self.l[start:stop]
        yy[1::2] = self.h[start:stop]

        connect = np.zeros(len(xx), dtype=np.ubyte)
        connect[0::2] = 1
        path = pg.functions.arrayToQPath(xx, yy, connect, finiteCheck=False)
        p.setPen(self.pen_wick)
        p.drawPath(path)

        up_mask = self.c[start:stop] >= self.o[start:stop]
        dn_mask = ~up_mask
        self._draw_bodies(p, start, stop, up_mask, 'g', w)
        self._draw_bodies(p, start, stop, dn_mask, 'r', w)
        p.end()
        return picture

    def _draw_bodies(self, p, start, stop, mask, color, w):
        idx = np.nonzero(mask)[0] + start
        if idx.size == 0:
            return
        xs = self.x[idx]
        o = self.o[idx]; c = self.c[idx]
        # y0 = np.minimum(o, c)
        h = c-o

        path = QPainterPath()
        for x, y, hh in zip(xs, o, h):
            # Rectangle as four lines in path (avoids QRects in Python)
            path.addRect(x - w/2, y, w, hh)

        p.setPen(self.pen_green if color=='g' else self.pen_red)
        p.setBrush(self.brush_green if color=='g' else self.brush_red)
        p.drawPath(path)

    def paint(self, p, *args):
        view = self.getViewBox()
        if view is None:
            return
        x_min, x_max = view.viewRange()[0]

        # 只畫可見的 tile
        for start, stop, pic in self.tiles:
            if stop < x_min or start > x_max:
                continue
            p.drawPicture(0, 0, pic)

    def boundingRect(self):
        if len(self.x) == 0:
            return QRectF()
        return self._bounding_rect
    def dataBounds(self, axis, frac, orthoRange):
        if orthoRange:
            x0, x1 = orthoRange
            x0 = max(0, math.floor(x0))
            x1 = min(self.x[-1]+1, math.ceil(x1))
            if x0<x1:
                return (self.l[x0:x1].min()-1,self.h[x0:x1].max()+1)
        return (None,None)
    def append_bar(self, idx, o, h, l, c):
        self.x = np.append(self.x, idx)
        self.o = np.append(self.o, o)
        self.h = np.append(self.h, h)
        self.l = np.append(self.l, l)
        self.c = np.append(self.c, c)
        # 只更新最後一個 tile
        if len(self.x) % self.tile_size == 1:
            # new tile
            self.tiles.append((len(self.x) - 1, len(self.x), 
                                self._make_tile(len(self.x) - 1, len(self.x))))
        else:
            start, _, _ = self.tiles[-1]
            self.tiles[-1] = (start, len(self.x),
                              self._make_tile(start, len(self.x)))
        self.setCacheMode(self.CacheMode.NoCache)
        self.prepareGeometryChange()
        self._bounding_rect = QRectF(0,self.l.min(),self.x[-1]+1,self.h.max()-self.l.min())
        self.update()
        self.setCacheMode(self.CacheMode.DeviceCoordinateCache)
        self.informViewBoundsChanged()

class LiveBarItem(pg.GraphicsObject, ChartScheme):
    def __init__(self, *args):
        super().__init__(*args)
        self.idx = None
        self.o = self.h = self.l = self.c = None
        self.fp = None
        self._bounding_rect = QRectF()

    def update_bar(self, data, idx):
        self.h = data.get('h')/100
        self.l = data.get('l')/100
        self.c = data.get('c')/100
        self.o = data.get('o')/100
        self.fp = data.get('pm')
        tmp = QRectF(idx-1, self.l, 2, self.h-self.l)
        if self.idx is None or self.idx!=idx or tmp!=self._bounding_rect:
            self.prepareGeometryChange()
            self.informViewBoundsChanged()
            self._bounding_rect = tmp
            self.idx = idx
        self.update()
    def paint(self, p: QPainter, *args):
        if self.idx is None:
            return
        p.setPen(self.pen_wick)
        p.drawLine(QPointF(self.idx, self.l), QPointF(self.idx, self.h))

        is_up = self.c >= self.o
        p.setPen(self.pen_green if is_up else self.pen_red)
        p.setBrush(self.brush_green if is_up else self.brush_red)
        w=.75
        p.drawRect(QRectF(self.idx-w/2,self.o,w,self.c-self.o))

    def boundingRect(self):
        return self._bounding_rect
    def dataBounds(self, axis, frac, orthoRange):
        if orthoRange and self.idx and orthoRange[1]>=self.idx:
            return (self.l-1, self.h+1)
        return (None,None)
    
class PriceChart(pg.PlotItem):
    def __init__(self, data, fp, parent=None, name=None, labels=None, title=None, viewBox=None, axisItems=None, enableMenu=True, **kargs):
        self.vb = FixedScaleViewBox()
        super().__init__(parent, name, labels, title, self.vb, axisItems, enableMenu, **kargs)
        self.showGrid(x=True, y=True, alpha=0.3)
        self.setLabel('left','Price')
        self.xAxis = self.getAxis('bottom')

        # history bar tiling
        self.tiling = CandleStickTile(data, 1500)
        self.addItem(self.tiling)
        
        # Live bar
        self.live = LiveBarItem()
        self.addItem(self.live)

        # crosshair
        self.vLine = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('#FFD54F', width=1))
        self.hLine = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('#FFD54F', width=1))
        self.addItem(self.vLine, ignoreBounds=True)
        self.addItem(self.hLine, ignoreBounds=True)
        # labels: price (right side) and time (bottom)
        self.price_label = pg.TextItem(anchor=(1,1), color='#FFFFFF')  # right-aligned
        self.time_label = pg.TextItem(anchor=(0.5,1), color='#FFFFFF')   # centered above bottom
        # keep labels always visible (ignore bounds)
        self.addItem(self.price_label, ignoreBounds=True)
        self.addItem(self.time_label, ignoreBounds=True)

        self.proxy = None
        self.cursor_callback = None
        self.last_x = -1
        
    def setup_crosshair(self, rateLimit=60):
        """Call this *after* the plot item has been added to a scene."""
        scene = self.scene()

        if scene is None:
            return

        if self.proxy is not None:
            try:
                self.proxy.disconnect()
            except Exception:
                pass
        self.proxy = pg.SignalProxy(scene.sigMouseMoved, rateLimit=rateLimit, slot=self.mouse_moved)

    def update_data(self, data, idx):
        # if new idx append to tile

        if self.live.idx is not None and self.live.idx!=idx:
            self.tiling.append_bar(self.live.idx, self.live.o, self.live.h, self.live.l,self.live.c)
        self.live.update_bar(data, idx)

    def mouse_moved(self, evt):
        """Handle scene mouse movement: update crosshair in this plot and call external sync callback."""
        pos = evt[0]  # SignalProxy passes a tuple; first element is QPointF (scene coords)
        if not self.sceneBoundingRect().contains(pos):
            return

        # map scene position to data coordinates in this viewbox
        mousePoint = self.vb.mapSceneToView(pos)
        x = mousePoint.x()
        y = mousePoint.y()
        # --- IMPROVEMENT 1: Snapping and Performance ---
        # Round to the nearest integer index. This is the "snapped" position.
        xi = int(round(x))

        # If we are still on the same candle, do nothing. This is a huge performance boost.
        if xi == self.last_x:
            # Still update the horizontal line for smooth Y movement
            self.hLine.setPos(y)
            # And the price label, as it depends on Y
            _, x_max = self.vb.viewRange()[0]
            self.price_label.setText(f"{y:.2f}")
            self.price_label.setPos(x_max, y)
            return
        
        # Check if the snapped index is valid data
        if 0 <= xi < len(self.xAxis.time):
            self.last_x = xi
            
            # --- IMPROVEMENT 2: Snapped Crosshair ---
            # Set the vertical line to the center of the candle
            # snapped_x = self.tiling.x[xi]
            self.vLine.setPos(xi)
            self.hLine.setPos(y)

            # --- IMPROVEMENT 3: Richer Data Display ---
            # o = self.tiling.o[xi]
            # h = self.tiling.h[xi]
            # l = self.tiling.l[xi]
            # c = self.tiling.c[xi]
            
            # A more informative label showing OHLC. You can add Volume if available.
            # data_text = f"O: {o:.2f}  H: {h:.2f}  L: {l:.2f}  C: {c:.2f}"
            
            # For this label, let's use the price_label and anchor it differently
            # so it doesn't collide with the crosshair's price label.
            # (For now, we'll just update the price label as before)
            _, x_max = self.vb.viewRange()[0]
            self.price_label.setText(f"{y:.2f}")
            self.price_label.setPos(x_max, y)

            # --- IMPROVEMENT 4: Robust Time Label ---
            # Your original time logic was already good. Let's reuse it.
            # (Assuming you have a way to map index to time, e.g., on self.xAxis)
            ts = self.xAxis.time[xi] 
            time_text = ts.strftime("%m-%d %H:%M")
            self.time_label.setText(time_text)
            view_range = self.vb.viewRange()
            self.time_label.setPos(xi, view_range[1][0]) # Position at bottom of view
            # --- IMPROVEMENT 5: Enhanced Callback ---
            # Pass the snapped index, which is much more useful for syncing.
            # if callable(self.cursor_callback):
            #     # Pass a dictionary for clarity and future extensibility
            #     callback_data = {
            #         'index': xi,
            #         'x': xi,
            #         'y': y,
            #         'open': o, 'high': h, 'low': l, 'close': c
            #     }
            #     self.cursor_callback(callback_data)
        else:
            # Mouse is over the chart but not on a valid candle index
            self.last_x = -1
            self.vLine.setPos(x) # Let vLine follow smoothly
            self.hLine.setPos(y)
            _, x_max = self.vb.viewRange()[0]
            self.price_label.setText(f"{y:.2f}")
            self.price_label.setPos(x_max, y)
            self.time_label.setText("") # Clear time if not on a candle
            if callable(self.cursor_callback):
                self.cursor_callback(None)
        # # move the infinite lines
        # self.vLine.setPos(x)
        # self.hLine.setPos(y)

        # # update price label (place at right edge of current view)
        # try:
        #     x_min, x_max = self.vb.viewRange()[0]
        #     y_min, y_max = self.vb.viewRange()[1]
        # except Exception:
        #     x_min, x_max = 0, 0
        #     y_min, y_max = 0, 0

        # # Price label on the right edge aligned with current y
        # self.price_label.setText(f"{y:.2f}")
        # self.price_label.setPos(x_max, y)

        # # Time label at bottom aligned with x (map x index to timestamp)
        # xi = int(round(x))
        # time_text = ''
        # if 0 <= xi < len(self.xAxis.time):
        #     ts = self.xAxis.time[xi]
        #     time_text = ts.strftime("%m-%d %H:%M")
        # self.time_label.setText(time_text)
        # # place time label near bottom of price plot (y_min is bottom)
        # # shift slightly up so it remains visible
        # self.time_label.setPos(x, y_min + (y_max - y_min) * 0.01)

        # # call external callback to allow synchronization with other charts (e.g. Volume)
        # if callable(self.cursor_callback):
        #     try:
        #         self.cursor_callback(x, y)
        #     except Exception:
        #         pass
class VolumeDeltaItem(pg.GraphicsObject, ChartScheme):
    def __init__(self, data:pd.DataFrame, width=.6, *args):
        super().__init__(*args)
        self.x = data.index.to_numpy()
        self.vol = data['vol'].to_numpy()
        self.delta = data['delta'].to_numpy()
        self.width = width
        self._generate_path()
        self.prepareGeometryChange()
        self._bounding_rect = QRectF(0,0,self.x.max()+1,self.vol.max())
        self.update()
        self.informViewBoundsChanged()

    def  _generate_path(self):
        self.up_path = QPainterPath()
        self.dn_path = QPainterPath()
        self.nt_path = QPainterPath()

        up_mask = self.delta > 0
        dn_mask = self.delta < 0
        # Anything not up or down is neutral
        delta_abs = np.abs(self.delta)
        gray_h = self.vol - delta_abs
        for xi,h in zip(self.x[up_mask],delta_abs[up_mask]):
            self.up_path.addRect(xi-self.width/2,0,self.width,h)
        for xi,h in zip(self.x[dn_mask],delta_abs[dn_mask]):
            self.dn_path.addRect(xi-self.width/2,0,self.width,h)    
        for xi,y0,h in zip(self.x, delta_abs,gray_h):
            self.nt_path.addRect(xi-self.width/2,y0,self.width,h)

    def paint(self, p, *args):
        p.setPen(self.pen_green)
        p.setBrush(self.brush_green)
        p.drawPath(self.up_path)

        p.setPen(self.pen_red)
        p.setBrush(self.brush_red)
        p.drawPath(self.dn_path)

        p.setPen(self.pen_wick)
        p.setBrush(pg.mkBrush('lightgray'))
        p.drawPath(self.nt_path)

    def boundingRect(self):
        return self._bounding_rect
    
    def dataBounds(self, ax, frac=1, orthoRange=None):
        if orthoRange:
            x0, x1 = orthoRange
            indices = np.where((self.x >= x0) & (self.x <= x1))[0]
            if indices.size:
                return (0, self.vol[indices].max())
        return (None, None)
    
    def update_data(self, data, idx):
        delta = data.get('vd')[2]
        if self.x[-1]!=idx:
            self.x = np.append(self.x, idx)
            self.vol = np.append(self.vol, data['v'])
            self.delta = np.append(self.delta, delta)
        else:
            self.vol[-1] = data['v']
            self.delta[-1] = delta
        self._generate_path()
        self.prepareGeometryChange()
        self._bounding_rect = QRectF(0,0,self.x.max()+1,self.vol.max())
        self.update()
        self.informViewBoundsChanged()

class VolumeChart(pg.PlotItem):
    def __init__(self, data, parent=None, name=None, labels=None, title=None, viewBox=None, axisItems=None, enableMenu=True, **kargs):
        super().__init__(parent, name, labels, title, viewBox, axisItems, enableMenu, **kargs)
        self.showGrid(True,True,.3)
        view = self.getViewBox()
        view.setLimits(yMin=0)
        view.setMouseEnabled(x=True, y=False)
        view.setAutoVisible(x=False, y=True)
        view.enableAutoRange(x=False, y=True)
        view.setAspectLocked(False)
        self.setLabel('left','Vol')
        self.vol = VolumeDeltaItem(data)
        self.getAxis('bottom').style['showValues']=False
        self.addItem(self.vol)
        
    def update_data(self, data, idx):
        self.vol.update_data(data, idx)

class ChartLayout(pg.GraphicsLayout):
    def __init__(self, ohlcv:pd.DataFrame,fp:pd.DataFrame, parent=None, border=None):
        super().__init__(parent, border)
        self.setSpacing(0)
        self.setContentsMargins(0,0,0,0)

        # self.ohlcv_df = ohlcv
        self.xAxis = MinuteAxisItem(ohlcv['time'])
        self.view = FixedScaleViewBox()
        self.last_bar_time = int(ohlcv['time'].iloc[-1].timestamp())
        self.last_bar_idx = len(ohlcv.index)-1
    
        self.price_plot = PriceChart(ohlcv, fp, axisItems={'bottom':self.xAxis})
        self.vol_plot = VolumeChart(ohlcv[['vol','delta']])
        self.addItem(self.price_plot, 0, 0)
        self.addItem(self.vol_plot, 1,0)
        self.vol_plot.setXLink(self.price_plot)

        self.layout.setRowStretchFactor(0,3)
        self._align()

    def _align(self):
        price_axis = self.price_plot.getAxis('left')
        vol_axis = self.vol_plot.getAxis('left')
        max_width = max(price_axis.width(), vol_axis.width())
        price_axis.setWidth(max_width)
        vol_axis.setWidth(max_width)

    def update_data(self, data:list):
        for bar in data:
            ts = bar.get('ts')
            if ts is None or ts<self.last_bar_time:
                return
            is_new_bar = ts != self.last_bar_time

            if is_new_bar:
                self.last_bar_time = ts
                self.last_bar_idx += 1
                self.xAxis.append_ts(ts)

            for plot in (self.vol_plot, self.price_plot):
                plot.update_data(bar, self.last_bar_idx)

    def setup_crosshair(self):
        self.price_plot.setup_crosshair(15)
            
class PlotWidget(pg.GraphicsLayoutWidget):
    def __init__(self,ohlcv=None, fp=None, parent=None, show=False, size=None, title=None, **kargs):
        super().__init__(parent, show, size, title, **kargs)

        self.ci.layout.setSpacing(0)
        self.ci.setContentsMargins(0,0,0,15)

        self.MTX = ChartLayout(ohlcv,fp)
        self.addItem(self.MTX,row=0,col=0)
        self.MTX.setup_crosshair()
        
    @Slot(str,str,bytes)
    def _msg_handler(self, pattern, channel, data):
        """Handles incoming messages from Redis."""
        if not pattern:
            # not quote
            channel, market, symbol = channel.split(':')
            # if channel == 'snap':
            #     ts= pd.Timestamp(data['time'])
            # print(data)
            data = msgspec.msgpack.decode(data, strict=False)
            # print(symbol, data)
            self.MTX.update_data(data)
