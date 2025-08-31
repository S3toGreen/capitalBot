from PySide6.QtWidgets import *
from PySide6.QtCore import *
from PySide6.QtGui import *
from core.SignalManager import SignalManager
import pyqtgraph as pg
from windows_toasts import WindowsToaster, Toast, ToastScenario
from copy import deepcopy

toaster = WindowsToaster('Trading App')
newToast = Toast(scenario=ToastScenario.Important,suppress_popup=False)
order_thresh = 60 # large order threshold

class VP(pg.PlotWidget):
    def __init__(self, parent=None, background='default', plotItem=None, **kargs):
        super().__init__(parent, background, plotItem, **kargs)
        self.showGrid(x=True,y=True,alpha=.3)
        self.setMouseEnabled(x=False,y=True)
        self.enableAutoRange(axis='y')
        self.setAutoVisible(x=True)
        self.setLimits(xMin=0)
        self.vol_data = [] 
        self.timer=QTimer()
        self.timer.timeout.connect(self.plot_bars)
        self.timer.start(150)
        self.time_idx = -1

    def update(self,price,side,amount):
        if price in self.vol_data[-1]:
            self.vol_data[-1][price][side] += amount
        else:
            self.vol_data[-1][price]=[0,amount] if side else [amount,0]

    def plot_bars(self):
        self.clear()
        if not self.vol_data:
            return
        t = self.vol_data[self.time_idx]
        # for i in range(self.time_idx+1):
        #     for j in self.vol_data[i]:
        #         if j in t:
        #             t[j] = [t[j][k]+self.vol_data[i][j][k] for k in range(2)]
        #         else:
        #             t[j]=self.vol_data[i][j]
        # print(t)
        prices = list(t.keys())

        deltas = [t[p][0]-t[p][1] for p in prices]
        vols = [t[p][0]+t[p][1] for p in prices]
        v_bars = pg.BarGraphItem(x0=0, y=prices, height=.9, width=vols,brush=(0.6))
        delta_bars = pg.BarGraphItem(x0=0, y=prices, height=.9, width=[abs(i) for i in deltas], brushes=['g' if i>0 else 'r' for i in deltas])
        self.addItem(v_bars)
        self.addItem(delta_bars)
        # b_vol = [t[p][0] for p in prices]
        # s_vol = [t[p][1] for p in prices]
        # s_bars = pg.BarGraphItem(x0=0, y=prices, height=0.9,width=s_vol,brush='r')
        # b_bars = pg.BarGraphItem(x0=s_vol, y=prices, height=0.9,width=b_vol,brush='g')
        # self.addItem(s_bars)
        # self.addItem(b_bars)

class VP_order(pg.PlotWidget):
    def __init__(self, parent=None, background='default', plotItem=None, **kargs):
        super().__init__(parent, background, plotItem, **kargs)
        self.showGrid(x=True,y=True,alpha=.3)
        self.setMouseEnabled(x=False,y=True)
        self.setLimits(xMin=0)
        self.vol_data = {}

        # self.plot_bars()
        self.timer=QTimer()
        self.timer.timeout.connect(self.plot_bars)
        self.timer.start(600)

    def update(self,price,side,amount):
        return

    def plot_bars(self):
        self.clear()

class LSdiff(pg.PlotWidget): #累計口/筆差
    def __init__(self, parent=None, background='default', plotItem=None, **kargs):
        super().__init__(parent, background, plotItem, **kargs)
        self.showGrid(x=False,y=True,alpha=.9)
        self.setMouseEnabled(x=False,y=True)
        self.setLimits(xMin=0)
        #累計筆差, 累計口差, 大單, 小單
        self.vol_data = [] 

        xval = [[(1,'筆差'),(2,'CVD'),(3,'散單'),(4,'大單')]]
        self.getAxis('bottom').setTicks(xval)
        # self.plot_bars()
        self.timer=QTimer()
        self.timer.timeout.connect(self.plot_bars)
        self.timer.start(300)
        self.time_idx = -1

    def update(self,side,amount):
        global order_thresh
        if side: #short
            self.vol_data[-1][0] -= 1
            self.vol_data[-1][1] -= amount
            if amount>=order_thresh:
                self.vol_data[-1][3] -= amount
            else:
                self.vol_data[-1][2] -= amount
        else: #long
            self.vol_data[-1][0] += 1
            self.vol_data[-1][1] += amount
            if amount>=order_thresh:
                self.vol_data[-1][3] += amount
            else:
                self.vol_data[-1][2] += amount

    def plot_bars(self):
        self.clear()
        if not self.vol_data:
            return
        t=self.vol_data[self.time_idx]
        # for i in range(self.time_idx+1):
        #     t = [t[j]+self.vol_data[i][j] for j in range(len(t))]

        bars = pg.BarGraphItem(x=range(1,5),height=t,width=0.6, brushes=['g' if i>0 else 'r' for i in t])
        self.addItem(bars)

class VolumeVisualize(QWidget):
    def __init__(self,parent=None):
        super().__init__(parent)
        self.setWindowTitle("Volume Profile")
        self.resize(450, 900)
        self.setWindowFlags(Qt.WindowType.Dialog)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, False)
        self.signals = SignalManager.get_instance()
        self.signals.vp_update_sig.connect(self.update, Qt.ConnectionType.QueuedConnection)
        
        self.tabwidget = QTabWidget()
        # add time value to slider
        self.slider = QSlider(Qt.Orientation.Horizontal)
        self.slider.setTickPosition(QSlider.TickPosition.TicksBelow)
        self.slider.setRange(-1,-1)
        self.slider.valueChanged.connect(self.time_update)
        layout = QVBoxLayout(self)
        layout.addWidget(self.tabwidget)
        layout.addWidget(self.slider)
        
        self.tab1 = QWidget()
        self.tabwidget.addTab(self.tab1, "分佈圖")
        self.tab2 = QWidget()
        self.tabwidget.addTab(self.tab2, "Filler")
        self.tab3 = QWidget()
        self.tabwidget.addTab(self.tab3, "多空差額")

        self.vp = VP()
        layout1 = QVBoxLayout()
        layout1.addWidget(self.vp)
        self.tab1.setLayout(layout1)

        self.vp_order = VP_order()
        layout2 =QVBoxLayout()
        layout2.addWidget(self.vp_order)
        self.tab2.setLayout(layout2)

        self.ls = LSdiff()
        layout3=QVBoxLayout()
        layout3.addWidget(self.ls)
        self.tab3.setLayout(layout3)

    # ToDo batch historical tick data
    def batch_update(self):
        pass

    def update(self,price,side,amount,sep,lived):
        global order_thresh,newToast,toaster
        if sep:
            if self.vp.vol_data:#not self.slider.minimum():
                self.vp.vol_data.append(deepcopy(self.vp.vol_data[-1]))
                self.ls.vol_data.append(deepcopy(self.ls.vol_data[-1])) 
            else:
                self.vp.vol_data.append({})
                self.ls.vol_data.append([0,0,0,0])

            self.slider.setMaximum(len(self.vp.vol_data)-1)
            self.slider.setMinimum(0)
            if self.slider.value()+1==self.slider.maximum():
                self.slider.setValue(self.slider.maximum())

        self.vp.update(price,side,amount)
        self.ls.update(side,amount)
        if amount>=order_thresh and lived:
            newToast.text_fields=[f"BigTrade volume at {price} {amount} ({'SELL' if side else 'BUY'})"]
            toaster.show_toast(newToast)
            newToast = newToast.clone()

    def time_update(self):
        t = self.slider.value()
        self.vp.time_idx=t
        self.ls.time_idx=t
