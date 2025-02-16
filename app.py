from Bot import TradingBot, SignalManager
from PyQt6.QtWidgets import *
from PyQt6.QtCore import *
from PyQt6.QtGui import *
import pyqtgraph as pg
import numpy as np
import Config


import sys

ANSI_COLOR={-1:QColorConstants.Red,1:QColorConstants.Green,0:QColorConstants.White}


class VP(pg.PlotWidget):
    def __init__(self, parent=None, background='default', plotItem=None, **kargs):
        super().__init__(parent, background, plotItem, **kargs)
        self.showGrid(x=True,y=True,alpha=.15)
        self.setMouseEnabled(x=False,y=True)
        self.setLimits(xMin=0)
        # self.plot_bars()

        self.vol_data = {23200:[150,200],23211:[6,9],23220:[100,90]}
        self.timer=QTimer()
        self.timer.timeout.connect(self.update)
        self.timer.start(150)

    def plot_bars(self):
        prices = sorted(self.vol_data.keys())
        b_vol = [self.vol_data[p][0] for p in prices]
        s_vol = [self.vol_data[p][1] for p in prices]

        s_bars = pg.BarGraphItem(x0=0, y=prices, height=0.4,width=s_vol,brush='r')
        b_bars = pg.BarGraphItem(x0=s_vol, y=prices, height=0.4,width=b_vol,brush='g')
        self.addItem(s_bars)
        self.addItem(b_bars)

    def update(self):
        prices = list(self.vol_data)
        # i = np.random.randint(0,len(prices))
        # t = np.random.randint(-1,2)
        # if prices[i]+t not in prices:
        #     self.vol_data[prices[i]+t]=[0,0]
        # self.vol_data[prices[i]+t][np.random.randint(0,2)]+=np.random.randint(1,4)
        self.clear()
        self.plot_bars()


class VolumeVisualize(QTabWidget):
    def __init__(self,parent=None):
        super().__init__(parent)
        self.setWindowTitle("Volume Profile")
        self.setFixedSize(400, 900)
        self.setWindowFlags(Qt.WindowType.Dialog)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, False)
        
        self.tab1 = QWidget()
        self.addTab(self.tab1, "Simple")
        self.tab2 = QWidget()
        self.addTab(self.tab2, "Order wise")
        self.tab3 = QWidget()
        self.addTab(self.tab3, "Minute wise")
        
        layout1 = QVBoxLayout()
        layout1.addWidget(VP())
        self.tab1.setLayout(layout1)


class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

        self.threads = QThread()
        self.worker = TradingBot()

        self.signals = SignalManager.get_instance()
        self.signals.log_sig.connect(self.log_handler)
        self.signals.data_sig.connect(self.data_handler)

        self.worker.moveToThread(self.threads)
        self.threads.started.connect(self.worker.run)
        self.volprof = VolumeVisualize(self)


    def init_ui(self):
        self.setWindowTitle("My App")
        self.resize(800,600)
        layout = QHBoxLayout()
        layout.setSpacing(15)

        # Login Form
        id = QLineEdit(parent=self)
        id.setMinimumWidth(150)
        id.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)
        passwd = QLineEdit(parent=self)
        passwd.setMinimumWidth(150)
        passwd.setEchoMode(QLineEdit.EchoMode.Password)
        self.form = QFormLayout()
        self.form.setSpacing(15)
        self.form.addRow("Username:", id)
        self.form.addRow("Password:", passwd)
        self.debug = QCheckBox("debug")
        self.form.addRow(self.debug)
        self.debug.checkStateChanged.connect(self.debug_trig)
        layout.addLayout(self.form,1)
        id.setText(Config.id)
        id.returnPressed.connect(self.run_init)
        passwd.setText(Config.passwd)
        passwd.returnPressed.connect(self.run_init)
        # Logging
        self.msg1 = QPlainTextEdit()
        self.msg1.setReadOnly(True)
        self.msg1.setMaximumBlockCount(150)
        layout.addWidget(self.msg1,3)
        
        layout3 = QVBoxLayout()
        layout3.setContentsMargins(30,15,30,15)
        layout3.setSpacing(15)
        layout3.addLayout(layout)

        layout = QHBoxLayout()
        # layout.setSpacing(15)
        self.msg2 = QPlainTextEdit()
        self.msg2.setReadOnly(True)
        self.msg2.setMaximumBlockCount(1500)
        self.msg3 = QPlainTextEdit()
        self.msg3.setReadOnly(True)
        self.msg3.setMinimumWidth(225)
        layout.addWidget(self.msg3,1)
        layout.addWidget(self.msg2,3)
        layout3.addLayout(layout,1)

        status = QStatusBar()

        container = QWidget()
        container.setLayout(layout3)
        
        self.setStatusBar(status)
        # Set the central widget of the Window.
        self.setCentralWidget(container)

    def data_handler(self, data, side=0):
        format = QTextCharFormat()
        format.setFontWeight(QFont.Weight.DemiBold)        
        format.setForeground(ANSI_COLOR[side])
        self.msg2.setCurrentCharFormat(format)
        self.msg2.appendPlainText(data)
        if self.msg2.verticalScrollBar().value() >= (self.msg2.verticalScrollBar().maximum()-3):
            self.msg2.moveCursor(QTextCursor.MoveOperation.End)
            self.msg2.ensureCursorVisible()
    
    def log_handler(self,data):
        self.msg1.appendPlainText(data)
        if self.msg2.verticalScrollBar().value() >= (self.msg2.verticalScrollBar().maximum()-3):
            self.msg2.moveCursor(QTextCursor.MoveOperation.End)
            self.msg2.ensureCursorVisible()

    def debug_trig(self):
        self.worker.debug_state=self.debug.isChecked()
        self.worker.update_debug()

    def run_init(self):
        res = self.worker.login(self.form.itemAt(1).widget().text(), self.form.itemAt(3).widget().text())
        if res:
            return
        self.debug.setDisabled(True)
        self.threads.start()

    def closeEvent(self, a0):
        self.worker.saveData()
        # if self.volprof:
        #     self.volprof.close()
        return super().closeEvent(a0)
    
    def show(self):
        self.volprof.show()
        self.volprof.move(self.x()+1200,self.volprof.y())
        return super().show()

if __name__=='__main__':
    app = QApplication([])
    window = MainWindow()
    window.show()
    sys.exit(app.exec())