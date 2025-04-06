from Bot import TickReceiver, SignalManager
from PySide6.QtWidgets import *
from PySide6.QtCore import *
from PySide6.QtGui import *
import pyqtgraph as pg
import numpy as np
import Config
import sys
from windows_toasts import WindowsToaster, Toast, ToastScenario
from sortedcontainers import SortedDict
from Profile import VolumeVisualize

pg.setConfigOptions(useOpenGL=True)

#累計口/筆差, 大/散單(Accumulate), Volume Profile 
ANSI_COLOR={-1:QColorConstants.Red,1:QColorConstants.Green,0:QColorConstants.White}

class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

        self.threads = QThread()
        self.worker = TickReceiver()

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
        self.threads.quit()
        self.threads.wait()
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