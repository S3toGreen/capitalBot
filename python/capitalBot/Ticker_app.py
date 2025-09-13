import sys
from brokers.skcom.Broker import QuoteBroker
from core.SignalManager import SignalManager
from PySide6.QtWidgets import * #QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QLineEdit, QFormLayout, QCheckBox, QPlainTextEdit
from PySide6.QtCore import QThread, Slot
from PySide6.QtGui import QColorConstants, QTextCharFormat, QFont, QTextCursor, QIcon, QAction
# from windows_toasts import WindowsToaster, Toast, ToastScenario
from dotenv import load_dotenv
import os
load_dotenv()

ANSI_COLOR={-1:QColorConstants.Red,1:QColorConstants.Green,0:QColorConstants.White}

class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("TickerApp")
        self.init_ui()
        
        self.broker_thread = QThread()
        self.broker = QuoteBroker()
        
        self.signals = SignalManager.get_instance()
        self.signals.log_sig.connect(self.log_handler)
        self.signals.data_sig.connect(self.data_handler)
        self.signals.alert.connect(self.toast_alert)

        self.broker.moveToThread(self.broker_thread)
        self.broker_thread.started.connect(self.broker.start)
        self.broker_thread.finished.connect(self.broker.stop)
        # self.volprof = VolumeVisualize(self)
        # self.watchlist = WatchList(self)
        # self.toaster=WindowsToaster('Tickers')
        # self.newToast = Toast(scenario=ToastScenario.Reminder,suppress_popup=False)

    def init_ui(self):
        self.resize(600,600)
        layout = QHBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(15,15,15,0)

        # Login Form
        layout2 = QVBoxLayout()
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

        self.msg2 = QPlainTextEdit()
        self.msg2.setReadOnly(True)
        self.msg2.setMinimumWidth(240)

        layout2.addLayout(self.form,1)
        layout2.addWidget(self.msg2,2)

        id.setText(os.getenv('BROKER_ID'))
        id.returnPressed.connect(self.run_init)
        passwd.setText(os.getenv('BROKER_PASS'))
        passwd.returnPressed.connect(self.run_init)
        
        # Logging
        self.msg1 = QPlainTextEdit()
        self.msg1.setReadOnly(True)
        self.msg1.setMaximumBlockCount(150)

        layout.addLayout(layout2,1)
        layout.addWidget(self.msg1,3)

        # layout3 = QVBoxLayout()
        # layout3.setContentsMargins(15,15,15,0)
        # layout3.setSpacing(15)
        # layout3.addLayout(layout)

        # layout = QHBoxLayout()
        # # layout.setSpacing(15)
        # self.msg2 = QPlainTextEdit()
        # self.msg2.setReadOnly(True)
        # self.msg2.setMaximumBlockCount(1500)

        # layout.addWidget(self.msg3,1)
        # layout.addWidget(self.msg2,3)
        # layout3.addLayout(layout,1)

        status = QStatusBar()

        container = QWidget()
        container.setLayout(layout)
        
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
            
    @Slot(str,str)
    def toast_alert(self, title, msg):
        self.newToast.text_fields=[title,msg]
        self.toaster.show_toast(self.newToast)
        self.newToast = self.newToast.clone()

    def debug_trig(self):
        self.broker.debug_state=self.debug.isChecked()
        self.broker.update_debug()

    def run_init(self):
        res = self.broker.login(self.form.itemAt(1).widget().text(), self.form.itemAt(3).widget().text())
        if res:
            return
        self.debug.setDisabled(True)
        self.broker_thread.start()

    def stop(self):
        self.broker_thread.quit()
        self.broker_thread.wait()

    def restart(self):
        self.signals.restart_dm.emit()
        self.signals.restart_os.emit()

if __name__=='__main__':
    import ctypes
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(u'com.S3toGreen.TickerApp')

    app = QApplication([])
    app.setQuitOnLastWindowClosed(False)

    icon = QIcon('./asset/ticker.ico')
    app.setWindowIcon(icon)

    window = MainWindow()
    window.show()
    app.aboutToQuit.connect(window.stop)

    tray = QSystemTrayIcon()
    tray.setIcon(icon)
    tray.setVisible(True)
    tray.setToolTip('TickerApp')

    menu = QMenu()
    quit = QAction('Quit')
    quit.triggered.connect(app.quit)
    menu.addAction(quit)
    restart = QAction('Restart')
    restart.triggered.connect(window.restart)
    menu.addAction(restart)

    tray.setContextMenu(menu)

    def on_tray_activated(reason):
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            window.show()
    tray.activated.connect(on_tray_activated)

    sys.exit(app.exec())