import sys
from PySide6.QtWidgets import * #QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QLineEdit, QFormLayout, QCheckBox, QPlainTextEdit
from PySide6.QtCore import Slot
from PySide6.QtGui import QColorConstants, QTextCharFormat, QFont, QTextCursor, QIcon, QAction
import asyncio
# import PySide6.QtAsyncio as QtAsyncio
from qasync import QEventLoop, asyncSlot, asyncClose
from dotenv import load_dotenv
load_dotenv()
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from brokers.skcom.Broker import QuoteBroker
from core.SignalManager import SignalManager
from core.TickerService import TickerService
import time
ANSI_COLOR={-1:QColorConstants.Red,1:QColorConstants.Green,0:QColorConstants.White}

import logging
logger = logging.getLogger("TickerApp")
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")

class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("TickerApp")
        self.init_ui()
        
        self.signals = SignalManager.get_instance()
        self.signals.log_sig.connect(self.log_handler)

        self.broker = QuoteBroker()

    def init_ui(self):
        self.resize(600,600)
        layout = QHBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(15,15,15,0)

        # Login Form
        layout2 = QVBoxLayout()
        acc = QLineEdit(parent=self)
        acc.setMinimumWidth(150)
        acc.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)
        passwd = QLineEdit(parent=self)
        passwd.setMinimumWidth(150)
        passwd.setEchoMode(QLineEdit.EchoMode.Password)

        self.form = QFormLayout()
        self.form.setSpacing(15)
        self.form.addRow("Username:", acc)
        self.form.addRow("Password:", passwd)

        self.debug = QCheckBox("debug")
        self.form.addRow(self.debug)
        self.debug.checkStateChanged.connect(self.debug_trig)

        self.msg2 = QPlainTextEdit()
        self.msg2.setReadOnly(True)
        self.msg2.setMinimumWidth(240)

        layout2.addLayout(self.form,1)
        layout2.addWidget(self.msg2,2)

        acc.setText(os.getenv('BROKER_ID'))
        passwd.setText(os.getenv('BROKER_PASS'))
        acc.returnPressed.connect(self.try_login)
        passwd.returnPressed.connect(self.try_login)
        
        # Logging
        self.msg1 = QPlainTextEdit()
        self.msg1.setReadOnly(True)
        self.msg1.setMaximumBlockCount(1500)

        layout.addLayout(layout2,1)
        layout.addWidget(self.msg1,3)

        status = QStatusBar()
        container = QWidget()
        container.setLayout(layout)
        
        self.setStatusBar(status)
        self.setCentralWidget(container)

    # def data_handler(self, data, side=0):
    #     format = QTextCharFormat()
    #     format.setFontWeight(QFont.Weight.DemiBold)        
    #     format.setForeground(ANSI_COLOR[side])
    #     self.msg2.setCurrentCharFormat(format)
    #     self.msg2.appendPlainText(data)
    #     if self.msg2.verticalScrollBar().value() >= (self.msg2.verticalScrollBar().maximum()-3):
    #         self.msg2.moveCursor(QTextCursor.MoveOperation.End)
    #         self.msg2.ensureCursorVisible()
    @Slot(str)
    def log_handler(self, data):
        self.msg1.appendPlainText(data)
        if self.msg2.verticalScrollBar().value() >= (self.msg2.verticalScrollBar().maximum()-3):
            self.msg2.moveCursor(QTextCursor.MoveOperation.End)
            self.msg2.ensureCursorVisible()
            
    @asyncSlot()
    async def try_login(self):
        self.form.itemAt(1).widget().setDisabled(True)
        self.form.itemAt(3).widget().setDisabled(True)
        self.debug.setDisabled(True)

        res = await asyncio.to_thread(self.broker.login,
            self.form.itemAt(1).widget().text(), 
            self.form.itemAt(3).widget().text()
        )
        if res == 0:
            self.log_handler("Login successful.")
            # Emit a signal to tell the async service it can start
            self.signals.login_success.emit(self.broker.skC) 
        else:
            self.form.itemAt(1).widget().setDisabled(False)
            self.form.itemAt(3).widget().setDisabled(False)
            self.debug.setDisabled(True)
            self.log_handler("Login failed.")
    @Slot()
    def debug_trig(self):
        self.broker.debug_state=self.debug.isChecked()
        self.broker.update_debug()
            
    def restart(self):
        logger.info("Restart Quote threads")
        self.signals.restart_dm.emit()
        self.signals.restart_os.emit()

async def main(app:QApplication):
    window = MainWindow()
    window.show()
    service = TickerService()

    tray = QSystemTrayIcon()
    tray.setIcon(icon)
    tray.setVisible(True)
    tray.setToolTip('TickerApp')

    @asyncClose
    async def quit():
        logger.info("Application quit, Signal the Services to stop.")
        await service.on_shutdown()
        app.quit()

    menu = QMenu()
    q = QAction('Quit')
    q.triggered.connect(quit)
    menu.addAction(q)
    restart = QAction('Restart')
    restart.triggered.connect(window.restart)
    menu.addAction(restart)
    tray.setContextMenu(menu)
    def on_tray_activated(reason):
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            window.show()
    tray.activated.connect(on_tray_activated)

    app_quit_event = asyncio.Event()
    app.aboutToQuit.connect(app_quit_event.set)
    await app_quit_event.wait()

if __name__=='__main__':
    # import ctypes
    # ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(u'com.S3toGreen.TickerApp')
    app = QApplication([])
    app.setQuitOnLastWindowClosed(False)
    icon = QIcon('./asset/ticker.ico')
    app.setWindowIcon(icon)

    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    # # Use loop as context manager so it is not closed unexpectedly
    with loop:
        loop.run_until_complete(main(app))
    
    # asyncio.run(main(app), loop_factory=QEventLoop)

    # sys.exit(app.exec())
