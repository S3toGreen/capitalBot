import asyncio
import sys

from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import QApplication, QPushButton, QVBoxLayout, QWidget

import qasync
from qasync import QEventLoop, asyncClose, asyncSlot
import traceback, logging
logger = logging.getLogger("TickerApp")

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        layout = QVBoxLayout()
        self.button = QPushButton("Load", self)
        self.button.clicked.connect(self.onButtonClicked)
        layout.addWidget(self.button)
        self.setLayout(layout)

    @asyncSlot()
    async def onButtonClicked(self):
        """
        Use async code in a slot by decorating it with @asyncSlot.
        """
        self.button.setText("Loading...")
        await asyncio.sleep(1)
        self.button.setText("Load")

    @asyncClose
    async def closeEvent(self, e):
        """
        Use async code in a closeEvent by decorating it with @asyncClose.
        """
        pass

@asyncClose
async def yo():
    # print("yo() called. stack:\n%s", "".join(traceback.format_stack(limit=5)))
    await asyncio.sleep(1)
    print("nah")

async def main(app):
    app_close_event = asyncio.Event()
    app.aboutToQuit.connect(app_close_event.set)
    main_window = MainWindow()
    main_window.show()
    app.aboutToQuit.connect(yo)
    # logger.info("main() waiting for shutdown_event...")
    await app_close_event.wait()
    # logger.info("shutdown_event set; running cleanup in asyncio context")
    # await asyncio.sleep(0)
    # try:
    #     await asyncio.wait_for(main_window.yo(), timeout=10.0)
    # except asyncio.TimeoutError:
    #     logger.warning("main_window.yo() timed out")
    # except Exception:
    #     logger.exception("error during cleanup")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    loop.set_debug(True)
    asyncio.set_event_loop(loop)

    # run main inside the QEventLoop context manager (do NOT use asyncio.run)
    with loop:
        loop.run_until_complete(main(app))

    # for python 3.11 or newer
    # asyncio.run(main(app), loop_factory=QEventLoop)
    # for python 3.10 or older
    # qasync.run(main(app))

