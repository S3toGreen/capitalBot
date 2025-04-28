from PySide6.QtWidgets import *
from PySide6.QtCore import *
from PySide6.QtGui import *
from Bot import SignalManager

class Table(QTableView):
    def __init__(self):
        super().__init__()
        
        model = QStandardItemModel(0,6)
        model.setHorizontalHeaderLabels(['Symbol','Price','Change','Volume','量比','連次連量'])
        self.setModel(model)
        self.setSortingEnabled(True)

class WatchList(QWidget):
    def __init__(self,parent=None):
        super().__init__(parent)
        self.setWindowTitle("Watch List")
        self.resize(750, 900)
        self.setWindowFlags(Qt.WindowType.Dialog)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, False)
        self.signals = SignalManager.get_instance()

        layout = QVBoxLayout(self)
        tabwidget = QTabWidget()
        layout.addWidget(tabwidget)

        self.tab1 = Table()
        tabwidget.addTab(self.tab1, "Domestic")
        self.tab2 = Table()
        tabwidget.addTab(self.tab2, "OverSea")

        # table = QTableView()
        # layout1 = QVBoxLayout()
        # layout1.addWidget(table)
        # self.tab1.setLayout(layout1)


