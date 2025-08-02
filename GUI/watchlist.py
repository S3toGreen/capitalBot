from PySide6.QtWidgets import *
from PySide6.QtCore import *
from PySide6.QtGui import *
from collections import defaultdict
from SignalManager import SignalManager
TABLE_VIEW_STYLE = """
QTableView {
    alternate-background-color: #3c3c3c;
}
"""
class QuoteModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._headers = ['Symbol','Price','Change','Vol','High','Low','OI','連次','連量'] #VolRatio(量比 similar 預估量) 連次連量
        self._data = [] 
        self._symbol_index = {}  # Map symbol to row index
        self._last_price = defaultdict(float)
        self._highlighted_rows = {}

    def columnCount(self, parent=QModelIndex()):
        return len(self._headers)
    def rowCount(self, parent=QModelIndex()):
        return len(self._data)
    
    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        
        row = index.row()
        col = index.column()

        if role == Qt.ItemDataRole.DisplayRole:
            value = self._data[row][col]
            if isinstance(value, float):
                return f"{value:,.2f}"
            elif isinstance(value, int):
                return f"{value:,}"
            return value
        if role == Qt.ItemDataRole.UserRole:
            return self._data[row][col]

        if role == Qt.ItemDataRole.BackgroundRole and row in self._highlighted_rows:
            from PySide6.QtGui import QColor
            if col==1:
                return QColor(self._highlighted_rows[row])
            
        if role == Qt.ItemDataRole.TextAlignmentRole:
            if not col:
                return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        
        if role == Qt.ItemDataRole.FontRole:
            from PySide6.QtGui import QFont
            font = QFont()
            font.setBold(True)
            font.setPointSize(12)
            font.setFamily("IBM Plex Sans")
            font.setStyleHint(QFont.StyleHint.SansSerif)
            return font
        
        if role == Qt.ItemDataRole.ForegroundRole and col==2:
            from PySide6.QtGui import QColor
            value = self._data[row][col]
            return QColor("#ff4444" if value<0 else '#00ff00' if value>0 else 'white')

        return None
    
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._headers[section]
        return super().headerData(section, orientation, role)

    def update_data(self, data:dict):
        row_data = [
            data.get('Symbol',None),
            data.get('C') if data.get('C') else data.get('Ref'),
            data.get('C',0)-data.get('Ref',0),
            data.get('Vol',0),
            data.get('H',0),
            data.get('L',0),
            # data.get('Open',0),
            data.get('OI'),
            data.get('CC'),
            data.get('CV')
        ]
        idx = data.get('ID')
        if idx in self._symbol_index:
            row = self._symbol_index[idx]
            old_price = self._data[row][1]
            self._data[row] = row_data

            # Highlight if price changed
            if row_data[1] > old_price:
                self._highlighted_rows[row] = "#375a3b"  # light green
            elif row_data[1] < old_price:
                self._highlighted_rows[row] = "#6a1b1b"  # light red

            self.dataChanged.emit(self.index(row, 0), self.index(row, self.columnCount() - 1))
            QTimer.singleShot(600, lambda r=row: self.clear_highlight(r))
        else:
            row = len(self._data)
            self.beginInsertRows(QModelIndex(), row, row)
            self._data.append(row_data)
            self._symbol_index[idx] = row
            self.endInsertRows()

    def clear_highlight(self, row):
        if row in self._highlighted_rows:
            del self._highlighted_rows[row]
            self.dataChanged.emit(self.index(row, 0), self.index(row, self.columnCount() - 1))

class OptionModel(QAbstractTableModel):
    def __init__(self,parent=None):
        super().__init__(parent)
        self._headers = [
            "OpenInt", "Vol", "Call Δ", "Call IV", "Last",
            "Strike",
            "Last", "Put IV", "Put Δ", "Vol", "OpenInt"
        ]
        self._data=[]

    def rowCount(self, /, parent = ...):
        return len(self._data)
    def columnCount(self, /, parent = ...):
        return len(self._headers)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._headers[section]
        return super().headerData(section, orientation, role)

class WatchList(QWidget):
    def __init__(self,parent=None):
        super().__init__(parent)
        # self.setWindowTitle("Watch List")
        # self.resize(750, 900)
        self.setStyleSheet(TABLE_VIEW_STYLE)
        self.setWindowFlags(Qt.WindowType.Dialog)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, False)
        # self.signals = SignalManager.get_instance()

        layout = QVBoxLayout(self)
        tabwidget = QTabWidget()
        layout.addWidget(tabwidget)

        self.model_dm = QuoteModel()
        self.proxy_dm = QSortFilterProxyModel()
        self.proxy_dm.setSourceModel(self.model_dm)
        self.proxy_dm.setSortRole(Qt.ItemDataRole.UserRole)

        self.tab1 = QTableView()
        self.tab1.setModel(self.proxy_dm)
        self.tab1.setAlternatingRowColors(True)
        self.tab1.setSortingEnabled(True)
        tabwidget.addTab(self.tab1, "DM")

        self.model_os = QuoteModel()
        self.proxy_os = QSortFilterProxyModel()
        self.proxy_os.setSourceModel(self.model_os)
        self.proxy_os.setSortRole(Qt.ItemDataRole.UserRole)

        self.tab2 = QTableView()
        self.tab2.setModel(self.proxy_os)
        self.tab2.setAlternatingRowColors(True)
        self.tab2.setSortingEnabled(True)
        tabwidget.addTab(self.tab2, "OS")

        #options chain
        self.tab3=QWidget()
        layout=QVBoxLayout(self.tab3)
        self.option_combo = QComboBox()
        self.table = QTableView()
        self.option_model = OptionModel()
        self.table.setModel(self.option_model)
        self.table.setSortingEnabled(True)

        layout.addWidget(self.option_combo)
        layout.addWidget(self.table)
        # self.tab3.resizeColumnsToContents()
        tabwidget.addTab(self.tab3, "Options(ToBeDone)")

        self.tab1.verticalHeader().setVisible(False)
        self.tab1.horizontalHeader().setMinimumSectionSize(75)
        self.tab1.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.tab2.verticalHeader().setVisible(False)
        self.tab2.horizontalHeader().setMinimumSectionSize(75)
        self.tab2.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
    def show(self):
        super().show()
        self.table.horizontalScrollBar().setValue(1)

    # @Slot(str,dict,str)
    def quote_update(self,market:str,data:dict):
        match market:
            case 'DM':
                self.model_dm.update_data(data)
            case 'OS':
                self.model_os.update_data(data)
            case _:
                return
    def update_option(self, oplist:list):

        #TX1,TX2,TXO,TX3,TX4
        # 5 digits for price
        # A-L call option for 12 month, M-X is put option for 12 month
        # last character is a last digit of the year 202'5'
        self.option_combo.addItems(oplist)
    