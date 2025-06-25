from PySide6.QtCore import QThread, Signal
from SignalManager import SignalManager

class StrategyEngine(QThread):
    def __init__(self, /, parent = ...):
        super().__init__(parent)
        self.signals = SignalManager.get_instance()
        
