from PySide6.QtCore import QObject,Signal

class SignalManager(QObject):
    log_sig = Signal(str)
    data_sig = Signal(str, int)
    order_sig = Signal(str,int,int)
    close_all_sig = Signal()
    vp_update_sig = Signal(int,int,int,bool,bool)
    option_update= Signal(list)
    quote_update=Signal(str, dict, str)
    future_info_update = Signal(str, dict)
    alert = Signal(str,str)
    OS_store_sig = Signal(int,int)
    _instance = None
    
    @staticmethod
    def get_instance():
        if SignalManager._instance is None:
            SignalManager._instance = SignalManager()
        return SignalManager._instance
    
    def __init__(self):
        if SignalManager._instance is not None:
            raise Exception("This class is a singleton!")
        super().__init__()

