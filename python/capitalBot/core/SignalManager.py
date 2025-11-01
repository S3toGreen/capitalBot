from PySide6.QtCore import QObject,Signal

class SignalManager(QObject):
    log_sig = Signal(str)
    data_sig = Signal(str, int)
    option_update= Signal(list)
    OS_reset = Signal()
    DM_reset = Signal()
    restart_dm = Signal()
    restart_os = Signal()
    login_success = Signal(object)
    shutdown = Signal()
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

