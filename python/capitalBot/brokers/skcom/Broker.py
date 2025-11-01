from PySide6.QtCore import QObject, QThread, Slot
import comtypes.client as cc
# cc.GetModule(r'./x64/SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
import comtypes
from core.SignalManager import SignalManager
from .quote.DMQuoteThread import DomesticQuote
from .quote.OSQuoteThread import OverseaQuote
from core.DBEngine.AsyncWorker import AsyncWorker
from core.DBEngine.Receiver import DataReceiver

# Simulated trading or dry run
# work flow send order to broker class  

class SKReplyLibEvent():
    def __init__(self, skC):
        super().__init__()
        self.singals = SignalManager.get_instance()
        self.skC = skC
        self.status = -1

    def OnReplyMessage(self, bstrUserID, bstrMessages):
        msg = "【Announcement】" + bstrMessages
        self.singals.log_sig.emit(msg)
        return -1
    def OnSolaceReplyConnection(self, bstrUserID, nCode):
        msg = "【ReplyConnection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.singals.log_sig.emit(msg)
    def OnComplete(self, bstrUserID):
        self.status = 1
    def OnStrategyData(self, bstrUserID, bstrData):
        msg = "【StrategyData】"+ bstrData
        # self.singals.log_sig.emit(msg)
        print(msg)
    def OnNewData(self, bstrUserID, bstrData):
        msg = "【NewData】"+bstrData
        # self.singals.log_sig.emit(msg)
        print(msg)

class SKCenterLibEvent():
    def __init__(self):
        self.signals = SignalManager.get_instance()
    def OnShowAgreement(self, bstrData):
        msg = "【OnShowAgreement】" + bstrData
    def OnTimer(self, nTime):
        # potential bug here: if disconnect it never reset
        # fix: fetch ptr again if reconnect
        hour = nTime//10000
        minute = (nTime//100)%100
        if (hour, minute)==(5, 30):
            self.signals.OS_reset.emit()
        elif (hour, minute)==(14, 45): 
            self.signals.DM_reset.emit()
        elif (hour, minute)==(8, 30):
            pass# reset stock ptr
        
class SKOrderLibEvent():
    def __init__(self, skC) -> None:
        super().__init__()
        self.singals = SignalManager.get_instance()
        self.skC = skC
    def OnAccount(self, bstrLogInID, bstrAccountData):
        global acclist
        msg = "【Account】"
        values = bstrAccountData.split(',')
        acclist.setdefault(values[0], values[1]+values[3])
    def OnProxyStatus(self, bstrUserId, nCode):
        msg = "【ProxyStatus】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.singals.log_sig.emit(msg)
    def OnProxyOrder(self, nStampID, nCode, bstrMessage):
        msg ='【ProxyOrder】'+ str(nStampID) + str(nCode) + bstrMessage
        self.singals.log_sig.emit(msg)
    # 新版期貨智慧單(包含停損單、移動停損、二擇一、觸價單)被動回報查詢。透過呼叫GetStopLossReport後，資訊由該事件回傳。
    def OnStopLossReport(self, bstrData):
        if bstrData[0]=='#':
            return
        msg = "【StrategyReport】" + bstrData
        print(msg)
    # 非同步委託結果。
    def OnAsyncOrder(self, nThreadID, nCode, bstrMessage):
        msg = "【OnAsyncOrder】" + str(nThreadID)+ ", "+ str(nCode) +", "+ bstrMessage
        print(msg)
    
    def OnOpenInterest(self, bstrData):
        global position
        if bstrData[0]=='#':
            return
        data = bstrData.split(',')[2:7]
        if not data:
            return
        position.append(data)
        print(position)
    def OnFutureRights(self, bstrData):
        global accinfo
        if bstrData[0]=='#':
            return
        info = bstrData.split(',')
        msg = f"【FutureRights】Value: ${info[0]} PnL: ${info[1]} Available: ${info[31]}"
        print(msg)
        accinfo = [info[0], info[31]]

class Broker(QObject): 
    def __init__(self, *args,**kwargs):
        super().__init__()
        self.signals = SignalManager.get_instance()

        self.skC = cc.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib)
        SKCenterEvent = SKCenterLibEvent()
        self.SKCenterLibEventHandler = cc.GetEvents(self.skC, SKCenterEvent)

        self.skR = cc.CreateObject(sk.SKReplyLib,interface=sk.ISKReplyLib)
        SKReplyEvent = SKReplyLibEvent(self.skC)
        self.SKReplyLibEventHandler = cc.GetEvents(self.skR, SKReplyEvent)

        nCode = self.skC.SKCenterLib_SetLogPath("CapitalLog")
        if nCode:
            msg = "【SetLogPath】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
        
        self.debug_state = False
        self.update_debug()

    def login(self, acc, passwd, setQuote='Y'):
        # nCode = self.skC.SKCenterLib_Login(acc, passwd)
        nCode = self.skC.SKCenterLib_LoginSetQuote(acc, passwd, setQuote)

        msg = "【Login】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        if nCode==0:
            self.ID = acc
        self.signals.log_sig.emit(msg)

        return nCode

    def update_debug(self):
        nCode = self.skC.SKCenterLib_Debug(self.debug_state)
        if nCode:
            msg = "【SetDebug】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)


class QuoteBroker(Broker):
    """
    TODO: dynamic add symbol for intraday only (no store)
    request tick,quote,klines than plot
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # self.domestic_thread = QThread()
        # self.oversea_thread = QThread()
        # self.domestic = DomesticQuote(self.skC)
        # self.oversea = OverseaQuote(self.skC)
        # self.domestic.moveToThread(self.domestic_thread)
        # self.oversea.moveToThread(self.oversea_thread)
        # self.domestic_thread.started.connect(self.domestic.run)
        # self.oversea_thread.started.connect(self.oversea.run)
        # self.domestic_thread.finished.connect(self.domestic.stop)
        # self.oversea_thread.finished.connect(self.oversea.stop)

    def start(self):
        self.domestic_thread.start()
        self.oversea_thread.start()

    def stop(self):
        self.oversea_thread.quit()
        self.domestic_thread.quit()
        self.domestic_thread.wait()
        self.oversea_thread.wait()
        return
    @Slot(str,str,bytes)
    def request_ticker(self, pattern, channel, data):
        #fetch candlestick, subscribe quote, tick
        print(data)
    
class OrderBroker(Broker):
    # run a api service to handle order
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
        SKOrderEvent = SKOrderLibEvent(self.skC)
        self.SKOrderLibEventHandler = cc.GetEvents(self.skO, SKOrderEvent)
    def start(self):
        self.skR.SKReplyLib_ConnectByID(self.ID)
        self.order_init()
        self.async_worker = AsyncWorker()
        self.orderSub = DataReceiver.create(self.async_worker, ['order:*'])
        self.orderSub.message_received.connect(self._handle_order)

    def order_init(self):
        nCode = self.skO.SKOrderLib_Initialize()
        msg = "【OrderLib_Init】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        
        nCode = self.skO.ReadCertByID(self.ID)
        msg = "【ReadCertByID】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        nCode = self.skO.SKOrderLib_InitialProxyByID(self.ID)
        msg = "【Proxy_init】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        nCode = self.skO.GetUserAccount()
        msg = "【GetUserAccount】"+ self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        nCode = self.skO.SKOrderLib_GetSpeedyType(self.ID)
        if nCode == 0:
            msg = "一般線路"
        else:
            msg = "Speedy線路"
        msg = "【OrderLib_SpeedyType】" + msg
        self.signals.log_sig.emit(msg)  

        self.getInfo()
        # nCode = self.skO.SKOrderLib_TelnetTest()
        # msg = "【OrderLib_TelnetTest】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)
    
    def getInfo(self):
        global acclist, position
        position=[]
        nCode = self.skO.GetOpenInterestGW(self.ID,acclist["TF"],1)
        # msg="【GetOpenInterest】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)
        
        nCode = self.skO.GetFutureRights(self.ID,acclist["TF"],1)
        # msg="【GetFutureRight】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # print(msg)

    @Slot(str,str,dict)
    def _handle_order(self, pattern, channel, data):
        # so far it only subscribe order channel
        print('Order command:', data)
        self.processOrder(data)

    def processOrder(self,*args):
        # modify or delete order
        self.skO.SendFutureProxyAlter(...)
        # place new order
        self.skO.SendFutureProxyOrderCLR(...)

    def stop(self):
        if hasattr(self, 'orderSub'):
            self.orderSub.stop()

        return