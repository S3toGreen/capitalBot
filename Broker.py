import pandas as pd
from PySide6.QtCore import *
import comtypes.client as cc
# cc.GetModule(r'./x64/SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
import numpy as np
import pythoncom
from SignalManager import SignalManager
from quote.DMQuoteThread import DomesticQuote
from quote.OSQuoteThread import OverseaQuote
from redisworker.AsyncWorker import AsyncWorker
from redisworker.Receiver import DataReceiver

# Simulated trading or dry run
# work flow send order to broker class  
acclist = {}
accinfo = [] # future right info
position = []

class SKReplyLibEvent():
    def __init__(self, skC):
        super().__init__()
        self.singals = SignalManager.get_instance()
        self.skC=skC
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
    def OnShowAgreement(self, bstrData):
        msg = "【OnShowAgreement】" + bstrData

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
        self.skC = cc.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib)
        SKCenterEvent = SKCenterLibEvent()
        self.SKCenterLibEventHandler = cc.GetEvents(self.skC, SKCenterEvent)

        self.skR = cc.CreateObject(sk.SKReplyLib,interface=sk.ISKReplyLib)
        self.SKReplyEvent = SKReplyLibEvent(self.skC)
        self.SKReplyLibEventHandler = cc.GetEvents(self.skR, self.SKReplyEvent)

        self.signals = SignalManager.get_instance()

        nCode = self.skC.SKCenterLib_SetLogPath("CapitalLog")

        if nCode:
            msg = "【SetLogPath】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
        
        self.debug_state = False
        self.update_debug()

    def login(self, acc, passwd):
        # nCode = self.skC.SKCenterLib_Login(acc, passwd)
        nCode = self.skC.SKCenterLib_LoginSetQuote(acc, passwd,'Y')

        msg = "【Login】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        if nCode==0:
            self.ID = acc
        self.signals.log_sig.emit(msg)
        # self.domestic.acc = self.oversea.acc = acc
        # self.domestic.passwd = self.oversea.passwd = passwd

        return nCode

    def update_debug(self):
        nCode = self.skC.SKCenterLib_Debug(self.debug_state)
        if nCode:
            msg = "【SetDebug】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

    

class QuoteBroker(Broker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.domestic_thread = QThread()
        self.oversea_thread = QThread()
        self.domestic = DomesticQuote(self.skC)
        self.oversea = OverseaQuote(self.skC)
        self.domestic.moveToThread(self.domestic_thread)
        self.oversea.moveToThread(self.oversea_thread)
        self.domestic_thread.started.connect(self.domestic.run)
        self.oversea_thread.started.connect(self.oversea.run)
        self.domestic_thread.finished.connect(self.domestic.stop)
        self.oversea_thread.finished.connect(self.oversea.stop)

    def init(self):
        # self.worker = AsyncWorker()
        # self.orderSub = DataReceiver.create(self.worker, ['order:*'])
        # self.orderSub.message_received.connect(self._handle_order)

        self.domestic_thread.start()
        self.oversea_thread.start()

    def stop(self):
        self.oversea_thread.quit()
        self.domestic_thread.quit()
        self.domestic_thread.wait()
        self.oversea_thread.wait()
        return
    
class OrderBroker(Broker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
        SKOrderEvent = SKOrderLibEvent(self.skC)
        self.SKOrderLibEventHandler = cc.GetEvents(self.skO, SKOrderEvent)

    def init(self):
        # self.worker = AsyncWorker()
        # self.orderSub = DataReceiver.create(self.worker, ['order:*'])
        # self.orderSub.message_received.connect(self._handle_order)

        self.skR.SKReplyLib_ConnectByID(self.ID)
        self.order_init()

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
        print('order msg:', data)
        self.processOrder(data)

    def stop(self):   
        if hasattr(self, 'orderSub'):
            self.orderSub.stop()
            self.worker.stop()              

        return