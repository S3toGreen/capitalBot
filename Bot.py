import pandas as pd
from PySide6.QtCore import *
import comtypes.client as cc
# cc.GetModule(r'./x64/SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
import numpy as np
from SignalManager import SignalManager
from DMQuoteThread import DomesticQuote
from OSQuoteThread import OverseaQuote

Klines = np.empty((0, 6)) 
acclist = {}
accinfo = [] # future right info
position = []
footprint = np.empty((0, 2),dtype=object)  #{Timestamp:{price:[aggbuy, aggsell]}} or [[timestamp,{price:aggbuy, aggsell}]
# sl=0
tmpK = np.empty((0,9),dtype=object)#np.pad(np.loadtxt("TX_Ticktmp.csv",delimiter=',', dtype=object),((0,0),(0,1)),constant_values=-1)  # daily 1 min klines
status=[-1,-1]   # download is 1, dc is 2, ready is 3

class SKReplyLibEvent():
    def __init__(self):
        super().__init__()
        self.singals = SignalManager.get_instance()

    def OnReplyMessage(self, bstrUserID, bstrMessages):
        # time.sleep(15)
        msg = "【Announcement】" + bstrMessages
        self.singals.log_sig.emit(msg)
        return -1
    def OnStrategyData(self, bstrUserID, bstrData):
        msg = "【StrategyData】"+ bstrData
        self.singals.log_sig.emit(msg)
    def OnNewData(self, bstrUserID, bstrData):
        msg = "【NewData】"+bstrData
        self.singals.log_sig.emit(msg)

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
        
        self.skR = cc.CreateObject(sk.SKReplyLib,interface=sk.ISKReplyLib)
        SKReplyEvent = SKReplyLibEvent()
        self.SKReplyLibEventHandler = cc.GetEvents(self.skR, SKReplyEvent)
        
        self.skC = cc.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib)
        SKCenterEvent = SKCenterLibEvent()
        self.SKCenterLibEventHandler = cc.GetEvents(self.skC, SKCenterEvent)

        self.skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
        SKOrderEvent = SKOrderLibEvent(self.skC)
        self.SKOrderLibEventHandler = cc.GetEvents(self.skO, SKOrderEvent)

        self.signals = SignalManager.get_instance()

        self.domestic_thread = QThread()
        self.oversea_thread = QThread()
        self.domestic_worker = DomesticQuote(self.skC)
        self.oversea_worker = OverseaQuote(self.skC)
        self.domestic_worker.moveToThread(self.domestic_thread)
        self.oversea_worker.moveToThread(self.oversea_thread)
        self.domestic_thread.started.connect(self.domestic_worker.run)
        # self.oversea_thread.started.connect(self.oversea_worker.run)

        nCode = self.skC.SKCenterLib_SetLogPath("CapitalLog")

        if nCode:
            msg = "【SetLogPath】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
        
        self.debug_state = False
        self.update_debug()

    def run(self):
        self.signals.order_sig.connect(self.placeOrder)
        self.signals.close_all_sig.connect(self.close_all)
        self.domestic_thread.start()
        # self.oversea_thread.start()
        self.init()

    def login(self, acc, passwd):
        nCode = self.skC.SKCenterLib_Login(acc, passwd)
        msg = "【Login】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        if nCode==0:
            self.ID = acc
        self.signals.log_sig.emit(msg)
        return nCode
    
    def init(self):
        global df, acclist, tmpK, footprint
        self.order_init()
            
    def order_init(self):
        nCode = self.skO.SKOrderLib_Initialize()
        msg = "【OrderLib_Init】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        
        nCode = self.skO.ReadCertByID(self.ID)
        msg = "【ReadCertByID】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        # nCode = self.skO.SKOrderLib_InitialProxyByID(self.ID)
        # msg = "【Proxy_init】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)

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

    def placeOrder(self):
        global df, accinfo, sl, position, tmpK
        tmp = pd.DataFrame(tmpK[:-1, :-1],columns=["Time","Open","High","Low","Close","Volume","aggBuy","aggSell"]).set_index("Time").resample('15min',closed='right',label='right').agg({
            'Open':'first',
            'High':'max',
            'Low':'min',
            'Close':'last',
            'Volume':'sum',
            'aggBuy': 'sum',
            'aggSell': 'sum',
        }).dropna()
        df = pd.concat([df[~df.index.isin(tmp.index)], tmp],join='inner').sort_index()

        print(df.tail(3))
        typical_price = (tmpK[:,2]+tmpK[:,3]+tmpK[:,4])/3
        vwap = np.average(typical_price,weights=tmpK[:,5])

        print("VWAP:",vwap)
        self.getInfo()

        print(f'{"-"*60}')

        if int(accinfo[0])<20000:
            self.signals.log_sig.emit("Insufficient funds!")
            return
        
        return
        
        order = sk.FUTUREORDER()
        order.bstrFullAccount = acclist["TF"]
        # TM0000
        order.bstrStockNo = "TM0000" #"MTX00"
        # 0:ROD  1:IOC  2:FOK
        order.sTradeType = 1
        # 0: buy 1: sell
        order.sBuySell = side
        order.sDayTrade = 0
        # 0:open 1:close 2:auto
        order.sNewClose = 2
        order.bstrPrice = "P"
        order.nQty = qty
        order.sReserved = 0

        self.skO.SendFutureOrderCLR(ID, 1, order)
        self.signals.log_sig.emit(f"Order Sent!")
        self.getInfo()

    def close_all(self):
        pass
    def update_debug(self):
        nCode = self.skC.SKCenterLib_Debug(self.debug_state)
        if nCode:
            msg = "【SetDebug】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

    def saveData(self):
        global tmpK,footprint
        self.oversea_worker.cleanup()
        self.domestic_worker.cleanup()
        self.oversea_thread.quit()
        self.domestic_thread.quit()
        self.domestic_thread.wait()
        self.oversea_thread.wait()
        return
        if tmpK.size:
            print("Saving min data...")
            
            t1=pd.read_parquet("TXfp.pq")
            t=np.flip(footprint,axis=0)
            _, j = np.unique(t[:,0],return_index=True)
            footprint=t[j]
            t = []
            filter = []
            for ts, price in footprint:
                for p,c in reversed(price.items()):
                    t.append({'Time':ts,'Price':int(p),'aggBuy':c[0],'aggSell':c[1]})
                    filter.append(ts)
            pd.concat([t1[~t1.index.get_level_values('Time').isin(filter)],pd.DataFrame(t).set_index(['Time','Price'])]).to_parquet('TXfp.pq')
            
            t1 = pd.read_csv("TX_Tick.csv",parse_dates=['Time'], dtype=np.int32)
            t = np.flip(tmpK, axis=0) 
            _, j = np.unique(t[:,0],return_index=True)
            tmpK = t[j]
            pd.concat([t1[~t1['Time'].isin(tmpK[:,0])], pd.DataFrame(tmpK[:,:-1],columns=t1.columns)]).set_index('Time').to_csv("TX_Tick.csv")

    