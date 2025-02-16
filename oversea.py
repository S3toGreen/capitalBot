import comtypes.client as cc
import comtypes.gen.SKCOMLib as sk
import numpy as np
import pythoncom

Klines = np.empty((0, 6)) 
class SKReplyLibEvent():
    def OnReplyMessage(self, bstrUserID, bstrMessages):
        # time.sleep(15)
        nConfirmCode = -1
        msg = "【Announcement】" + bstrMessages
        return nConfirmCode 
    
class SKQuoteLibEvent():
    def OnConnection(self, nKind, nCode): 
        global status
        status = nKind-3000
        msg = "【OnConnection】" + skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        print(msg)
        #     print("done")

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        global tmpK,df
        tf=15
        if sSecond or sMinute%5!=0:
            return
        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')
        Klines = np.vstack([Klines, data])

class SKOSQuoteLibEvent():
    def OnConnect(self, nKind, nCode): 
        global status
        status = nKind-3000
        msg = "【OnConnection】" + skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        print(msg, status)

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        global tmpK,df
        tf=15
        if sSecond or sMinute%5!=0:
            return
        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)

    def OnKLineData(self, bstrStockNo, bstrData):
        global Klines
        msg = "【OnKLineData】" + bstrStockNo + "_" + bstrData 
        print(msg)
        data = bstrData.split(',')
        Klines = np.vstack([Klines, data])

    def OnOverseaProducts(self, bstrValue):
        if bstrValue.split(',')[0]!='CME':
            return
        msg = "【OnOverseaProducts】" + bstrValue
        print(msg)

class SKCenterLibEvent():
    def OnShowAgreement(self, bstrData):
        msg = "【OnShowAgreement】" + bstrData
    
if __name__=='__main__':
    skR = cc.CreateObject(sk.SKReplyLib,interface=sk.ISKReplyLib)
    SKReplyEvent = SKReplyLibEvent()
    SKReplyLibEventHandler = cc.GetEvents(skR, SKReplyEvent)
    
    skC = cc.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib)
    SKCenterEvent = SKCenterLibEvent()
    SKCenterEventHandler = cc.GetEvents(skC, SKCenterEvent)

    # skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
    # SKOrderEvent = SKOrderLibEvent(skC)
    # SKOrderLibEventHandler = cc.GetEvents(skO, SKOrderEvent)

    # skQ = cc.CreateObject(sk.SKQuoteLib,interface=sk.ISKQuoteLib)
    # SKQuoteEvent = SKQuoteLibEvent()
    # SKQuoteEventHandler = cc.GetEvents(skQ, SKQuoteEvent)

    skOSQ = cc.CreateObject(sk.SKOSQuoteLib,interface=sk.ISKOSQuoteLib)
    SKOSQuoteEvent = SKOSQuoteLibEvent()
    SKOSQuoteEventHandler = cc.GetEvents(skOSQ, SKOSQuoteEvent)

    skC.SKCenterLib_Login("H125488697","Seto927098")
    nCode = skOSQ.SKOSQuoteLib_EnterMonitorLONG()
    # print(skC.SKCenterLib_GetReturnCodeMessage(nCode))
    status=0
    while status<1:
        pythoncom.PumpWaitingMessages()

    # nCode = skOSQ.SKOSQuoteLib_RequestOverseaProducts()
    # print(skC.SKCenterLib_GetReturnCodeMessage(nCode))
    nCode = skOSQ.SKOSQuoteLib_RequestKLineByDate("CME,NQ0000",0,"20241010","20241210",15)
    print(skC.SKCenterLib_GetReturnCodeMessage(nCode))
    
    while 1:
        pythoncom.PumpWaitingMessages()
