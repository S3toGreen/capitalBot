from PySide6.QtCore import QObject, QThread, Slot
import shioaji as sj
import os, pytz
from dotenv import load_dotenv
load_dotenv()

class Broker(QObject):
    def __init__(self, /, parent = ..., *, objectName = ...):
        # super().__init__(parent, objectName=objectName)
        self.api = sj.Shioaji(simulation=True)
        self.api.login(os.getenv("SINOPAC_KEY"),os.getenv('SINOPAC_SECRET'))

        self.tz = pytz.timezone('Asia/Taipei')
        self.symlist = set(["TXFR1","MXFR1"])
        self.api.quote.set_on_tick_fop_v1_callback(self.tick_callback)
        self.api.quote.set_on_bidask_fop_v1_callback(self.bidask_callback)
        self.start()

    def tick_callback(self,exchange:sj.Exchange, tick:sj.TickFOPv1):
        tick.datetime = self.tz.localize(tick.datetime)
        print(f"Tick: {tick}")

    def bidask_callback(self,exchange:sj.Exchange, bidask:sj.BidAskFOPv1):
        bidask.datetime = self.tz.localize(bidask.datetime)
        print(f"BidAsk: {bidask}")

    def start(self):
        for i in self.symlist:
            self.api.quote.subscribe(contract=self.api.Contracts.Futures[i],
                                    quote_type=sj.constant.QuoteType.Tick,
                                    version = sj.constant.QuoteVersion.v1)

if __name__=="__main__":
    Broker()
    while True:
        pass