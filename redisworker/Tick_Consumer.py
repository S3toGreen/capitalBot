# import redis.asyncio as redis
import redis
import json
from multiprocessing import Process
from collections import defaultdict
from windows_toasts import WindowsToaster, Toast, ToastScenario
import threading
import time

toaster = WindowsToaster('Trading App')
newToast = Toast(scenario=ToastScenario.Important,suppress_popup=False)
order_thresh = 30 # large order threshold

class TickConsumer:
    def __init__(self, market='DM',tick_callback=None, bar_callback=None):
        self.redis = redis.Redis.from_pool(redis.ConnectionPool(decode_responses=True))
        # self.redis=redis.Redis(decode_responses=True)
        # self.symbols = []
        self.market=market

        self.pubsub = self.redis.pubsub()
        self.tick_callback = tick_callback  # Optional callbacks
        self.bar_callback = bar_callback
        self.last_ids = defaultdict(int)

    def start(self):
        self.pubsub.subscribe(**{'channel:DM': self.handle_pubsub})
        self.pubsub.run_in_thread(daemon=True)
        # threading.Thread(target=self.pubsub.run_in_thread, daemon=True).start()
        print("Sub on channel:DM...")
        # self.create_group('TX00')
        self.consume_Tick()

    def handle_pubsub(self, message):
        if message['type'] != 'message':
            return
        
        data = json.loads(message['data'])
        print(data)
        symbol=data["id"]
        # if 'unclosed' in data:
            
        print(f"[Alert] Tick received for {symbol}")
        self.last_ids[f"Tick:{self.market}:{symbol}"]

    def consume_Tick(self):
        # Trim stream / update lastest ptr /build candlestick
        global toaster, newToast
        try:
            while True:
                if self.last_ids:
                    response = self.redis.xread(self.last_ids)
                    # if not response:
                    #     break  # No more data
                    # print(response)
                    for stream, entries in response:
                        for entry_id, data in entries:
                            print(f"[Tick]  @ {data}")
                            self.last_ids[stream]=entry_id
                            # Update latest ID
                            if int(data['q'])>order_thresh:
                                newToast.text_fields=[f"[{stream}] BigTrade @ ${data['p']}{' SELL' if int(data['s'])<0 else (' BUY' if int(data['s'])>0 else '')} {data['q']} "]
                                toaster.show_toast(newToast)
                                newToast = newToast.clone()
        except Exception as e:
            print(f"[Error] Failed to read: {e}")

if __name__ == '__main__':
    consumer = TickConsumer()
    consumer.start()
    while 1:
        time.sleep(0.3)