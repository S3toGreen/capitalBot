import asyncio
from PySide6.QtCore import QObject, Slot
from qasync import asyncSlot,asyncClose
from core.SignalManager import SignalManager
from core.DBEngine.Producer import DataProducer # Refactored Producer
from brokers.skcom.quote.DMQuoteThread import DomesticQuote # Refactored Thread
# from core.quote.OSQuoteThread import OverseaQuote # Refactored Thread

import logging
logger = logging.getLogger("TickerApp.Service")

class TickerService(QObject):
    """
    The main async orchestrator. Runs in the main QtAsyncio thread.
    Manages all services, threads, and data flow.
    """
    def __init__(self):
        super().__init__()
        self.signals = SignalManager.get_instance()
        
        # The thread-safe bridge from broker threads to this async loop
        self.event_queue = asyncio.Queue()
        
        self.producer = None
        self.dm_thread = None
        self.os_thread = None
        
        self.skC = None # Will be set on login
        self.login_event = asyncio.Event() # A flag to wait for login
        
        # Connect signals
        self.signals.login_success.connect(self.on_login)
        self.signals.shutdown.connect(self.on_shutdown)
        self._tasks=[]

    @asyncSlot(object)
    async def on_login(self, skC_object):
        """Receives the skC object from the GUI and starts the services."""
        logger.info("Logged in, starting services...")
        self.skC = skC_object
        await self.run()

    async def on_shutdown(self):
        """Triggers a graceful shutdown."""
        # This will be handled by the main() exception block
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks)
        logger.info("------All Service stopped------")

    async def run_event_processor(self):
        """
        The main event loop. This one task reads from the bridge queue
        and dispatches work to the async producer.
        """
        # logger.info("Event processor started. Waiting for events...")
        while True:
            try:
                # 1. Wait for an event from either broker thread
                (market, command, *args) = await self.event_queue.get()

                # 2. Dispatch the event to the correct async function
                match command:
                    case "XADD":
                        # args = (market, symbol, tick)
                        await self.producer.xadd_tick(market, args[0], args[1])
                    case "PTR":
                        # args = (market, symbol, nPtr)
                        # This is safe. It's in the same async thread.
                        self.producer.lastest_ptr[args[0]] = args[1]
                    case "QUOTE":
                        # args = (market, symbol, data)
                        await self.producer.pub_quote(market, args[0], args[1])
                    case "DEPTH":
                        # args = (market, symbol, data)
                        await self.producer.pub_depth(market, args[0], args[1])

                    case _:
                        logger.warning(f"Unknown command: {command}")
                        
                self.event_queue.task_done()
                
            except asyncio.CancelledError:
                logger.info("Event processor stopped.")
                break
            except Exception as e:
                logger.exception(f"Error in event processor: {e}")

    async def _request_handler(self, msg:dict):
        try:
            print(msg)
            symbol = msg.get("data").decode()
            if symbol is None:
                return
            ch = msg.get("channel").decode()
            logger.info(f"Received dynamic subscribe request for{symbol}")
        except Exception as e:
            logger.warning(f"Invalid symbol request: {e}")

    async def run(self):
        """The main entry point for the service."""        
        try:
            # 1. Create the 100% async producer
            self.producer = await DataProducer.create_async() # No worker needed
            pubsub = self.producer.redis.pubsub()
            await pubsub.psubscribe(**{"request:*": self._request_handler})
            self.ptr = await self.producer.get_ptr_async()
            # 2. Start the broker threads, passing them the bridge queue
            # self.dm_thread = DomesticQuote(self.skC, self.event_queue)
            # # self.os_thread = OverseaQuote(self.skC, self.event_queue)
            
            # self.dm_thread.start() # This is a QThread.start()
            # # self.os_thread.start() # This is a QThread.start()
            
            # # 3. Start the background async tasks
            # save_task = asyncio.create_task(self.producer.run_pointer_saver())
            self._tasks.append(asyncio.create_task(self.run_event_processor()))
            # self._tasks.append(asyncio.create_task(pubsub.run()))
            
        except asyncio.CancelledError:
            logger.info("TickerService.run() cancelled.")
        except Exception as e:
            logger.exception(f"Error on starting task: {e}")
        finally:
            logger.info("Service started, waiting for events...")

    # async def stop(self):
    #     """Graceful shutdown logic."""
    #     # This is called from the main() finally block
    #     logger.info("Stopping TickerService...")
    #     # The tasks will be cancelled by the on_shutdown signal
    #     # We just need to ensure the threads are stopped
    #     if self.dm_thread:
    #         self.dm_thread.quit()
    #     if self.os_thread:
    #         self.os_thread.quit()