import asyncio
import threading

class AsyncWorker:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.running = True
        
        # Start the loop in a background thread
        self.thread = threading.Thread(target=self._start_loop, daemon=True)
        self.thread.start()

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.queue = asyncio.Queue()

        self.loop.create_task(self._consume_queue())
        self.loop.run_forever()

        self.loop.close()

    async def _consume_queue(self):
        while True:
            coro = await self.queue.get()
            if coro is None:
                break
            try:
                await coro
            except Exception as e:
                print(f"[RedisWorker] Error running task: {e}{coro}")

    def submit(self, coro):
        """Non-blocking way to schedule an async redis task from anywhere"""
        # fut = asyncio.get_event_loop().create_future()
        self.loop.call_soon_threadsafe(self.queue.put_nowait, coro)
        # return fut

    # def start_background_task(self, coro):
    #     future = asyncio.run_coroutine_threadsafe(coro, self.loop)
    #     return future

    def stop(self):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, None)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()