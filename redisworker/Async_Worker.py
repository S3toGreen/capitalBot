import asyncio
import threading

class AsyncRedisWorker:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.queue = asyncio.Queue()
        self.running = True
        
        # Start the loop in a background thread
        self.thread = threading.Thread(target=self._start_loop, daemon=True)
        self.thread.start()

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(self._consume_queue())
        self.loop.run_forever()

    async def _consume_queue(self):
        while self.running:
            coro = await self.queue.get()
            try:
                await coro
            except Exception as e:
                print(f"[RedisWorker] Error running task: {e}")

    def submit(self, coro):
        """Non-blocking way to schedule an async redis task from anywhere"""
        self.loop.call_soon_threadsafe(self.queue.put_nowait, coro)

    def stop(self):
        self.running = False
        self.loop.call_soon_threadsafe(self.loop.stop)