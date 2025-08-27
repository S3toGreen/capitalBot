import asyncio
import threading

class AsyncWorker:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.running = True

        self.thread = threading.Thread(target=self._start_loop, daemon=True)
        self.thread.start()

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.queue = asyncio.Queue()

        task = self.loop.create_task(self._consume_queue())
        task.add_done_callback(lambda t: self.loop.stop())

        self.loop.run_forever()

        self.loop.close()

    async def _consume_queue(self):
        # if not running and task done break
        while True:
            coro = await self.queue.get()
            try:
                if coro is None:
                    break
                await coro
            except Exception as e:
                print(f"[RedisWorker] Error running task: {e}{coro}")
            finally:
                self.queue.task_done()

    def submit(self, coro):
        """Non-blocking way to schedule an async redis task from anywhere"""
        if self.running:
            self.loop.call_soon_threadsafe(self.queue.put_nowait, coro)


    def stop(self):
        if not self.running:
            return
        self.running=False

        fut = asyncio.run_coroutine_threadsafe(self.queue.join(), self.loop)
        fut.result()
        self.loop.call_soon_threadsafe(self.queue.put_nowait, None)

        self.thread.join()

        
    # def start_background_task(self, coro):
    #     future = asyncio.run_coroutine_threadsafe(coro, self.loop)
    #     return future