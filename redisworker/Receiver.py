import json
import asyncio
from clickhouse_connect import get_async_client
from .AsyncWorker import AsyncWorker
from PySide6.QtCore import QObject, Signal, Slot
from redis.asyncio import Redis

class DataReceiver(QObject):
    message_received = Signal(str,str,dict)
    def __init__(self, worker:AsyncWorker, redis:Redis, channels, patterns):
        super().__init__()
        self.async_worker = worker
        self.redis = redis

        self.channels = set(channels) if channels else set()
        self.patterns = set(patterns) if patterns else set()

        self._pubsub = None
        self._task = None
        self.start()

    @classmethod
    def create(cls, worker, channels:list=None,patterns:list=None):
        return asyncio.run_coroutine_threadsafe(cls.create_async(worker,channels, patterns), worker.loop).result()
    @classmethod
    async def create_async(cls, worker, channels=None, patterns=None):
        redis = Redis(decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=6,
                socket_keepalive=True,
                health_check_interval=30
            )
        return cls(worker, redis, channels, patterns)
    
    def start(self):
        if self._pubsub is None:
            self._pubsub = self.redis.pubsub()

        if not self._task or self._task.done():
            future = asyncio.run_coroutine_threadsafe(self._listen_async(), self.async_worker.loop)
            self._task = asyncio.wrap_future(future)

    async def _listen_async(self):
        if self.patterns:
            await self._pubsub.psubscribe(*self.patterns)
        if self.channels:
            await self._pubsub.subscribe(*self.channels)
        try:
            async for msg in self._pubsub.listen():
                if msg["type"] not in ("pmessage", "message"):
                    continue
                try:
                    data = json.loads(msg.get("data"))
                    self.message_received.emit(msg.get('pattern'), msg.get('channel'), data)
                except Exception as e:
                    print(f"[RedisSubscriber] error: {e}")
                    continue
        except asyncio.CancelledError:
            print("[RedisSubscriber] Cancelled")

    def add_patterns(self, *patterns:str):
        if not patterns:
            return
        if self._pubsub and not self._task.done():
            for pattern in patterns:
                if pattern in self.patterns:
                    print(f"Pattern {pattern} already exists, skipping.")
                    continue
                coro = self._pubsub.psubscribe(pattern)
                asyncio.run_coroutine_threadsafe(coro, self.async_worker.loop)
                self.patterns.update(patterns)
        else:
            print("Warning: Listener is not running.")
    def remove_patterns(self, *patterns:str):
        if not patterns:
            return
        if self._pubsub and not self._task.done():
            coro = self._pubsub.punsubscribe(*patterns)
            asyncio.run_coroutine_threadsafe(coro, self.async_worker.loop)
            self.channels.difference_update(patterns)
        else:
            print("Warning: Listener is not running.")

    def add_channels(self, *channels:str):
        if not channels:
            return
        if self._pubsub and not self._task.done():
            for channel in channels:
                if channel in self.channels:
                    print(f"Channel {channel} already exists, skipping.")
                    continue
                coro = self._pubsub.subscribe(*channels)
                asyncio.run_coroutine_threadsafe(coro, self.async_worker.loop)
                self.channels.update(channels)
        else:
            print("Warning: Listener is not running.")

    def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            asyncio.run_coroutine_threadsafe(self._pubsub.aclose(), self.async_worker.loop).result()
            

    # async def _listen_snapshots(self):
    #     pubsub = self.redis.pubsub()
    #     await pubsub.subscribe(f"snap:*")
    #     async for msg in pubsub.listen():
    #         if msg["type"] == "message":
    #             data = json.loads(msg["data"])
    #             self.snap_received.emit(data)
    #         if not self._running:
    #             break

    # async def _listen_quotes(self):
    #     pubsub = self.redis.pubsub()
    #     await pubsub.subscribe(f"quote:*")
    #     async for msg in pubsub.listen():
    #         if msg["type"] == "message":
    #             _, market, id = msg['channel'].split(':')
    #             data = json.loads(msg["data"])
    #             self.quote_received.emit(id,data,market)
    #         if not self._running:
    #             break

        """
        async with self.redis.pubsub() as pubsub:
            await pubsub.subscribe(*self.channels)
            try:
                while True:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg:
                        try:
                            data = json.loads(msg['data'])
                            self.message_received.emit(msg['channel'], data)
                        except Exception as e:
                            print(f"[DataReceiver] JSON parse error: {e}")
            except asyncio.CancelledError:
                print("[DataReceiver] Listener cancelled")
            except Exception as e:
                print(f"[DataReceiver] Redis listen error: {e}")
            finally:
                await pubsub.unsubscribe(*self.channels)
                print("[DataReceiver] Unsubscribed and cleaned up.")
        """