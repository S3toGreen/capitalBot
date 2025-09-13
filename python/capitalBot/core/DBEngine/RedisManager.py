import asyncio
from redis.asyncio import Redis

class RedisManager:
    """
    Provide two clients: pub (general commands & publish) and sub (for pubsub listening).
    Manage reconnection logic minimally.
    """
    def __init__(self, *, url=None, decode_responses=False):
        self.url = url
        self.decode_responses = decode_responses
        self.pub: Redis = None
        self.sub: Redis = None
        self._lock = asyncio.Lock()

    @classmethod
    async def init(self):
        async with self._lock:
            if self.pub is None:
                self.pub = Redis.from_url(self.url) if self.url else \
                    Redis(decode_responses=False,
                    socket_connect_timeout=3,
                    socket_timeout=6,
                    socket_keepalive=True,
                    health_check_interval=30,
                    max_connections=15
                    )
            if self.sub is None:
                # sub should be a separate client
                self.sub = Redis.from_url(self.url) if self.url else \
                    Redis(decode_responses=False,
                    socket_connect_timeout=3,
                    socket_timeout=6,
                    socket_keepalive=True,
                    health_check_interval=30,
                    max_connections=3
                    )
    async def close(self):
        async with self._lock:
            if self.pub:
                await self.pub.aclose()
                self.pub = None
            if self.sub:
                await self.sub.aclose()
                self.sub = None

    # convenience wrappers
    async def publish(self, channel, data):
        if self.pub is None:
            await self.init()
        return await self.pub.publish(channel, data)

    async def xadd(self, key, fields, maxlen=None, approximate=True):
        if self.pub is None:
            await self.init()
        res = await self.pub.xadd(key, fields)
        if maxlen:
            await self.pub.xtrim(key, maxlen=maxlen, approximate=approximate)
        return res

    # create pubsub object from sub client
    async def pubsub(self, **kwargs):
        if self.sub is None:
            await self.init()
        return self.sub.pubsub(**kwargs)
