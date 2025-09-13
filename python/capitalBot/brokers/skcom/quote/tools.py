from pandas import Timestamp
from dataclasses import dataclass, field
from collections import defaultdict
import msgspec
# from sortedcontainers import SortedDict
# class DefaultSortedDict(SortedDict):
#     def __init__(self, default_factory, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.default_factory = default_factory
#     def __missing__(self, key):
#         self[key] = self.default_factory()
#         return super().__getitem__(key)
    
@dataclass(slots=True)
class Bar:
    time: Timestamp
    open: int
    high: int
    low: int
    close: int
    vol: int
    delta_hlc: list = field(default_factory=lambda: [0, 0, 0])
    trades_delta: int = 0
    price_map: defaultdict = field(default_factory=lambda: defaultdict(lambda: [0, 0, 0])) # neutral, aggb, aggs

    def to_dict(self):
        return {
            "ts": int(self.time.timestamp()),
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "v": self.vol,
            "vd": self.delta_hlc,
            "td": self.trades_delta,
            "pm": self.price_map
        }
    
@dataclass(slots=True, frozen=True)
class Tick:
    ptr: int
    time: Timestamp
    price: int # with scale
    side: int
    qty: int
    
    def to_dict(self):
        return {
            'ptr': self.ptr,
            'ts': self.time.isoformat(),
            'p': self.price,
            's': self.side,
            'q': self.qty
        }