from pandas import Timestamp
from dataclasses import dataclass, field
from collections import defaultdict
import msgpack
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
    open: float
    high: float
    low: float
    close: float
    vol: int
    delta_hlc: list = field(default_factory=lambda: [0, 0, 0])
    trades_delta: int = 0
    price_map: defaultdict = field(default_factory=lambda: defaultdict(lambda: [0, 0, 0])) # neutral, aggb, aggs

    def to_dict(self):
        return {
            'ts': self.time.isoformat(),
            'o': self.open,
            'h': self.high,
            'l': self.low,
            'c': self.close,
            'v': self.vol,
            'vd': self.delta_hlc,
            'td': self.trades_delta,
            'pm': {str(p):v for p,v in self.price_map.items()} # key as string is required
        }
    def to_bytes(self) -> bytes:
        return msgpack.packb({
            'ts': int(self.time.timestamp()),  # store as int for compactness self.time.isoformat(),#
            'o': int(self.open*100),
            'h': int(self.high*100),
            'l': int(self.low*100),
            'c': int(self.close*100),
            'v': self.vol,
            'vd': self.delta_hlc,
            'td': self.trades_delta,
            'pm': {int(p*100):v for p,v in self.price_map.items()}  # msgpack can handle dicts natively
        }, use_bin_type=True)
    
@dataclass(slots=True, frozen=True)
class Tick:
    ptr: int
    time: Timestamp
    price: float
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