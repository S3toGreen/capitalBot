from pandas import Timestamp
from dataclasses import dataclass, field
from collections import defaultdict

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
            'pm': {str(p):v for p,v in reversed(self.price_map.items())} # key as string is required
        }
    
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