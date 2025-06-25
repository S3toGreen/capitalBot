from sortedcontainers import SortedDict
from pandas import Timestamp
from dataclasses import dataclass, field

class DefaultSortedDict(SortedDict):
    def __init__(self, default_factory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_factory = default_factory
    def __missing__(self, key):
        self[key] = self.default_factory()
        return super().__getitem__(key)
    
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
    price_map: DefaultSortedDict = field(default_factory=lambda: DefaultSortedDict(lambda: [0, 0, 0])) # Volume, delta, trades_delta