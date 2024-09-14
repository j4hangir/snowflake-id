from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from time import time

__author__ = ['j4hangir', 'vd2org']
__all__ = ['Snowflake', 'SnowflakeGenerator', 'MAX_TS', 'MAX_INSTANCE', 'MAX_SEQ']

MAX_TS = 0b11111111111111111111111111111111111111111
MAX_DATACENTER = 31  # 0b11111
MAX_INSTANCE = 31  # 0b11111
MAX_SEQ = 4095  # 0b111111111111


@dataclass(frozen=True)
class Snowflake:
    timestamp: int
    datacenter: int
    instance: int
    epoch: int = 0
    seq: int = 0
    
    def __post_init__(self):
        if self.epoch < 0:
            raise ValueError("Time travel not supported. Keep epoch positive!")
        if not 0 <= self.timestamp <= MAX_TS:
            raise ValueError(f"Timestamp out of bounds. Stick between 0 and {MAX_TS}, please!")
        if not 0 <= self.datacenter <= MAX_DATACENTER:
            raise ValueError(f"Datacenter ID too extreme. Stay within 0-{MAX_DATACENTER} range!")
        if not 0 <= self.instance <= MAX_INSTANCE:
            raise ValueError(f"Instance ID gone wild. Tame it to 0-{MAX_INSTANCE}!")
        if not 0 <= self.seq <= MAX_SEQ:
            raise ValueError(f"Sequence number off the charts. Rein it in to 0-{MAX_SEQ}!")
    
    @classmethod
    def parse(cls, snowflake: int, epoch: int = 0) -> Snowflake:
        return cls(
            epoch=epoch,
            timestamp=snowflake >> 22,
            datacenter=(snowflake >> 17) & MAX_DATACENTER,
            instance=(snowflake >> 12) & MAX_INSTANCE,
            seq=snowflake & MAX_SEQ
        )
    
    @property
    def value(self) -> int:
        return (self.timestamp << 22) | (self.datacenter << 17) | (self.instance << 12) | self.seq
    
    @property
    def milliseconds(self) -> int:
        return self.timestamp + self.epoch
    
    @property
    def seconds(self) -> float:
        return self.milliseconds / 1000
    
    @property
    def datetime(self) -> datetime:
        return datetime.utcfromtimestamp(self.seconds)
    
    def datetime_tz(self, tz: tzinfo = None) -> datetime:
        return datetime.fromtimestamp(self.seconds, tz=tz)
    
    @property
    def timedelta(self) -> timedelta:
        return timedelta(milliseconds=self.epoch)
    
    def __int__(self) -> int:
        return self.value


class SnowflakeGenerator:
    def __init__(self, datacenter: int, instance: int, *, seq: int = 0, epoch: int = 0, timestamp: int = None):
        current = int(time() * 1000)
        
        if current - epoch >= MAX_TS:
            raise OverflowError("Maximum timestamp reached for the selected epoch. Unable to generate more IDs.")
        
        timestamp = timestamp or current
        
        if timestamp < 0 or timestamp > current:
            raise ValueError(f"Timestamp must be between 0 and {current}.")
        if epoch < 0 or epoch > current:
            raise ValueError(f"Epoch must be between 0 and {current}.")
        
        if datacenter < 0 or datacenter > MAX_DATACENTER:
            raise ValueError(f"Datacenter must be between 0 and {MAX_DATACENTER}.")
        if instance < 0 or instance > MAX_INSTANCE:
            raise ValueError(f"Instance must be between 0 and {MAX_INSTANCE}.")
        if seq < 0 or seq > MAX_SEQ:
            raise ValueError(f"Sequence must be between 0 and {MAX_SEQ}.")
        
        self._epoch = epoch
        self._ts = timestamp - self._epoch
        self._inf = (datacenter << 17) | (instance << 12)
        self._seq = seq
    
    @classmethod
    def from_snowflake(cls, sf: Snowflake) -> 'SnowflakeGenerator':
        return cls(sf.datacenter, sf.instance, seq=sf.seq, epoch=sf.epoch, timestamp=sf.timestamp)
    
    def __next__(self) -> int | None:
        current = int(time() * 1000) - self._epoch
        
        if current >= MAX_TS:
            raise OverflowError("Maximum timestamp reached for the selected epoch. Unable to generate more IDs.")
        if self._ts == current:
            if self._seq == MAX_SEQ: return None
            self._seq += 1
        elif self._ts > current: return None
        else: self._seq = 0
        
        self._ts = current
        
        return (self._ts << 22) | self._inf | self._seq


if __name__ == '__main__':
    gen = SnowflakeGenerator(1, 0)
    print(next(gen))
