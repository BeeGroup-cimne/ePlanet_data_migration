from dataclasses import dataclass
from enum import Enum
from typing import Optional


@dataclass
class Location(object):
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


@dataclass
class Element(object):
    instance: int
    id_project: int
    code: str
    use: str
    typology: int
    name: str
    begin_date: str  # 2020-01-16
    end_date: str  # 2020-01-16
    location: Optional[dict] = None


@dataclass
class Supply(object):
    instance: int
    id_project: int
    code: str
    cups: str
    id_source: int
    element_code: str
    use: str
    id_zone: int
    begin_date: str  # 2020-01-16
    end_date: str  # 2020-01-16
    description: Optional[str] = None


class SupplyEnum(Enum):
    ELECTRICITY = 0
    GAS = 1
    FUEL = 2
    WATER = 3


@dataclass
class RequestHourlyData(object):
    instance: int
    id_project: int
    cups: str
    sensor: str
    hourly_data: dict


class SensorEnum(Enum):
    ELECTRICITY = 'Consumption_1'
    GAS = 'Consumption_1'
    WATER = 'WaterConsumption'


@dataclass
class HourlyData(object):
    value: float
    timestamp: str  # 2020-01-16T10:00:00Z
