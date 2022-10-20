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
    begin_date: str
    end_date: str
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
    begin_date: str
    end_date: str
    description: Optional[str] = None


class SupplyEnum(Enum):
    ELECTRICITY = 0
    GAS = 1
    FUEL = 2
    WATER = 3
