from dataclasses import dataclass
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
