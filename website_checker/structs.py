from dataclasses import dataclass
from dataclasses_json import dataclass_json, Undefined
from datetime import datetime, timedelta


@dataclass
class CheckResult:
    website: str
    httpcode: int
    response_time: float
    timestamp: float
    details: str = ''


@dataclass_json(undefined=Undefined.RAISE)
@dataclass
class Website:
    uri: str
    page_regex: str = None
    timeout: float = 0.0
