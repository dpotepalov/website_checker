from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class CheckResult:
    website: str
    httpcode: int
    response_time: float
    timestamp: float
    details: str = ''