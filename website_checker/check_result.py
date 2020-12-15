from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class CheckResult:
    website: str
    httpcode: int
    response_time: timedelta
    timestamp: datetime
    details: str = ''