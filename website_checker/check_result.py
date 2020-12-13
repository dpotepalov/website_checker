from dataclasses import dataclass


@dataclass
class CheckResult:
    website: str
    httpcode: int
    details: str = ''