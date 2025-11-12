from dataclasses import dataclass
from typing import Dict, Optional

Headers = Dict[str, str]

@dataclass
class Message:
    key: str
    value: bytes
    headers: Optional[Headers] = None

@dataclass
class DriftQConfig:
    address: str  # broker gRPC endpoint (Note that this is future use!)
