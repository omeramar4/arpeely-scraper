from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any


@dataclass
class UrlToProcess:
    """
    Data class representing a URL to be processed during web scraping.
    """
    url: str
    source_url: Optional[str]
    depth: int

    @property
    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class UrlProcessingResult(UrlToProcess):
    """
    Data class representing the result of processing a URL, extending UrlToProcess.
    """
    status: str
    topic: str