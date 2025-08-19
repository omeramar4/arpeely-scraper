from dataclasses import dataclass
from typing import Dict


@dataclass
class URLMetadata:
    url: str
    page_title: str
    source_url: str
    depth_from_source: int
    links_to_texts: Dict[str, str]
