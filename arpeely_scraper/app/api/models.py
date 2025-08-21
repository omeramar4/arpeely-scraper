from pydantic import BaseModel
from typing import List, Optional, Any


class ScrapeRequest(BaseModel):
    base_url: str
    max_depth: int = 2
    start_fresh: bool = True


class AsyncScrapeRequest(BaseModel):
    base_url: str
    max_depth: int = 2
    max_concurrency: int = 10
    start_fresh: bool = True


class ScrapeResponse(BaseModel):
    status: str
    scraped_count: int


class UrlStatusRecord(BaseModel):
    url: str
    source_url: Optional[str]
    depth: int
    status: str
    topic: str = "other"


class StatusOnlyResponse(BaseModel):
    base_url: str
    status: str


class ResultsResponse(BaseModel):
    base_url: str
    results: List[UrlStatusRecord]


class ResultRecord(BaseModel):
    base_url: str
    url: str
    source_url: Optional[str]
    depth: int
    title: Optional[str]
    links_to_texts: Any
    topic: str = "other"
    status: str


class AddTopicsRequest(BaseModel):
    topics: List[str]
