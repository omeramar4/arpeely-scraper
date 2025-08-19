from dependency_injector.wiring import Provide, inject
from fastapi import HTTPException, Depends, Query

from arpeely_scraper.app.models import ScrapeRequest, AsyncScrapeRequest, ScrapeResponse, StatusOnlyResponse, ResultsResponse, \
    UrlStatusRecord
from arpeely_scraper.app.service import ScraperApp
from arpeely_scraper.core.scraper import WebScraper
from arpeely_scraper.db_connector.di_container import Container
from arpeely_scraper.db_connector.scraped_url_db_connector import ScrapedUrlDBConnector


app = ScraperApp()


@app.post("/scrape", response_model=ScrapeResponse)
def scrape(request: ScrapeRequest):
    scraper = WebScraper()
    try:
        results = scraper.scrape(request.base_url, request.max_depth)
        return ScrapeResponse(status="completed", scraped_count=len(results))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ascrape", response_model=ScrapeResponse)
async def ascrape(request: AsyncScrapeRequest):
    scraper = WebScraper()
    try:
        results = await scraper.ascrape(request.base_url, request.max_depth, request.max_concurrency)
        return ScrapeResponse(status="completed", scraped_count=len(results))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status", response_model=StatusOnlyResponse)
@inject
def status(
    base_url: str = Query(..., description="Base URL to check status for"),
    db_connector: ScrapedUrlDBConnector = Depends(Provide[Container.db_connector])
):
    all_urls = db_connector.get_all_urls_with_status(base_url)
    queued_urls = [r for r in all_urls if r[3] == "queued"]
    status_str = "completed" if not queued_urls and all_urls else "interrupted" if queued_urls else "not_started"
    return StatusOnlyResponse(base_url=base_url, status=status_str)


@app.get("/results", response_model=ResultsResponse)
@inject
def results(
    base_url: str = Query(..., description="Base URL to get results for"),
    db_connector: ScrapedUrlDBConnector = Depends(Provide[Container.db_connector])
):
    all_urls = db_connector.get_all_urls_with_status(base_url)
    result_objs = [UrlStatusRecord(
        url=url,
        source_url=source_url,
        depth=depth,
        status=status
    ) for url, source_url, depth, status in all_urls]
    return ResultsResponse(base_url=base_url, results=result_objs)
