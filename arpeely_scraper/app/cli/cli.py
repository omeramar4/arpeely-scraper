import click
import asyncio
import json
from typing import List

from arpeely_scraper.app.service import ScraperApp
from arpeely_scraper.core.scraper import WebScraper
from arpeely_scraper.db_connector.scraped_url_db_connector import ScrapedUrlDBConnector


app = ScraperApp()


@click.group()
@click.version_option(version="0.1.0")
def main():
    """Arpeely Scraper CLI - Web scraping with topic classification."""
    pass


@main.command()
@click.argument('base_url')
@click.option('--max-depth', '-d', default=2, help='Maximum crawl depth (default: 2)')
@click.option('--start-fresh', '-f', is_flag=True, help='Start fresh by clearing existing data')
def scrape(base_url: str, max_depth: int, start_fresh: bool):
    """Scrape a website synchronously."""
    click.echo(f"Starting synchronous scrape of {base_url}")
    click.echo(f"Max depth: {max_depth}, Start fresh: {start_fresh}")

    try:
        scraper = WebScraper(start_fresh=start_fresh)
        scraped_urls = scraper.scrape(base_url, max_depth)
        click.echo(f"Scraping completed! Scraped {len(scraped_urls)} URLs")

    except Exception as e:
        click.echo(f"Error during scraping: {e}", err=True)
        raise click.ClickException(str(e))


@main.command()
@click.argument('base_url')
@click.option('--max-depth', '-d', default=2, help='Maximum crawl depth (default: 2)')
@click.option('--max-concurrency', '-c', default=10, help='Maximum concurrent requests (default: 10)')
@click.option('--start-fresh', '-f', is_flag=True, help='Start fresh by clearing existing data')
def ascrape(base_url: str, max_depth: int, max_concurrency: int, start_fresh: bool):
    """Scrape a website asynchronously."""
    click.echo(f"Starting asynchronous scrape of {base_url}")
    click.echo(f"Max depth: {max_depth}, Max concurrency: {max_concurrency}, Start fresh: {start_fresh}")

    async def _ascrape():
        try:
            scraper = WebScraper(start_fresh=start_fresh)
            scraped_urls = await scraper.ascrape(base_url, max_depth, max_concurrency)
            click.echo(f"Async scraping completed! Scraped {len(scraped_urls)} URLs")

        except Exception as e:
            click.echo(f"Error during async scraping: {e}", err=True)
            raise click.ClickException(str(e))

    asyncio.run(_ascrape())


@main.command()
@click.argument('base_url')
def status(base_url: str):
    """Check the scraping status for a base URL."""
    try:
        db_connector: ScrapedUrlDBConnector = app.container.db_connector()

        all_urls = db_connector.get_all_urls_with_status(base_url)
        queued_urls = [r for r in all_urls if r.status == "queued"]

        if not all_urls:
            status_str = "not_started"
        elif queued_urls:
            status_str = "interrupted"
        else:
            status_str = "completed"

        click.echo(f"Status for {base_url}: {status_str}")
        click.echo(f"Total URLs found: {len(all_urls)}")
        click.echo(f"Queued URLs remaining: {len(queued_urls)}")

    except Exception as e:
        click.echo(f"Error checking status: {e}", err=True)
        raise click.ClickException(str(e))


@main.command()
@click.argument('base_url')
def results(base_url: str):
    """Get scraping results for a base URL."""
    try:
        db_connector: ScrapedUrlDBConnector = app.container.db_connector()
        all_urls = db_connector.get_all_urls_with_status(base_url)

        if not all_urls:
            click.echo(f"No results found for {base_url}")
            return

        results_data = []
        for url in all_urls:
            results_data.append(url.as_dict)
            click.echo(json.dumps({
                'base_url': base_url,
                'results': results_data
            }, indent=2, ensure_ascii=False))
        else:
            click.echo(f"\nResults for {base_url}:")
            click.echo("-" * 100)
            click.echo(f"{'URL':<50} {'Status':<12} {'Topic':<15} {'Depth':<5}")
            click.echo("-" * 100)

            for result in results_data:
                click.echo(f"{result['url']:<50} {result['status']:<12} {result['topic']:<15} {result['depth']:<5}")

            click.echo(f"\nTotal: {len(results_data)} URLs")

    except Exception as e:
        click.echo(f"Error getting results: {e}", err=True)
        raise click.ClickException(str(e))


@main.command()
@click.argument('topics', nargs=-1)
def add_topics(topics: List[str]):
    """Add topics to the topic classifier (comma-separated or space-separated)."""
    if not topics:
        click.echo("No topics provided. Please specify topics as arguments.", err=True)
        return

    # Support comma-separated input as a single argument
    if len(topics) == 1 and ',' in topics[0]:
        topics_list = [t.strip() for t in topics[0].split(',') if t.strip()]
    else:
        topics_list = [t for t in topics if t.strip()]

    if not topics_list:
        click.echo("No valid topics found after parsing.", err=True)
        return

    topic_classifier = app.container.topic_classifier()
    topic_classifier.set_topics(topics_list)
    click.echo(f"Topics updated: {topic_classifier.TOPICS}")


if __name__ == '__main__':
    main()
