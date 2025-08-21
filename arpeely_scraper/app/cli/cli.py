#!/usr/bin/env python3
"""
CLI interface for the Arpeely Scraper application.
"""
import click
import asyncio
import json
from typing import Optional

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
@click.option('--output', '-o', type=click.Path(), help='Output results to JSON file')
def scrape(base_url: str, max_depth: int, start_fresh: bool, output: Optional[str]):
    """Scrape a website synchronously."""
    click.echo(f"Starting synchronous scrape of {base_url}")
    click.echo(f"Max depth: {max_depth}, Start fresh: {start_fresh}")

    try:
        # app = get_app()
        scraper = WebScraper(start_fresh=start_fresh)
        scraped_urls = scraper.scrape(base_url, max_depth)

        click.echo(f"✅ Scraping completed! Scraped {len(scraped_urls)} URLs")

        if output:
            # Get detailed results from database
            db_connector: ScrapedUrlDBConnector = app.container.db_connector()
            all_urls = db_connector.get_all_urls_with_status(base_url)

            results_data = []
            for url, source_url, depth, status, topic in all_urls:
                results_data.append({
                    'base_url': base_url,
                    'url': url,
                    'source_url': source_url,
                    'depth': depth,
                    'status': status,
                    'topic': topic
                })

            with open(output, 'w') as f:
                json.dump(results_data, f, indent=2, ensure_ascii=False)
            click.echo(f"Results saved to {output}")

    except Exception as e:
        click.echo(f"❌ Error during scraping: {e}", err=True)
        raise click.ClickException(str(e))


@main.command()
@click.argument('base_url')
@click.option('--max-depth', '-d', default=2, help='Maximum crawl depth (default: 2)')
@click.option('--max-concurrency', '-c', default=10, help='Maximum concurrent requests (default: 10)')
@click.option('--start-fresh', '-f', is_flag=True, help='Start fresh by clearing existing data')
@click.option('--output', '-o', type=click.Path(), help='Output results to JSON file')
def ascrape(base_url: str, max_depth: int, max_concurrency: int, start_fresh: bool, output: Optional[str]):
    """Scrape a website asynchronously."""
    click.echo(f"Starting asynchronous scrape of {base_url}")
    click.echo(f"Max depth: {max_depth}, Max concurrency: {max_concurrency}, Start fresh: {start_fresh}")

    async def _ascrape():
        try:
            # app = get_app()
            scraper = WebScraper(start_fresh=start_fresh)
            scraped_urls = await scraper.ascrape(base_url, max_depth, max_concurrency)

            click.echo(f"✅ Async scraping completed! Scraped {len(scraped_urls)} URLs")

            if output:
                # Get detailed results from database
                db_connector: ScrapedUrlDBConnector = app.container.db_connector()
                all_urls = db_connector.get_all_urls_with_status(base_url)

                results_data = []
                for url, source_url, depth, status, topic in all_urls:
                    results_data.append({
                        'base_url': base_url,
                        'url': url,
                        'source_url': source_url,
                        'depth': depth,
                        'status': status,
                        'topic': topic
                    })

                with open(output, 'w') as f:
                    json.dump(results_data, f, indent=2, ensure_ascii=False)
                click.echo(f"Results saved to {output}")

        except Exception as e:
            click.echo(f"❌ Error during async scraping: {e}", err=True)
            raise click.ClickException(str(e))

    asyncio.run(_ascrape())


@main.command()
@click.argument('base_url')
def status(base_url: str):
    """Check the scraping status for a base URL."""
    try:
        # app = get_app()
        db_connector: ScrapedUrlDBConnector = app.container.db_connector()

        all_urls = db_connector.get_all_urls_with_status(base_url)
        queued_urls = [r for r in all_urls if r[3] == "queued"]

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
        click.echo(f"❌ Error checking status: {e}", err=True)
        raise click.ClickException(str(e))


@main.command()
@click.argument('base_url')
@click.option('--output', '-o', type=click.Path(), help='Output results to JSON file')
@click.option('--format', 'output_format', type=click.Choice(['json', 'table']), default='table', help='Output format')
def results(base_url: str, output: Optional[str], output_format: str):
    """Get scraping results for a base URL."""
    try:
        # app = get_app()
        db_connector: ScrapedUrlDBConnector = app.container.db_connector()

        all_urls = db_connector.get_all_urls_with_status(base_url)

        if not all_urls:
            click.echo(f"No results found for {base_url}")
            return

        results_data = []
        for url, source_url, depth, status, topic in all_urls:
            results_data.append({
                'url': url,
                'source_url': source_url,
                'depth': depth,
                'status': status,
                'topic': topic
            })

        if output_format == 'json' or output:
            if output:
                with open(output, 'w') as f:
                    json.dump({
                        'base_url': base_url,
                        'results': results_data
                    }, f, indent=2, ensure_ascii=False)
                click.echo(f"Results saved to {output}")
            else:
                click.echo(json.dumps({
                    'base_url': base_url,
                    'results': results_data
                }, indent=2, ensure_ascii=False))
        else:
            # Table format
            click.echo(f"\nResults for {base_url}:")
            click.echo("-" * 100)
            click.echo(f"{'URL':<50} {'Status':<12} {'Topic':<15} {'Depth':<5}")
            click.echo("-" * 100)

            for result in results_data:
                url_display = result['url'][:47] + "..." if len(result['url']) > 50 else result['url']
                click.echo(f"{url_display:<50} {result['status']:<12} {result['topic']:<15} {result['depth']:<5}")

            click.echo(f"\nTotal: {len(results_data)} URLs")

    except Exception as e:
        click.echo(f"❌ Error getting results: {e}", err=True)
        raise click.ClickException(str(e))


@main.command()
@click.option('--host', default='127.0.0.1', help='Host to bind to (default: 127.0.0.1)')
@click.option('--port', default=8000, help='Port to bind to (default: 8000)')
@click.option('--reload', is_flag=True, help='Enable auto-reload for development')
def serve(host: str, port: int, reload: bool):
    """Start the FastAPI web server."""
    try:
        import uvicorn
        click.echo(f"Starting FastAPI server on {host}:{port}")
        if reload:
            click.echo("Auto-reload enabled")

        uvicorn.run(
            "arpeely_scraper.app.main:app",
            host=host,
            port=port,
            reload=reload
        )
    except ImportError:
        click.echo("❌ uvicorn is required to run the web server", err=True)
        raise click.ClickException("uvicorn not found")
    except Exception as e:
        click.echo(f"❌ Error starting server: {e}", err=True)
        raise click.ClickException(str(e))


if __name__ == '__main__':
    main()
