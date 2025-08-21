import asyncio
import logging
import time
from typing import Set, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
import requests
from bs4 import BeautifulSoup
from dependency_injector.wiring import inject, Provide

from arpeely_scraper.core.di_container import Container


UrlToProcessType = Tuple[str, Optional[str], int]  # (url, source_url, depth)


class WebScraper:

    @inject
    def __init__(
            self,
            db_connector=Provide[Container.db_connector],
            topic_classifier=Provide[Container.topic_classifier],
            delay: float = 1.0, timeout: int = 10,
            start_fresh: bool = False
    ):
        """
        Initialize the web scraper.

        Args:
            db_connector: Instance of ScrapedUrlDBConnector (injected)
            topic_classifier: Instance of TopicClassifier (injected)
            delay: Delay between requests in seconds
            timeout: Request timeout in seconds
        """
        self.db_connector = db_connector
        self.topic_classifier = topic_classifier
        self.delay = delay
        self.timeout = timeout
        self.visited_urls: Set[str] = set()
        self.scraped_data: Set[str] = set()
        self.start_fresh = start_fresh

        self.session_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def scrape(self, initial_url: str, max_depth: int) -> Set[str]:
        """
        Main scraping function that recursively crawls pages (synchronous version).

        Args:
            initial_url: Starting URL
            max_depth: Maximum depth to crawl

        Returns:
            Dictionary containing all scraped data
        """

        self._validate_input(initial_url, max_depth)
        self.logger.info(f"Starting scrape from {initial_url} with max depth {max_depth}")
        session = requests.Session()
        session.headers.update(self.session_headers)

        urls_to_process = self._recover_previous_state(initial_url)

        while urls_to_process:
            current_url, source_url, current_depth = urls_to_process.pop(0)

            if current_url in self.visited_urls or current_depth > max_depth:
                continue

            self._insert_to_db_as_queued(initial_url=initial_url, url=current_url, source_url=source_url, depth=current_depth)

            self.logger.info(f"Scraping: {current_url} (depth: {current_depth})")
            self.visited_urls.add(current_url)
            soup = self._get_page_content(current_url, session=session)

            if soup is not None:
                page_text = self._extract_page_text(soup)
                topic = self.topic_classifier.classify_topic(page_text)
                links_to_texts = self._extract_links_with_text(soup, base_url=current_url)

                self._insert_completed_scrape_to_db(
                    initial_url=initial_url, url=current_url, source_url=source_url, soup=soup,
                    depth=current_depth, links_to_texts=links_to_texts, topic=topic
                )

                self.scraped_data.add(current_url)

                if current_depth < max_depth:
                    for link_url in links_to_texts.keys():
                        if link_url not in self.visited_urls:
                            urls_to_process.append((link_url, current_url, current_depth + 1))
            else:
                self.db_connector.update_status(
                    base_url=initial_url,
                    url=current_url,
                    status="completed"
                )

            if self.delay > 0:
                time.sleep(self.delay)

        self.logger.info(f"Scraping completed. Visited {len(self.visited_urls)} URLs")
        return self.scraped_data

    async def ascrape(self, initial_url: str, max_depth: int, max_concurrency: int = 10) -> Set[str]:
        """
        Main concurrent scraping function that recursively crawls pages.

        Args:
            initial_url: Starting URL
            max_depth: Maximum depth to crawl
            max_concurrency: Maximum number of concurrent requests
        Returns:
            Dictionary containing all scraped data
        """
        self._validate_input(initial_url, max_depth)
        self.logger.info(f"Starting concurrent scrape from {initial_url} with max depth {max_depth}")

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrency)
        visited_lock = asyncio.Lock()
        scraped_lock = asyncio.Lock()

        # Create aiohttp session
        connector = aiohttp.TCPConnector(limit=max_concurrency * 2, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        ) as session:
            urls_to_process = self._recover_previous_state(initial_url)

            # Process URLs level by level to maintain depth control
            for current_depth in range(max_depth + 1):
                # Get URLs for current depth
                current_level_urls = [
                    (url, source_url, depth) for url, source_url, depth in urls_to_process
                    if depth == current_depth
                ]

                if not current_level_urls:
                    continue

                self.logger.info(f"Processing {len(current_level_urls)} URLs at depth {current_depth}")

                # Process current level concurrently
                tasks = []
                for url, source_url, depth in current_level_urls:
                    if url not in self.visited_urls:
                        task = asyncio.create_task(
                            self._scrape_url_concurrent(
                                session=session, semaphore=semaphore,
                                visited_lock=visited_lock, scraped_lock=scraped_lock,
                                initial_url=initial_url, url=url, source_url=source_url, depth=depth
                            )
                        )
                        tasks.append(task)

                # Wait for all tasks at current level to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Collect new URLs for next level
                next_level_urls = []
                for result in results:
                        next_level_urls.extend(result)
                urls_to_process.extend(next_level_urls)

        self.logger.info(f"Concurrent scraping completed. Visited {len(self.visited_urls)} URLs")
        return self.scraped_data

    def _validate_input(self, initial_url: str, max_depth: int):
        if max_depth < 0:
            raise ValueError("Max depth must be >= 0")
        if not self._is_valid_url(initial_url):
            raise ValueError(f"Invalid initial URL: {initial_url}")

    def _recover_previous_state(self, initial_url: str) -> List[UrlToProcessType]:
        """
        Recover previous scraping state from the database.

        :param initial_url:
        :return:
        """
        urls_to_process: List[Tuple[str, Optional[str], int]]
        if not self.start_fresh:
            queued_records = self.db_connector.get_queued_urls(initial_url)
            if queued_records:
                self.logger.info(f"Resuming from {len(queued_records)} queued URLs for base_url {initial_url}")
                urls_to_process = [(url, source_url, depth) for url, source_url, depth in queued_records]
            else:
                self.logger.info(f"No queued URLs found for base_url {initial_url}, starting fresh.")
                urls_to_process = [(initial_url, None, 0)]
        else:
            urls_to_process = [(initial_url, None, 0)]

        return urls_to_process

    async def _scrape_url_concurrent(
            self,
            session: aiohttp.ClientSession,
            semaphore: asyncio.Semaphore,
            visited_lock: asyncio.Lock,
            scraped_lock: asyncio.Lock,
            initial_url: str,
            url: str,
            source_url: Optional[str],
            depth: int
    ) -> List[Tuple[str, str, int]]:
        """
        Scrape a single URL concurrently.

        Returns:
            List of new URLs to process at the next depth level
        """
        async with semaphore:
            # Check if already visited (with lock)
            async with visited_lock:
                if url in self.visited_urls:
                    return []
                self.visited_urls.add(url)

            # Insert as queued before scraping
            self._insert_to_db_as_queued(initial_url=initial_url, url=url, source_url=source_url, depth=depth)

            self.logger.info(f"Scraping: {url} (depth: {depth})")
            try:
                # Add delay to be respectful to servers
                if self.delay > 0:
                    await asyncio.sleep(self.delay)

                soup = await self._get_page_content_async(session, url)

                if soup is not None:
                    page_text = self._extract_page_text(soup)
                    topic = await self.topic_classifier.classify_topic_async(page_text)
                    links_to_texts = self._extract_links_with_text(soup, base_url=url)

                    # Store scraped data (with lock)
                    async with scraped_lock:
                        self.scraped_data.add(url)

                    # Update with complete data including topic
                    self._insert_completed_scrape_to_db(
                        initial_url=initial_url, url=url, source_url=source_url, soup=soup,
                        depth=depth, links_to_texts=links_to_texts, topic=topic
                    )

                    # Return new URLs for next level
                    return [(link_url, url, depth + 1) for link_url in links_to_texts.keys()]
                else:
                    # Update status to completed even if scraping failed
                    self.db_connector.update_status(
                        base_url=self.visited_urls.__iter__().__next__(),
                        url=url,
                        status="completed"
                    )
                    return []

            except Exception as e:
                self.logger.error(f"Error scraping {url}: {e}")
                self.db_connector.update_status(
                    base_url=self.visited_urls.__iter__().__next__(),
                    url=url,
                    status="completed"
                )
                return []

    async def _get_page_content_async(self, session: aiohttp.ClientSession, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse page content asynchronously.

        Args:
            session: aiohttp session
            url: URL to scrape

        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            async with session.get(url) as response:
                response.raise_for_status()

                content_type = response.headers.get('content-type', '')
                if 'text/html' not in content_type.lower():
                    self.logger.warning(f"Skipping non-HTML content: {url}")
                    return None

                content = await response.text()
                return BeautifulSoup(content, 'html.parser')

        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None

    def _insert_to_db_as_queued(self, initial_url: str, url: str, source_url: Optional[str], depth: int):
        """
        Insert a URL into the database as queued.

        Args:
            initial_url: Base URL being scraped
            url: URL to insert
            source_url: Source URL that led to this URL (optional)
            depth: Crawl depth
        """
        self.db_connector.upsert_scraped_url(
            base_url=initial_url,
            url=url,
            source_url=source_url,
            depth=depth,
            title=None,
            links_to_texts={},
            status="queued"
        )

    def _insert_completed_scrape_to_db(
            self,
            initial_url: str,
            url: str,
            source_url: Optional[str],
            depth: int,
            soup: BeautifulSoup,
            links_to_texts: Dict[str, str],
            topic: str
    ):
        """
        Insert a completed scrape result into the database.

        Args:
            initial_url: Base URL being scraped
            url: URL that was scraped
            source_url: Source URL that led to this URL (optional)
            depth: Crawl depth
            title: Page title
            links_to_texts: Dictionary of links and their text
            topic: Classified topic of the page
        """
        self.db_connector.upsert_scraped_url(
            base_url=initial_url,
            url=url,
            source_url=source_url,
            depth=depth,
            title=self._get_title(soup),
            links_to_texts=links_to_texts,
            topic=topic,
            status="completed"
        )

    def _extract_links_with_text(self, soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
        """
        Extract all links from a BeautifulSoup object with their text.

        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative links

        Returns:
            List of tuples (absolute_url, link_text)
        """
        links = {}

        # Find all anchor tags with href attribute
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()

            # Skip empty links, javascript, mailto, etc.
            if not href or href.startswith(('#', 'javascript:', 'mailto:')):
                continue

            # Convert relative URLs to absolute
            absolute_url = urljoin(base_url, href)

            # Validate URL
            if self._is_valid_url(absolute_url):
                # Get link text (clean it up)
                link_text = link.get_text(strip=True)
                if not link_text:
                    # If no text, try to get alt text from images or use URL
                    img = link.find('img')
                    if img and img.get('alt'):
                        link_text = img.get('alt').strip()
                    else:
                        link_text = absolute_url

                links[absolute_url] = link_text

        return links

    def _get_page_content(self, url: str, session: requests.Session) -> Optional[BeautifulSoup]:
        """
        Fetch and parse page content.

        Args:
            url: URL to scrape

        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            response = session.get(url, timeout=self.timeout)
            response.raise_for_status()

            content_type = response.headers.get('content-type', '')
            if 'text/html' not in content_type.lower():
                self.logger.warning(f"Skipping non-HTML content: {url}")
                return None

            return BeautifulSoup(response.content, 'html.parser')

        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None

    @staticmethod
    def _get_title(soup: BeautifulSoup) -> str:
        title = soup.find('title')
        if title:
            return title.get_text().strip()
        return ''

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """Check if URL is valid and accessible."""
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)

    @staticmethod
    def _extract_page_text(soup: BeautifulSoup) -> str:
        """
        Extract visible text content from a BeautifulSoup object.

        Args:
            soup: BeautifulSoup object

        Returns:
            Cleaned and concatenated string of visible text
        """
        # Get text from all paragraph tags
        texts = [p.get_text(strip=True) for p in soup.find_all('p')]

        # Join all texts with a space separator
        return ' '.join(texts)
