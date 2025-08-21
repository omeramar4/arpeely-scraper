import unittest
from unittest.mock import MagicMock, patch
from arpeely_scraper.core.scraper import WebScraper


class TestWebScraper(unittest.TestCase):

    def setUp(self):
        # Mock db_connector and topic_classifier
        self.mock_db_connector = MagicMock()
        self.mock_topic_classifier = MagicMock()
        self.mock_topic_classifier.classify_topic.return_value = "other"
        self.mock_topic_classifier.classify_topic_async = MagicMock(return_value="other")

        # Patch requests.Session.get and BeautifulSoup
        patcher = patch('arpeely_scraper.core.scraper.requests.Session')
        self.addCleanup(patcher.stop)
        self.mock_session_class = patcher.start()
        self.mock_session = self.mock_session_class.return_value
        self.mock_response = MagicMock()
        self.mock_response.headers = {'content-type': 'text/html'}
        self.mock_response.content = b'<html><head><title>Test</title></head><body><p>Text</p><a href="http://test.com/page2">Link</a></body></html>'
        self.mock_response.text = '<html><head><title>Test</title></head><body><p>Text</p><a href="http://test.com/page2">Link</a></body></html>'
        self.mock_response.raise_for_status = MagicMock()
        self.mock_session.get.return_value = self.mock_response

    def test_scrape(self):
        # Simulate no previous state, start fresh
        self.mock_db_connector.get_queued_urls.return_value = []
        scraper = WebScraper(
            db_connector=self.mock_db_connector,
            topic_classifier=self.mock_topic_classifier,
            delay=0,
            start_fresh=True
        )
        result = scraper.scrape('http://test.com', max_depth=1)
        self.assertIn('http://test.com', result)
        self.mock_db_connector.upsert_scraped_url.assert_any_call(
            base_url='http://test.com',
            url='http://test.com',
            source_url=None,
            depth=0,
            title=None,
            links_to_texts={},
            status='queued'
        )
        self.mock_db_connector.upsert_scraped_url.assert_any_call(
            base_url='http://test.com',
            url='http://test.com',
            source_url=None,
            depth=0,
            title='Test',
            links_to_texts={'http://test.com/page2': 'Link'},
            topic='other',
            status='completed'
        )

    def test_scrape_recovery_after_crash(self):
        # Simulate crash after queuing URLs
        # First run: only queue URLs, do not complete scrape
        self.mock_db_connector.get_queued_urls.return_value = []
        scraper1 = WebScraper(
            db_connector=self.mock_db_connector,
            topic_classifier=self.mock_topic_classifier,
            delay=0,
            start_fresh=True
        )
        # Simulate that upsert_scraped_url only queues, not completes
        def upsert_scraped_url_side_effect(**kwargs):
            if kwargs['status'] == 'queued':
                # Simulate DB storing queued URL
                pass
        self.mock_db_connector.upsert_scraped_url.side_effect = upsert_scraped_url_side_effect
        # Simulate scraping only queues the initial URL
        scraper1.scrape('http://test.com', max_depth=1)
        # Now simulate app crash and restart
        # Second run: DB returns queued URLs
        self.mock_db_connector.get_queued_urls.return_value = [
            ('http://test.com', None, 0),
            ('http://test.com/page2', 'http://test.com', 1)
        ]
        # Remove side effect so upsert works normally
        self.mock_db_connector.upsert_scraped_url.side_effect = None
        scraper2 = WebScraper(
            db_connector=self.mock_db_connector,
            topic_classifier=self.mock_topic_classifier,
            delay=0,
            start_fresh=False
        )
        result = scraper2.scrape('http://test.com', max_depth=1)
        # Should process both URLs
        self.assertIn('http://test.com', result)
        self.assertIn('http://test.com/page2', result)
        self.mock_db_connector.get_queued_urls.assert_called_with('http://test.com')

    async def test_ascrape(self):
        self.mock_db_connector.get_queued_urls.return_value = []
        scraper = WebScraper(
            db_connector=self.mock_db_connector,
            topic_classifier=self.mock_topic_classifier,
            delay=0,
            start_fresh=True
        )
        result = await scraper.ascrape('http://test.com', max_depth=1)
        self.assertIn('http://test.com', result)
        self.mock_db_connector.upsert_scraped_url.assert_any_call(
            base_url='http://test.com',
            url='http://test.com',
            source_url=None,
            depth=0,
            title=None,
            links_to_texts={},
            status='queued'
        )
        self.mock_db_connector.upsert_scraped_url.assert_any_call(
            base_url='http://test.com',
            url='http://test.com',
            source_url=None,
            depth=0,
            title='Test',
            links_to_texts={'http://test.com/page2': 'Link'},
            topic='other',
            status='completed'
        )

    async def test_ascrape_recovery_after_crash(self):
        # Simulate crash after queuing URLs
        self.mock_db_connector.get_queued_urls.return_value = []
        scraper1 = WebScraper(
            db_connector=self.mock_db_connector,
            topic_classifier=self.mock_topic_classifier,
            delay=0,
            start_fresh=True
        )
        def upsert_scraped_url_side_effect(**kwargs):
            if kwargs['status'] == 'queued':
                pass
        self.mock_db_connector.upsert_scraped_url.side_effect = upsert_scraped_url_side_effect
        await scraper1.ascrape('http://test.com', max_depth=1)
        self.mock_db_connector.get_queued_urls.return_value = [
            ('http://test.com', None, 0),
            ('http://test.com/page2', 'http://test.com', 1)
        ]
        self.mock_db_connector.upsert_scraped_url.side_effect = None
        scraper2 = WebScraper(
            db_connector=self.mock_db_connector,
            topic_classifier=self.mock_topic_classifier,
            delay=0,
            start_fresh=False
        )
        result = await scraper2.ascrape('http://test.com', max_depth=1)
        self.assertIn('http://test.com', result)
        self.assertIn('http://test.com/page2', result)
        self.mock_db_connector.get_queued_urls.assert_called_with('http://test.com')
