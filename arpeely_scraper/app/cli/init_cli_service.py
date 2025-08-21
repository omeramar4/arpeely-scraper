import logging
from arpeely_scraper.app.service import ScraperApp


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cli-init")


if __name__ == "__main__":
    logger.info("Initializing ScraperApp and loading topic classifier model...")
    container = ScraperApp.init_services()
    container.topic_classifier()
    logger.info("Topic classifier model loaded and ready.")
