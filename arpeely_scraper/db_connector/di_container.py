from dependency_injector import containers, providers
from arpeely_scraper.db_connector.scraped_url_db_connector import ScrapedUrlDBConnector

class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    db_connector = providers.Singleton(
        ScrapedUrlDBConnector,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port
    )
