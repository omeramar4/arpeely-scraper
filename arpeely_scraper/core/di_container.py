from dependency_injector import containers, providers
from arpeely_scraper.db_connector.scraped_url_db_connector import ScrapedUrlDBConnector
from arpeely_scraper.models.topic_classifier import TopicClassifier


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    # Topic classifier singleton - loaded once at startup with model from config
    topic_classifier = providers.Singleton(
        TopicClassifier,
        model_name=config.topic_model_name
    )

    db_connector = providers.Singleton(
        ScrapedUrlDBConnector,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port
    )
