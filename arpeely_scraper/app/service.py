from fastapi import FastAPI
import os
from sqlalchemy import text

from arpeely_scraper.core.di_container import Container
from arpeely_scraper.db_connector.scraped_url_db_connector import ScrapedUrlDBConnector


class ScraperApp(FastAPI):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.container = self.__init_services()
        self.add_event_handler("startup", self._init_db_table)
        self.add_event_handler("startup", self._init_topic_classifier)

    @staticmethod
    def __init_services() -> Container:
        container = Container()

        # Configure each config attribute individually
        container.config.dbname.from_env('POSTGRES_DB', default='arpeely_db')
        container.config.user.from_env('POSTGRES_USER', default='arpeely_user')
        container.config.password.from_env('POSTGRES_PASSWORD', default='arpeely_pass')
        container.config.host.from_env('POSTGRES_HOST', default='localhost')
        container.config.port.from_env('POSTGRES_PORT', default=5432)
        container.config.topic_model_name.from_env('TOPIC_MODEL_NAME', default='valhalla/distilbart-mnli-12-1')

        container.wire(modules=[__name__, "arpeely_scraper.core.scraper"])
        return container

    def _init_topic_classifier(self):
        """Initialize the topic classifier at startup to ensure the model is loaded once."""
        topic_classifier = self.container.topic_classifier()
        print("Topic classifier initialized successfully.")

    def _init_db_table(self):
        db_connector: ScrapedUrlDBConnector = self.container.db_connector()
        engine = db_connector.engine
        with engine.connect() as conn:
            sql_path = os.path.join(os.path.dirname(__file__), 'scripts/create_scraped_url_table.sql')
            with open(sql_path, 'r') as f:
                sql = f.read()
            for statement in sql.split(';'):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
            conn.commit()
            print("scraped_urls table creation SQL executed (idempotent).")
