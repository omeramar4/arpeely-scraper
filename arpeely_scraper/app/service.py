from fastapi import FastAPI
import os
import logging
from sqlalchemy import text

from arpeely_scraper.db_connector.di_container import Container
from arpeely_scraper.db_connector.scraped_url_db_connector import ScrapedUrlDBConnector


class ScraperApp(FastAPI):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.container = self.__init_services()
        self.add_event_handler("startup", self._init_db_table)

    @staticmethod
    def __init_services() -> Container:
        container = Container()
        container.config.dbname.from_env('POSTGRES_DB', default='arpeely_db')
        container.config.user.from_env('POSTGRES_USER', default='arpeely_user')
        container.config.password.from_env('POSTGRES_PASSWORD', default='arpeely_pass')
        container.config.host.from_env('POSTGRES_HOST', default='localhost')
        container.config.port.from_env('POSTGRES_PORT', default=5432)
        container.wire(modules=[__name__, "arpeely_scraper.core.scraper"])
        return container

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
