from typing import Optional, Dict, List, Tuple

from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text
from sqlalchemy import create_engine, Column, String, Integer, Enum, JSON, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
import enum

from arpeely_scraper.utils.dataclasses import UrlToProcess, UrlProcessingResult


Base = declarative_base()


class UrlStatusEnum(enum.Enum):
    queued = "queued"
    completed = "completed"


class ScrapedUrl(Base):
    __tablename__ = "scraped_urls"
    base_url = Column(String, primary_key=True)
    url = Column(String, primary_key=True)
    source_url = Column(String, nullable=True)
    depth = Column(Integer, nullable=False)
    title = Column(String)
    links_to_texts = Column(JSON)
    topic = Column(String, default="other")
    status = Column(Enum(UrlStatusEnum), nullable=False)


class ScrapedUrlDBConnector:
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: int = 5432):
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}",
            pool_size=10, max_overflow=20, pool_pre_ping=True
        )
        self.Session = sessionmaker(bind=self.engine)
        # Create tables if not exist
        if not inspect(self.engine).has_table("scraped_urls"):
            Base.metadata.create_all(self.engine)
        self._create_url_status_enum_type()

    def upsert_scraped_url(self,
                           base_url: str,
                           url: str,
                           source_url: Optional[str],
                           depth: int,
                           title: Optional[str],
                           links_to_texts: Dict[str, str],
                           status: str,
                           topic: str = "other"):
        session = self.Session()
        try:
            obj = session.query(ScrapedUrl).filter_by(base_url=base_url, url=url).first()
            if obj:
                obj.source_url = source_url
                obj.depth = depth
                obj.title = title
                obj.links_to_texts = links_to_texts
                obj.topic = topic
                obj.status = UrlStatusEnum(status)
            else:
                obj = ScrapedUrl(
                    base_url=base_url,
                    url=url,
                    source_url=source_url,
                    depth=depth,
                    title=title,
                    links_to_texts=links_to_texts,
                    topic=topic,
                    status=UrlStatusEnum(status)
                )
                session.add(obj)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def update_status(self, base_url: str, url: str, status: str):
        session = self.Session()
        try:
            obj = session.query(ScrapedUrl).filter_by(base_url=base_url, url=url).first()
            if obj:
                obj.status = UrlStatusEnum(status)
                session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def update_topic(self, base_url: str, url: str, topic: str):
        session = self.Session()
        try:
            obj = session.query(ScrapedUrl).filter_by(base_url=base_url, url=url).first()
            if obj:
                obj.topic = topic
                session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_queued_urls(self, base_url: str) -> List[UrlToProcess]:
        session = self.Session()
        try:
            records = session.query(ScrapedUrl).filter_by(base_url=base_url, status=UrlStatusEnum.queued).all()
            return [
                UrlToProcess(
                    url=str(r.url),
                    source_url=str(r.source_url) if r.source_url is not None else None,
                    depth=int(r.depth)
                )
                for r in records
            ]
        finally:
            session.close()

    def get_results(self, base_url: str):
        session = self.Session()
        try:
            records = session.query(ScrapedUrl).filter_by(base_url=base_url).all()
            return records
        finally:
            session.close()

    def get_all_urls_with_status(self, base_url: str) -> List[UrlProcessingResult]:
        session = self.Session()
        try:
            records = session.query(ScrapedUrl).filter_by(base_url=base_url).all()
            return [
                UrlProcessingResult(
                    url=str(r.url),
                    source_url=str(r.source_url) if r.source_url is not None else None,
                    depth=int(r.depth),
                    status=r.status.value if hasattr(r.status, 'value') else str(r.status),
                    topic=str(r.topic)
                )
                for r in records
            ]
        finally:
            session.close()

    def _create_url_status_enum_type(self):
        session = self.Session()
        try:
            session.execute(text("CREATE TYPE url_status_enum AS ENUM ('processing', 'queued', 'completed');"))
        except ProgrammingError:
            pass

    def close(self):
        self.engine.dispose()
