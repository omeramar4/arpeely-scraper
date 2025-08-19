from sqlalchemy import create_engine, Column, String, Integer, Enum, JSON, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
import threading
import enum


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
    status = Column(Enum(UrlStatusEnum), nullable=False)


class ScrapedUrlDBConnector:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}",
            pool_size=10, max_overflow=20, pool_pre_ping=True
        )
        self.Session = sessionmaker(bind=self.engine)
        # Create tables if not exist
        if not inspect(self.engine).has_table("scraped_urls"):
            Base.metadata.create_all(self.engine)

    def upsert_scraped_url(self, base_url, url, source_url, depth, title, links_to_texts, status):
        session = self.Session()
        try:
            obj = session.query(ScrapedUrl).filter_by(base_url=base_url, url=url).first()
            if obj:
                obj.source_url = source_url
                obj.depth = depth
                obj.title = title
                obj.links_to_texts = links_to_texts
                obj.status = UrlStatusEnum(status)
            else:
                obj = ScrapedUrl(
                    base_url=base_url,
                    url=url,
                    source_url=source_url,
                    depth=depth,
                    title=title,
                    links_to_texts=links_to_texts,
                    status=UrlStatusEnum(status)
                )
                session.add(obj)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def update_status(self, base_url, url, status):
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

    def get_queued_urls(self, base_url):
        session = self.Session()
        try:
            records = session.query(ScrapedUrl).filter_by(base_url=base_url, status=UrlStatusEnum.queued).all()
            return [(r.url, r.source_url, r.depth) for r in records]
        finally:
            session.close()

    def get_results(self, base_url):
        session = self.Session()
        try:
            records = session.query(ScrapedUrl).filter_by(base_url=base_url).all()
            return records
        finally:
            session.close()

    def get_all_urls_with_status(self, base_url):
        session = self.Session()
        try:
            records = session.query(ScrapedUrl).filter_by(base_url=base_url).all()
            return [(r.url, r.source_url, r.depth, r.status.value if hasattr(r.status, 'value') else str(r.status)) for r in records]
        finally:
            session.close()

    def close(self):
        self.engine.dispose()
