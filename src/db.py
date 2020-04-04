import logging
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, Column, String, Float, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

logger = logging.getLogger(__name__)

Base = declarative_base()


class PriceTrack(Base):  # type: ignore
    __tablename__ = "price_track"
    store = Column(String, primary_key=True)
    product_id = Column(String, primary_key=True)
    product_name = Column(String, nullable=False)
    category_1 = Column(String)
    category_2 = Column(String)
    price = Column(Float, nullable=False)
    scrape_timestamp = Column(DateTime, primary_key=True)


class PostgresManager:
    """
    Manager class used to handle the Postgres database.
    It also interacts with SqlAlchemy in order to create session objects.
    """

    def __init__(self, uri: str):
        self.engine: Engine = create_engine(uri)
        self._session_maker: sessionmaker = sessionmaker(bind=self.engine)

    def initialize_schema(self):
        """
        Creates the schema defined through models in an empty database.
        """
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self, expunge_after_commit: bool = True) -> Iterator[Session]:
        """Provide a transactional scope around a series of operations."""
        session = self._session_maker()
        try:
            yield session
            session.commit()
            if expunge_after_commit:
                session.expunge_all()
        except Exception:
            session.rollback()
            logger.exception("")
            raise
        finally:
            session.close()


db = PostgresManager("postgresql://postgres:password@localhost:5432/price-tracking-dev")
db.initialize_schema()
