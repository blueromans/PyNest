from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, Dict

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker, declarative_base

from nest.core.database.orm_config import AsyncConfigFactory, ConfigFactory

Base = declarative_base()


class BaseOrmProvider(ABC):
    def __init__(
            self,
            db_type: str = "postgresql",
            config_params: dict = None,
            **kwargs,
    ):
        """
        Initializes the BaseOrmProvider instance.

        Args:
            db_type (str): The type of database. Defaults to "postgresql".
            config_params (dict): Configuration parameters for the database.
            async_mode (bool): Flag to indicate if the provider is asynchronous.
        """
        self.Base = Base

        config_factory = ConfigFactory

        engine_function = create_engine
        if "engine_params" in kwargs:
            engine_params: Dict[str, Any] = kwargs.pop("engine_params")
        else:
            engine_params = {}

        session_function = sessionmaker
        if "session_params" in kwargs:
            session_params: Dict[str, Any] = kwargs.pop("session_params")
        else:
            session_params = {}

        self.config = config_factory(db_type=db_type).get_config()
        self.config_url = self.config(**config_params).get_engine_url()
        self.engine = engine_function(self.config_url, **engine_params)
        self.session = session_function(self.engine, **session_params)

    @abstractmethod
    def create_all(self):
        pass

    @abstractmethod
    def drop_all(self):
        pass

    @abstractmethod
    def get_db(self):
        pass


class OrmProvider(BaseOrmProvider):
    """
    Synchronous ORM provider.
    """

    def __init__(self, db_type: str = "postgresql", config_params: dict = None):
        super().__init__(db_type=db_type, config_params=config_params)

    def create_all(self):
        self.Base.metadata.create_all(bind=self.engine)

    def drop_all(self):
        self.Base.metadata.drop_all(bind=self.engine)

    def get_db(self) -> Session:
        db = self.session()
        try:
            return db
        except Exception as e:
            raise e
        finally:
            db.close()
