from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import settings


def make_engine(url):
    kwargs = {'pool_pre_ping': True}
    if url.startswith('sqlite'):
        kwargs['connect_args'] = {'check_same_thread': False}
    return create_engine(url.replace('postgres://', 'postgresql+psycopg://', 1)
                         .replace('postgresql://', 'postgresql+psycopg://', 1), **kwargs)


engine = make_engine(settings.database_url)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


def get_session():
    with SessionLocal() as session:
        yield session
