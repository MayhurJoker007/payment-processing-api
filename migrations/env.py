import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.models import Base

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
if os.environ.get('DATABASE_URL'):
    url = os.environ['DATABASE_URL']
    for old in ('postgres://', 'postgresql://'):
        if url.startswith(old):
            url = 'postgresql+psycopg://' + url[len(old):]
    config.set_main_option('sqlalchemy.url', url)

target_metadata = Base.metadata


def run_migrations_offline():
    context.configure(url=config.get_main_option('sqlalchemy.url'), target_metadata=target_metadata,
                      literal_binds=True, render_as_batch=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    engine = engine_from_config(config.get_section(config.config_ini_section), prefix='sqlalchemy.', poolclass=pool.NullPool)
    with engine.connect() as connection:
        # batch mode so ALTERs work on SQLite too
        context.configure(connection=connection, target_metadata=target_metadata, render_as_batch=True)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
