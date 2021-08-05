import os
from logging import getLogger
from typing import Literal, Sequence, Set, Union
from sqlalchemy import select, or_, orm, exc
from sqlalchemy.sql.selectable import FromClause, Select
from sqlalchemy.ext.asyncio import AsyncEngine, \
    create_async_engine, AsyncSession
from sqlalchemy.sql.visitors import Visitable
from constants import HOME_DIR
from utils.config import database

DB_SETTING = dict(
    type=database.type,
    driver=database.driver,
    host=database.host,
    port=database.port,
    user=database.user,
    password=database.password,
    schema=database.db_schema,
    dsn=database.dsn,
)

logger = getLogger('api_ethpch')

_short_mapping = {
    'sqlite': 'sqlite',
    'pgsql': 'postgresql',
    'mysql': 'mysql',
    'oracle': 'oracle',
    'mssql': 'mssql',
}


class Session(object):
    __url: str = None
    __engine: AsyncEngine = None
    __session: AsyncSession = None
    __engine_extra_args = {}

    @classmethod
    def create_url(cls, type: str, **kwargs) -> str:
        Session.__engine_extra_args.clear()
        url = _short_mapping[type]
        if type == 'sqlite':
            driver = kwargs['driver']
            if driver:
                url += f'+{driver}'
            url += ':///'
            schema = kwargs['schema']
            if schema == ':memory:':
                url += ':memory:'
            else:
                if schema.endswith('.db') is False:
                    schema += '.db'
                if os.path.isabs(schema):
                    url += schema
                else:
                    url += str(HOME_DIR / schema)
        elif type in ('pgsql', 'mysql', 'oracle', 'mssql'):
            driver = kwargs['driver']
            host = kwargs['host']
            port = kwargs['port']
            user = kwargs['user']
            password = kwargs['password']
            schema = kwargs['schema']
            if driver:
                url += f'+{driver}'
            url += f'://{user}:{password}@{host}:{port}/{schema}'
        else:
            raise NotImplementedError('Database type is unsupported.')
        if url:
            # add optional arguments
            if type == 'sqlite':
                url += '?check_same_thread=true'
                # foreign key support
                # see https://stackoverflow.com/a/2615603/13829771
                import sqlite3
                if sqlite3.sqlite_version_info >= (3, 6, 19):
                    from sqlalchemy import event
                    from sqlalchemy.engine import Engine

                    @event.listens_for(Engine, 'connect')
                    def set_sqlite_pragma(dbapi_connection, connection_record):
                        cursor = dbapi_connection.cursor()
                        cursor.execute("PRAGMA foreign_keys=ON")
                        cursor.close()

            elif type == 'pgsql':
                if driver in ('psycopg2', 'pg8000'):
                    Session.__engine_extra_args.update(
                        dict(client_encoding='utf8'))
            elif type == 'mysql':
                url += '?charset=utf8mb4'
                if driver in ('mysqldb', 'pymysql'):
                    url += '&binary_prefix=true'
            elif type == 'oracle':
                url += '?encoding=UTF-8&nencoding=UTF-8'
            elif type == 'msserver':
                if driver == 'pyodbc':
                    import pyodbc
                    pyodbc.pooling = False
                dsn_str = '&'.join(DB_SETTING['dsn']).replace(' ', '+')
                url += '?driver=' + dsn_str
            return url
        else:
            return ''

    @classmethod
    def get_url(cls) -> str:
        if cls.__url is None:
            raise RuntimeError(
                'Url is uninitialized. Call "Session.init()" first.')
        return cls.__url

    @classmethod
    def get_engine(cls) -> AsyncEngine:
        if cls.__engine is None:
            raise RuntimeError(
                'Engine is uninitialized. Call "Session.init()" first.')
        return cls.__engine

    @classmethod
    def get_session(cls) -> AsyncSession:
        if cls.__session is None:
            raise RuntimeError(
                'Session is uninitialized. Call "Session.init()" first.')
        return cls.__session

    async def __aenter__(self) -> AsyncSession:
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if all((exc_type, exc_val, exc_tb)):
            if issubclass(exc_type, exc.DataError):
                logger.info(f'SQLAlchemy invalid request. \n{exc_val}')
            elif issubclass(exc_type, exc.SQLAlchemyError):
                logger.error(f'SQLAlchemy error occurred. \n{exc_val}')
            await self.session.rollback()
        await self.session.close()

    @classmethod
    def init(cls):
        cls.__url = cls.create_url(**DB_SETTING)
        cls.__engine = create_async_engine(cls.__url,
                                           pool_recycle=3600,
                                           **cls.__engine_extra_args)
        cls.__session = orm.sessionmaker(bind=cls.__engine,
                                         expire_on_commit=False,
                                         class_=AsyncSession,
                                         future=True)
        logger.info('SQLAlchemy startup complete.')

    @classmethod
    async def reset(cls):
        orm.close_all_sessions()
        del cls.__session
        cls.__session = None
        await cls.__engine.dispose()
        del cls.__engine
        cls.__engine = None
        cls.__engine_extra_args.clear()
        cls.__url = None
        raise RuntimeWarning('Session data is dropped. '
                             'Call "Session.init()" before reusing it.')

    @classmethod
    async def shutdown(cls):
        await cls.reset()
        logger.info('SQLAlchemy shutdown complete.')

    def __init__(self, *args, **kwargs):
        try:
            self.session = type(self).get_session()(*args, **kwargs)
        except RuntimeError:
            Session.init()
            self.session = type(self).get_session()(*args, **kwargs)

    @staticmethod
    def select_constructor(
        *table_or_column,
        eagerloads: Union[Sequence[str], Set[str]] = [],
        eagerload_strategy: Literal['joinedload', 'subqueryload',
                                    'selectinload'] = None,
        joins: Sequence[FromClause] = [],
        whereclauses: Sequence[Union[str, bool, Visitable,
                                     Sequence[Union[str, bool,
                                                    Visitable]]]] = [],
        select_from: FromClause = None,
        order_by: Union[str, bool, Visitable, None] = None,
        limit: Union[int, str, Visitable, None] = None,
        offset: Union[int, str, Visitable, None] = None,
    ) -> Select:
        statement = select(table_or_column)
        eagerloads = list(set(eagerloads))
        if eagerloads:
            eagerload_strategy = eagerload_strategy or 'joinedload'
            eagerload_func = getattr(orm, eagerload_strategy)
            for eagerload in eagerloads:
                if '.' in eagerload:
                    main = eagerload.split('.')
                    eager = eagerload_func(main[0])
                    for item in main[1:]:
                        eager = getattr(eager, eagerload_strategy)(item)
                    statement = statement.options(eagerload_func(eager))
                else:
                    statement = statement.options(eagerload_func(eagerload))
        for join in joins:
            statement = statement.join(join)
        for i in range(len(whereclauses)):
            if isinstance(whereclauses[i], (list, tuple)):
                whereclauses[i] = or_(*whereclauses[i])
        statement = statement.where(*whereclauses)
        if select_from is not None:
            statement = statement.select_from(select_from)
        if order_by is not None:
            statement = statement.order_by(order_by)
        if limit is not None:
            statement = statement.limit(limit)
        if offset is not None:
            statement = statement.offset(offset)
        return statement


__all__ = ('DB_SETTING', 'Session')
