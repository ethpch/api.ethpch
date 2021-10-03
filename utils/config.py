import os
from typing import List, Optional, Literal, Union, Any
from pathlib import Path
from yaml import safe_load
from pydantic import BaseModel, Field, AnyUrl, HttpUrl, constr, IPvAnyAddress
from constants import CONFIG_FILE_PATH

__all__ = [
    'debug', 'server', 'asgi_framework', 'markdown_theme', 's3', 'database',
    'apps'
]

DomainUrl = constr(
    regex=r'((?=[a-z0-9-]{1,63}\.)(xn--)?[a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,63}'
)


class SocksUrl(AnyUrl):
    allowed_schemes = {'socks4', 'socks5'}


class serverModel(BaseModel):
    host: str
    port: int

    class sslModel(BaseModel):
        enable: bool
        ssl_certfile: Optional[str] = None
        ssl_keyfile: Optional[str] = None
        ssl_keyfile_password: Optional[str] = None
        ssl_version: Optional[str] = None
        ssl_ciphers: Optional[str] = None

        def __init__(__pydantic_self__, **data: Any) -> None:
            if data.get('enable') is None:
                data['enable'] = False
            if data.get('ssl_certfile'):
                data['ssl_certfile'] = Path(data['ssl_certfile']).read_text()
            if data.get('ssl_keyfile'):
                data['ssl_keyfile'] = Path(data['ssl_keyfile']).read_text()
            super().__init__(**data)

    ssl: Optional[sslModel] = None

    def __init__(__pydantic_self__, **data: Any) -> None:
        if data.get('host') is None:
            data['host'] = '127.0.0.1'
        if data.get('port') is None:
            data['port'] = 8000
        super().__init__(**data)


class s3Model(BaseModel):
    endpoint_url: HttpUrl = 'https://s3.amazonaws.com'
    aws_access_key_id: Optional[str] = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key: Optional[str] = os.getenv('AWS_SECRET_ACCESS_KEY')
    api_bucket: Optional[str] = None

    def __init__(__pydantic_self__, **data: Any) -> None:
        if data.get('endpoint_url') is None:
            data['endpoint_url'] = 'https://s3.amazonaws.com'
        super().__init__(**data)


_database_default_mapping = {
    'sqlite': {
        'driver': 'aiosqlite',
        'port': None,
    },
    'pgsql': {
        'driver': 'asyncpg',
        'port': 5432
    },
    'mysql': {
        'driver': 'asyncmy',
        'port': 3306
    },
    'oracle': {
        'driver': None,
        'port': 1521
    },
    'mssql': {
        'driver': None,
        'port': 1433
    },
}


class databaseModel(BaseModel):
    type: Literal['sqlite', 'pgsql', 'mysql', 'oracle', 'mssql'] = 'sqlite'
    driver: str
    host: Union[IPvAnyAddress, DomainUrl] = '127.0.0.1'
    port: Optional[int] = Field(..., ge=1024, le=49151)
    user: str = 'user'
    password: str = 'password'
    db_schema: str = Field('api.ethpch', alias='schema')
    dsn: List[str] = []

    def __init__(__pydantic_self__, **data: Any) -> None:
        try:
            data['driver'] = data.get('driver') or \
                _database_default_mapping[data['type']]['driver']
            data['port'] = data.get('port') or \
                _database_default_mapping[data['type']]['port']
        except KeyError:  # database type is invalid
            super().__init__(**data)  # raise pydantic error
        data['host'] = data.get('host') or '127.0.0.1'
        data['user'] = data.get('user') or 'user'
        data['password'] = data.get('password') or 'password'
        data['schema'] = data.get('schema') or 'api.ethpch'
        data['dsn'] = data.get('dsn') or []
        super().__init__(**data)


class pixivModel(BaseModel):
    refresh_token: str
    proxy: Union[HttpUrl, SocksUrl, None] = None
    bypass: Optional[bool] = False
    transfer: Optional[bool] = False


CONFIG_TEMPLATE = """server:
  host:
  port:
  ssl:
    enable: false
    ssl_certfile:
    ssl_keyfile:
    ssl_keyfile_password:
    ssl_version:
    ssl_ciphers:

# core config
# ASGI Framework, options: uvicorn
asgi framework: uvicorn

allow reload: false

# markdown theme, options:
# amelia, cerulean, cyborg, journal, readable, simplex,
# slate, spacelab, spruce, superhero, united
# defaults to united
markdown theme:

s3:
  endpoint_url:
  aws_access_key_id:
  aws_secret_access_key:
  api_bucket:

database:
  # type options: sqlite, pgsql, mysql, oracle, mssql
  # see https://docs.sqlalchemy.org/en/14/dialects/index.html
  type: sqlite

  # drivers help
  # PostgreSQL: defaults to asyncpg
  # MySQL and Mariadb: defaults to aiomysql
  # SQLite: defaults to aiosqlite
  # Oracle: no defaults
  # Microsoft SQL Server: no defaults
  # stay blank to use default driver
  # async driver required
  driver:
  # when using "sqlite", the following items except "schema" are useless
  host:
  port:
  user:
  password:
  schema: api.ethpch
  # required for odbc driver
  dsn: [

  ]

enable_apps: [
    pixiv,
    shorturl,
]

# apps config
pixiv:
  # see https://gist.github.com/ZipFile/c9ebedb224406f4f11845ab700124362
  # or see https://gist.github.com/upbit/6edda27cb1644e94183291109b8a5fde
  refresh_token:
  # support socks5/socks4/http (not support https)
  proxy:
  # bypass option
  bypass:
  # transfer to storage
  transfer: false
"""

try:
    RAW_CONFIG = safe_load((CONFIG_FILE_PATH).read_text())
except FileNotFoundError:
    print(f'No config file found. Generate new config "{CONFIG_FILE_PATH}", '
          'please edit it.')
    CONFIG_FILE_PATH.write_text(CONFIG_TEMPLATE)
    os._exit(0)
else:
    if os.getenv('API_ETHPCH_RUNNING_MODE', 'prod') == 'dev':
        debug = True
    else:
        debug = False
    server = serverModel(**RAW_CONFIG['server'])
    asgi_framework: str = RAW_CONFIG.get('asgi framework', 'uvicorn')
    allow_reload: bool = RAW_CONFIG.get('allow reload', False)
    markdown_theme: str = RAW_CONFIG.get('markdown theme', 'united')
    s3 = s3Model(**RAW_CONFIG['s3'])
    database = databaseModel(**RAW_CONFIG['database'])
    apps = RAW_CONFIG['enable_apps']
    if 'pixiv' in apps:
        pixiv = pixivModel(**RAW_CONFIG['pixiv'])
        __all__.append('pixiv')
