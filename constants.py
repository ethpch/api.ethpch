from pathlib import Path, PosixPath
import platform

__version__ = '0.2.2'

ROOT_DIR = Path('.').resolve()
VENV_DIR = ROOT_DIR / 'venv'
README = ROOT_DIR / 'README.md'
TODO = ROOT_DIR / 'TODO.md'
CONFIG_FILE_PATH = ROOT_DIR / 'config.yaml'
APP_DIR = ROOT_DIR / 'app'
ASGI = 'app.base', 'APP'
UTILS_DIR = ROOT_DIR / 'utils'
HOME_DIR = Path.home() / '.api_ethpch'

LOG_DIR = HOME_DIR / 'log'
LOG_CONFIG_PATH = UTILS_DIR / 'log' / 'log_config.yaml'

ALEMBIC_MIGRATION_PATH = UTILS_DIR / 'database' / 'migrations'

GIT_SOURCE = 'https://github.com/ethpch/api.ethpch.git'

if platform.system() == 'Linux':
    SYSTEMD_DIR = PosixPath('/etc/systemd/system/')
