from pathlib import Path

ROOT_DIR = Path('.').resolve()
README = ROOT_DIR / 'README.md'
TODO = ROOT_DIR / 'TODO.md'
CONFIG_FILE_PATH = ROOT_DIR / 'config.yaml'
APP_DIR = ROOT_DIR / 'app'
UTILS_DIR = ROOT_DIR / 'utils'
HOME_DIR = Path.home() / '.api_ethpch'

LOG_DIR = HOME_DIR / 'log'
LOG_CONFIG_PATH = UTILS_DIR / 'log' / 'log_config.yaml'

ALEMBIC_MIGRATION_PATH = UTILS_DIR / 'database' / 'migrations'
