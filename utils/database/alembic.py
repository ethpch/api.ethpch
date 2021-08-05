import os
from hashlib import md5
from importlib import import_module
from typing import List
from alembic.config import main
from constants import APP_DIR, ALEMBIC_MIGRATION_PATH
from .session import DB_SETTING

if DB_SETTING['type'] == 'sqlite':
    _hash = md5('_'.join([DB_SETTING['type'],
                          DB_SETTING['schema']]).encode()).hexdigest()
else:
    _hash = md5('_'.join(
        [DB_SETTING['type'], DB_SETTING['host'],
         DB_SETTING['schema']]).encode()).hexdigest()


def call_alembic(args: List[str]):
    print(f'Connect hash is "{_hash}".')
    for p in APP_DIR.rglob('tables.py'):
        _skip = False
        for part in p.parts:
            if part.startswith('_') is True:
                _skip = True
                break
        if _skip is False:
            relpath = p.relative_to(APP_DIR)
            importpath = APP_DIR.name + '.' + ('.'.join(relpath.parts))[:-3]
            import_module(importpath)
    ALEMBIC_MIGRATION_PATH.mkdir(exist_ok=True)
    os.chdir(ALEMBIC_MIGRATION_PATH)
    args = list(args)
    remove_args = ('-c', '--config')
    for arg in remove_args:
        try:
            index = args.index(arg)
            args.pop(index)
            args.pop(index)
        except (ValueError, IndexError):
            pass
    options = ('branches', 'current', 'downgrade', 'edit', 'heads', 'history',
               'init', 'list_templates', 'merge', 'revision', 'show', 'stamp',
               'upgrade')
    for arg in options:
        try:
            index = args.index(arg)
            args.insert(index, '-c')
            args.insert(index + 1, _hash + '.ini')
            break
        except ValueError:
            pass
    main(argv=args, prog='api.ethpch alembic')


def alembic_init():
    call_alembic(['init', _hash, '-t', 'async'])
    env = ALEMBIC_MIGRATION_PATH / _hash / 'env.py'
    injected_py = []
    with open(env, mode='r') as f:
        for line in f.readlines():
            if line == 'target_metadata = None\n':
                injected_py.append('from utils.database import Base\n')
                line = 'target_metadata = Base.metadata\n'
            injected_py.append(line)
            if line == 'config = context.config\n':
                injected_py.append("""
# this will overwrite the ini-file sqlalchemy.url path
# with the path given in the config of the main code
from utils.database.session import Session, DB_SETTING
config.set_main_option('sqlalchemy.url', Session.create_url(**DB_SETTING))
""")
    env.write_text(''.join(injected_py))


def alembic_makemigrations():
    call_alembic(['revision', '--autogenerate'])


def alembic_migrate():
    call_alembic(['upgrade', 'head'])


__all__ = ('call_alembic', 'alembic_init', 'alembic_makemigrations',
           'alembic_migrate')
