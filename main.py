""" api.ethpch
Ethpch's personal API backend.
"""
from argparse import ArgumentParser
from functools import partial
from importlib import import_module


def runserver():
    from utils.config import asgi_framework
    module = import_module(f'utils.server.{asgi_framework}')
    module.run()


def update(force: bool = False, **kwargs):
    from utils.scripts.git import pull
    pull(force=force)


def alembic(mode: int, *args):
    from utils.database import alembic
    if mode == 0:
        alembic.call_alembic(args)
    elif mode == 1:
        alembic.alembic_init()
    elif mode == 2:
        alembic.alembic_makemigrations()
    elif mode == 3:
        alembic.alembic_migrate()


def main():
    parser = ArgumentParser(prog='api.ethpch',
                            description='api.ethpch launcher')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.add_parser('alembic', add_help=False)
    subparsers.add_parser('init')
    subparsers.add_parser('makemigrations')
    subparsers.add_parser('migrate')
    subparsers.add_parser('runserver')
    sp_u = subparsers.add_parser('update')
    sp_u.add_argument('-f', '--force', action='store_true', dest='force')
    args, other_args = parser.parse_known_args()
    from utils.log import setup_main_logger, init_logging, add_stdout
    setup_main_logger()
    init_logging()
    add_stdout()
    commanddict = {
        'alembic': partial(alembic, 0, *other_args),
        'init': partial(alembic, 1),
        'makemigrations': partial(alembic, 2),
        'migrate': partial(alembic, 3),
        'runserver': runserver,
        'update': partial(update, **vars(args))
    }
    try:
        commanddict[args.command]()
    except KeyError:
        parser.print_help()


if __name__ == '__main__':
    main()
