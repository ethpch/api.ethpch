""" api.ethpch
Ethpch's personal API backend.
"""
import os
from argparse import ArgumentParser
from functools import partial
from utils.config import server, debug


def runserver():
    from utils import uvicorn
    kws = dict(host=server.host, port=server.port)
    if server.ssl.enable is True:
        kws.update(server.ssl.dict(exclude='enable'))
    uvicorn.run(
        'app.base:APP',
        debug=debug,
        workers=os.cpu_count(),
        **kws,
    )


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
    args, other_args = parser.parse_known_args()
    commanddict = {
        'alembic': partial(alembic, 0, *other_args),
        'init': partial(alembic, 1),
        'makemigrations': partial(alembic, 2),
        'migrate': partial(alembic, 3),
        'runserver': runserver
    }
    try:
        commanddict[args.command]()
    except KeyError:
        parser.print_help()


if __name__ == '__main__':
    main()
