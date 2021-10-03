""" api.ethpch
Ethpch's personal API backend.
"""
import sys
import logging
from argparse import ArgumentParser
from functools import partial
from constants import __version__

logger = logging.getLogger('api_ethpch')
logger.propagate = False
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | '
    '%(module)s.%(funcName)s:%(lineno)s | %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def runserver(debug: bool = False, allow_reload: bool = False, **kwargs):
    from utils import config
    if debug is True:
        setattr(config, 'debug', True)
    from utils.server import run
    run(allow_reload=allow_reload)


def install():
    from platform import system
    from utils.scripts import run_subprocess
    run_subprocess(['pip', 'install', '-U', '-r', 'requirements.txt'])
    from utils.config import asgi_framework, database
    run_subprocess(['pip', 'install', '-U', database.driver])
    tips = 'Install api.ethpch accomplished. '
    if system() == 'Linux':
        install_systemd_unit()
        tips += f'Service {asgi_framework} starts serving.'
    else:
        tips += 'Call "python main.py runserver" to start serving.'
    logger.info(tips)


def install_systemd_unit(force: bool = False, **kwargs):
    from utils.scripts import systemd
    try:
        systemd.create_systemd_unit(force_install=force)
        systemd.enable_systemd_unit()
        systemd.start_service()
        logger.info('Install systemd unit accomplished.')
    except AttributeError:
        logger.error('Systemd only works on Linux!')


def uninstall_systemd_unit():
    from utils.scripts import systemd
    try:
        systemd.stop_service()
        systemd.disable_systemd_unit()
        logger.info('Uninstall systemd unit accomplished.')
    except AttributeError:
        logger.error('Systemd only works on Linux!')


def update(force: bool = False, **kwargs):
    from utils.scripts.git import pull
    from constants import ROOT_DIR
    requirements = ROOT_DIR / 'requirements.txt'
    stat0 = requirements.stat().st_size
    pull(force=force)
    stat1 = requirements.stat().st_size
    if stat0 != stat1:
        from utils.scripts import run_subprocess
        run_subprocess(['pip', 'install', '-r', 'requirements.txt', '-U'])
    logger.info('Update source code accomplished.')


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


def main(argv: list = []):
    parser = ArgumentParser(
        prog='api.ethpch',
        description='api.ethpch launcher',
    )
    parser.add_argument(
        '-v',
        '--version',
        action='version',
        help='show api version and exit',
        version='%(prog)s ' + __version__,
    )
    subparsers = parser.add_subparsers(dest='command')
    subparsers.add_parser('alembic', add_help=False)
    subparsers.add_parser('init')
    subparsers.add_parser('install')
    is_u = subparsers.add_parser('install-systemd')
    is_u.add_argument('-f', '--force', action='store_true', dest='force')
    subparsers.add_parser('makemigrations')
    subparsers.add_parser('migrate')
    rs_u = subparsers.add_parser('runserver')
    rs_u.add_argument('--debug', action='store_true', dest='debug')
    rs_u.add_argument('--allow_reload',
                      action='store_true',
                      dest='allow_reload')
    subparsers.add_parser('uninstall-systemd')
    sp_u = subparsers.add_parser('update')
    sp_u.add_argument('-f', '--force', action='store_true', dest='force')
    args, other_args = parser.parse_known_args(argv or sys.argv[1:])
    commanddict = {
        'alembic': partial(alembic, 0, *other_args),
        'init': partial(alembic, 1),
        'install': install,
        'install-systemd': partial(install_systemd_unit, **vars(args)),
        'makemigrations': partial(alembic, 2),
        'migrate': partial(alembic, 3),
        'runserver': partial(runserver, **vars(args)),
        'uninstall-systemd': uninstall_systemd_unit,
        'update': partial(update, **vars(args))
    }
    try:
        commanddict[args.command]()
    except KeyError:
        parser.print_help()


if __name__ == '__main__':
    main()
