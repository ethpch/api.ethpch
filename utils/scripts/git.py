from constants import ROOT_DIR
from . import run_subprocess


def pull(branch: str = 'master', force: bool = False):
    git_dir = rf'--git-dir={ROOT_DIR / ".git"}'
    if force is True:
        commands = [
            [
                'git', git_dir, 'fetch', '--update-head-ok', 'origin',
                f'+{branch}:{branch}'
            ],
            ['git', git_dir, 'checkout', branch],
            ['git', git_dir, 'reset', '--hard', f'origin/{branch}'],
        ]
    else:
        commands = [
            [
                'git', git_dir, 'pull', '--ff-only', 'origin',
                f'{branch}:{branch}'
            ],
            ['git', git_dir, 'checkout', branch],
        ]
    return [run_subprocess(command) for command in commands]


def push(branch: str = 'master'):
    git_dir = rf'--git-dir={ROOT_DIR / ".git"}'
    commands = [['git', git_dir, 'push', 'origin', f'{branch}:{branch}']]
    return [run_subprocess(command) for command in commands]
