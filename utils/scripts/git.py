from constants import ROOT_DIR, GIT_SOURCE
from . import run_subprocess


def pull(branch: str = 'master',
         source: str = GIT_SOURCE,
         force: bool = False):
    git_dir = rf'--git-dir={ROOT_DIR / ".git"}'
    if force is True:
        commands = [
            ['git', git_dir, 'fetch', '-u', source, f'+{branch}:{branch}'],
            ['git', git_dir, 'checkout', branch],
            ['git', git_dir, 'reset', '--hard', f'origin/{branch}'],
        ]
    else:
        commands = [
            [
                'git', git_dir, 'pull', '--ff-only', source,
                f'{branch}:{branch}'
            ],
            ['git', git_dir, 'checkout', branch],
        ]
    return [run_subprocess(command) for command in commands]


def push(branch: str = 'master', source: str = GIT_SOURCE):
    git_dir = rf'--git-dir={ROOT_DIR / ".git"}'
    commands = [['git', git_dir, 'push', source, f'{branch}:{branch}']]
    return [run_subprocess(command) for command in commands]
