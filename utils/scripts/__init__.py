from logging import getLogger
from subprocess import run, CompletedProcess, PIPE, STDOUT
from typing import List

logger = getLogger('api_ethpch')


def run_subprocess(cmd: List[str], **kwargs) -> CompletedProcess:
    to_pop = ('stdout', 'stderr', 'encoding')
    for i in to_pop:
        kwargs.pop(i, None)
    cp = run(cmd, stdout=PIPE, stderr=STDOUT, encoding='utf-8', **kwargs)
    logger.info(f'Calling subprocess:\n$ {" ".join(cmd)}\n{cp.stdout}')
    return cp
