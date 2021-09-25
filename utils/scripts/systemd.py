import platform

__all__ = ()

if platform.system() == 'Linux':
    from pathlib import Path
    from constants import ROOT_DIR, VENV_DIR, SYSTEMD_DIR
    from utils.config import asgi_framework
    from . import run_subprocess

    def create_systemd_unit(venv: Path = VENV_DIR,
                            force_install: bool = False):
        exec_cmd = f'{venv / "bin/python"} {ROOT_DIR / "main.py"} runserver'
        template = ('[Unit]'
                    f'Description={asgi_framework}'
                    '[Service]'
                    'TimeoutSec=3'
                    f'WorkingDirectory={ROOT_DIR}'
                    f'ExecStart={exec_cmd}'
                    'Restart=on-failure'
                    f'ExecReload={exec_cmd}'
                    'RestartSec=3'
                    '[Install]'
                    'WantedBy=multi-user.target')
        unit = SYSTEMD_DIR / f'{asgi_framework}.service'
        if unit.exists() is False or force_install is True:
            unit.write_text(template, encoding='utf-8')
            run_subprocess(['systemctl', 'daemon-reload'])

    def enable_systemd_unit():
        run_subprocess(['systemctl', 'enable', asgi_framework])

    def start_service():
        run_subprocess(['service', asgi_framework, 'start'])

    def disable_systemd_unit():
        run_subprocess(['systemctl', 'disable', asgi_framework])

    def stop_service():
        run_subprocess(['service', asgi_framework, 'stop'])

    __all__ = ('create_systemd_unit', 'enable_systemd_unit', 'start_service',
               'disable_systemd_unit', 'stop_service')
