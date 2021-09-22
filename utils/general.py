from typing import Optional, Dict
from types import ModuleType
from pathlib import Path
from importlib import import_module
from constants import APP_DIR


def importer(target: str,
             scope: Path = APP_DIR) -> Optional[Dict[str, ModuleType]]:
    if target.endswith('.py') is False:
        target += '.py'
    try:
        scope.relative_to(APP_DIR)
    except ValueError:
        return
    imported = {}
    for p in scope.rglob(target):
        _skip = False
        for part in p.parts:
            if part.startswith('_') is True:
                _skip = True
                break
        if _skip is False:
            relpath = p.relative_to(APP_DIR)
            importpath = scope.name + '.' + ('.'.join(relpath.parts))[:-3]
            imported[importpath] = import_module(importpath)
    return imported
