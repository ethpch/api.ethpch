from typing import Literal, Optional, Dict
from types import ModuleType
from pathlib import Path
from importlib import import_module
from constants import APP_DIR
from utils.config import markdown_theme


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


def markdown_html(
    md_text: str,
    theme: Literal['amelia', 'cerulean', 'cyborg', 'journal', 'readable',
                   'simplex', 'slate', 'spacelab', 'spruce', 'superhero',
                   'united'] = markdown_theme,
) -> str:
    # markdown html use strapdown.js
    # see https://github.com/arturadib/strapdown
    return (
        f'<xmp theme="{theme}" style="display:none;">{md_text}</xmp>'
        '<script src="https://strapdownjs.com/v/0.2/strapdown.js"></script>')
