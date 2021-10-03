from importlib import import_module
from fastapi import FastAPI, Request, File, UploadFile
from fastapi import responses
from fastapi.background import BackgroundTasks
from utils.config import debug
from utils.database.session import Session
from utils.schedule import ConcurrencyScheduler
from utils.general import markdown_html
from constants import ROOT_DIR, __version__, README, TODO

response_class_choices = {
    'orjson': responses.ORJSONResponse,
    'ujson': responses.UJSONResponse,
    'json': responses.JSONResponse,
}
for module, _class in response_class_choices.items():
    try:
        import_module(module)
    except ModuleNotFoundError:
        continue
    else:
        response_class = _class
        break


def add_reload():
    if hasattr(APP, '_config') and \
            getattr(APP._config, '_allow_reload', False) is True:
        APP.reload = APP._config._reload_event.set


APP = FastAPI(
    title='API.Ethpch',
    description="Ethpch's personal API backend.",
    version=__version__,
    docs_url='/docs' if debug else None,
    redoc_url='/redoc' if debug else '/docs',
    on_startup=[Session.init, ConcurrencyScheduler.start_all, add_reload],
    on_shutdown=[Session.shutdown, ConcurrencyScheduler.shutdown_all],
    debug=debug,
    default_response_class=response_class)


@APP.get('/')
async def hello():
    return responses.HTMLResponse(
        markdown_html(README.read_text(encoding='utf-8')))


@APP.get('/todo')
async def todo():
    try:
        todo = TODO.read_text(encoding='utf-8')
    except FileNotFoundError:
        todo = 'Nothing new in plan.'
    return responses.HTMLResponse(markdown_html(todo))


@APP.post('/todo', include_in_schema=False)
async def todo_update(md: UploadFile = File(...), req: Request = ...):
    if md.filename.endswith('.md'):
        try:
            import aiofiles
            async with aiofiles.open(ROOT_DIR / 'TODO.md',
                                     mode='w',
                                     encoding='utf-8') as f:
                await f.write((await md.read()).decode('utf-8'))
        except ModuleNotFoundError:
            with open(ROOT_DIR / 'TODO.md', mode='w', encoding='utf-8') as f:
                f.write((await md.read()).decode('utf-8'))
    return responses.RedirectResponse(req.url_for('todo'), status_code=303)


@APP.post('/reload', include_in_schema=False)
async def reload(tasks: BackgroundTasks):
    if hasattr(APP, 'reload'):
        if callable(APP.reload):
            tasks.add_task(APP.reload)
            return 'Reload success.'
    return 'Reload is not allowed.'


@APP.post('/update', include_in_schema=False)
async def update(req: Request):
    from utils.scripts.git import pull
    requirements = ROOT_DIR / 'requirements.txt'
    stat0 = requirements.stat().st_size
    pull()
    stat1 = requirements.stat().st_size
    if stat0 != stat1:
        from utils.scripts import run_subprocess
        run_subprocess(['pip', 'install', '-r', 'requirements.txt', '-U'])
    return responses.RedirectResponse(req.url_for('reload'))
