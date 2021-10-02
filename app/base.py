from importlib import import_module
from fastapi import FastAPI
from fastapi import responses
from fastapi.background import BackgroundTasks
from starlette.requests import Request
from utils.config import debug
from utils.database.session import Session
from utils.schedule import ConcurrencyScheduler
from utils.general import markdown_html
from constants import __version__, README, TODO

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
    pull()
    return responses.RedirectResponse(req.url_for('/reload'))
