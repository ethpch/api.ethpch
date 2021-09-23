from importlib import import_module
from fastapi import FastAPI
from fastapi import responses
from utils.config import debug
from utils.database.session import Session
from utils.schedule import ConcurrencyScheduler
from utils.general import markdown_html
from constants import README, TODO

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

APP = FastAPI(
    title='API.Ethpch',
    description="Ethpch's personal API backend.",
    version='0.2.0',
    docs_url='/docs' if debug else None,
    redoc_url='/redoc' if debug else '/docs',
    on_startup=[Session.init, ConcurrencyScheduler.start_all],
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
