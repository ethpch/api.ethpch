from fastapi import FastAPI
from fastapi.responses import HTMLResponse
try:
    import orjson
    from fastapi.responses import ORJSONResponse
    response_class = ORJSONResponse
except ModuleNotFoundError:
    try:
        import ujson
        from fastapi.responses import UJSONResponse
        response_class = UJSONResponse
    except ModuleNotFoundError:
        from fastapi.responses import JSONResponse
        response_class = JSONResponse
from markdown import markdown
from utils.config import debug
from utils.database.session import Session
from utils.schedule import ConcurrencyScheduler
from utils.schedule.apscheduler import scheduler
from constants import README, TODO

APP = FastAPI(
    title='API.Ethpch',
    description="Ethpch's personal API backend.",
    version='0.2.0',
    docs_url='/docs' if debug else None,
    redoc_url='/redoc' if debug else '/docs',
    on_startup=[Session.init, ConcurrencyScheduler.start_all, scheduler.start],
    on_shutdown=[
        Session.shutdown, ConcurrencyScheduler.shutdown_all, scheduler.shutdown
    ],
    debug=debug,
    default_response_class=response_class)


@APP.get('/')
async def hello():
    return HTMLResponse(markdown(README.read_text(encoding='utf-8')))


@APP.get('/todo')
async def todo():
    return HTMLResponse(markdown(TODO.read_text(encoding='utf-8')))
