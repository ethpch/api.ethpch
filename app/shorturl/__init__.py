from fastapi import APIRouter, status
from fastapi.requests import Request
from fastapi.responses import RedirectResponse
from pydantic import HttpUrl
from .shorturl import src_to_dst, dst_to_src

shorturl_router = APIRouter(prefix='/a', tags=['shorturl'])


@shorturl_router.get('/{shorturl}')
async def getshort(shorturl: str):
    url = await dst_to_src(short=shorturl)
    return RedirectResponse(url, status_code=status.HTTP_301_MOVED_PERMANENTLY)


@shorturl_router.post('/{source:path}')
async def postshort(source: HttpUrl, req: Request):
    return str(req.base_url) + 'a/' + await src_to_dst(source=source)
