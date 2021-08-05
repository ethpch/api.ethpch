from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import RedirectResponse
from .shorturl import src_to_dst, dst_to_src

shorturl_router = APIRouter(prefix='/a')


@shorturl_router.get('/{shorturl}')
async def getshort(shorturl: str):
    url = await dst_to_src(short=shorturl)
    return RedirectResponse(url)


@shorturl_router.post('/{source:path}')
async def postshort(source: str, req: Request):
    return str(req.base_url) + await src_to_dst(source=source)
