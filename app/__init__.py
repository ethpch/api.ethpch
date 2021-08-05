from .base import APP
from utils.config import apps

if 'pixiv' in apps:
    from .pixiv import pixiv_router
    APP.include_router(pixiv_router)

if 'shorturl' in apps:
    from .shorturl import shorturl_router
    APP.include_router(shorturl_router)
