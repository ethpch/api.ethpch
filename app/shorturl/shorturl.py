import string
from utils.database.session import Session
from utils.database.crud import select
from .tables import Shorturl

digit62 = string.digits + string.ascii_letters


async def src_to_dst(source: str) -> str:
    async with Session() as session:
        async with session.begin():
            stmt = select(
                Shorturl,
                whereclauses=[Shorturl.source == str(source)],
                limit=1,
            )
            result = await session.execute(stmt)
            if dst := result.scalar():
                pass
            else:
                dst = Shorturl(source=source)
                session.add(dst)
                await session.commit()
            return int_to_str62(dst.id)


async def dst_to_src(short: str) -> str:
    async with Session() as session:
        async with session.begin():
            id = str62_to_int(short)
            stmt = select(
                Shorturl,
                whereclauses=[Shorturl.id == id],
                limit=1,
            )
            result = await session.execute(stmt)
            if src := result.scalar():
                return src.source
            else:
                return ''


def int_to_str62(id: int) -> str:
    s = ''
    x = id
    while x > 62:
        x1 = x % 62
        s = digit62[x1] + s
        x = x // 62
    if x > 0:
        s = digit62[x] + s
    return s


def str62_to_int(short: str) -> int:
    x = 0
    s = str(short)
    for ch in s:
        k = digit62.find(ch)
        if k >= 0:
            x = x * 62 + k
    return x
