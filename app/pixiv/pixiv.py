import logging
from functools import wraps
from os import PathLike, path
from io import BytesIO
from random import choice, choices
from datetime import datetime, timedelta, timezone, time
from typing import List, Dict, Any, Literal, Optional, Union
from pixivpy_async import AppPixivAPI
from pixivpy_async import error
from sqlalchemy.sql.functions import func
from sqlalchemy.orm.relationships import RelationshipProperty
from utils.config import pixiv, debug
from utils.database.session import Session
from utils.database.crud import select
from utils.schedule import ConcurrencyScheduler
from utils.storage import s3
from . import tables

try:
    import imageio
except ModuleNotFoundError:
    pass  # NameError if not installed while calling


def catch_pixiv_error(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        redo = True
        trycount = 0
        if func.__annotations__['return']._name == 'List':
            data = []
        elif type(None) in func.__annotations__['return'].__args__:
            data = None
        else:
            data = None  # construct others
        while redo is True:
            try:
                data = await func(*args, **kwargs)
            except (error.NoLoginError, error.TokenError, error.NoTokenError):
                await Pixiv.login()
                trycount += 1
            except ResponseError as e:
                if ('OAuth' in e.message or 'Access Token' in e.message) \
                        and trycount <= 1:
                    await Pixiv.login()
                else:
                    logging.getLogger('api_ethpch').error(
                        f'Response error occurred. Message: "{e}"')
                    redo = False
                trycount += 1
            else:
                redo = False
        return data

    return wrapper


scheduler = ConcurrencyScheduler('pixiv', limit=5)


class ResponseError(error.PixivError):
    def __init__(
        self,
        **response_body,
    ):
        self._reason = 'API returns error message, '
        if len(response_body) == 3:
            self.message: str = response_body['message']
            self.body: list = response_body['body']
            if self.message:
                self._reason += f'message={self.message}, '
        else:
            self.user_message: str = response_body['error']['user_message']
            self.message: str = response_body['error']['message']
            self.reason: str = response_body['error']['reason']
            self.user_message_details: list = response_body['error'][
                'user_message_details']
            if self.user_message:
                self._reason += f'user_message={self.user_message}, '
            if self.message:
                self._reason += f'message={self.message}, '
            if self.reason:
                self._reason += f'reason={self.reason}, '
            if self.user_message_details:
                s = ', '.join([
                    f"{k}: {v}" for k, v in self.user_message_details.items()
                ])
                self._reason += f'user_message_details="{s}", '
        self._reason = self._reason[:-2] + '.'
        super().__init__()

    def __str__(self) -> str:
        return str(self._reason)


class AppPixivAPI(AppPixivAPI):
    async def requests_(self, *args, **kwargs):
        result = await super().requests_(*args, **kwargs)
        if 'error' in result.keys() and result['error']:
            raise ResponseError(**result)
        else:
            return result


class Pixiv(object):
    DEBUG = debug
    TOKEN = pixiv.refresh_token
    PROXY = pixiv.proxy
    BYPASS = pixiv.bypass
    TRANSFER = pixiv.transfer

    if PROXY:
        app = AppPixivAPI(proxy=PROXY)
    elif BYPASS:
        app = AppPixivAPI(bypass=True)
    else:
        app = AppPixivAPI(env=True)

    app.set_accept_language('zh-cn')

    SELF_USER_ID = None

    RESULT_LIMIT = 30

    def __init__(self) -> None:
        self.raw_data = None
        self.db_session = None
        self.downloads: List[tables.PixivStorage] = []
        self._temp = None
        self.urls: Dict[str, str] = {}
        self.users: Dict[int, tables.User] = {}
        self.illusts: Dict[int, tables.Illust] = {}
        self._ugoira_frames: Dict[int, list] = {}
        self._novel_next: Dict[int, int] = {}
        self.novels: Dict[int, tables.Novel] = {}
        self.tags: Dict[str, tables.Tag] = {}
        self.comments: Dict[int, tables.IllustComment] = {}
        self.showcases: Dict[int, tables.Showcase] = {}

    @classmethod
    async def login(cls):
        await cls.app.login(refresh_token=cls.TOKEN)
        cls.SELF_USER_ID = int(cls.app.user_id)

    def background_download(self):
        self.downloads = set(self.downloads)
        if self.DEBUG is False and self.TRANSFER:
            for obj in self.downloads:
                scheduler.add_job(self.transfer_storage, args=(obj.id, ))
        self.downloads = []

    async def transfer_storage(self, storage_id: int):
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.PixivStorage,
                    whereclauses=[tables.PixivStorage.id == storage_id])
                result = await session.execute(stmt)
                if obj := result.scalar():
                    if obj.source and obj.useable is False:
                        dst = f'/{s3.API_BUCKET}/pixiv/' + \
                            path.basename(obj.source)
                        if obj._illust_u_id:
                            try:
                                imageio
                                dst = dst[:-4] + '.gif'
                            except NameError:
                                pass
                        if await s3.has(dst):
                            obj.url = await s3.url(dst)
                            obj.useable = True
                            await session.commit()
                        else:
                            _ = await Pixiv.app.down(
                                _url=obj.source,
                                _referer='https://app-api.pixiv.net/',
                                _request_content_type=False)
                            content = await _.__anext__()
                            await _.aclose()
                            if obj._illust_u_id:
                                try:
                                    frames = self._ugoira_frames[
                                        obj._illust_u_id]
                                except KeyError:
                                    frames = (await self.ugoira_metadata(
                                        obj._illust_u_id))['frames']
                                content = self._zip_to_gif(
                                    BytesIO(content), frames)
                            if await s3.put(content,
                                            dst,
                                            replace=False,
                                            public_read=True):
                                obj.url = await s3.url(dst)
                                obj.useable = True
                                await session.commit()
                            del content

    @staticmethod
    def _zip_to_gif(
        file: Union[str, PathLike[str], BytesIO],
        frames: List[Dict[str, Any]],
    ) -> bytes:
        from zipfile import ZipFile
        images = []
        durations = []
        with ZipFile(file, 'r') as zf:
            for i in range(len(frames)):
                durations.append(frames[i].delay / 1000)
                with zf.open(frames[i].file) as f:
                    images.append(imageio.imread(f.read()))
        output = BytesIO()
        imageio.mimsave(output, images, format='GIF', duration=durations)
        data = output.getvalue()
        del output
        return data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.background_download()

    @catch_pixiv_error
    async def user_detail(self, user_id: int) -> Optional[tables.User]:
        self.raw_data = await self.app.user_detail(user_id=user_id)
        self._temp = self.raw_data
        userdata = self._userdata_constructor(full=True)
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                user = await self._main_user_check(
                    userdata['id'],
                    'background_image',
                    **userdata,
                )
                self.users[user.id] = user
            self.db_session = None
            await session.commit()
        return user

    async def user_detail_local(
        self,
        user_id: int,
    ) -> Optional[tables.User]:
        async with Session() as session:
            async with session.begin():
                stmt = select(tables.User,
                              eagerloads=['background_image'],
                              whereclauses=[tables.User.id == user_id],
                              limit=1)
                result = await session.execute(stmt)
                user = result.scalar()
                if user.profile and user.profile.useable is False:
                    self.downloads.append(user.profile)
                if user.background_image and user.background_image. \
                        useable is False:
                    self.downloads.append(user.background)
                return user

    @catch_pixiv_error
    async def user_illusts(
        self,
        user_id: int,
        type: Literal['illust', 'manga'] = 'illust',
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        self.raw_data = await self.app.user_illusts(
            user_id=user_id,
            type=type,
            offset=offset,
        )
        if not self.raw_data.illusts:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                self._temp = self.raw_data.illusts[0].user
                userdata = self._userdata_constructor(full=False)
                await self._main_user_check(userdata['id'], **userdata)
                all_tags = {}
                all_illusts = {}
                for illust in self.raw_data.illusts:
                    all_tags.update({tag.name: tag for tag in illust.tags})
                    all_illusts[illust.id] = illust
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
            self.db_session = None
            await session.commit()
        return sorted(
            self.illusts.values(),
            key=lambda i: i.create_date,
            reverse=True,
        )

    async def user_illusts_local(
        self,
        user_id: int,
        type: Literal['illust', 'manga'] = 'illust',
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        async with Session() as session:
            async with session.begin():
                stmt = select(tables.Illust,
                              whereclauses=[
                                  tables.Illust.user_id == user_id,
                                  tables.Illust.type == type
                              ],
                              order_by=tables.Illust.create_date.desc(),
                              limit=Pixiv.RESULT_LIMIT,
                              offset=offset)
                result = await session.execute(stmt)
                illusts = result.scalars().unique().all()
                self.downloads.extend([
                    stor for illust in illusts if illust._original
                    for stor in illust._original if stor.useable is False
                ])
                self.downloads.extend([
                    illust.ugoira for illust in illusts
                    if illust.type == 'ugoira' and illust.ugoira
                    and illust.ugoira.useable is False
                ])
                return illusts

    @catch_pixiv_error
    async def user_bookmarks_illust(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        self.raw_data = await self.app.user_bookmarks_illust(user_id=user_id)
        if not self.raw_data.illusts:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_illusts = {}
                for illust in self.raw_data.illusts:
                    all_users[illust.user.id] = illust.user
                    all_tags.update({tag.name: tag for tag in illust.tags})
                    all_illusts[illust.id] = illust
                muser = await self._main_user_check(user_id=user_id)
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(
                    all_illusts,
                    bookmarked_by=[muser],
                )
            self.db_session = None
            await session.commit()
        return sorted(
            self.illusts.values(),
            key=lambda i: i.create_date,
            reverse=True,
        )

    async def user_bookmarks_illust_local(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.Illust,
                    eagerloads=['bookmarked_by'],
                    joins=[tables.Illust.bookmarked_by],
                    whereclauses=[tables.User.id == user_id],
                    order_by=tables.Illust.create_date.desc(),
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                illusts = result.scalars().unique().all()
                self.downloads.extend([
                    stor for illust in illusts if illust._original
                    for stor in illust._original if stor.useable is False
                ])
                self.downloads.extend([
                    illust.ugoira for illust in illusts
                    if illust.type == 'ugoira' and illust.ugoira
                    and illust.ugoira.useable is False
                ])
                return illusts

    @catch_pixiv_error
    async def user_related(
        self,
        seed_user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        self.raw_data = await self.app.user_related(
            seed_user_id=seed_user_id,
            offset=offset,
        )
        if not self.raw_data.user_previews:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_illusts = {}
                all_novels = {}
                all_tags = {}
                for item in self.raw_data.user_previews:
                    all_users[item.user.id] = item.user
                    all_illusts.update({_.id: _ for _ in item.illusts})
                    all_novels.update({_.id: _ for _ in item.novels})
                    all_tags.update({
                        tag.name: tag
                        for _ in (item.illusts + item.novels) for tag in _.tags
                    })
                await self._users_into_db(all_users, 'illusts', 'novels')
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return [
            self.users[item.user.id] for item in self.raw_data.user_previews
        ]

    async def user_related_local(
        self,
        seed_user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        return []

    @catch_pixiv_error
    async def illust_follow(
        self,
        restrict: Literal['public', 'private'] = 'public',
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        self.raw_data = await self.app.illust_follow(
            restrict=restrict,
            offset=offset,
        )
        if not self.raw_data.illusts:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_illusts = {}
                for illust in self.raw_data.illusts:
                    all_users[illust.user.id] = illust.user
                    all_tags.update({tag.name: tag for tag in illust.tags})
                    all_illusts[illust.id] = illust
                await self._users_into_db(all_users, 'following')
                await self._main_user_check(
                    self.SELF_USER_ID,
                    following=[*self.users.values()],
                )
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
            self.db_session = None
            await session.commit()
        return sorted(
            self.illusts.values(),
            key=lambda i: i.create_date,
            reverse=True,
        )

    async def illust_follow_local(
        self,
        restrict: Literal['public', 'private'] = 'public',
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.Illust,
                    joins=[tables.Illust.user],
                    whereclauses=[
                        tables.User.followers.any(
                            tables.User.id == self.SELF_USER_ID)
                    ],
                    order_by=tables.Illust.create_date.desc(),
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                illusts = result.scalars().unique().all()
                self.downloads.extend([
                    stor for illust in illusts if illust._original
                    for stor in illust._original if stor.useable is False
                ])
                self.downloads.extend([
                    illust.ugoira for illust in illusts
                    if illust.type == 'ugoira' and illust.ugoira
                    and illust.ugoira.useable is False
                ])
                return illusts

    @catch_pixiv_error
    async def illust_detail(
        self,
        illust_id: int,
    ) -> Optional[tables.Illust]:
        self.raw_data = await self.app.illust_detail(illust_id=illust_id)
        self._temp = self.raw_data.illust
        illustdata = self._illustdata_constructor()
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_tags = {tag.name: tag for tag in self.raw_data.illust.tags}
                await self._tags_into_db(all_tags)
                self._temp = self.raw_data.illust.user
                userdata = self._userdata_constructor()
                await self._main_user_check(user_id=userdata['id'], **userdata)
                illust = await self._main_illust_check(
                    illustdata['id'],
                    'square_medium',
                    'medium',
                    'large',
                    eagerload_strategy='selectinload',
                    **illustdata,
                )
                self.illusts[illust.id] = illust
            self.db_session = None
            await session.commit()
        return illust

    async def illust_detail_local(
        self,
        illust_id: int,
    ) -> Optional[tables.Illust]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.Illust,
                    eagerloads=['square_medium', 'medium', 'large'],
                    eagerload_strategy='selectinload',
                    whereclauses=[tables.Illust.id == illust_id],
                    limit=1,
                )
                result = await session.execute(stmt)
                illust = result.scalar()
                if illust:
                    if illust._original:
                        self.downloads.extend([
                            stor for stor in illust._original
                            if stor.useable is False
                        ])
                    if illust.type == 'ugoira' and illust.ugoira and \
                            illust.ugoira.useable is False:
                        await self.ugoira_metadata(illust_id)
                        self.downloads.append(illust.ugoira)
                return illust

    @catch_pixiv_error
    async def illust_comments(
        self,
        illust_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.IllustComment]:
        self.raw_data = await self.app.illust_comments(
            illust_id=illust_id,
            offset=offset,
        )
        if not self.raw_data.comments:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_comments = {}
                all_users = {}
                for comment in self.raw_data.comments:
                    all_comments[comment.id] = comment
                    all_users[comment.user.id] = comment.user
                await self._main_illust_check(illust_id=illust_id)
                await self._users_into_db(all_users)
                await self._comments_into_db(all_comments, illust_id, 'user')
            self.db_session = None
            await session.commit()
        return sorted(
            self.comments.values(),
            key=lambda i: i.date,
            reverse=True,
        )

    async def illust_comments_local(
        self,
        illust_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.IllustComment]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.IllustComment,
                    eagerloads=['user'],
                    whereclauses=[tables.IllustComment.illust_id == illust_id],
                    order_by=tables.IllustComment.date.desc(),
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                return result.scalars().unique().all()

    @catch_pixiv_error
    async def illust_related(
        self,
        illust_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        self.raw_data = await self.app.illust_related(
            illust_id=illust_id,
            offset=offset,
        )
        if not self.raw_data.illusts:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_illusts = {}
                for illust in self.raw_data.illusts:
                    all_users[illust.user.id] = illust.user
                    all_tags.update({tag.name: tag for tag in illust.tags})
                    all_illusts[illust.id] = illust
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
            self.db_session = None
            await session.commit()
        return [self.illusts[illust.id] for illust in self.raw_data.illusts]

    async def illust_related_local(
        self,
        illust_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        return []

    @catch_pixiv_error
    async def illust_recommended(
        self,
        content_type: Literal['illust', 'manga'] = 'illust',
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        self.raw_data = await self.app.illust_recommended(
            content_type=content_type,
            offset=offset,
        )
        if not self.raw_data.illusts:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_illusts = {}
                for illust in self.raw_data.illusts:
                    all_users[illust.user.id] = illust.user
                    all_tags.update({tag.name: tag for tag in illust.tags})
                    all_illusts[illust.id] = illust
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
            self.db_session = None
            await session.commit()
        return [self.illusts[illust.id] for illust in self.raw_data.illusts]

    async def illust_recommended_local(
        self,
        content_type: Literal['illust', 'manga'] = 'illust',
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        return []

    @catch_pixiv_error
    async def illust_ranking(
        self,
        mode: Literal['day', 'week', 'month', 'day_male', 'day_female',
                      'week_original', 'week_rookie', 'day_manga', 'day_r18',
                      'day_male_r18', 'day_female_r18', 'week_r18',
                      'week_r18g'] = 'day',
        date: Optional[str] = None,
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        self.raw_data = await self.app.illust_ranking(
            mode=mode,
            date=date,
            offset=offset,
        )
        if not self.raw_data.illusts:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_illusts = {}
                for illust in self.raw_data.illusts:
                    all_users[illust.user.id] = illust.user
                    all_tags.update({tag.name: tag for tag in illust.tags})
                    all_illusts[illust.id] = illust
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
                await self._rank_into_db(
                    mode, date, offset,
                    [illust.id for illust in self.raw_data.illusts])
            self.db_session = None
            await session.commit()
        return [self.illusts[illust.id] for illust in self.raw_data.illusts]

    async def illust_ranking_local(
        self,
        mode: Literal['day', 'week', 'month', 'day_male', 'day_female',
                      'week_original', 'week_rookie', 'day_manga', 'day_r18',
                      'day_male_r18', 'day_female_r18', 'week_r18',
                      'week_r18g'] = 'day',
        date: Optional[str] = None,
        offset: Optional[int] = None,
    ) -> List[tables.Illust]:
        if date is None:
            now = datetime.now(tz=timezone(timedelta(hours=9)))
            if now.time() >= time(12):
                date = now.date()
            else:
                date = (now - timedelta(days=1)).date()
        elif isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d').date()
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.Illust,
                    joins=[tables._AssociationIllustRank, tables.IllustRank],
                    whereclauses=[
                        tables.IllustRank.mode == mode,
                        tables.IllustRank.date == date,
                    ],
                    order_by=tables._AssociationIllustRank.c.ranking.asc(),
                    offset=offset,
                )
                result = await session.execute(stmt)
                return result.scalars().unique().all()

    @catch_pixiv_error
    async def trending_tags_illust(self) -> List[Dict[str, Any]]:
        self.raw_data = await self.app.trending_tags_illust()
        if not self.raw_data.trend_tags:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_illusts = {}
                for item in self.raw_data.trend_tags:
                    all_users[item.illust.user.id] = item.illust.user
                    all_tags.update(
                        {tag.name: tag
                         for tag in item.illust.tags})
                    all_illusts[item.illust.id] = item.illust
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
            self.db_session = None
            await session.commit()
        return [{
            'tag': self.tags[item.tag],
            'illust': self.illusts[item.illust.id]
        } for item in self.raw_data.trend_tags]

    async def trending_tags_illust_local(self) -> List[Dict[str, Any]]:
        return []

    @catch_pixiv_error
    async def search_illust(
        self,
        word: str,
        search_target: Literal['partial_match_for_tags',
                               'exact_match_for_tags',
                               'title_and_caption'] = 'partial_match_for_tags',
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        duration: Optional[Literal['within_last_day', 'within_last_week',
                                   'within_last_month']] = None,
        offset: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        min_bookmarks: Optional[int] = None,
        max_bookmarks: Optional[int] = None,
    ) -> List[tables.Illust]:
        self.raw_data = await self.app.search_illust(
            word=word,
            search_target=search_target,
            sort=sort,
            duration=duration,
            offset=offset,
            start_date=start_date,
            end_date=end_date,
            min_bookmarks=min_bookmarks,
            max_bookmarks=max_bookmarks,
        )
        if not self.raw_data.illusts:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_illusts = {}
                for illust in self.raw_data.illusts:
                    all_users[illust.user.id] = illust.user
                    all_tags.update({tag.name: tag for tag in illust.tags})
                    all_illusts[illust.id] = illust
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
            self.db_session = None
            await session.commit()
        return [self.illusts[illust.id] for illust in self.raw_data.illusts]

    async def search_illust_local(
        self,
        word: str,
        search_target: Literal['partial_match_for_tags',
                               'exact_match_for_tags',
                               'title_and_caption'] = 'partial_match_for_tags',
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        duration: Literal['within_last_day', 'within_last_week',
                          'within_last_month'] = None,
        offset: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        min_bookmarks: Optional[int] = None,
        max_bookmarks: Optional[int] = None,
    ) -> List[tables.Illust]:
        async with Session() as session:
            async with session.begin():
                whereclauses = []
                if duration is not None:
                    if duration == 'within_last_day':
                        td = timedelta(days=1)
                    elif duration == 'within_last_week':
                        td = timedelta(weeks=1)
                    elif duration == 'within_last_month':
                        td = timedelta(days=30)
                    whereclauses.append(
                        tables.Illust.create_date >= datetime.utcnow() - td)
                if start_date is not None:
                    whereclauses.append(
                        tables.Illust.create_date >= datetime.strptime(
                            start_date, '%Y-%m-%d'))
                if end_date is not None:
                    whereclauses.append(
                        tables.Illust.create_date <= datetime.strptime(
                            end_date, '%Y-%m-%d'))
                if min_bookmarks is not None:
                    whereclauses.append(
                        tables.Illust.total_bookmarks >= min_bookmarks)
                if max_bookmarks is not None:
                    whereclauses.append(
                        tables.Illust.total_bookmarks <= max_bookmarks)
                for kw in word.strip().split(' '):
                    _not = False
                    if kw.startswith('-'):
                        kw = kw[1:]
                        _not = True
                    if search_target == 'partial_match_for_tags':
                        clause = tables.Illust.id.in_(
                            select(
                                tables.Illust.id,
                                joins=[tables.Illust.tags],
                                whereclauses=[
                                    (tables.Tag.name.like('%' +
                                                          '%'.join(list(kw)) +
                                                          '%'),
                                     tables.Tag.translated_name.like(
                                         '%' + '%'.join(list(kw)) + '%'))
                                ]))
                    elif search_target == 'exact_match_for_tags':
                        clause = tables.Illust.id.in_(
                            select(tables.Illust.id,
                                   joins=[tables.Illust.tags],
                                   whereclauses=[
                                       (tables.Tag.name == kw,
                                        tables.Tag.translated_name == kw)
                                   ]))
                    elif search_target == 'title_and_caption':
                        clause = tables.Illust.id.in_(
                            select(tables.Illust.id,
                                   whereclauses=[
                                       (tables.Illust.title.like(f'%{kw}%'),
                                        tables.Illust.caption.like(f'%{kw}%'))
                                   ]))
                    whereclauses.append(~clause if _not else clause)
                stmt = select(
                    tables.Illust,
                    whereclauses=whereclauses,
                    order_by=getattr(tables.Illust.create_date, sort[5:])(),
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                illusts = result.scalars().unique().all()
                self.downloads.extend([
                    stor for illust in illusts if illust._original
                    for stor in illust._original if stor.useable is False
                ])
                self.downloads.extend([
                    illust.ugoira for illust in illusts
                    if illust.type == 'ugoira' and illust.ugoira
                    and illust.ugoira.useable is False
                ])
                return illusts

    @catch_pixiv_error
    async def illust_bookmark_detail(
        self,
        illust_id: int,
    ) -> Optional[bool]:
        self.raw_data = await self.app.illust_bookmark_detail(
            illust_id=illust_id)
        if not self.raw_data.bookmark_detail.tags:
            return
        async with Session() as session:
            self.db_session = session
            async with session.begin():

                class _inner:
                    def __init__(self, name, translated_name):
                        self.name = name
                        self.translated_name = translated_name

                all_tags = {
                    tag.name: _inner(tag.name, None)
                    for tag in self.raw_data.bookmark_detail.tags
                }
                await self._tags_into_db(all_tags)
            self.db_session = None
            await session.commit()
        return self.raw_data.bookmark_detail.is_bookmarked

    @catch_pixiv_error
    async def illust_bookmark_add(
        self,
        illust_id: int,
        restrict: Literal['public', 'private'] = 'public',
    ) -> None:
        await self.app.illust_bookmark_add(
            illust_id=illust_id,
            restrict=restrict,
        )

    @catch_pixiv_error
    async def illust_bookmark_delete(self, illust_id: int) -> None:
        await self.app.illust_bookmark_delete(illust_id=illust_id)

    @catch_pixiv_error
    async def user_follow_add(
        self,
        user_id: int,
        restrict: Literal['public', 'private'] = 'public',
    ) -> None:
        await self.app.user_follow_add(user_id=user_id, restrict=restrict)

    @catch_pixiv_error
    async def user_follow_del(self, user_id: int) -> None:
        await self.app.user_follow_del(user_id=user_id)

    @catch_pixiv_error
    async def user_bookmark_tags_illust(
        self,
        restrict: Literal['public', 'private'] = 'public',
        offset: Optional[int] = None,
    ):
        self.raw_data = await self.app.user_bookmark_tags_illust(
            restrict=restrict,
            offset=offset,
        )
        return getattr(self.raw_data, 'bookmark_tags', [])

    @catch_pixiv_error
    async def user_following(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        self.raw_data = await self.app.user_following(
            user_id=user_id,
            offset=offset,
        )
        if not self.raw_data.user_previews:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_illusts = {}
                all_novels = {}
                all_tags = {}
                for item in self.raw_data.user_previews:
                    all_users[item.user.id] = item.user
                    all_illusts.update({_.id: _ for _ in item.illusts})
                    all_novels.update({_.id: _ for _ in item.novels})
                    all_tags.update({
                        tag.name: tag
                        for _ in (item.illusts + item.novels) for tag in _.tags
                    })
                await self._users_into_db(all_users, 'illusts', 'novels')
                await self._main_user_check(user_id,
                                            following=[*self.users.values()])
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return [
            self.users[item.user.id] for item in self.raw_data.user_previews
        ]

    async def user_following_local(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.User,
                    eagerloads=['illusts', 'novels'],
                    whereclauses=[
                        tables.User.followers.any(tables.User.id == user_id)
                    ],
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                users = result.scalars().unique().all()
                self.downloads.extend([
                    user.profile for user in users
                    if user.profile and user.profile.useable is False
                ])
                return users

    @catch_pixiv_error
    async def user_follower(
        self,
        user_id: int = SELF_USER_ID,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        self.raw_data = await self.app.user_follower(
            user_id=user_id,
            offset=offset,
        )
        if not self.raw_data.user_previews:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_illusts = {}
                all_novels = {}
                all_tags = {}
                for item in self.raw_data.user_previews:
                    all_users[item.user.id] = item.user
                    all_illusts.update({_.id: _ for _ in item.illusts})
                    all_novels.update({_.id: _ for _ in item.novels})
                    all_tags.update({
                        tag.name: tag
                        for _ in (item.illusts + item.novels) for tag in _.tags
                    })
                await self._users_into_db(all_users, 'illusts', 'novels')
                await self._main_user_check(user_id,
                                            followers=[*self.users.values()])
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return [
            self.users[item.user.id] for item in self.raw_data.user_previews
        ]

    async def user_follower_local(
        self,
        user_id: int = SELF_USER_ID,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.User,
                    eagerloads=['illusts', 'novels'],
                    whereclauses=[
                        tables.User.following.any(tables.User.id == user_id)
                    ],
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                users = result.scalars().unique().all()
                self.downloads.extend([
                    user.profile for user in users
                    if user.profile and user.profile.useable is False
                ])
                return users

    @catch_pixiv_error
    async def user_mypixiv(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        self.raw_data = await self.app.user_mypixiv(
            user_id=user_id,
            offset=offset,
        )
        if not self.raw_data.user_previews:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_illusts = {}
                all_novels = {}
                all_tags = {}
                for item in self.raw_data.user_previews:
                    all_users[item.user.id] = item.user
                    all_illusts.update({_.id: _ for _ in item.illusts})
                    all_novels.update({_.id: _ for _ in item.novels})
                    all_tags.update({
                        tag.name: tag
                        for _ in (item.illusts + item.novels) for tag in _.tags
                    })
                await self._users_into_db(all_users, 'illusts', 'novels')
                mu_stmt = select(
                    tables.User,
                    eagerloads=['_mypixiv', 'mypixiv'],
                    whereclauses=[tables.User.id == user_id],
                    limit=1,
                )
                mu_result = await session.execute(mu_stmt)
                if muser := mu_result.scalar():
                    to_add = set(self.users.values()).difference(muser.mypixiv)
                    muser._column_attr_update('_mypixiv', to_add)
                else:
                    self._user_create({
                        'id': user_id,
                        '_mypixiv': [*self.users.values()]
                    })
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return [
            self.users[item.user.id] for item in self.raw_data.user_previews
        ]

    async def user_mypixiv_local(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.User,
                    eagerloads=['illusts', 'novels'],
                    whereclauses=[
                        tables.User.mypixiv.any(tables.User.id == user_id)
                    ],
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                users = result.scalars().unique().all()
                self.downloads.extend([
                    user.profile for user in users
                    if user.profile and user.profile.useable is False
                ])
                return users

    @catch_pixiv_error
    async def user_list(
        self,
        user_id: int = SELF_USER_ID,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        self.raw_data = await self.app.user_list(
            user_id=user_id,
            offset=offset,
        )
        raise NotImplementedError

    async def user_list_local(
        self,
        user_id: int = SELF_USER_ID,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.User,
                    eagerloads=['illusts', 'novels'],
                    whereclauses=[
                        tables.User.listed_by.any(tables.User.id == user_id)
                    ],
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                users = result.scalars().unique().all()
                self.downloads.extend([
                    user.profile for user in users
                    if user.profile and user.profile.useable is False
                ])
                return users

    @catch_pixiv_error
    async def ugoira_metadata(self, illust_id: int) -> Dict[str, Any]:
        self._ugoira_raw_data = await self.app.ugoira_metadata(
            illust_id=illust_id)
        self.urls['ugoira'] = url = \
            self._ugoira_raw_data.ugoira_metadata.zip_urls.medium
        self._ugoira_frames[illust_id] = frames = \
            self._ugoira_raw_data.ugoira_metadata.frames
        return {'url': url, 'frames': frames}

    @catch_pixiv_error
    async def search_user(
        self,
        word: str,
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        duration: Optional[Literal['within_last_day', 'within_last_week',
                                   'within_last_month']] = None,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        self.raw_data = await self.app.search_user(
            word=word,
            sort=sort,
            duration=duration,
            offset=offset,
        )
        if not self.raw_data.user_previews:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_illusts = {}
                all_novels = {}
                all_tags = {}
                for item in self.raw_data.user_previews:
                    all_users[item.user.id] = item.user
                    all_illusts.update({_.id: _ for _ in item.illusts})
                    all_novels.update({_.id: _ for _ in item.novels})
                    all_tags.update({
                        tag.name: tag
                        for _ in (item.illusts + item.novels) for tag in _.tags
                    })
                await self._users_into_db(all_users, 'illusts', 'novels')
                await self._tags_into_db(all_tags)
                await self._illusts_into_db(all_illusts)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return [
            self.users[item.user.id] for item in self.raw_data.user_previews
        ]

    async def search_user_local(
        self,
        word: str,
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        duration: Literal['within_last_day', 'within_last_week',
                          'within_last_month'] = None,
        offset: Optional[int] = None,
    ) -> List[tables.User]:
        # no detail for "sort" and "duration" parameter
        async with Session() as session:
            async with session.begin():
                whereclauses = []
                for kw in word.strip().split(' '):
                    clause = tables.User.id.in_(
                        select(
                            tables.User.id,
                            whereclauses=[tables.User.name.like(f'%{kw}%')],
                        ))
                    whereclauses.append(clause)
                stmt = select(
                    tables.User,
                    eagerloads=['illusts', 'novels'],
                    whereclauses=whereclauses,
                    offset=offset,
                )
                result = await session.execute(stmt)
                users = result.scalars().unique().all()
                self.downloads.extend([
                    user.profile for user in users
                    if user.profile and user.profile.useable is False
                ])
                return users

    @catch_pixiv_error
    async def search_novel(
        self,
        word: str,
        search_target: Literal['partial_match_for_tags',
                               'exact_match_for_tags', 'text',
                               'Keyword'] = 'partial_match_for_tags',
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: Optional[int] = None,
    ) -> List[tables.Novel]:
        self.raw_data = await self.app.search_novel(
            word=word,
            search_target=search_target,
            sort=sort,
            offset=offset,
            start_date=start_date,
            end_date=end_date,
        )
        if not self.raw_data.novels:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_novels = {}
                for novel in self.raw_data.novels:
                    all_users[novel.user.id] = novel.user
                    all_tags.update({tag.name: tag for tag in novel.tags})
                    all_novels[novel.id] = novel
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return sorted(
            self.novels.values(),
            key=lambda n: n.create_date,
            reverse=True,
        )

    async def search_novel_local(
        self,
        word: str,
        search_target: Literal['partial_match_for_tags',
                               'exact_match_for_tags', 'text',
                               'Keyword'] = 'partial_match_for_tags',
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: Optional[int] = None,
    ) -> List[tables.Novel]:
        async with Session() as session:
            async with session.begin():
                whereclauses = []
                if start_date is not None:
                    whereclauses.append(
                        tables.Novel.create_date >= datetime.strptime(
                            start_date, '%Y-%m-%d'))
                if end_date is not None:
                    whereclauses.append(
                        tables.Novel.create_date <= datetime.strptime(
                            end_date, '%Y-%m-%d'))
                for kw in word.strip().split(' '):
                    _not = False
                    if kw.startswith('-'):
                        kw = kw[1:]
                        _not = True
                    if search_target == 'partial_match_for_tags':
                        clause = tables.Novel.id.in_(
                            select(
                                tables.Novel.id,
                                joins=[tables.Novel.tags],
                                whereclauses=[
                                    (tables.Tag.name.like('%' +
                                                          '%'.join(list(kw)) +
                                                          '%'),
                                     tables.Tag.translated_name.like(
                                         '%' + '%'.join(list(kw)) + '%'))
                                ]))
                    elif search_target == 'exact_match_for_tags':
                        clause = tables.Novel.id.in_(
                            select(tables.Novel.id,
                                   joins=[tables.Novel.tags],
                                   whereclauses=[
                                       (tables.Tag.name == kw,
                                        tables.Tag.translated_name == kw)
                                   ]))
                    elif search_target == 'text':
                        clause = tables.Novel.id.in_(
                            select(tables.Novel.id,
                                   whereclauses=[
                                       tables.Novel.content.like(f'%{kw}%')
                                   ]))
                    elif search_target == 'Keyword':
                        clause = tables.Novel.id.in_(
                            select(tables.Novel.id,
                                   whereclauses=[
                                       tables.Illust.title.like(f'%{kw}%'),
                                       tables.Novel.caption.like(f'%{kw}%')
                                   ]))
                    whereclauses.append(~clause if _not else clause)
                stmt = select(
                    tables.Novel,
                    whereclauses=whereclauses,
                    order_by=getattr(tables.Novel.create_date, sort[5:])(),
                    limit=self.RESULT_LIMIT,
                    offset=offset,
                )
                result = await session.execute(stmt)
                novels = result.scalars().unique().all()
                self.downloads.extend([
                    novel.large for novel in novels
                    if novel.large and novel.large.useable is False
                ])
                return novels

    @catch_pixiv_error
    async def user_novels(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.Novel]:
        self.raw_data = await self.app.user_novels(
            user_id=user_id,
            offset=offset,
        )
        if not self.raw_data.novels:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                self._temp = self.raw_data.novels[0].user
                userdata = self._userdata_constructor(full=False)
                await self._main_user_check(userdata['id'], **userdata)
                all_tags = {}
                all_novels = {}
                for novel in self.raw_data.novels:
                    all_tags.update({tag.name: tag for tag in novel.tags})
                    all_novels[novel.id] = novel
                await self._tags_into_db(all_tags)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return sorted(
            self.novels.values(),
            key=lambda n: n.create_date,
            reverse=True,
        )

    async def user_novels_local(
        self,
        user_id: int,
        offset: Optional[int] = None,
    ) -> List[tables.Novel]:
        async with Session() as session:
            async with session.begin():
                stmt = select(tables.Novel,
                              whereclauses=[tables.Novel.user_id == user_id],
                              order_by=tables.Novel.create_date.desc(),
                              limit=Pixiv.RESULT_LIMIT,
                              offset=offset)
                result = await session.execute(stmt)
                novels = result.scalars().unique().all()
                self.downloads.extend([
                    novel.large for novel in novels
                    if novel.large and novel.large.useable is False
                ])
                return novels

    @catch_pixiv_error
    async def novel_series(self, series_id: int) -> List[tables.Novel]:
        self.raw_data = await self.app.novel_series(series_id=series_id)
        if not self.raw_data.novels:
            return []
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_users = {}
                all_tags = {}
                all_novels = {}
                for novel in self.raw_data.novels:
                    all_users[novel.user.id] = novel.user
                    all_tags.update({tag.name: tag for tag in novel.tags})
                    all_novels[novel.id] = novel
                await self._users_into_db(all_users)
                await self._tags_into_db(all_tags)
                await self._novels_into_db(all_novels)
            self.db_session = None
            await session.commit()
        return sorted(
            self.novels.values(),
            key=lambda n: n.create_date,
            reverse=True,
        )

    async def novel_series_local(self, series_id: int) -> List[tables.Novel]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.Novel,
                    whereclauses=[tables.Novel.series_id == series_id],
                    order_by=tables.Novel.create_date.desc(),
                    limit=self.RESULT_LIMIT,
                )
                result = await session.execute(stmt)
                novels = result.scalars().unique().all()
                self.downloads.extend([
                    novel.large for novel in novels
                    if novel.large and novel.large.useable is False
                ])
                return novels

    @catch_pixiv_error
    async def novel_detail(self, novel_id: int) -> Optional[tables.Novel]:
        self.raw_data = await self.app.novel_detail(novel_id=novel_id)
        self._temp = self.raw_data.novel
        noveldata = self._noveldata_constructor()
        async with Session() as session:
            self.db_session = session
            async with session.begin():
                all_tags = {tag.name: tag for tag in self.raw_data.novel.tags}
                await self._tags_into_db(all_tags)
                self._temp = self.raw_data.novel.user
                userdata = self._userdata_constructor()
                await self._main_user_check(user_id=userdata['id'], **userdata)
                novel = await self._main_novel_check(
                    noveldata['id'],
                    'square_medium',
                    'medium',
                    'large',
                    **noveldata,
                )
                self.novels[novel.id] = novel
            self.db_session = None
            await session.commit()
        return novel

    async def novel_detail_local(
        self,
        novel_id: int,
    ) -> Optional[tables.Novel]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.Novel,
                    eagerloads=['square_medium', 'medium', 'large'],
                    whereclauses=[tables.Novel.id == novel_id],
                    limit=1,
                )
                result = await session.execute(stmt)
                novel = result.scalar()
                if novel and novel.large and novel.large.useable is False:
                    self.downloads.append(novel.large)
                return novel

    @catch_pixiv_error
    async def novel_text(self, novel_id: int) -> Dict[str, Any]:
        self._text_raw_data = await self.app.novel_text(novel_id=novel_id)
        self._novel_text = text = self._text_raw_data.novel_text
        self._novel_next[
            novel_id] = next = self._text_raw_data.series_next.get('id', None)
        return {'id': novel_id, 'text': text, 'next': next}

    async def novel_text_local(self, novel_id: int) -> Dict[str, Any]:
        async with Session() as session:
            async with session.begin():
                stmt = select(
                    tables.Novel.content,
                    tables.Novel.series_id,
                    whereclauses=[tables.Novel.id == novel_id],
                    limit=1,
                )
                result = await session.execute(stmt)
                content, series_id = result.first()
                if content:
                    d = {'id': novel_id, 'text': content, 'next': None}
                    if series_id:
                        _stmt = select(
                            tables.Novel.id,
                            whereclauses=[tables.Novel.series_id == series_id],
                            order_by=tables.Novel.create_date.asc(),
                        )
                        _result = await session.execute(_stmt)
                        _series = _result.scalars().all()
                        try:
                            d['next'] = _series[_series.index(novel_id) + 1]
                        except IndexError:
                            pass
                    return d
                else:
                    return {'id': novel_id, 'text': None, 'next': None}

    @catch_pixiv_error
    async def showcase_article(
        self,
        showcase_id: int,
    ) -> Optional[tables.Showcase]:
        self.raw_data = await self.app.showcase_article(
            showcase_id=showcase_id, )
        if not self.raw_data.body:
            return []
        else:
            self._temp = self.raw_data.body[0]
            showcasedata = self._showcase_constructor()
            async with Session() as session:
                self.db_session = session
                async with session.begin():

                    class _:
                        def __init__(self, **kwargs) -> None:
                            for k, v in kwargs.items():
                                setattr(self, k, v)

                    class _inner_tag:
                        def __init__(self, name, translated_name=None):
                            self.name = name
                            self.translated_name = translated_name

                    class _inner_user:
                        def __init__(self, id, name, account, comment,
                                     profile):
                            self.id = int(id)
                            self.name = name
                            self.account = account
                            self.comment = comment
                            self.profile_image_urls = _(medium=profile)

                    class _inner_illust:
                        def __init__(self, id, title, caption, user_id, width,
                                     height, create_date, sanity_level,
                                     page_count):
                            self.id = int(id)
                            self.title = title
                            self.type = 'illust'
                            self.caption = caption
                            self.user = _(id=int(user_id))
                            self.series = _(id=None, title=None)
                            self.create_date = datetime.strptime(
                                create_date, '%Y-%m-%d %H:%M:%S').isoformat()
                            self.page_count = int(page_count)
                            self.width = int(width)
                            self.height = int(height)
                            self.sanity_level = int(sanity_level)
                            self.total_view = None
                            self.total_bookmarks = None
                            self.total_comments = None
                            self.tags = []
                            self.image_urls = _(square_medium=None,
                                                medium=None,
                                                large=None)
                            self.meta_single_page = _(original_image_url=None)
                            self.meta_pages = []

                    all_tags = {}
                    all_users = {}
                    all_illusts = {}
                    all_tags.update({
                        tag.name: _inner_tag(name=tag.name)
                        for tag in self.raw_data.body[0].tags
                    })
                    for illust in self.raw_data.body[0].illusts:
                        all_users[int(illust.illust_user_id)] = _inner_user(
                            illust.illust_user_id, illust.user_name,
                            illust.user_account, illust.user_comment,
                            illust.user_icon)
                        all_illusts[int(illust.illust_id)] = _inner_illust(
                            illust.illust_id, illust.illust_title,
                            illust.illust_comment, illust.illust_user_id,
                            illust.illust_width, illust.illust_height,
                            illust.illust_create_date,
                            illust.illust_sanity_level,
                            illust.illust_page_count)
                    await self._users_into_db(all_users)
                    await self._tags_into_db(all_tags)
                    await self._illusts_into_db(all_illusts)
                    showcase = await self._main_showcase_check(
                        showcasedata['id'], **showcasedata)
                    self.showcases[showcase.id] = showcase
                self.db_session = None
                await session.commit()
            return showcase

    async def showcase_article_local(self, showcase_id: int):
        async with Session() as session:
            async with session.begin():
                stmt = select(tables.Showcase,
                              whereclauses=[tables.Showcase.id == showcase_id],
                              limit=1)
                result = await session.execute(stmt)
                showcase = result.scalar()
                if showcase.thumbnail and showcase.thumbnail.useable is False:
                    self.downloads.append(showcase.thumbnail)
                return showcase

    async def random_illust(
            self,
            min_view=10000,
            min_bookmarks=1000,
            tags=[],
            allow_r18=False,
            allow_r18g=False,
            limit=1) -> Union[tables.Illust, List[tables.Illust]]:
        if not tags:
            tags.append('ロリ')
        async with Session() as session:
            async with session.begin():
                whereclauses = [
                    tables.Illust.type != 'manga',
                    (
                        tables.Illust.total_view >= min_view,
                        tables.Illust.total_bookmarks >= min_bookmarks,
                    )
                ]
                if allow_r18 is False:
                    tags.append('-R-18')
                if allow_r18g is False:
                    tags.append('-R-18G')
                tags = set(tags)
                tags = [
                    tag for tag in tags
                    if not (tag.startswith('-') and tag[1:] in tags) and not (
                        tag.startswith('-') is False and ('-' + tag) in tags)
                ]
                for tag in tags:
                    _not = False
                    if tag.startswith('-'):
                        tag = tag[1:]
                        _not = True
                    clause_0 = tables.Illust.tags.any(tables.Tag.name == tag)
                    clause_1 = tables.Illust.tags.any(
                        tables.Tag.translated_name == tag)
                    if _not:
                        whereclauses.extend((~clause_0, ~clause_1))
                    else:
                        whereclauses.append((clause_0, clause_1))
                count_stmt = select(
                    func.count(),
                    select_from=tables.Illust,
                    whereclauses=whereclauses,
                )
                count_result = await session.execute(count_stmt)
                count = count_result.scalar()
                stmt = select(
                    tables.Illust,
                    joins=[tables.Illust.tags],
                    whereclauses=whereclauses,
                    order_by=func.random(),
                    limit=limit,
                )
                if count < limit * 10:
                    offset = 0
                    illusts = []
                    seq = [500, 1000, 2000, 5000, 10000]
                    if min_bookmarks < 500:
                        tags.append('500users入り')
                    elif min_bookmarks > 10000:
                        tags.append('10000users入り')
                    else:
                        for i in seq:
                            if i >= min_bookmarks:
                                tags.append(f'{i}users入り')
                                break
                    while len(illusts) < limit * 10:
                        _raw = await self.search_illust(
                            word=' '.join(tags),
                            offset=offset,
                        )
                        _filtered = list(
                            filter(
                                lambda i: i.total_bookmarks >= min_bookmarks,
                                _raw))
                        if _raw:
                            illusts.extend(_filtered)
                            offset += len(_raw)
                        else:
                            break
                    if count > 0:
                        result = await session.execute(stmt)
                        illusts.extend(result.scalars().unique().all())
                    illusts = list(set(illusts))
                    if limit == 1:
                        return choice(illusts) if illusts else None
                    else:
                        return choices(illusts, k=limit) \
                            if len(illusts) >= limit else illusts
                else:
                    result = await session.execute(stmt)
                    illusts = result.scalars().unique().all()
                    self.downloads.extend([
                        stor for illust in illusts if illust._original
                        for stor in illust._original if stor.useable is False
                    ])
                    self.downloads.extend([
                        illust.ugoira for illust in illusts
                        if illust.type == 'ugoira' and illust.ugoira
                        and illust.ugoira.useable is False
                    ])
                    if limit == 1:
                        return illusts[0]
                    else:
                        return illusts

    def _userdata_constructor(
        self,
        full: bool = False,
        **construct_params,
    ) -> Dict[str, Any]:
        if full is True:
            _ = dict(
                id=self._temp.user.id,
                name=self._temp.user.name,
                account=self._temp.user.account,
                comment=self._temp.user.comment or None,
                webpage=self._temp.profile.webpage or None,
                gender=self._temp.profile.gender or None,
                birth=datetime.strptime(self._temp.profile.birth, '%Y-%m-%d')
                if self._temp.profile.birth else None,
                region=self._temp.profile.region or None,
                job=self._temp.profile.job or None,
                total_follow_users=self._temp.profile.total_follow_users,
                total_mypixiv_users=self._temp.profile.total_mypixiv_users,
                total_illusts=self._temp.profile.total_illusts,
                total_manga=self._temp.profile.total_manga,
                total_novels=self._temp.profile.total_novels,
                total_illust_bookmarks_public=self._temp.profile.
                total_illust_bookmarks_public,
                total_illust_series=self._temp.profile.total_illust_series,
                total_novel_series=self._temp.profile.total_novel_series,
                twitter_account=self._temp.profile.twitter_account or None,
                twitter_url=self._temp.profile.twitter_url or None,
                pawoo_url=self._temp.profile.pawoo_url or None,
                is_premium=self._temp.profile.is_premium,
                **construct_params,
            )
            self.urls.update({
                'profile':
                self._temp.user.profile_image_urls.medium,
                'background':
                self._temp.profile.background_image_url
            })
            self._temp = None
            return _
        else:
            _ = dict(
                id=self._temp.id,
                name=self._temp.name,
                account=self._temp.account,
                comment=getattr(self._temp, 'comment', None),
                **construct_params,
            )
            self.urls.update({'profile': self._temp.profile_image_urls.medium})
            self._temp = None
            return _

    def _user_attr_check(self, user: tables.User, userdata: Dict[str, Any]):
        for k, v in userdata.items():
            user._column_attr_update(
                k,
                v,
                add_only=True if k not in (
                    'following',
                    'followers',
                    '_mypixiv',
                    'mypixiv',
                    'list',
                ) else False,
            )
        if profile := self.urls.get('profile', None):
            if user.profile.source != profile:
                user.profile.useable = False
                user.profile.source = profile
            if user.profile.useable is False:
                self.downloads.append(user.profile)
        if bg := self.urls.get('background', None):
            if user.background_image is None:
                new_bg = tables.PixivStorage(source=bg)
                user.background_image = new_bg
                self.db_session.add(new_bg)
                self.downloads.append(new_bg)
            elif user.background_image.source != bg:
                user.background_image.useable = False
                user.background_image.source = bg
            if user.background_image.useable is False:
                self.downloads.append(user.background_image)

    def _user_create(self, userdata: Dict[str, Any]) -> tables.User:
        user = tables.User(**userdata)
        user.profile = profile = tables.PixivStorage(
            source=self.urls.get('profile', None))
        self.db_session.add(profile)
        self.downloads.append(profile)
        if bg := self.urls.get('background', None):
            user.background_image = background = tables.PixivStorage(source=bg)
            self.db_session.add(background)
            self.downloads.append(background)
        self.db_session.add(user)
        self.users[user.id] = user
        return user

    async def _main_user_check(
        self,
        user_id: int,
        *eagerloads,
        eagerload_strategy: str = None,
        **construct_params,
    ) -> tables.User:
        construct_params.pop('id', None)
        eagerloads = [*eagerloads] + [
            attr for attr in construct_params.keys() if isinstance(
                getattr(tables.User, attr).property, RelationshipProperty)
        ]
        mu_stmt = select(
            tables.User,
            eagerloads=eagerloads,
            eagerload_strategy=eagerload_strategy,
            whereclauses=[tables.User.id == user_id],
            limit=1,
        )
        mu_result = await self.db_session.execute(mu_stmt)
        if muser := mu_result.scalar():
            self._user_attr_check(muser, construct_params)
        else:
            muser = self._user_create({'id': user_id, **construct_params})
        return muser

    async def _users_into_db(
        self,
        all_users: Dict[int, Dict[str, Any]],
        *eagerloads,
        eagerload_strategy: str = None,
        **construct_params,
    ):
        user_ids = list(all_users.keys())
        u_stmt = select(
            tables.User,
            eagerloads=[*eagerloads] + [
                attr for attr in construct_params.keys() if isinstance(
                    getattr(tables.User, attr).property, RelationshipProperty)
            ],
            eagerload_strategy=eagerload_strategy,
            whereclauses=[tables.User.id.in_(user_ids)],
        )
        u_result = await self.db_session.execute(u_stmt)
        if eagerloads:
            self.users.update(
                {user.id: user
                 for user in u_result.scalars().unique().all()})
        else:
            self.users.update(
                {user.id: user
                 for user in u_result.scalars().all()})
        users_to_add = set(user_ids).difference(self.users.keys())
        for uid in user_ids:
            self._temp = all_users[uid]
            userdata = self._userdata_constructor(full=False,
                                                  **construct_params)
            if uid in users_to_add:
                self._user_create(userdata)
            else:
                self._user_attr_check(self.users[uid], userdata)

    def _illustdata_constructor(self, **construct_params) -> Dict[str, Any]:
        _ = dict(
            id=self._temp.id,
            title=self._temp.title,
            type=self._temp.type,
            caption=self._temp.caption or None,
            user_id=self._temp.user.id,
            series_id=self._temp.series.id if self._temp.series else None,
            series_title=self._temp.series.title
            if self._temp.series else None,
            create_date=datetime.fromisoformat(self._temp.create_date),
            page_count=self._temp.page_count,
            width=self._temp.width,
            height=self._temp.height,
            sanity_level=self._temp.sanity_level,
            total_view=self._temp.total_view,
            total_bookmarks=self._temp.total_bookmarks,
            total_comments=self._temp.total_comments,
            tags=[tag.name for tag in self._temp.tags],
            **construct_params,
        )
        if self._temp.page_count == 1:
            if self._temp.image_urls.square_medium:
                self.urls.update({
                    'square_medium': [self._temp.image_urls.square_medium],
                    'medium': [self._temp.image_urls.medium],
                    'large': [self._temp.image_urls.large],
                    'original':
                    [self._temp.meta_single_page.original_image_url],
                })
        else:
            self.urls.update({
                'square_medium': [],
                'medium': [],
                'large': [],
                'original': []
            })
            for page in self._temp.meta_pages:
                self.urls['square_medium'].append(
                    page.image_urls.square_medium)
                self.urls['medium'].append(page.image_urls.medium)
                self.urls['large'].append(page.image_urls.large)
                self.urls['original'].append(page.image_urls.original)
        self._temp = None
        return _

    def __illust_create_minimum(
        self,
        illustdata: Dict[str, Any],
    ) -> tables.Illust:
        try:
            illust = tables.Illust(
                **illustdata,
                square_medium=[
                    tables.PixivStorage(
                        source=url, page=self.urls['square_medium'].index(url))
                    for url in self.urls['square_medium']
                ],
                medium=[
                    tables.PixivStorage(source=url,
                                        page=self.urls['medium'].index(url))
                    for url in self.urls['medium']
                ],
                large=[
                    tables.PixivStorage(source=url,
                                        page=self.urls['large'].index(url))
                    for url in self.urls['large']
                ],
                original=[
                    tables.PixivStorage(source=url,
                                        page=self.urls['original'].index(url))
                    for url in self.urls['original']
                ],
            )
            self.db_session.add_all([
                illust, *illust._square_medium, *illust._medium,
                *illust._large, *illust._original
            ])
            self.downloads.extend(illust._original)
        except KeyError:
            illust = tables.Illust(**illustdata)
            self.db_session.add(illust)
        return illust

    def _illust_attr_check(
        self,
        illust: tables.Illust,
        illustdata: Dict[str, Any],
    ):
        illustdata['tags'] = [
            self.tags[tag] for tag in illustdata.get('tags', [])
        ]
        if not illust.original and self.urls.get('original', None):
            self.db_session.sync_session.delete(illust)
            illust = self.__illust_create_minimum(illustdata)
        else:
            for k, v in illustdata.items():
                illust._column_attr_update(
                    k,
                    v,
                    add_only=True if k not in (
                        'tags',
                        'bookmarked_by',
                    ) else False,
                )
            self.downloads.extend(
                [stor for stor in illust._original if stor.useable is False])
        if illust.type == 'ugoira':
            ugoira_url = self.urls.get('ugoira', None)
            if illust.ugoira is None:
                illust.ugoira = tables.PixivStorage(source=ugoira_url)
                self.db_session.add(illust.ugoira)
            elif ugoira_url and illust.ugoira.source != ugoira_url:
                illust.ugoira.useable = False
                illust.ugoira.source = ugoira_url
            if illust.ugoira.source and illust.ugoira.useable is False:
                self.downloads.append(illust.ugoira)

    def _illust_create(self, illustdata: Dict[str, Any]) -> tables.Illust:
        illustdata['tags'] = [
            self.tags[tag] for tag in illustdata.get('tags', [])
        ]
        illust = self.__illust_create_minimum(illustdata)
        if illust.type == 'ugoira':
            illust.ugoira = ugoira = tables.PixivStorage(
                source=self.urls.get('ugoira', None))
            self.db_session.add(ugoira)
            if ugoira.source:
                self.downloads.append(ugoira)
        self.db_session.add(illust)
        self.illusts[illust.id] = illust
        return illust

    async def _main_illust_check(
        self,
        illust_id: int,
        *eagerloads,
        eagerload_strategy: str = None,
        **construct_params,
    ) -> tables.Illust:
        construct_params.pop('id', None)
        eagerloads = [*eagerloads] + [
            attr for attr in construct_params.keys() if isinstance(
                getattr(tables.Illust, attr).property, RelationshipProperty)
        ]
        mi_stmt = select(
            tables.Illust,
            eagerloads=eagerloads,
            eagerload_strategy=eagerload_strategy,
            whereclauses=[tables.Illust.id == illust_id],
            limit=1,
        )
        mi_result = await self.db_session.execute(mi_stmt)
        if millust := mi_result.scalar():
            if millust.type == 'ugoira' and millust.ugoira is None:
                await self.ugoira_metadata(illust_id)
            self._illust_attr_check(millust, construct_params)
        else:
            if user_id := construct_params.get('user_id', None):
                await self._main_user_check(user_id=user_id)
            if construct_params.get('type', None) == 'ugoira':
                await self.ugoira_metadata(illust_id)
            millust = self._illust_create({
                'id': illust_id,
                **construct_params
            })
        return millust

    async def _illusts_into_db(
        self,
        all_illusts: Dict[int, Dict[str, Any]],
        *eagerloads,
        eagerload_strategy: str = None,
        **construct_params,
    ):
        illust_ids = list(all_illusts.keys())
        i_stmt = select(
            tables.Illust,
            eagerloads=[*eagerloads] + [
                attr for attr in construct_params.keys() if isinstance(
                    getattr(tables.Illust, attr).property,
                    RelationshipProperty)
            ],
            eagerload_strategy=eagerload_strategy,
            whereclauses=[tables.Illust.id.in_(illust_ids)],
        )
        i_result = await self.db_session.execute(i_stmt)
        self.illusts.update({
            illust.id: illust
            for illust in i_result.scalars().unique().all()
        })
        illusts_to_add = set(illust_ids).difference(self.illusts.keys())
        for pid in illust_ids:
            self._temp = all_illusts[pid]
            illustdata = self._illustdata_constructor(**construct_params)
            if pid in illusts_to_add:
                if illustdata['type'] == 'ugoira':
                    await self.ugoira_metadata(illust_id=illustdata['id'])
                self._illust_create(illustdata)
            else:
                if illustdata['type'] == 'ugoira' and \
                        (self.illusts[pid].ugoira is None or
                         self.illusts[pid].ugoira.source is None):
                    await self.ugoira_metadata(illust_id=illustdata['id'])
                self._illust_attr_check(self.illusts[pid], illustdata)

    async def _rank_into_db(self, mode, date, offset, illust_ids):
        # SQLAlchemy core query
        if date is None:
            now = datetime.now(tz=timezone(timedelta(hours=9)))
            if now.time() >= time(12):
                date = now.date()
            else:
                date = (now - timedelta(days=1)).date()
        elif isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d').date()
        r_asso_stmt = select(
            tables._AssociationIllustRank,
            joins=[tables.IllustRank],
            whereclauses=[
                tables.IllustRank.mode == mode,
                tables.IllustRank.date == date,
            ],
        )
        r_asso_result = await self.db_session.execute(r_asso_stmt)
        rank_in_db = r_asso_result.all()
        if rank_in_db:
            rank_id = rank_in_db[0].rank_id
            rankings = [rank.ranking for rank in rank_in_db]
        else:
            _i_stmt = tables.IllustRank.__table__.insert().values(mode=mode,
                                                                  date=date)
            _i_result = await self.db_session.execute(_i_stmt)
            rank_id = _i_result.inserted_primary_key[0]
            rankings = []
        inserts = []
        for i in range(len(illust_ids)):
            ranking = (offset or 0) + i + 1
            if ranking not in rankings:
                inserts.append(
                    dict(illust_id=illust_ids[i],
                         rank_id=rank_id,
                         ranking=ranking))
        if inserts:
            await self.db_session.execute(
                tables._AssociationIllustRank.insert(), inserts)

    def _noveldata_constructor(self, **construct_params) -> Dict[str, Any]:
        _ = dict(
            id=self._temp.id,
            title=self._temp.title,
            caption=self._temp.caption,
            is_original=self._temp.is_original,
            create_date=datetime.fromisoformat(self._temp.create_date),
            page_count=self._temp.page_count,
            text_length=self._temp.text_length,
            user_id=self._temp.user.id,
            series_id=getattr(self._temp.series, 'id', None),
            series_title=getattr(self._temp.series, 'title', None),
            total_bookmarks=self._temp.total_bookmarks,
            total_view=self._temp.total_view,
            total_comments=self._temp.total_comments,
            tags=[tag.name for tag in self._temp.tags],
            **construct_params,
        )
        self.urls['square_medium'] = self._temp.image_urls.square_medium
        self.urls['medium'] = self._temp.image_urls.medium
        self.urls['large'] = self._temp.image_urls.large
        self._temp = None
        return _

    def _novel_attr_check(
        self,
        novel: tables.Novel,
        noveldata: Dict[str, Any],
    ):
        noveldata['tags'] = [
            self.tags[tag] for tag in noveldata.get('tags', [])
        ]
        for k, v in noveldata.items():
            novel._column_attr_update(
                k,
                v,
                add_only=True if k not in ('tags', 'bookmarked_by') else False,
            )
        if novel.large.useable is False:
            self.downloads.append(novel.large)

    def _novel_create(self, noveldata: Dict[str, Any]) -> tables.Novel:
        noveldata['tags'] = [
            self.tags[tag] for tag in noveldata.get('tags', [])
        ]
        square_medium = tables.PixivStorage(source=self.urls['square_medium'])
        self.db_session.add(square_medium)
        medium = tables.PixivStorage(source=self.urls['medium'])
        self.db_session.add(medium)
        large = tables.PixivStorage(source=self.urls['large'])
        self.db_session.add(large)
        self.downloads.append(large)
        novel = tables.Novel(
            **noveldata,
            square_medium=square_medium,
            medium=medium,
            large=large,
        )
        self.db_session.add(novel)
        self.novels[novel.id] = novel
        return novel

    async def _main_novel_check(
        self,
        novel_id: int,
        *eagerloads,
        eagerload_strategy: str = None,
        **construct_params,
    ) -> tables.Novel:
        construct_params.pop('id', None)
        eagerloads = [*eagerloads] + [
            attr for attr in construct_params.keys() if isinstance(
                getattr(tables.Novel, attr).property, RelationshipProperty)
        ]
        mn_stmt = select(
            tables.Novel,
            eagerloads=eagerloads,
            eagerload_strategy=eagerload_strategy,
            whereclauses=[tables.Novel.id == novel_id],
            limit=1,
        )
        mn_result = await self.db_session.execute(mn_stmt)
        if mnovel := mn_result.scalar():
            if mnovel.content is None:
                await self.novel_text(novel_id)
                construct_params['content'] = self._novel_text
            self._novel_attr_check(mnovel, construct_params)
        else:
            if user_id := construct_params.get('user_id', None):
                await self._main_user_check(user_id=user_id)
            await self.novel_text(novel_id)
            construct_params['content'] = self._novel_text
            mnovel = self._novel_create({'id': novel_id, **construct_params})
        return mnovel

    async def _novels_into_db(
        self,
        all_novels: Dict[int, Dict[str, Any]],
        *eagerloads,
        eagerload_strategy: str = None,
        **construct_params,
    ):
        novel_ids = list(all_novels.keys())
        n_stmt = select(
            tables.Novel,
            eagerloads=[*eagerloads] + [
                attr for attr in construct_params.keys() if isinstance(
                    getattr(tables.Novel, attr).property, RelationshipProperty)
            ],
            eagerload_strategy=eagerload_strategy,
            whereclauses=[tables.Novel.id.in_(novel_ids)],
        )
        n_result = await self.db_session.execute(n_stmt)
        self.novels.update(
            {novel.id: novel
             for novel in n_result.scalars().unique().all()})
        novels_to_add = set(novel_ids).difference(self.novels.keys())
        for nid in novel_ids:
            self._temp = all_novels[nid]
            noveldata = self._noveldata_constructor(**construct_params)
            if nid in novels_to_add:
                noveldata['content'] = (await self.novel_text(nid))['text']
                self._novel_create(noveldata)
            else:
                if self.novels[nid].content is None:
                    noveldata['content'] = (await self.novel_text(nid))['text']
                self._novel_attr_check(self.novels[nid], noveldata)

    async def _tags_into_db(self, all_tags: Dict[str, Dict[str, str]]):
        self.tags.clear()
        tag_names = list(all_tags.keys())
        t_stmt = select(
            tables.Tag,
            whereclauses=[tables.Tag.name.in_(tag_names)],
        )
        t_result = await self.db_session.execute(t_stmt)
        self.tags.update({_.name: _ for _ in t_result.scalars().all()})
        for tag in self.tags.values():
            if tag.translated_name != all_tags[tag.name].translated_name:
                tag.translated_name = all_tags[tag.name].translated_name
        tags_to_add = set(tag_names).difference(self.tags.keys())
        tags_to_add = {
            tag: tables.Tag(name=all_tags[tag].name,
                            translated_name=all_tags[tag].translated_name)
            for tag in tags_to_add
        }
        self.db_session.add_all(tags_to_add.values())
        self.tags.update(tags_to_add)

    def _commentdata_constructor(self, **construct_params) -> Dict[str, Any]:
        _ = dict(
            id=self._temp.id,
            comment=self._temp.comment,
            date=datetime.fromisoformat(self._temp.date),
            user_id=self._temp.user.id,
            **construct_params,
        )
        self._temp = None
        return _

    async def _comments_into_db(
        self,
        all_comments: Dict[str, Dict[str, Any]],
        illust_id: int,
        *eagerloads,
        eagerload_strategy: str = None,
    ) -> tables.IllustComment:
        comment_ids = []
        comment_ids_extra = []
        for k, v in all_comments.items():
            comment_ids.append(k)
            if v.parent_comment:
                comment_ids_extra.append(v.parent_comment.id)
        comment_ids_all = set(comment_ids + comment_ids_extra)
        c_stmt = select(
            tables.IllustComment,
            eagerloads=[*eagerloads],
            eagerload_strategy=eagerload_strategy,
            whereclauses=[tables.IllustComment.id.in_(set(comment_ids_all))])
        c_result = await self.db_session.execute(c_stmt)
        self.comments.update({
            comment.id: comment
            for comment in c_result.scalars().unique().all()
        })
        comments_to_add = comment_ids_all.difference(self.comments.keys())
        for cid in comment_ids_all:
            if cid in all_comments.keys():
                self._temp = all_comments[cid]
                commentdata = self._commentdata_constructor(
                    illust_id=illust_id)
            else:
                commentdata = {'id': cid, 'illust_id': illust_id}
            if cid in comments_to_add:
                comment = tables.IllustComment(**commentdata)
                self.db_session.add(comment)
                self.comments[cid] = comment
            else:
                for k, v in commentdata.items():
                    self.comments[cid]._column_attr_update(k, v)
        for cid in comments_to_add:
            if parent_id := getattr(self.comments[cid].parent_comment, 'id',
                                    None):
                self.comments[cid].parent_comment = self.comments[parent_id]
        for cid in comment_ids_extra:
            self.comments.pop(cid, None)

    def _showcase_constructor(self) -> Dict[str, Any]:
        _ = dict(
            id=int(self._temp.id),
            lang=self._temp.lang,
            title=self._temp.title,
            publish_date=datetime.fromtimestamp(self._temp.publishDate),
            category=self._temp.category,
            subcategory=self._temp.subCategory,
            subcategorylabel=self._temp.subCategoryLabel,
            subcategoryintroduction=self._temp.subCategoryIntroduction,
            introduction=self._temp.introduction,
            tags=[tag.name for tag in self._temp.tags],
            illusts=[int(illust.illust_id) for illust in self._temp.illusts],
            footer=self._temp.footer,
            is_onlyoneuser=self._temp.isOnlyOneUser,
        )
        self.urls['thumbnail'] = self._temp.thumbnailUrl
        self._temp = None
        return _

    async def _main_showcase_check(
        self,
        showcase_id: int,
        **construct_params,
    ) -> tables.Showcase:
        construct_params.pop('id', None)
        msc_stmt = select(
            tables.Showcase,
            whereclauses=[tables.Showcase.id == showcase_id],
            limit=1,
        )
        msc_result = await self.db_session.execute(msc_stmt)
        if mshowcase := msc_result.scalar():
            pass  # invariable
        else:
            construct_params['tags'] = [
                self.tags[tag] for tag in construct_params['tags']
            ]
            construct_params['illusts'] = [
                self.illusts[illust] for illust in construct_params['illusts']
            ]
            construct_params['thumbnail'] = tables.PixivStorage(
                source=self.urls['thumbnail'])
            self.db_session.add(construct_params['thumbnail'])
            mshowcase = tables.Showcase(id=showcase_id, **construct_params)
            self.db_session.add(mshowcase)
        return mshowcase

    async def _showcases_into_db(
        self,
        all_showcases: Dict[int, Dict[str, Any]],
    ):
        showcase_ids = all_showcases.keys()
        sc_stmt = select(
            tables.Showcase,
            whereclauses=[tables.Showcase.id.in_(showcase_ids)],
        )
        sc_result = await self.db_session.execute(sc_stmt)
        self.showcases.update({
            showcase.id: showcase
            for showcase in sc_result.scalars().unique().all()
        })
        showcases_to_add = set(showcase_ids).difference(self.showcases.keys())
        for scid in showcases_to_add:
            self._temp = all_showcases[scid]
            showcasedata = self._showcase_constructor()
            showcasedata['tags'] = [
                self.tags[tag] for tag in showcasedata['tags']
            ]
            showcasedata['illusts'] = [
                self.illusts[illust] for illust in showcasedata['illusts']
            ]
            showcasedata['thumbnail'] = tables.PixivStorage(
                source=self.urls['thumbnail'])
            self.db_session.add(showcasedata['thumbnail'])
            self.showcases[scid] = tables.Showcase(**showcasedata)
            self.db_session.add(self.showcases[scid])
