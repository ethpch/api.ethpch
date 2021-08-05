from typing import List, Literal, Union, Optional
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from utils.schedule.apscheduler import scheduler
from .pixiv import Pixiv
from . import models

pixiv_router = APIRouter(prefix='/pixiv')


# user
@pixiv_router.post('/u/{user_id}', response_model=models.User)
async def user_detail(user_id: int, local: bool = False):
    if local is True:
        call = Pixiv.user_detail_local
    else:
        call = Pixiv.user_detail
    async with Pixiv() as p:
        user = await call(p, user_id)
    return user


@pixiv_router.post('/u/{user_id}/illusts', response_model=List[models.Illust])
async def user_illusts(user_id: int,
                       type: Literal['illust', 'manga'] = 'illust',
                       offset: Optional[int] = None,
                       local: bool = False):
    if local is True:
        call = Pixiv.user_illusts_local
    else:
        call = Pixiv.user_illusts
    async with Pixiv() as p:
        illusts = await call(p, user_id=user_id, type=type, offset=offset)
    return illusts


@pixiv_router.post('/u/{user_id}/novels', response_model=List[models.Novel])
async def user_novels(user_id: int,
                      offset: Optional[int] = None,
                      local: bool = False):
    if local is True:
        call = Pixiv.user_novels_local
    else:
        call = Pixiv.user_novels
    async with Pixiv() as p:
        novels = await call(p, user_id=user_id, offset=offset)
    return novels


@pixiv_router.post('/u/{user_id}/bookmarks_illust',
                   response_model=List[models.Illust])
async def user_bookmarks_illust(user_id: int,
                                offset: Optional[int] = None,
                                local: bool = False):
    if local is True:
        call = Pixiv.user_bookmarks_illust_local
    else:
        call = Pixiv.user_bookmarks_illust
    async with Pixiv() as p:
        illusts = await call(p, user_id=user_id, offset=offset)
    return illusts


@pixiv_router.post('/u/{user_id}/related', response_model=List[models.User])
async def user_related(user_id: int, offset: Optional[int] = None):
    call = Pixiv.user_related
    async with Pixiv() as p:
        users = await call(p, seed_user_id=user_id, offset=offset)
    return users


@pixiv_router.post('/u/{user_id}/follow_add',
                   include_in_schema=False,
                   status_code=204)
async def user_follow_add(
        user_id: int,
        restrict: Literal['public', 'private'] = 'public') -> None:
    call = Pixiv.user_follow_add
    await call(Pixiv(), user_id=user_id, restrict=restrict)


@pixiv_router.post('/u/{user_id}/follow_del',
                   include_in_schema=False,
                   status_code=204)
async def user_follow_del(user_id: int):
    call = Pixiv.user_follow_del
    await call(Pixiv(), user_id=user_id)


@pixiv_router.post('/user_bookmark_tags_illust', include_in_schema=False)
async def user_bookmark_tags_illust(restrict: Literal['public',
                                                      'private'] = 'public',
                                    offset: Optional[int] = None):
    call = Pixiv.user_bookmark_tags_illust
    async with Pixiv() as p:
        data = await call(p, restrict=restrict, offset=offset)
    return data


@pixiv_router.post('/u/{user_id}/following', response_model=List[models.User])
async def user_following(user_id: int,
                         offset: Optional[int] = None,
                         local: bool = False):
    if local is True:
        call = Pixiv.user_following_local
    else:
        call = Pixiv.user_following
    async with Pixiv() as p:
        users = await call(p, user_id=user_id, offset=offset)
    return users


@pixiv_router.post('/u/{user_id}/follower', include_in_schema=False)
async def user_follower(offset: Optional[int] = None, local: bool = False):
    if local is True:
        call = Pixiv.user_follower_local
    else:
        call = Pixiv.user_follower
    async with Pixiv() as p:
        users = await call(p, offset=offset)
    return users


@pixiv_router.post('/u/{user_id}/mypixiv', response_model=List[models.User])
async def user_mypixiv(user_id: int,
                       offset: Optional[int] = None,
                       local: bool = False):
    if local is True:
        call = Pixiv.user_mypixiv_local
    else:
        call = Pixiv.user_mypixiv
    async with Pixiv() as p:
        users = await call(p, user_id=user_id, offset=offset)
    return users


@pixiv_router.post('/u/{user_id}/list', include_in_schema=False)
async def user_list(offset: Optional[int] = None, local: bool = False):
    if local is True:
        call = Pixiv.user_list_local
    else:
        call = Pixiv.user_list
    async with Pixiv() as p:
        users = await call(p, offset=offset)
    return users


# illust
@pixiv_router.get('/i/{illust_id}')
async def illust_image(illust_id: int):
    async with Pixiv() as p:
        illust = await p.illust_detail_local(illust_id=illust_id)
        if illust is None:
            illust = await p.illust_detail(illust_id=illust_id)
    if illust is not None:
        return HTMLResponse(f'<title>{illust_id}</title>'
                            f'<img src="{illust.image()}" '
                            'style="max-height: 100%; max-width: 100%">')
    else:
        return HTMLResponse(f'Cannot find illust {illust_id}!')


@pixiv_router.get('/i/{illust_id}/p{page}')
async def illust_image_page(illust_id: int, page: int, req: Request):
    async with Pixiv() as p:
        illust = await p.illust_detail_local(illust_id=illust_id)
        if illust is None:
            illust = await p.illust_detail(illust_id=illust_id)
    if illust is not None:
        try:
            return HTMLResponse(f'<title>{illust_id}-p{page}</title>'
                                f'<img src="{illust.image(page)}" '
                                'style="max-height: 100%; max-width: 100%">')
        except ValueError as e:
            message, minimum, maximum = e.args
            if page < minimum:
                return RedirectResponse(
                    req.url_for('illust_image_page',
                                illust_id=illust_id,
                                page=minimum))
            elif page > maximum:
                return RedirectResponse(
                    req.url_for('illust_image_page',
                                illust_id=illust_id,
                                page=maximum))
    else:
        return HTMLResponse(f'Cannot find illust {illust_id}!')


@pixiv_router.post('/i/{illust_id}', response_model=models.Illust)
async def illust_detail(illust_id: int, local: bool = False):
    if local is True:
        call = Pixiv.illust_detail_local
    else:
        call = Pixiv.illust_detail
    async with Pixiv() as p:
        illust = await call(p, illust_id=illust_id)
    return illust


@pixiv_router.post('/illust_follow', include_in_schema=False)
async def illust_follow(restrict: Literal['public', 'private'] = 'public',
                        offset: Optional[int] = None,
                        local: bool = False):
    if local is True:
        call = Pixiv.illust_follow_local
    else:
        call = Pixiv.illust_follow
    async with Pixiv() as p:
        illusts = await call(p, restrict=restrict, offset=offset)
    return illusts


@pixiv_router.post('/i/{illust_id}/comments',
                   response_model=List[models.IllustComment])
async def illust_comments(illust_id: int,
                          offset: Optional[int] = None,
                          local: bool = False):
    if local is True:
        call = Pixiv.illust_comments_local
    else:
        call = Pixiv.illust_comments
    async with Pixiv() as p:
        comments = await call(p, illust_id=illust_id, offset=offset)
    return comments


@pixiv_router.post('/i/{illust_id}/related',
                   response_model=List[models.Illust])
async def illust_related(illust_id: int, offset: Optional[int] = None):
    call = Pixiv.illust_related
    async with Pixiv() as p:
        illusts = await call(p, illust_id=illust_id, offset=offset)
    return illusts


@pixiv_router.post('/illust_recommended', response_model=List[models.Illust])
async def illust_recommended(content_type: Literal['illust',
                                                   'manga'] = 'illust',
                             offset: Optional[int] = None):
    call = Pixiv.illust_recommended
    async with Pixiv() as p:
        illusts = await call(p, content_type=content_type, offset=offset)
    return illusts


@pixiv_router.post('/illust_ranking', response_model=List[models.Illust])
async def illust_ranking(
        mode: Literal['day', 'week', 'month', 'day_male', 'day_female',
                      'week_original', 'week_rookie', 'day_manga', 'day_r18',
                      'day_male_r18', 'day_female_r18', 'week_r18',
                      'week_r18g'] = 'day',
        date: Optional[str] = Query(None, regex=r'^\d{4}-\d{2}-\d{2}$'),
        offset: Optional[int] = None,
        local: bool = False):
    if local is True:
        call = Pixiv.illust_ranking_local
    else:
        call = Pixiv.illust_ranking
    async with Pixiv() as p:
        illusts = await call(p, mode=mode, date=date, offset=offset)
    return illusts


@pixiv_router.post('/trending_tags_illust',
                   response_model=List[models.TrendingTagsIllust])
async def trending_tags_illust():
    call = Pixiv.trending_tags_illust
    async with Pixiv() as p:
        trending = await call(p)
    return trending


@pixiv_router.post('/i/{illust_id}/bookmark_detail', include_in_schema=False)
async def illust_bookmark_detail(illust_id: int):
    call = Pixiv.illust_bookmark_detail
    async with Pixiv() as p:
        detail = await call(p, illust_id=illust_id)
    return detail


@pixiv_router.post('/i/{illust_id}/bookmark_add',
                   include_in_schema=False,
                   status_code=204)
async def illust_bookmark_add(illust_id: int,
                              restrict: Literal['public',
                                                'private'] = 'public'):
    call = Pixiv.illust_bookmark_add
    async with Pixiv() as p:
        await call(p, illust_id=illust_id, restrict=restrict)


@pixiv_router.post('/i/{illust_id}/bookmark_del',
                   include_in_schema=False,
                   status_code=204)
async def illust_bookmark_del(illust_id: int):
    call = Pixiv.illust_bookmark_delete
    async with Pixiv() as p:
        await call(p, illust_id=illust_id)


@pixiv_router.post('/i/{illust_id}/ugoira_metadata',
                   response_model=models.UgoiraMetadata)
async def ugoira_metadata(illust_id: int):
    call = Pixiv.ugoira_metadata
    async with Pixiv() as p:
        ugoira_meta = await call(p, illust_id=illust_id)
    return ugoira_meta


# novel
@pixiv_router.get('/n/{novel_id}')
async def novel_article(novel_id: int):
    async with Pixiv() as p:
        novel = await p.novel_detail_local(novel_id=novel_id)
        if novel is None:
            novel = await p.novel_detail(novel_id=novel_id)
    if novel is not None:
        return HTMLResponse(f'<title>{novel.id}</title>'
                            f'<img src="{novel.large}" '
                            'style="max-height: 100%; max-width: 100%">'
                            '<article><p>' +
                            '</p><p>'.join(novel.content.split('\n')) +
                            '</p></article>')
    else:
        return HTMLResponse(f'Cannot find novel {novel_id}!')


@pixiv_router.post('/n/{novel_id}', response_model=models.Novel)
async def novel_detail(novel_id: int, local: bool = False):
    if local is True:
        call = Pixiv.novel_detail_local
    else:
        call = Pixiv.novel_detail
    async with Pixiv() as p:
        novel = await call(p, novel_id=novel_id)
    return novel


@pixiv_router.post('/n/{novel_id}/text', response_model=models.NovelText)
async def novel_text(novel_id: int, local: bool = False):
    if local is True:
        call = Pixiv.novel_text_local
    else:
        call = Pixiv.novel_text
    async with Pixiv() as p:
        text = await call(p, novel_id=novel_id)
    return text


@pixiv_router.post('/n/series/{series_id}', response_model=List[models.Novel])
async def novel_series(series_id: int, local: bool = False):
    if local is True:
        call = Pixiv.novel_series_local
    else:
        call = Pixiv.novel_series
    async with Pixiv() as p:
        novels = await call(p, series_id=series_id)
    return novels


# showcase
@pixiv_router.post('/sc/{showcase_id}', response_model=models.Showcase)
async def showcase_article(showcase_id: int, local: bool = False):
    if local is True:
        call = Pixiv.showcase_article_local
    else:
        call = Pixiv.showcase_article
    async with Pixiv() as p:
        showcases = await call(p, showcase_id=showcase_id)
    return showcases


# search
@pixiv_router.post('/s/u', response_model=List[models.User])
async def search_user(word: List[str] = Query(...),
                      sort: Literal['date_desc', 'date_asc'] = 'date_desc',
                      duration: Optional[Literal['within_last_day',
                                                 'within_last_week',
                                                 'within_last_month']] = None,
                      offset: Optional[int] = None,
                      local: bool = False):
    if local is True:
        call = Pixiv.search_user_local
    else:
        call = Pixiv.search_user
    async with Pixiv() as p:
        users = await call(
            p,
            word=' '.join(word),
            sort=sort,
            duration=duration,
            offset=offset,
        )
    return users


@pixiv_router.post('/s/i', response_model=List[models.Illust])
async def search_illust(
        word: List[str] = Query(...),
        search_target: Literal['partial_match_for_tags',
                               'exact_match_for_tags',
                               'title_and_caption'] = 'partial_match_for_tags',
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        duration: Optional[Literal['within_last_day', 'within_last_week',
                                   'within_last_month']] = None,
        offset: Optional[int] = None,
        start_date: Optional[str] = Query(None, regex=r'^\d{4}-\d{2}-\d{2}$'),
        end_date: Optional[str] = Query(None, regex=r'^\d{4}-\d{2}-\d{2}$'),
        min_bookmarks: Optional[int] = None,
        max_bookmarks: Optional[int] = None,
        local: bool = False):
    if local is True:
        call = Pixiv.search_illust_local
    else:
        call = Pixiv.search_illust
    async with Pixiv() as p:
        illusts = await call(
            p,
            word=' '.join(word),
            search_target=search_target,
            sort=sort,
            duration=duration,
            offset=offset,
            start_date=start_date,
            end_date=end_date,
            min_bookmarks=min_bookmarks,
            max_bookmarks=max_bookmarks,
        )
    return illusts


@pixiv_router.post('/s/n', response_model=List[models.Novel])
async def search_novel(
        word: List[str] = Query(...),
        search_target: Literal['partial_match_for_tags',
                               'exact_match_for_tags', 'text',
                               'Keyword'] = 'partial_match_for_tags',
        sort: Literal['date_desc', 'date_asc'] = 'date_desc',
        start_date: Optional[str] = Query(None, regex=r'^\d{4}-\d{2}-\d{2}$'),
        end_date: Optional[str] = Query(None, regex=r'^\d{4}-\d{2}-\d{2}$'),
        offset: Optional[int] = None,
        local: bool = False):
    if local is True:
        call = Pixiv.search_novel_local
    else:
        call = Pixiv.search_novel
    async with Pixiv() as p:
        novels = await call(
            p,
            word=' '.join(word),
            search_target=search_target,
            sort=sort,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
        )
    return novels


@pixiv_router.get('/r/i')
async def random_illust_image(min_view: int = 10000,
                              min_bookmarks: int = 1000,
                              tag: List[str] = Query(['ロリ']),
                              allow_r18: bool = False,
                              allow_r18g: bool = False):
    async with Pixiv() as p:
        choice = await p.random_illust(
            min_view=min_view,
            min_bookmarks=min_bookmarks,
            tags=tag,
            allow_r18=allow_r18,
            allow_r18g=allow_r18g,
            limit=1,
        )
    if choice is not None:
        return HTMLResponse('<title>Random Image</title>'
                            f'<img src="{choice.image(random=True)}" '
                            'style="max-height: 100%; max-width: 100%">')
    else:
        return HTMLResponse('Cannot get random image! Check parameters.')


@pixiv_router.post('/r/i',
                   response_model=Union[models.Illust, List[models.Illust]])
async def random_illust(min_view: int = 10000,
                        min_bookmarks: int = 1000,
                        tag: List[str] = Query(['ロリ']),
                        allow_r18: bool = False,
                        allow_r18g: bool = False,
                        limit: int = 1):
    async with Pixiv() as p:
        choices = await p.random_illust(
            min_view=min_view,
            min_bookmarks=min_bookmarks,
            tags=tag,
            allow_r18=allow_r18,
            allow_r18g=allow_r18g,
            limit=limit,
        )
    return choices


@scheduler.scheduled_job('cron', hour=1, jitter=3600)
async def auto_ranking():
    async with Pixiv() as p:
        await p.illust_ranking('day')


@scheduler.scheduled_job('cron', hour='*/6', jitter=600)
async def auto_trend_tags():
    async with Pixiv() as p:
        await p.trending_tags_illust()
