from utils.schedule.apscheduler import scheduler
from .pixiv import Pixiv


@scheduler.scheduled_job('cron', hour=1, jitter=3600)
async def auto_ranking():
    async with Pixiv() as p:
        await p.illust_ranking('day')


@scheduler.scheduled_job('cron', hour='*/6', jitter=600)
async def auto_trend_tags():
    async with Pixiv() as p:
        await p.trending_tags_illust()
