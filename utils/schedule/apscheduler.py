"""
Depend on APScheduler.
https://github.com/agronholm/apscheduler
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utils.general import importer

scheduler = AsyncIOScheduler()


def import_scheduled_job():
    importer('scheduled')
