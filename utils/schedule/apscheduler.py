"""
Depend on APScheduler.
https://github.com/agronholm/apscheduler
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()
