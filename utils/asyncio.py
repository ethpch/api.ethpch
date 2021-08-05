import asyncio
import platform

policy = None


def auto_event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    global policy
    if platform.system() == 'Windows':
        policy = asyncio.WindowsSelectorEventLoopPolicy()
    elif platform.system() == 'Linux':
        try:
            import uvloop
            policy = uvloop.EventLoopPolicy()
        except ModuleNotFoundError:
            pass
    asyncio.set_event_loop_policy(policy)
    return policy


def auto_event_loop() -> asyncio.AbstractEventLoop:
    if isinstance(asyncio.get_event_loop_policy(), type(policy)):
        loop = asyncio.get_event_loop()
    else:
        auto_event_loop_policy()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop
