import asyncio
from contextvars import copy_context
from functools import wraps, partial
from inspect import isgenerator, isfunction
from typing import Callable, Coroutine, Generator, AsyncGenerator, Any


def run_sync(func: Callable[..., Any]) \
        -> Callable[..., Coroutine[None, None, Any]]:
    """Ensure that the sync function is run within the event loop.

    If the *func* is not a coroutine it will be wrapped such that
    it runs in the default executor (use loop.set_default_executor
    to change). This ensures that synchronous functions do not
    block the event loop.
    """
    @wraps(func)
    async def _wrapper(*args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            copy_context().run,
            partial(func, *args, **kwargs),
        )
        if isgenerator(result):
            return run_sync_iterable(result)  # type: ignore
        else:
            return result

    _wrapper.__async_wrapped__ = True
    return _wrapper


def run_sync_iterable(iterable: Generator[Any, None, None]) \
        -> AsyncGenerator[Any, None]:
    async def _gen_wrapper() -> AsyncGenerator[Any, None]:
        # Wrap the generator such that each iteration runs
        # in the executor. Then rationalise the raised
        # errors so that it ends.
        def _inner() -> Any:
            # https://bugs.python.org/issue26221
            # StopIteration errors are swallowed by the
            # run_in_exector method
            try:
                return next(iterable)
            except StopIteration:
                raise StopAsyncIteration()

        loop = asyncio.get_running_loop()
        while True:
            try:
                yield await loop.run_in_executor(
                    None,
                    copy_context().run,
                    _inner,
                )
            except StopAsyncIteration:
                return

    return _gen_wrapper()


def _asyncify_wrap(t, method_name):
    method = getattr(t, method_name)

    @wraps(method)
    def asyncified(*args, **kwargs):
        return run_sync(method)(*args, **kwargs)

    # Save an accessible reference to the original method
    setattr(asyncified, '__sync_origin__', method)
    setattr(t, method_name, asyncified)


def asyncify(*types):
    for t in types:
        for name in dir(t):
            if not name.startswith('_') or name == '__call__':
                if isfunction(getattr(t, name)):
                    _asyncify_wrap(t, name)


def new_types_asyncify(*types):
    new_types = []
    for t in types:
        _ = type(t.__name__ + '_async', t, {})
        for name in dir(_):
            if not name.startswith('_') or name == '__call__':
                if isfunction(getattr(_, name)):
                    _asyncify_wrap(_, name)
        new_types.append(_)
    if len(new_types) == 1:
        return new_types[0]
    else:
        return new_types
