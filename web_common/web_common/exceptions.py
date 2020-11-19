from aiohttp import web
from hailtop.utils import HailHTTPUserError
from .web_common import set_message


async def handle_error_for_web(session, f, *args, **kwargs):
    try:
        await f(*args, **kwargs)
    except HailHTTPUserError as e:
        set_message(session, e.message, e.severity)
        return True
    else:
        return False
