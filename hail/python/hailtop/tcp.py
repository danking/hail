from typing import Tuple
import asyncio
from .auth import session_id_decode_from_str, get_tokens
from .config import get_deploy_config
from .tls import get_context_specific_client_ssl_context


BYTE_ORDER = 'big'
STRING_ENCODING = 'utf-8'


class HailTCPProxyConnectionError(Exception):
    pass


CLIENT_TLS_CONTEXT = get_context_specific_client_ssl_context()


async def open_connection(
        service: str,
        port: int,
        **kwargs
) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    assert port < (1 << 16)
    deploy_config = get_deploy_config()
    ns = deploy_config.service_ns(service)
    location = deploy_config.location()

    if location == 'k8s':
        return await asyncio.open_connection(
            f'{service}.{ns}',
            port,
            loop=asyncio.get_event_loop(),
            ssl=CLIENT_TLS_CONTEXT,
            **kwargs)

    if location == 'gce':
        proxy_hostname = 'hail'  # fixme will this direct to the right IP?
    else:
        assert location == 'external'
        proxy_hostname = 'hail.is'
    proxy_port = 5000

    tokens = get_tokens()
    default_session_id = tokens.namespace_token_or_error('default')
    namespace_session_id = None
    if ns != 'default':
        namespace_session_id = tokens.namespace_token_or_error('default')

    r, w = await asyncio.open_connection(
        proxy_hostname,
        proxy_port,
        loop=asyncio.get_event_loop(),
        ssl=CLIENT_TLS_CONTEXT,
        **kwargs)
    session_id_bytes = session_id_decode_from_str(default_session_id)
    assert len(session_id_bytes) == 32
    w.write(session_id_bytes)
    if namespace_session_id:
        namespace_session_id_bytes = session_id_decode_from_str(namespace_session_id)
        assert len(namespace_session_id_bytes) == 32
        w.write(namespace_session_id_bytes)
    else:
        w.write(b'\x00' * 32)

    w.write(len(ns).to_bytes(4, BYTE_ORDER))
    w.write(ns.encode(STRING_ENCODING))

    w.write(len(service).to_bytes(4, BYTE_ORDER))
    w.write(service.encode(STRING_ENCODING))

    w.write((port).to_bytes(2, BYTE_ORDER))

    await w.drain()
    is_success = await r.read(1)
    if not is_success:
        raise HailTCPProxyConnectionError(f'{service}.{ns}:{port}')
    return r, w
