from typing import Tuple, Optional
import uuid
import asyncio
from .auth import session_id_decode_from_str, get_tokens
from .config import get_deploy_config
from .tls import get_context_specific_client_ssl_context


BYTE_ORDER = 'big'
STRING_ENCODING = 'utf-8'


class HailTCPConnectionError(Exception):
    pass


CLIENT_TLS_CONTEXT = get_context_specific_client_ssl_context()


async def open_connection(
        service: str,
        port: int,
        **kwargs
) -> Tuple[uuid.UUID, asyncio.StreamReader, asyncio.StreamWriter]:
    assert port < (1 << 16)
    deploy_config = get_deploy_config()
    ns = deploy_config.service_ns(service)
    location = deploy_config.location()

    if location == 'k8s':
        return await open_direct_connection(service, ns, port, **kwargs)

    if location == 'gce':
        proxy_hostname = 'hail'
    else:
        assert location == 'external'
        proxy_hostname = 'hail.is'

    return await open_proxied_connection(proxy_hostname, 5000, service, ns, port, **kwargs)


async def open_direct_connection(service: str,
                                 ns: str,
                                 port: int,
                                 *,
                                 session_ids: Optional[Tuple[bytes, bytes]] = None,
                                 request_id: Optional[uuid.UUID] = None,
                                 **kwargs
                                 ) -> Tuple[uuid.UUID, asyncio.StreamReader, asyncio.StreamWriter]:
    if 'ssl' not in kwargs:
        kwargs['ssl'] = CLIENT_TLS_CONTEXT,
    if 'loop' not in kwargs:
        kwargs['loop'] = asyncio.get_event_loop(),

    reader, writer = await asyncio.open_connection(
        f'{service}.{ns}',
        port,
        **kwargs)
    write_session_ids(writer, ns, session_ids=session_ids)

    await writer.drain()
    is_success = await reader.read(1)
    if is_success != b'\x01':
        raise HailTCPConnectionError(f'{service}.{ns}:{port}')

    request_id = uuid.uuid4()
    assert len(request_id.bytes) == 16
    writer.write(request_id.bytes)

    return request_id, reader, writer


async def open_proxied_connection(proxy_hostname: str,
                                  proxy_port: int,
                                  service: str,
                                  ns: str,
                                  port: int,
                                  *,
                                  session_ids: Optional[Tuple[bytes, bytes]] = None,
                                  request_id: Optional[uuid.UUID] = None,
                                  **kwargs
                                  ) -> Tuple[uuid.UUID, asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection(
        proxy_hostname,
        proxy_port,
        loop=asyncio.get_event_loop(),
        ssl=CLIENT_TLS_CONTEXT,
        **kwargs)

    await write_session_ids(writer, ns, session_ids=session_ids)

    writer.write(len(ns).to_bytes(4, BYTE_ORDER))
    writer.write(ns.encode(STRING_ENCODING))

    writer.write(len(service).to_bytes(4, BYTE_ORDER))
    writer.write(service.encode(STRING_ENCODING))

    writer.write((port).to_bytes(2, BYTE_ORDER))

    await writer.drain()
    is_success = await reader.read(1)
    if is_success != b'\x01':
        raise HailTCPConnectionError(f'{service}.{ns}:{port} {is_success!r}')

    request_id_bytes = await reader.read(16)
    request_id = uuid.UUID(bytes=request_id_bytes)

    return request_id, reader, writer


async def write_session_ids(writer: asyncio.StreamWriter,
                            ns: str,
                            *,
                            session_ids: Optional[Tuple[bytes, bytes]] = None):
    if session_ids is None:
        tokens = get_tokens()
        default_session_id = session_id_decode_from_str(tokens.namespace_token_or_error('default'))
        assert len(default_session_id) == 32
        namespace_session_id = b'\x00' * 32
        if ns != 'default':
            namespace_session_id = session_id_decode_from_str(tokens.namespace_token_or_error(ns))
            assert len(namespace_session_id) == 32
    else:
        assert len(session_ids) == 2, session_ids
        default_session_id, namespace_session_id = session_ids

    writer.write(default_session_id)
    writer.write(namespace_session_id)
