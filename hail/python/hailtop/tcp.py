from typing import Tuple, Optional
import uuid
import asyncio
from .auth import session_id_decode_from_str, get_tokens
from .config import get_deploy_config
from .tls import get_context_specific_client_ssl_context


BYTE_ORDER = 'big'
STRING_ENCODING = 'utf-8'


### Router TCP Protocol
# IN: 32 bytes, session_id, bytes
# IN: 32 bytes, internal_session_id, bytes, may be all zeros
# IN: 4 bytes, namespace_name length, unsigned integer
# IN: ?? bytes, namespace_name, UTF-8 string
# IN: 4 bytes, service_name length, unsigned integer
# IN: ?? bytes, service_name, UTF-8 string
# IN: 2 bytes, port, unsigned integer
# OUT: 1 byte, connect_is_successful, unsigned integer, 1 = success, 0 = not-success
# OUT: 16 bytes, connection_id, bytes

### Other Service Authentication Header
# IN: 32 bytes, session_id, bytes
# IN: 32 bytes, internal_session_id, bytes, may be all zeros
# OUT: 1 byte, connect_is_successful, unsigned integer, 1 = success, 0 = not-success
# OUT: 16 bytes, connection_id, bytes


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

    connection_id_bytes = await reader.read(16)
    connection_id = uuid.UUID(bytes=connection_id_bytes)

    return connection_id, reader, writer


async def open_proxied_connection(proxy_hostname: str,
                                  proxy_port: int,
                                  service: str,
                                  ns: str,
                                  port: int,
                                  *,
                                  session_ids: Optional[Tuple[bytes, bytes]] = None,
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

    connection_id_bytes = await reader.read(16)
    connection_id = uuid.UUID(bytes=connection_id_bytes)

    return connection_id, reader, writer


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
