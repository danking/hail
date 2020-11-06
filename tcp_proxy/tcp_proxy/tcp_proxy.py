import os
import ssl
import signal
import uuid
import asyncio
from contextlib import closing
import logging
from .constants import BYTE_ORDER, STRING_ENCODING, BUFFER_SIZE
from hailtop.tls import (get_in_cluster_client_ssl_context,
                         get_in_cluster_server_ssl_context)
from hailtop.tcp import (open_direct_connection, open_proxied_connection,
                         HailTCPConnectionError)


log = logging.getLogger('scorecard')
HAIL_DEFAULT_NAMESPACE = os.environ['HAIL_DEFAULT_NAMESPACE']

### Router TCP Protocol
# IN: 32 bytes, session_id, bytes
# IN: 32 bytes, internal_session_id, bytes, may be all zeros
# IN: 4 bytes, namespace_name length, unsigned integer
# IN: ?? bytes, namespace_name, UTF-8 string
# IN: 4 bytes, service_name length, unsigned integer
# IN: ?? bytes, service_name, UTF-8 string
# IN: 2 bytes, port, unsigned integer
# OUT: 1 byte, connect_is_successful, unsigned integer, 1 = success, 0 = not-success
# OUT: 16 bytes, request_id, bytes

### Other Service TCP Protocol
# IN: 32 bytes, session_id, bytes
# IN: 32 bytes, internal_session_id, bytes, may be all zeros
# OUT: 1 byte, connect_is_successful, unsigned integer, 1 = success, 0 = not-success
# IN: 16 bytes, request_id, bytes


async def pipe(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        connection_lost: asyncio.Event,
        name: str,
        log_info
):
    while not reader.at_eof() and not connection_lost.is_set():
        try:
            incoming_data = await asyncio.wait_for(reader.read(BUFFER_SIZE), timeout=10)
        except asyncio.TimeoutError:
            continue
        except ConnectionResetError:
            log_info(f'{name}: read-side dropped the connection')
            connection_lost.set()
            return

        writer.write(incoming_data)

        try:
            await writer.drain()
        except ConnectionResetError:
            log_info(f'{name} write-side dropped the connection')
            connection_lost.set()
            return

    if reader.at_eof():
        log_info(f'{name}: EOF')
    else:
        assert connection_lost.is_set()
        log.info(f'{name}: other side of pipe was lost')

    if writer.can_write_eof():
        writer.write_eof()
    connection_lost.set()
    return


SERVER_TLS_CONTEXT = get_in_cluster_server_ssl_context()

async def handle(source_reader: asyncio.StreamReader, source_writer: asyncio.StreamWriter):
    source_addr = source_writer.get_extra_info('peername')

    session_id = await source_reader.read(32)
    maybe_internal_session_id = await source_reader.read(32)
    namespace_name_length = int.from_bytes(await source_reader.read(4), byteorder=BYTE_ORDER, signed=False)
    namespace_name = (await source_reader.read(namespace_name_length)).decode(encoding=STRING_ENCODING)
    service_name_length = int.from_bytes(await source_reader.read(4), byteorder=BYTE_ORDER, signed=False)
    service_name = (await source_reader.read(service_name_length)).decode(encoding=STRING_ENCODING)
    port = int.from_bytes(await source_reader.read(2), byteorder=BYTE_ORDER, signed=False)

    try:
        if '.' in service_name or '.' in namespace_name:
            raise HailTCPConnectionError(f'malformed request: {namespace_name} {service_name}')

        hostname = f'{service_name}.{namespace_name}'
        target_addr = (hostname, port)

        if namespace_name == HAIL_DEFAULT_NAMESPACE:
            request_id, target_reader, target_writer = await open_direct_connection(
                service_name, namespace_name, port,
                session_ids=(session_id, maybe_internal_session_id))
        elif HAIL_DEFAULT_NAMESPACE == 'default':  # default router may forward to namespaced ones
            # we do not verify namespaced certs
            client_tls_context = ssl.create_default_context()
            client_tls_context.check_hostname = False
            client_tls_context.verify_mode = ssl.CERT_NONE
            request_id, target_reader, target_writer = await open_proxied_connection(
                proxy_hostname=f'router.{namespace_name}',
                proxy_port=5000,
                service=service_name,
                ns=namespace_name,
                port=port,
                session_ids=(maybe_internal_session_id, b'\x00' * 32),
                ssl=client_tls_context)
        else:
            raise HailTCPConnectionError(
                f'denying request to access {service_name}.{namespace_name} sent to me in {HAIL_DEFAULT_NAMESPACE}')
    except HailTCPConnectionError:
        log.info(f'could not connect to {hostname}:{port} on behalf of {source_addr}', exc_info=True)
        source_writer.write(b'\x00')
        source_writer.close()
        return

    request_metadata = {
        'request_id': request_id,
        'source_addr': source_addr,
        'target_addr': target_addr
    }

    def log_info(*args, **kwargs):
        kwargs['extra'] = request_metadata
        log.info(*args, **kwargs)

    log_info(f'OPEN')

    source_writer.write(b'\x01')
    assert len(request_id.bytes) == 16
    source_writer.write(request_id.bytes)

    source_to_target = None
    target_to_source = None
    try:
        connection_lost = asyncio.Event()
        source_to_target = asyncio.create_task(
            pipe(source_reader, target_writer, connection_lost, 'source->target', log_info))
        target_to_source = asyncio.create_task(
            pipe(target_reader, source_writer, connection_lost, 'target->source', log_info))
        await source_to_target
        await target_to_source
    finally:
        log_info(f'CLOSING')
        target_writer.close()
        source_writer.close()
        if source_to_target is not None:
            source_to_target.cancel()
        if target_to_source is not None:
            target_to_source.cancel()
        log_info(f'CLOSED')


async def run_server(host, port):
    with closing(await asyncio.start_server(
            handle,
            host,
            port,
            loop=asyncio.get_event_loop(),
            ssl=SERVER_TLS_CONTEXT)):
        print(f'Serving on {host}:{port}')
        while True:
            await asyncio.sleep(3600)


async def shutdown(signal, loop):
    log.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    for task in tasks:
        task.cancel()

    logging.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


def run():
    host = '0.0.0.0'
    port = 5000
    with closing(asyncio.get_event_loop()) as loop:
        for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

        loop.run_until_complete(run_server(host, port))
