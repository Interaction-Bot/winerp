"""The server with real clients: a client that connects again under the same name replaces its old connection."""
import asyncio
import base64
import json
import os
import socket
import sys

import pytest

import winerp


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


@pytest.fixture
def server():
    srv = winerp.Server(host='127.0.0.1', port=free_port())
    errors = []
    handle_error = srv.websocket.handle_error

    def record(request, address):
        errors.append(sys.exc_info()[1])
        handle_error(request, address)

    srv.websocket.handle_error = record
    srv.errors = errors
    srv.websocket.run_forever(threaded=True)
    yield srv
    srv.websocket.shutdown_abruptly()


async def make_client(server, name, **routes):
    client = winerp.Client(local_name=name, host='127.0.0.1', port=server.websocket.port)
    client.reconnect_threshold = 0.2
    for route_name, value in routes.items():
        async def handler(source, value=value):
            return value
        await client.add_route(handler, route_name)
    await client.start()
    await asyncio.wait_for(client.wait_until_ready(), 5)
    return client


async def make_client_on_hold(server, name):
    client = winerp.Client(local_name=name, host='127.0.0.1', port=server.websocket.port)
    await client.start()
    await until(lambda: client.on_hold)
    return client


async def until(predicate, timeout=5):
    async def loop():
        while not predicate():
            await asyncio.sleep(0.02)
    await asyncio.wait_for(loop(), timeout)


def raw_verified_connection(server, name):
    """A websocket connection that verifies under `name` then never reads nor answers again (the peer is gone)."""
    sock = socket.create_connection(('127.0.0.1', server.websocket.port))
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall((
        'GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
        'Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n' % key
    ).encode())
    assert b'101' in sock.recv(1024)
    data = json.dumps({'type': 1, 'id': name, 'uuid': 'v'}).encode()
    mask = os.urandom(4)
    sock.sendall(bytes([0x81, 0x80 | len(data)]) + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))
    return sock


async def test_same_name_replaces_the_old_connection(server):
    old = await make_client(server, 'Dashboard')
    old.reconnect = False                                       # the old process is going away
    server.active_clients['Dashboard']['since'] -= 60                  # connected for a while
    asker = await make_client(server, 'Cluster 0')
    new = await make_client(server, 'Dashboard', who='new')     # before: put on hold, "Already authorized."
    assert new.authorized and not new.on_hold
    assert await asker.request('who', 'Dashboard', timeout=5) == 'new'
    await until(lambda: len(server.active_clients) == 2)
    # the disconnection of the replaced connection does not remove the new one
    await asyncio.sleep(0.3)
    assert server.active_clients['Dashboard']['client'] is not None
    assert await asker.request('who', 'Dashboard', timeout=5) == 'new'
    assert sorted(server.active_clients) == ['Cluster 0', 'Dashboard']
    assert not server.errors


async def test_half_open_old_connection_does_not_block_the_new_one(server):
    """The incident of 2026-11-23: the old "Dashboard" connection never left, the new backend stayed on hold."""
    ghost = raw_verified_connection(server, 'Dashboard')
    await until(lambda: 'Dashboard' in server.active_clients)
    server.active_clients['Dashboard']['since'] -= 60                  # connected for a while
    asker = await make_client(server, 'Cluster 0')
    new = await make_client(server, 'Dashboard', who='new')
    assert new.authorized and not new.on_hold
    assert await asker.request('who', 'Dashboard', timeout=5) == 'new'
    ghost.settimeout(2)
    received = b''
    while chunk := ghost.recv(4096):                           # the server closed the ghost
        received += chunk
    assert received.endswith(b'Replaced by a new connection')
    ghost.close()


async def test_two_live_processes_with_the_same_name_do_not_flap(server):
    """Both processes alive (should not happen): the replaced one reconnects at once and waits on hold, no loop."""
    first = await make_client(server, 'Dashboard', who='first')
    server.active_clients['Dashboard']['since'] -= 60                  # connected for a while
    asker = await make_client(server, 'Cluster 0')
    second = await make_client(server, 'Dashboard', who='second')
    await until(lambda: first.on_hold)                          # replaced, reconnected, on hold
    connections = server.websocket.id_counter
    await asyncio.sleep(0.5)
    assert server.websocket.id_counter == connections           # no reconnection loop
    assert await asker.request('who', 'Dashboard', timeout=5) == 'second'
    second.reconnect = False
    await second.websocket.close()                              # the other one leaves: the held one takes over
    await until(lambda: first.authorized and not first.on_hold)
    assert await asker.request('who', 'Dashboard', timeout=5) == 'first'


async def test_recent_connection_is_not_replaced(server):
    first = await make_client(server, 'Dashboard', who='first')
    asker = await make_client(server, 'Cluster 0')
    second = await make_client_on_hold(server, 'Dashboard')
    assert second.on_hold
    assert await asker.request('who', 'Dashboard', timeout=5) == 'first'


async def test_tcp_probe_does_not_crash_the_server(server):
    for _ in range(3):
        socket.create_connection(('127.0.0.1', server.websocket.port)).close()
    probe = socket.create_connection(('127.0.0.1', server.websocket.port))
    probe.sendall(b'HEAD / HTTP/1.0\r\n\r\n')
    probe.close()
    await asyncio.sleep(0.3)
    # only the handshake refusal of websocket_server, no TypeError from the disconnection handler
    assert server.errors and all(isinstance(e, AssertionError) for e in server.errors)
    client = await make_client(server, 'Cluster 1', ping='pong')
    asker = await make_client(server, 'Cluster 2')
    assert await asker.request('ping', 'Cluster 1', timeout=5) == 'pong'


async def test_disconnect_frees_the_name(server):
    client = await make_client(server, 'Cluster 3')
    client.reconnect = False
    await client.websocket.close()
    await until(lambda: 'Cluster 3' not in server.active_clients)
    asker = await make_client(server, 'Cluster 4')
    with pytest.raises(Exception):
        await asker.request('x', 'Cluster 3', timeout=2)


async def test_replaced_connection_closing_keeps_the_new_one(server):
    sock = raw_verified_connection(server, 'Dashboard')
    await until(lambda: 'Dashboard' in server.active_clients)
    server.active_clients['Dashboard']['since'] -= 60                  # connected for a while
    new = await make_client(server, 'Dashboard')
    sock.close()
    await asyncio.sleep(0.3)
    assert server.active_clients['Dashboard']['client']['id'] == max(c['id'] for c in server.websocket.clients)
