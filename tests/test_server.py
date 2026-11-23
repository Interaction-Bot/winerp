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
    srv.liveness_timeout = 0.5
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


async def make_client_on_hold(server, name, **routes):
    client = winerp.Client(local_name=name, host='127.0.0.1', port=server.websocket.port)
    client.reconnect_threshold = 0.2
    for route_name, value in routes.items():
        async def handler(source, value=value):
            return value
        await client.add_route(handler, route_name)
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
    send_frame(sock, json.dumps({'type': 1, 'id': name, 'uuid': 'v'}).encode())
    return sock


def send_frame(sock, data):
    mask = os.urandom(4)
    sock.sendall(bytes([0x81, 0x80 | len(data)]) + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))


async def test_new_connection_replaces_a_dead_one(server):
    """The incident of 2026-11-23: the old "Dashboard" connection never left, the new backend stayed on hold."""
    ghost = raw_verified_connection(server, 'Dashboard')       # verified, then never answers (gone, frozen)
    await until(lambda: 'Dashboard' in server.active_clients)
    asker = await make_client(server, 'Cluster 0')
    new = await make_client_on_hold(server, 'Dashboard', who='new')
    await until(lambda: new.authorized and not new.on_hold)     # no pong from the ghost: replaced
    assert await asker.request('who', 'Dashboard', timeout=5) == 'new'
    ghost.settimeout(2)
    while ghost.recv(4096):                                    # the server closed the ghost
        pass
    ghost.close()
    await asyncio.sleep(0.3)                                   # its disconnection does not remove the new one
    assert await asker.request('who', 'Dashboard', timeout=5) == 'new'
    assert sorted(server.active_clients) == ['Cluster 0', 'Dashboard']
    assert not server.on_hold_connections and not server.errors


async def test_live_connection_is_never_ejected(server):
    """Two live processes with the same name (should not happen): the first keeps the name, no reconnection loop."""
    first = await make_client(server, 'Dashboard', who='first')
    asker = await make_client(server, 'Cluster 0')
    second = await make_client_on_hold(server, 'Dashboard', who='second')
    connections = server.websocket.id_counter
    await asyncio.sleep(server.liveness_timeout * 3)
    assert second.on_hold and first.authorized
    assert server.websocket.id_counter == connections
    assert await asker.request('who', 'Dashboard', timeout=5) == 'first'
    first.reconnect = False
    await first.websocket.close()                               # the first one leaves: the held one takes over
    await until(lambda: second.authorized and not second.on_hold)
    assert await asker.request('who', 'Dashboard', timeout=5) == 'second'


async def test_held_client_is_promoted_when_the_old_one_dies_later(server):
    server.liveness_interval = 0.5
    ghost = raw_verified_connection(server, 'Dashboard')
    await until(lambda: 'Dashboard' in server.active_clients)
    answer = server._pong_received                             # the ghost answers the first pings...
    server.websocket._pong_received_ = answer
    alive = {'yes': True}
    real_ping = server._ping

    def ping(handler):
        if alive['yes']:
            answer(handler, '')
        else:
            real_ping(handler)
    server._ping = ping
    new = await make_client_on_hold(server, 'Dashboard', who='new')
    await asyncio.sleep(1)
    assert new.on_hold                                          # still answering: stays
    alive['yes'] = False                                        # ...then stops answering
    await until(lambda: new.authorized and not new.on_hold)
    ghost.close()


async def test_blocked_send_to_the_old_connection_does_not_block_the_new_one(server):
    """The old peer is gone with a full buffer: the sends to it block, the new client is still authorized."""
    ghost = raw_verified_connection(server, 'Dashboard')
    ghost.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    await until(lambda: 'Dashboard' in server.active_clients)
    cluster = await make_client(server, 'Cluster 0')
    # not awaited: the server thread of the cluster blocks on the ghost, so it stops reading the cluster too
    flood = [asyncio.create_task(cluster.inform({'x': 'y' * 200_000}, destinations=['Dashboard'])) for _ in range(40)]
    await asyncio.sleep(0.5)
    assert not all(task.done() for task in flood)               # the sends are really blocked
    new = await make_client_on_hold(server, 'Dashboard', who='new')
    await until(lambda: new.authorized and not new.on_hold)
    await asyncio.wait_for(asyncio.gather(*flood), 10)         # closing the ghost unblocked the cluster
    assert await cluster.request('who', 'Dashboard', timeout=5) == 'new'
    ghost.close()


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


async def test_unreadable_message_does_not_close_the_connection(server):
    sock = raw_verified_connection(server, 'Cluster 5')
    await until(lambda: 'Cluster 5' in server.active_clients)
    send_frame(sock, b'not json')
    asker = await make_client(server, 'Cluster 6')
    await asyncio.sleep(0.3)
    assert 'Cluster 5' in server.active_clients
    sock.close()
