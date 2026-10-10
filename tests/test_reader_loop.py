"""The reader loop of the client must survive anything the server (or a peer) sends it."""
import asyncio
import json

import pytest
import websockets
from websockets.asyncio.server import serve

import winerp

PORT = 18765


class FakeServer:
    """Accepts one client at a time, answers the verification with a success, records what the client sends."""

    def __init__(self):
        self.received = []
        self.current = None
        self.connected = asyncio.Event()

    async def handler(self, ws):
        self.current = ws
        await ws.recv()                                    # verification
        await ws.send(json.dumps({'type': 0}))           # success
        self.connected.set()
        try:
            async for raw in ws:
                self.received.append(json.loads(raw))
        finally:
            self.connected.clear()

    async def wait_response(self, uuid, timeout=5):
        async def loop():
            while True:
                for m in self.received:
                    if m.get('uuid') == uuid:
                        return m
                await asyncio.sleep(0.02)
        return await asyncio.wait_for(loop(), timeout)


def request(route, uuid):
    return json.dumps({'type': 2, 'route': route, 'id': 'peer', 'destination': 'client', 'uuid': uuid, 'data': {}})


async def make_client(server):
    client = winerp.Client(local_name='client', host='127.0.0.1', port=PORT)
    client.reconnect_threshold = 0.2

    @client.route()
    async def hello(source):
        return 'world'

    await client.start()
    await asyncio.wait_for(server.connected.wait(), 5)
    return client


@pytest.fixture
async def server():
    fake = FakeServer()
    async with serve(fake.handler, '127.0.0.1', PORT):
        yield fake


async def test_unknown_route_does_not_kill_the_reader(server):
    client = await make_client(server)
    await server.current.send(request('does_not_exist', 'u1'))
    answer = await server.wait_response('u1')
    assert answer['type'] == 4 and answer['data'] == 'Route not found'
    await server.current.send(request('hello', 'u2'))                  # before the fix: never answered
    assert (await server.wait_response('u2'))['data'] == 'world'


async def test_garbage_does_not_kill_the_reader(server):
    client = await make_client(server)
    await server.current.send('not json at all')
    await server.current.send(json.dumps({'no_type': True}))
    await server.current.send(request('hello', 'u3'))
    assert (await server.wait_response('u3'))['data'] == 'world'


async def test_clean_close_reconnects(server):
    client = await make_client(server)
    await server.current.close(code=1000)                              # ConnectionClosedOK, not ...Error
    await asyncio.wait_for(server.connected.wait(), 5)                  # the client reconnected and verified again
    await server.current.send(request('hello', 'u4'))
    assert (await server.wait_response('u4'))['data'] == 'world'


async def test_connects_when_the_server_starts_late():
    client = winerp.Client(local_name='client', host='127.0.0.1', port=PORT)
    client.reconnect_threshold = 0.2
    @client.route()
    async def hello(source):
        return 'world'
    task = asyncio.create_task(client.start())                         # nobody listens yet
    await asyncio.sleep(0.5)
    fake = FakeServer()
    async with serve(fake.handler, '127.0.0.1', PORT):
        await asyncio.wait_for(fake.connected.wait(), 5)
        await fake.current.send(request('hello', 'u5'))
        assert (await fake.wait_response('u5'))['data'] == 'world'
    await asyncio.wait_for(task, 5)
