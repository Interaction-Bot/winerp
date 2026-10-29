"""The listener of a request must not outlive its request (it used to stay in client.listeners for ever, with the whole answer)."""
import asyncio
import json

import pytest
from websockets.asyncio.server import serve

import winerp

PORT = 18766


class Peer:
    """A server that verifies the client, then answers (or not) the requests it receives."""

    def __init__(self, answer=True):
        self.answer = answer
        self.current = None
        self.connected = asyncio.Event()
        self.requests = []

    async def handler(self, ws):
        self.current = ws
        await ws.recv()                                                  # verification
        await ws.send(json.dumps({'type': 0}))
        self.connected.set()
        async for raw in ws:
            message = json.loads(raw)
            if message.get('type') == 2:
                self.requests.append(message)
                if self.answer:
                    await ws.send(json.dumps({'type': 3, 'uuid': message['uuid'], 'id': 'peer', 'destination': 'client', 'data': {'ok': True, 'blob': 'x' * 2000}}))


@pytest.fixture
async def make():
    peers = []

    async def build(answer=True):
        peer = Peer(answer)
        server = serve(peer.handler, '127.0.0.1', PORT)
        await server.__aenter__()
        peers.append(server)
        client = winerp.Client(local_name='client', host='127.0.0.1', port=PORT)
        await client.start()
        await asyncio.wait_for(peer.connected.wait(), 5)
        for _ in range(100):                                              # the success of the verification is handled by the reader loop
            if client.authorized:
                break
            await asyncio.sleep(0.05)
        return peer, client

    yield build
    for server in peers:
        await server.__aexit__(None, None, None)


async def test_answered_requests_leave_no_listener(make):
    peer, client = await make()

    for _ in range(50):
        assert (await client.request('route', source='peer', timeout=5))['ok'] is True

    assert client.listeners == {}


async def test_a_request_that_times_out_leaves_no_listener_and_its_late_answer_is_ignored(make):
    peer, client = await make(answer=False)

    with pytest.raises(asyncio.TimeoutError):
        await client.request('route', source='peer', timeout=0.2)

    assert client.listeners == {}

    late = peer.requests[0]['uuid']
    await peer.current.send(json.dumps({'type': 3, 'uuid': late, 'id': 'peer', 'destination': 'client', 'data': 'too late'}))
    await asyncio.sleep(0.2)                                              # the answer is dispatched and ignored: no listener comes back, the client still works
    assert client.listeners == {}

    peer.answer = True
    assert (await client.request('route', source='peer', timeout=5))['ok'] is True


async def test_a_cancelled_request_leaves_no_listener(make):
    peer, client = await make(answer=False)
    task = asyncio.create_task(client.request('route', source='peer', timeout=30))
    await asyncio.sleep(0.2)
    assert len(client.listeners) == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.listeners == {}
