def noop(*args, **kwargs):
    pass
    
import logging
import socket
import sys
import threading
import time
original_basicConfig = logging.basicConfig
logging.basicConfig = noop

from websocket_server import WebsocketServer
from websocket_server.websocket_server import OPCODE_PING
logging.basicConfig = original_basicConfig

import orjson
from .lib.message import WsMessage
from .lib.payload import Payloads, MessagePayload

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

class Server:
    """
    Represents a winerp Server.
    This class is used as the central communication center for all connected clients.
    All requests and responses pass through the server

    If the library is installed using PyPi, you can also use terminal to start server using `winerp --port 1234`
    
    Parameters
    -----------
    host: Optional[:class:`str`]
        The host on which the server is running. Defaults to 127.0.0.1.
    port: Optional[:class:`int`]
        The port on which the server is running. Defaults to 13254.
    """

    # A client that verifies under a name already active waits on hold while the server checks the old connection:
    # a websocket ping, and the old one is closed if no pong comes back within `liveness_timeout` seconds. The check is
    # repeated every `liveness_interval` seconds while someone waits. A live client is never ejected.
    liveness_timeout = 10.0
    liveness_interval = 30.0

    def __init__(
            self,
            host: str = "127.0.0.1",
            port: int = 13254
    ):
        self.websocket = WebsocketServer(host=host, port=port)
        self.websocket.set_fn_new_client(self.__on_client_connect)
        self.websocket.set_fn_message_received(self.__on_message)
        self.websocket.set_fn_client_left(self.__on_client_disconnect)
        self.websocket.handle_error = self._handle_error
        self.websocket._pong_received_ = self._pong_received
        self.active_clients = {}
        self.pending_verification = {}
        self.on_hold_connections = {}                                   # name -> connections waiting, oldest first
        self._checking = set()                                          # names whose old connection is being checked
        # Each connection runs in its own thread (websocket_server): the dicts above are only touched under this lock,
        # and nothing is sent while holding it (a send to a dead peer can block).
        self._lock = threading.RLock()

    @property
    def client_count(self) -> int:
        """
        :class:`int`: Returns the number of connected clients
        """
        return len(self.active_clients)

    @staticmethod
    def _handle_error(_request, client_address):
        # A connection that is not a websocket (TCP probe of a health check, port scan) fails the handshake with an
        # AssertionError: one debug line instead of a full traceback every few seconds, which flushed the useful
        # lines out of the container log.
        error = sys.exc_info()[1]
        if isinstance(error, (AssertionError, ConnectionError, KeyError, ValueError)):
            logger.debug("Not a websocket connection from %s: %r" % (client_address, error))
            return
        logger.exception("Error on the connection from %s" % (client_address,))

    @staticmethod
    def _cid(client):
        # Unique id of the connection. The source port (used before) is not unique: two containers can use the same one.
        return client["id"]

    @staticmethod
    def _peer(client):
        address = client.get("address") or ("?", "?")
        return "%s:%s" % (address[0], address[1])

    @staticmethod
    def _pong_received(handler, _msg):
        handler.winerp_pong = time.monotonic()

    def __on_client_connect(self, client, _):
        logger.info("Client connected from %s (connection %s)" % (self._peer(client), self._cid(client)))
        with self._lock:
            self.pending_verification[self._cid(client)] = client

    def __on_client_disconnect(self, client, _):
        # websocket_server calls this with None for a connection that never finished the handshake (a TCP probe,
        # every few seconds in production): it used to raise a TypeError each time.
        if not client:
            return
        cid = self._cid(client)
        promoted = None
        with self._lock:
            self.pending_verification.pop(cid, None)
            for name, waiting in list(self.on_hold_connections.items()):
                waiting[:] = [each for each in waiting if each["id"] != cid]
                if not waiting:
                    del self.on_hold_connections[name]
            for name, each_client in list(self.active_clients.items()):
                if each_client["id"] == cid:
                    del self.active_clients[name]
                    logger.info("Client %s disconnected (connection %s)" % (name, cid))
                    promoted = self._pop_held(name)
                    if promoted is not None:
                        self.active_clients[name] = promoted
                    break
            else:
                logger.info("Client disconnected (connection %s)" % cid)
        if promoted is not None:
            logger.info("Client %s on hold moved to active (connection %s)" % (name, promoted["id"]))
            self.__send_message(promoted["client"], MessagePayload(type=Payloads.success, data="Authorized."))

    def _pop_held(self, name):
        # the newest connection on hold: the most likely to be alive. Called under the lock.
        waiting = self.on_hold_connections.get(name)
        if not waiting:
            return None
        held = waiting.pop()
        if not waiting:
            del self.on_hold_connections[name]
        return held

    def _check_liveness(self, name, delay):
        """Pings the active connection of `name` (a client waits on hold for this name), then decides.

        Before, the client on hold waited until the old connection left. When the old one never left (half-open TCP
        connection of a container that is gone, frozen process), it waited forever: on 2026-11-23 the dashboard could
        not reach the bot for 21 minutes after a deploy of the website backend.
        """
        with self._lock:
            active = self.active_clients.get(name)
            if active is None or not self.on_hold_connections.get(name):
                self._checking.discard(name)                            # nobody waits any more: the check ends
                return
        handler = active["client"]["handler"]
        sent = time.monotonic()
        # in its own thread: the send blocks if the old peer is gone with a full buffer
        threading.Thread(target=self._ping, args=(handler,), daemon=True).start()
        timer = threading.Timer(delay, self._decide, args=(name, active["id"], handler, sent))
        timer.daemon = True
        timer.start()

    @staticmethod
    def _ping(handler):
        try:
            handler.send_text("", OPCODE_PING)
        except Exception:  # pylint: disable=W0718
            pass

    def _decide(self, name, old_id, handler, sent):
        promoted = None
        with self._lock:
            active = self.active_clients.get(name)
            if active is None or not self.on_hold_connections.get(name):
                self._checking.discard(name)                            # nobody waits any more
                return
            restart = active["id"] != old_id                            # another one became active meanwhile
            if restart:
                alive = None
            else:
                alive = getattr(handler, "winerp_pong", 0) >= sent
            if alive is False:
                promoted = self._pop_held(name)
                self.active_clients[name] = promoted
                self.pending_verification.pop(promoted["id"], None)
                restart = bool(self.on_hold_connections.get(name))     # others still wait: check the new one
                if not restart:
                    self._checking.discard(name)
            elif alive:
                waiting = [each["id"] for each in self.on_hold_connections[name]]
        if alive is None:
            self._check_liveness(name, self.liveness_timeout)
            return
        if alive:
            logger.info("Client %s: connection %s is alive, connections %s stay on hold" % (name, old_id, waiting))
            timer = threading.Timer(self.liveness_interval, self._check_liveness, args=(name, self.liveness_timeout))
            timer.daemon = True
            timer.start()
            return
        logger.info("Client %s: connection %s does not answer, replaced by connection %s" % (name, old_id, promoted["id"]))
        self.__send_message(promoted["client"], MessagePayload(type=Payloads.success, data="Authorized."))
        self._close(handler)
        if restart:
            self._check_liveness(name, self.liveness_timeout)

    @staticmethod
    def _close(handler):
        # shutdown first: it also unblocks a thread stuck sending to this peer; its thread then ends on its own
        handler.keep_alive = False
        try:
            handler.request.shutdown(socket.SHUT_RDWR)
        except Exception:  # pylint: disable=W0718
            pass

    def _target(self, name):
        with self._lock:
            entry = self.active_clients.get(name)
        return entry["client"] if entry is not None else None

    def __send_message(self, client, message):
        if not isinstance(message, dict):
            message = message.to_dict()
        try:
            self.websocket.send_message(
                client,
                orjson.dumps(message)
            )
        except OSError as error:
            # the peer is gone: its thread will report the disconnection, the sender must not crash
            logger.info("Could not send to connection %s: %s" % (client.get("id"), error))

    def __send_error(self, client, payload):
        self.__send_message(client, payload)

    def __on_message(self, client, _, msg):
        try:
            msg = WsMessage(orjson.loads(msg))
        except Exception:  # pylint: disable=W0718
            logger.exception("Unreadable message from connection %s, skipped" % self._cid(client))
            return
        payload = MessagePayload().from_message(msg)
        cid = self._cid(client)

        if msg.type.verification:
            with self._lock:
                old = self.active_clients.get(msg.id)
                if old is not None and old["id"] == cid:
                    old = None                                          # same connection verifying twice
                if old is not None:
                    self.on_hold_connections.setdefault(msg.id, []).append({"client": client, "id": cid})
                    check = msg.id not in self._checking                # one check at a time per name
                    self._checking.add(msg.id)
                else:
                    self.active_clients[msg.id] = {"client": client, "id": cid}
                    self.pending_verification.pop(cid, None)
            if old is not None:
                logger.info("Client %s connected again (connection %s): on hold while connection %s is checked" % (msg.id, cid, old["id"]))
                payload.uuid = None
                payload.type = Payloads.error
                payload.data = "Already authorized."
                payload.traceback = "Already authorized."
                self.__send_error(client, payload)
                if check:
                    self._check_liveness(msg.id, self.liveness_timeout)
                return
            logger.info("Client %s verified (connection %s from %s)" % (msg.id, cid, self._peer(client)))
            payload.type = Payloads.success
            payload.data = "Authorized."
            self.__send_message(client, payload)
            return
        else:
            with self._lock:
                verified = any(each["id"] == cid for each in self.active_clients.values())
            if not verified:
                logger.info('Unverified client tried to send message')
                payload.type = Payloads.error
                payload.data = "Not authorized."
                payload.traceback = "Not authorized."
                self.__send_error(client, payload)
                return

        if msg.type.information:
            logger.debug("Received Information Message from client %s" % client['address'][1])
            payload.type = Payloads.information
            if msg.route:
                for destination in msg.route:
                    target = self._target(destination)
                    if target is not None:
                        payload.destination = destination
                        self.__send_message(target, payload)
            else:
                with self._lock:
                    clients = list(self.active_clients.items())
                for client_id, client_obj in clients:
                    if client_id != payload.id:
                        payload.destination = client_id
                        self.__send_message(client_obj["client"], payload)

        if msg.type.ping:
            logger.debug("Received Ping Message from client %s" % client['address'][1])
            payload.type = Payloads.ping
            if msg.destination is None or self._target(msg.destination) is not None:
                payload.data = {"success": True}
            else:
                payload.data = {"success": False}

            self.__send_message(
                client,
                payload
            )

        if msg.type.request:
            logger.debug("Received Request Message from client %s" % client['address'][1])
            if msg.id == msg.destination:
                payload.type = Payloads.error
                payload.data = "Source and destination are the same."
                payload.traceback = "Source and destination are the same."
                self.__send_error(client, payload)

            elif (target := self._target(msg.destination)) is None:
                payload.type = Payloads.error
                payload.data = "Destination not found."
                payload.traceback = "Destination not found."
                self.__send_error(client, payload)

            else:
                payload.type = Payloads.request
                payload.id = msg.destination
                payload.destination = msg.id
                self.__send_message(target, payload)
                logger.debug("Request Message Forwarded to %s" % msg.destination)

        if msg.type.client_count:
            logger.debug("Received Client_count Message from client %s" % client['address'][1])
            payload.type = Payloads.client_count

            with self._lock:
                payload.data = list(self.active_clients.keys())

            self.__send_message(
                client,
                payload
            )

        if msg.type.response or msg.type.error or msg.type.function_call:
            logger.debug("Received Response Message from client %s" % client['address'][1])
            target = self._target(msg.destination)
            if target is None:
                payload.type = Payloads.error
                payload.data = "The data requester is no longer connected"
                payload.traceback = "The data requester is no longer connected"
                self.__send_error(client, payload)
                return

            payload.type = Payloads.response if msg.type.response else (
                Payloads.function_call if msg.type.function_call else Payloads.error)

            self.__send_message(target, payload)
            logger.debug("Response forwarded to %s" % msg.destination)

    def start(self):
        """
        Starts the server on the given port.
        """
        logger.info("Started Websocket Server")
        self.websocket.run_forever()
