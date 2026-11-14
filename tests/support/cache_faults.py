"""Bounded, owned protocol fault boundaries; never used by the service."""
import http.client
import http.server
import socket
import socketserver
import struct
import threading


def exact(sock, size, stopping):
    data = bytearray()
    while len(data) < size:
        if stopping.is_set():
            raise EOFError
        try:
            block = sock.recv(size - len(data))
        except socket.timeout:
            continue
        if not block:
            raise EOFError
        data.extend(block)
    return bytes(data)


class PgProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, port):
        self.port = int(port)
        self.lock = threading.Lock()
        self.fault = None
        self.hit = threading.Event()
        self.release = threading.Event()
        self.stopping = threading.Event()
        self.connections = set()
        self.accepted = 0
        self.reject = False
        super().__init__(("127.0.0.1", 0), PgConnection)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def arm(self, sql, kind):
        with self.lock:
            assert self.fault is None
            self.hit.clear()
            self.release.clear()
            self.fault = (sql, kind)

    def take(self, sql):
        with self.lock:
            if self.fault and self.fault[0] in sql:
                kind = self.fault[1]
                self.fault = None
                return kind
        return None

    def close(self):
        self.stopping.set()
        self.release.set()
        with self.lock:
            sockets = list(self.connections)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()


class PgConnection(socketserver.BaseRequestHandler):
    def handle(self):
        with self.server.lock:
            self.server.accepted += 1
            if self.server.reject:
                return
        upstream = socket.create_connection(("127.0.0.1", self.server.port), 5)
        # Framed forwarding must not add Nagle/delayed-ACK latency to every tiny
        # protocol header, especially in the one-second deadline/recovery case.
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.request.settimeout(.1)
        upstream.settimeout(.1)
        stopping = threading.Event()
        state = {"kind": None, "complete": False}
        with self.server.lock:
            self.server.connections.update((self.request, upstream))

        def client():
            statements = {}
            bound = ""
            try:
                header = exact(self.request, 4, stopping)
                size = struct.unpack("!I", header)[0]
                assert 8 <= size <= 65536
                upstream.sendall(header + exact(self.request, size - 4, stopping))
                while True:
                    header = exact(self.request, 5, stopping)
                    size = struct.unpack("!I", header[1:])[0]
                    assert 4 <= size <= 16 * 1024 * 1024
                    body = exact(self.request, size - 4, stopping)
                    if header[:1] == b"P":
                        name, sql, _ = body.split(b"\0", 2)
                        statements[name] = sql.decode()
                    elif header[:1] == b"B":
                        _, statement, _ = body.split(b"\0", 2)
                        bound = statements[statement]
                    elif header[:1] == b"E":
                        state["kind"] = self.server.take(bound)
                        state["complete"] = False
                    upstream.sendall(header + body)
            except (OSError, EOFError):
                pass
            finally:
                stopping.set()

        sender = threading.Thread(target=client, daemon=True)
        sender.start()
        try:
            while True:
                header = exact(upstream, 5, stopping)
                size = struct.unpack("!I", header[1:])[0]
                assert 4 <= size <= 16 * 1024 * 1024
                body = exact(upstream, size - 4, stopping)
                kind = state["kind"]
                if kind == "truncate" and header[:1] == b"D":
                    self.request.sendall(header + body[:len(body) // 2])
                    self.server.hit.set()
                    break
                if kind == "stall" and header[:1] == b"D":
                    self.server.hit.set()
                    if not self.server.release.wait(timeout=10):
                        raise EOFError
                if kind in ("commit", "commit-gate") and header[:1] in (b"D", b"C"):
                    if header[:1] == b"C":
                        state["complete"] = True
                    continue
                if kind in ("commit", "commit-gate") and state["complete"] and header[:1] == b"Z":
                    # ReadyForQuery follows the implicit COMMIT for Execute/Sync.
                    self.server.hit.set()
                    if kind == "commit-gate":
                        self.server.release.wait(timeout=10)
                    break
                self.request.sendall(header + body)
        except (OSError, EOFError):
            pass
        finally:
            stopping.set()
            for sock in (upstream, self.request):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
            sender.join(timeout=5)
            with self.server.lock:
                self.server.connections.difference_update((upstream, self.request))


class ObjectProxy(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port):
        self.port = int(port)
        self.lock = threading.Lock()
        self.fault = None
        self.hit, self.release, self.finished = [threading.Event() for _ in range(3)]
        super().__init__(("127.0.0.1", 0), ObjectConnection)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def arm(self, method, prefix, kind="gate"):
        with self.lock:
            assert self.fault is None
            self.fault = (method, prefix, kind)
            self.hit.clear()
            self.release.clear()
            self.finished.clear()

    def close(self):
        self.release.set()
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()


class ObjectConnection(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def transfer(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.port, timeout=10)
        gated = False
        kind = None
        try:
            self.connection.settimeout(10)
            size = int(self.headers.get("Content-Length", "0"))
            assert 0 <= size <= 8 * 1024 * 1024
            body = self.rfile.read(size)
            assert len(body) == size
            with self.server.lock:
                if self.server.fault and self.command == self.server.fault[0] and self.path.startswith(self.server.fault[1]):
                    kind = self.server.fault[2]
                    self.server.fault = None
                    gated = True
            if gated and kind != "response":
                self.server.hit.set()
                if kind == "forbidden":
                    self.send_error(403, "synthetic cache delete permission denied")
                    self.server.finished.set()
                    return
                if kind in ("Enabled", "Suspended"):
                    content = (f'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                               f'<Status>{kind}</Status></VersioningConfiguration>').encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/xml")
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                    self.server.finished.set()
                    return
                assert self.server.release.wait(timeout=25), "object gate deadline"
            # Preserve the signed Host header even though the upstream socket has
            # another port. The S3 server verifies the same signed request.
            connection.request(self.command, self.path, body, dict(self.headers))
            response = connection.getresponse()
            content = response.read(8 * 1024 * 1024 + 1)
            assert len(content) <= 8 * 1024 * 1024
            if gated and kind == "response":
                self.server.hit.set()
                assert self.server.release.wait(timeout=25), "object response gate deadline"
            if gated:
                self.server.finished.set()
            self.send_response(response.status)
            for name, value in response.getheaders():
                if name.lower() not in ("connection", "transfer-encoding", "content-length"):
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            connection.close()

    do_GET = do_PUT = do_DELETE = transfer
