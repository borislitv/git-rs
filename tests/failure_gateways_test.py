#!/usr/bin/env python3
"""Real-socket ownership contracts for dependency-fault gateways."""
from contextlib import contextmanager
import http.client
import socket
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from failures import CommitGateway, FaultGateway, close_gateways


@contextmanager
def upstream(handle):
    """One bounded external peer; no PostgreSQL, S3 or containers required."""
    ready, completed = threading.Event(), threading.Event()
    observations, errors, streams = [], [], []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(3)

        def serve():
            try:
                with listener.accept()[0] as stream:
                    streams.append(stream)
                    stream.settimeout(5)
                    handle(stream, observations, ready)
            except BaseException as error:
                errors.append(error)
            finally:
                completed.set()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            yield SimpleNamespace(port=listener.getsockname()[1], ready=ready,
                                  completed=completed, observations=observations)
        finally:
            original = sys.exception()
            for stream in streams:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            thread.join(timeout=6)
            if thread.is_alive():
                errors.append(AssertionError("synthetic upstream did not stop"))
            if errors:
                if original is not None:
                    original.add_note("Synthetic upstream also failed: " + "; ".join(
                        str(error)[:1024] for error in errors))
                elif len(errors) == 1:
                    raise errors[0]
                else:
                    raise BaseExceptionGroup("Synthetic upstream failed", errors)


def http_request(stream):
    with stream.makefile("rb") as reader:
        lines = []
        for _ in range(100):
            line = reader.readline(65537)
            if line == b"\r\n":
                return b"".join(lines)
            if not line or len(line) > 65536:
                break
            lines.append(line)
    raise AssertionError("synthetic peer did not receive complete HTTP headers")


class GatewayOwnershipContracts(unittest.TestCase):
    def assert_stopped(self, gateway):
        self.assertFalse(gateway.thread.is_alive())
        self.assertFalse([thread.name for thread in threading.enumerate()
                          if thread.name.startswith(gateway.thread.name + "-request")])
        with socket.socket() as stream:
            stream.settimeout(1)
            self.assertNotEqual(stream.connect_ex(gateway.server_address), 0)

    def test_http_forwarding_preserves_signed_host_range_and_response(self):
        def respond(stream, observations, ready):
            observations.append(http_request(stream))
            stream.sendall(b"HTTP/1.1 206 Partial Content\r\ncontent-length: 3\r\n"
                           b"content-range: bytes 0-2/3\r\netag: \"synthetic\"\r\n"
                           b"Connection: close\r\n\r\nabc")
            ready.set()

        with upstream(respond) as peer:
            gateway = FaultGateway(f"http://127.0.0.1:{peer.port}")
            connection = http.client.HTTPConnection(*gateway.server_address, timeout=3)
            try:
                connection.request("GET", "/bucket/object.pack", headers={
                    "Host": "object.example.invalid", "Range": "bytes=0-2"})
                response = connection.getresponse()
                self.assertEqual((response.status, response.read()), (206, b"abc"))
                self.assertEqual(response.getheader("Content-Range"), "bytes 0-2/3")
                self.assertEqual(response.getheader("ETag"), '"synthetic"')
                self.assertIn(b"Host: object.example.invalid\r\n", peer.observations[0])
                self.assertIn(b"Range: bytes=0-2\r\n", peer.observations[0])
            finally:
                connection.close()
                gateway.close()
            self.assert_stopped(gateway)

    def test_close_releases_an_http_fault_gate_without_forwarding(self):
        with socket.socket() as unused:
            unused.bind(("127.0.0.1", 0))
            gateway = FaultGateway(f"http://127.0.0.1:{unused.getsockname()[1]}")
            gateway.arm("pause-get")
            try:
                with socket.create_connection(gateway.server_address, timeout=3) as client:
                    client.sendall(b"GET /bucket/object.pack HTTP/1.1\r\nHost: example.invalid\r\n\r\n")
                    self.assertTrue(gateway.reached.wait(3), "request did not reach the fault gate")
                    gateway.close()
                    self.assertEqual(client.recv(1), b"")
            finally:
                gateway.close()
            self.assert_stopped(gateway)

    def test_head_preserves_object_length_without_forwarding_a_body(self):
        def respond(stream, observations, ready):
            observations.append(http_request(stream))
            stream.sendall(b"HTTP/1.1 200 OK\r\ncontent-length: 12345\r\n"
                           b"etag: \"synthetic\"\r\nConnection: close\r\n\r\n")
            ready.set()

        with upstream(respond) as peer:
            gateway = FaultGateway(f"http://127.0.0.1:{peer.port}")
            connection = http.client.HTTPConnection(*gateway.server_address, timeout=3)
            try:
                connection.request("HEAD", "/bucket/object.pack")
                response = connection.getresponse()
                self.assertEqual((response.status, response.read()), (200, b""))
                self.assertEqual(response.getheader("Content-Length"), "12345")
                self.assertEqual(response.getheader("ETag"), '"synthetic"')
                self.assertTrue(peer.observations[0].startswith(b"HEAD /bucket/object.pack "))
            finally:
                connection.close()
                gateway.close()
            self.assert_stopped(gateway)

    def test_upstream_failure_does_not_replace_the_foreground_failure(self):
        def fail(stream, observations, ready):
            ready.set()
            raise OSError("synthetic upstream unavailable")

        with self.assertRaisesRegex(ValueError, "synthetic foreground failure") as caught:
            with upstream(fail) as peer:
                with socket.create_connection(("127.0.0.1", peer.port), timeout=3):
                    self.assertTrue(peer.ready.wait(3))
                    raise ValueError("synthetic foreground failure")
        self.assertTrue(any("synthetic upstream unavailable" in note
                            for note in caught.exception.__notes__))

    def test_close_interrupts_an_http_upstream_that_never_replies(self):
        def stall(stream, observations, ready):
            observations.append(http_request(stream))
            ready.set()
            observations.append(stream.recv(1))

        with upstream(stall) as peer:
            gateway = FaultGateway(f"http://127.0.0.1:{peer.port}")
            try:
                with socket.create_connection(gateway.server_address, timeout=3) as client:
                    client.sendall(b"GET /bucket/object.pack HTTP/1.1\r\nHost: example.invalid\r\n\r\n")
                    self.assertTrue(peer.ready.wait(3), "gateway did not reach upstream")
                    gateway.close()
                    self.assertEqual(client.recv(1), b"")
                    self.assertTrue(peer.completed.wait(3))
                    self.assertEqual(peer.observations[-1], b"")
            finally:
                gateway.close()
            self.assert_stopped(gateway)

    def test_close_interrupts_idle_postgres_connections_on_both_sides(self):
        startup = b"\x00\x00\x00\x08\x00\x03\x00\x00"

        def stall(stream, observations, ready):
            with stream.makefile("rb") as reader:
                observations.append(reader.read(8))
                ready.set()
                observations.append(reader.read(1))

        with upstream(stall) as peer:
            gateway = CommitGateway(peer.port)
            try:
                with socket.create_connection(gateway.server_address, timeout=3) as client:
                    client.sendall(startup)
                    self.assertTrue(peer.ready.wait(3), "gateway did not forward PostgreSQL startup")
                    self.assertEqual(peer.observations[0], startup)
                    gateway.close()
                    self.assertEqual(client.recv(1), b"")
                    self.assertTrue(peer.completed.wait(3))
                    self.assertEqual(peer.observations[-1], b"")
            finally:
                gateway.close()
            self.assert_stopped(gateway)

    def test_close_releases_a_withheld_commit_response(self):
        startup = b"\x00\x00\x00\x08\x00\x03\x00\x00"
        query = b"SELECT publish(); COMMIT;\x00"
        commit = b"C\x00\x00\x00\x0bCOMMIT\x00"

        def commit_then_wait(stream, observations, ready):
            with stream.makefile("rb") as reader:
                observations.append(reader.read(8 + 5 + len(query)))
                stream.sendall(commit)
                ready.set()
                observations.append(reader.read(1))

        with upstream(commit_then_wait) as peer:
            gateway = CommitGateway(peer.port)
            gateway.arm("pause-after")
            try:
                with socket.create_connection(gateway.server_address, timeout=3) as client:
                    message = startup + b"Q" + (len(query) + 4).to_bytes(4, "big") + query
                    client.sendall(message)
                    self.assertTrue(gateway.dropped.wait(3), "gateway did not withhold COMMIT")
                    self.assertEqual(peer.observations[0], message)
                    gateway.close()
                    self.assertEqual(client.recv(1), b"")
                    self.assertTrue(peer.completed.wait(3))
                    self.assertEqual(peer.observations[-1], b"")
            finally:
                gateway.close()
            self.assert_stopped(gateway)

    def test_gateway_failure_is_attached_to_a_foreground_error_after_cleanup(self):
        def oversized(stream, observations, ready):
            observations.append(http_request(stream))
            stream.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n"
                           b"Connection: close\r\n\r\nabcde")
            ready.set()

        with upstream(oversized) as peer, patch.object(FaultGateway, "BODY_LIMIT", 4):
            gateway = FaultGateway(f"http://127.0.0.1:{peer.port}")
            connection = http.client.HTTPConnection(*gateway.server_address, timeout=3)
            with self.assertRaisesRegex(ValueError, "synthetic publication failure") as caught:
                try:
                    connection.request("GET", "/bucket/object.pack")
                    with self.assertRaises(http.client.RemoteDisconnected):
                        connection.getresponse()
                    raise ValueError("synthetic publication failure")
                finally:
                    connection.close()
                    gateway.close()
            self.assertTrue(any("fixture upstream body exceeds its bound" in note
                                for note in caught.exception.__notes__))
            self.assert_stopped(gateway)

    def test_cleanup_failure_does_not_skip_another_live_gateway(self):
        class BrokenCleanup:
            def close(self):
                raise OSError("synthetic cleanup unavailable")

        with socket.socket() as unused:
            unused.bind(("127.0.0.1", 0))
            gateway = FaultGateway(f"http://127.0.0.1:{unused.getsockname()[1]}")
            try:
                with self.assertRaisesRegex(OSError, "synthetic cleanup unavailable"):
                    close_gateways(BrokenCleanup(), None, gateway)
                self.assert_stopped(gateway)
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
