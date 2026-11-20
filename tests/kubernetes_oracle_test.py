#!/usr/bin/env python3
"""Serial native-client preconditions against real local metrics responders."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest

from kubernetes_oracle import wait_idle


class SerialCleanupContracts(unittest.TestCase):
    def peer(self, responses, status=200):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.server.requests += 1
                body = self.server.responses[0]
                if len(self.server.responses) > 1:
                    self.server.responses.pop(0)
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.responses, server.requests = list(responses), 0
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01})
        thread.start()

        def close():
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive(), "metrics responder did not stop")

        self.addCleanup(close)
        return server, "127.0.0.1:" + str(server.server_port)

    def test_serial_precondition_observes_cleanup_on_both_replicas(self):
        first, address = self.peer([b"git_rs_active_requests 1\n", b"git_rs_active_requests 0\n"])
        second, other = self.peer([b"git_rs_active_requests 0\n"])
        wait_idle([address, other])
        self.assertGreaterEqual(first.requests, 2)
        self.assertGreaterEqual(second.requests, 2)

    def test_invalid_or_failed_metrics_never_acknowledge_cleanup(self):
        for status, body in ((503, b"git_rs_active_requests 0\n"), (200, b"unrelated 0\n"),
                             (200, b"git_rs_active_requests 0\ngit_rs_active_requests 0\n")):
            with self.subTest(status=status, body=body):
                _, address = self.peer([body], status)
                _, other = self.peer([b"git_rs_active_requests 0\n"])
                with self.assertRaises(AssertionError):
                    wait_idle([address, other])

    def test_busy_replica_cannot_outlive_the_serial_cleanup_budget(self):
        _, address = self.peer([b"git_rs_active_requests 1\n"])
        _, other = self.peer([b"git_rs_active_requests 0\n"])
        with self.assertRaisesRegex(AssertionError, "did not finish cleanup"):
            wait_idle([address, other], seconds=0)


if __name__ == "__main__":
    unittest.main()
