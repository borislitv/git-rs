"""Controlled DNS for the real cache-pool TLS PostgreSQL contract; see docs/pg-dns.md."""

import argparse
import http.server
import ipaddress
import os
import socketserver
import subprocess
import threading
from pathlib import Path

from dnslib import A, DNSRecord, QTYPE, RCODE, RR


TEST = "cache_pg_dns_cancel_recovery_without_blocking_backlog"
STATE = threading.Condition()
SEEN = {}
ROUND_START = set()
GATED = set()
RECOVERED = False
RELEASED = False
FRESH_RECOVERY_QUERIES = 0


class DnsPeer(socketserver.BaseRequestHandler):
    def handle(self):
        global FRESH_RECOVERY_QUERIES
        packet, _transport = self.request
        query = DNSRecord.parse(packet)
        name = str(query.q.qname).lower().rstrip(".")
        key = (self.client_address, query.header.id, name, query.q.qtype)
        with STATE:
            SEEN[key] = packet
            if not RECOVERED:
                GATED.add(key)
            elif not RELEASED and key not in GATED and name == self.server.hostname:
                FRESH_RECOVERY_QUERIES += 1
            STATE.notify_all()
            reply = RELEASED or (RECOVERED and key not in GATED)
        if not reply:
            return
        self.server.reply(query, self.client_address)


class DnsServer(socketserver.UDPServer):
    def reply(self, query, peer):
        name = str(query.q.qname).lower().rstrip(".")
        response = query.reply()
        if name != self.hostname:
            response.header.rcode = RCODE.NXDOMAIN
        elif query.q.qtype == QTYPE.A:
            response.add_answer(RR(query.q.qname, QTYPE.A, ttl=0, rdata=A(self.address)))
        self.socket.sendto(response.pack(), peer)


class Control(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global ROUND_START, RECOVERED, RELEASED, FRESH_RECOVERY_QUERIES
        status = 200
        replay = []
        with STATE:
            if self.path == "/start-stall":
                SEEN.clear()
                GATED.clear()
                ROUND_START = set()
                RECOVERED = RELEASED = False
                FRESH_RECOVERY_QUERIES = 0
            elif self.path == "/next-round":
                ROUND_START = set(SEEN)
            elif self.path == "/observed":
                def both_families():
                    types = {
                        key[3] for key in SEEN
                        if key not in ROUND_START and key[2] == self.server.dns.hostname
                    }
                    return {QTYPE.A, QTYPE.AAAA}.issubset(types)

                if not STATE.wait_for(both_families, timeout=3):
                    status = 503
            elif self.path == "/recover":
                RECOVERED = True
            elif self.path == "/release-all":
                RECOVERED = RELEASED = True
                replay = list(SEEN.items())
            else:
                status = 404
        for key, packet in replay:
            self.server.dns.reply(DNSRecord.parse(packet), key[0])
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("test_binary", type=Path)
    parser.add_argument("--expect-starvation", action="store_true")
    args = parser.parse_args()
    # These assertions never configure networking or alter the operator's files.
    assert Path("/etc/resolv.conf").read_text() == (
        "nameserver 127.0.0.1\noptions ndots:1 timeout:30 attempts:1\n"
    ), "requires the isolated fixture resolv.conf"
    assert "hosts: files dns" in Path("/etc/nsswitch.conf").read_text()
    hostname = os.environ["PGHOST"].lower().rstrip(".")
    assert "." in hostname and not hostname.endswith(".localhost")
    assert hostname not in Path("/etc/hosts").read_text()
    address = str(ipaddress.IPv4Address(os.environ["GIT_RS_PG_DNS_ADDRESS"]))
    assert os.environ["PGSSLMODE"] == "verify-full"
    assert os.environ.get("GIT_RS_CACHE_PGSSLMODE", "verify-full") == "verify-full"
    # Let the cache use exactly the same hostname and TLS policy, not a DSN
    # hostaddr which would silently bypass the regression under test.
    forbidden = ("GIT_RS_CACHE_DSN", "GIT_RS_CACHE_PGHOST", "GIT_RS_CACHE_S3_ENDPOINT")
    assert not any(name in os.environ for name in forbidden)
    with DnsServer(("127.0.0.1", 53), DnsPeer) as dns:
        dns.hostname, dns.address = hostname, address
        with http.server.ThreadingHTTPServer(("127.0.0.1", 18054), Control) as control:
            control.dns = dns
            servers = (dns, control)
            threads = [threading.Thread(target=server.serve_forever) for server in servers]
            for thread in threads:
                thread.start()
            try:
                result = subprocess.run(
                    [str(args.test_binary.resolve()), "--exact", TEST, "--ignored", "--nocapture", "--test-threads=1"],
                    env=dict(os.environ, GIT_RS_PG_DNS_ISOLATED_FIXTURE="1"),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=60,
                    check=False,
                )
                print(result.stdout, end="", flush=True)
                with STATE:
                    print(f"gated DNS messages: {len(GATED)}; all DNS messages: {len(SEEN)}", flush=True)
                if result.returncode == 0 and "1 passed;" not in result.stdout:
                    raise RuntimeError("prebuilt pg_dns binary did not run exactly one contract")
                if args.expect_starvation:
                    assert result.returncode != 0, "GAI mutant unexpectedly passed"
                    assert "pg-dns: TLS SQL preflight passed" in result.stdout
                    for round_number in (1, 2):
                        assert f"pg-dns: cancelled round {round_number} after observed DNS" in result.stdout
                    assert "fresh cache acquisition must not queue behind cancelled blocking DNS" in result.stdout
                    old_peers = {key[0] for key in GATED if key[2] == hostname}
                    assert len(old_peers) >= 2, "RED requires two independent old DNS flows"
                    assert FRESH_RECOVERY_QUERIES == 0, "RED must queue before DNS, not fail at TLS/SQL"
                elif result.returncode == 0:
                    assert FRESH_RECOVERY_QUERIES > 0, "GREEN must actually resolve after cancellation"
            finally:
                for server in servers:
                    server.shutdown()
                for thread in threads:
                    thread.join()
            raise SystemExit(0 if args.expect_starvation else result.returncode)


if __name__ == "__main__":
    main()
