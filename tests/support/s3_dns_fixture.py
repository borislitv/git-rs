"""Finite, isolated real-DNS fixture. Requires dnslib==0.9.26 and docs/s3-dns.md."""

import argparse
import http.server
import os
import socketserver
import subprocess
import threading
import urllib.parse
from pathlib import Path

from dnslib import A, DNSRecord, QTYPE, RCODE, RR


DNS_TEST = (
    "blob::transport::dns_tests::"
    "s3_dns_blackhole_cancel_recovery_without_blocking_backlog"
)
CONFIG_TEST = "blob::transport::dns_tests::s3_dns_system_config_failure_is_fail_closed"
HOST_TESTS = {
    "hosts-fqdn": "blob::transport::dns_tests::s3_dns_ipv4_hosts_only_does_not_query_dns",
    "hosts-short": "blob::transport::dns_tests::s3_dns_short_hosts_alias_does_not_query_dns",
}
HOST_NAMES = ("hosts.dns-fixture.test", "hostsalias")
STATE = threading.Condition()
SEEN = set()
RECOVERED = False
HOST_DNS_QUERIES = 0


def is_hosts_name(name):
    # Include original names and every search expansion, for both address
    # families (and any other queried type). Match a label boundary, not a
    # wildcard answer that could accidentally satisfy the requested name.
    return any(name == host or name.startswith(host + ".") for host in HOST_NAMES)


class DnsPeer(socketserver.BaseRequestHandler):
    def handle(self):
        global HOST_DNS_QUERIES
        packet, socket = self.request
        query = DNSRecord.parse(packet)
        name = str(query.q.qname).lower().rstrip(".")
        hosts_name = is_hosts_name(name)
        with STATE:
            SEEN.add(name)
            if hosts_name:
                HOST_DNS_QUERIES += 1
            STATE.notify_all()
            recovered = RECOVERED
        if hosts_name:
            return  # Missing hosts address families/searches must not use DNS.
        stalled = name.endswith(".stall.dns-fixture.test")
        if stalled and not recovered:
            return  # Real silent DNS peer; no retained thread or synthetic error.
        response = query.reply()
        if name != "searchprobe.dns-fixture.test" and not stalled:
            response.header.rcode = RCODE.NXDOMAIN
        elif query.q.qtype == QTYPE.A:
            response.add_answer(RR(query.q.qname, QTYPE.A, ttl=0, rdata=A("127.0.0.1")))
        socket.sendto(response.pack(), self.client_address)


class HttpPeer(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global RECOVERED
        url = urllib.parse.urlsplit(self.path)
        status, body = 200, b"ok"
        if url.path == "/observed":
            name = urllib.parse.parse_qs(url.query)["name"][0].lower()
            with STATE:
                if not STATE.wait_for(lambda: name in SEEN, timeout=3):
                    status, body = 503, b"no DNS query observed"
        elif url.path == "/hosts-unqueried":
            with STATE:
                if HOST_DNS_QUERIES:
                    status, body = 409, b"hosts-only name or search expansion queried DNS"
        elif url.path == "/recover":
            with STATE:
                RECOVERED = True
        elif url.path == "/synthetic/probe":
            if self.headers.get("Range") != "bytes=0-3":
                status, body = 400, b"incorrect immutable range"
            else:
                status, body = 206, b"DATA"
        elif url.path != "/health":
            status, body = 404, b"unknown fixture path"
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        if status == 206:
            self.send_header("Content-Range", "bytes 0-3/4")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def run_test(test_binary, mode):
    env = dict(
        os.environ,
        GIT_RS_S3_DNS_ISOLATED_FIXTURE="1" if mode == "dns" else mode,
        GIT_RS_S3_ENDPOINT="https://unused.dns-fixture.test",
        GIT_RS_S3_BUCKET="synthetic",
        GIT_RS_S3_REGION="us-east-1",
        GIT_RS_S3_ACCESS_KEY="synthetic",
        GIT_RS_S3_SECRET_KEY="synthetic",
    )
    result = subprocess.run(
        [
            str(test_binary.resolve()),
            "--exact",
            DNS_TEST if mode == "dns" else HOST_TESTS.get(mode, CONFIG_TEST),
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ],
        env=env,
        timeout=60,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    print(result.stdout, end="", flush=True)
    if result.returncode == 0 and "1 passed;" not in result.stdout:
        raise RuntimeError("the prebuilt binary did not run exactly one fixture test")
    if mode in HOST_TESTS:
        with STATE:
            count = HOST_DNS_QUERIES
        print(f"hosts-dns-queries: {count}", flush=True)
        if result.returncode == 0 and count:
            raise RuntimeError(
                "successful hosts range read must issue zero corresponding DNS queries"
            )
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("test_binary", type=Path)
    parser.add_argument(
        "--mode",
        choices=("dns", "hosts-fqdn", "hosts-short", "invalid-config", "unreadable-config"),
        default="dns",
    )
    args = parser.parse_args()
    # Never rewrite the operator's DNS configuration. Refuse ordinary host runs;
    # the operator must supply a private network and these read-only bind mounts.
    if args.mode == "invalid-config":
        assert Path("/etc/resolv.conf").read_text() == "search dns-fixture.test\n"
        raise SystemExit(run_test(args.test_binary, args.mode))
    if args.mode == "unreadable-config":
        try:
            Path("/etc/resolv.conf").read_bytes()
        except PermissionError:
            pass
        else:
            raise RuntimeError("fixture UID must be denied read access to /etc/resolv.conf")
        raise SystemExit(run_test(args.test_binary, args.mode))
    assert Path("/etc/resolv.conf").read_text() == (
        "nameserver 127.0.0.1\nsearch dns-fixture.test\n"
        "options ndots:1 timeout:30 attempts:1\n"
    ), "use the isolated fixture resolv.conf"
    assert "hosts: files dns" in Path("/etc/nsswitch.conf").read_text()
    assert Path("/etc/hosts").read_text() == (
        "127.0.0.1 localhost\n127.0.0.1 hosts.dns-fixture.test hostsalias\n"
    ), "use the exact IPv4-only hosts fixture, including its short alias"
    with socketserver.UDPServer(("127.0.0.1", 53), DnsPeer) as dns:
        with http.server.ThreadingHTTPServer(("127.0.0.1", 18053), HttpPeer) as http_peer:
            threads = [threading.Thread(target=server.serve_forever) for server in (dns, http_peer)]
            for thread in threads:
                thread.start()
            try:
                status = run_test(args.test_binary, args.mode)
            finally:
                dns.shutdown()
                http_peer.shutdown()
                for thread in threads:
                    thread.join()
            raise SystemExit(status)


if __name__ == "__main__":
    main()
