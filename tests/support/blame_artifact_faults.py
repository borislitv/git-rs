"""Bounded test-only process/protocol controls for the complete-artifact APIs."""
import os
import queue
import signal
import socket
import subprocess
import threading

from support.cache_faults import ObjectProxy, PgProxy


def disconnect_and_reject(proxy):
    """Reject new peers and close actual existing sessions, not just a flag."""
    with proxy.lock:
        proxy.reject = True
        peers = list(proxy.connections)
    for peer in peers:
        try:
            peer.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


class ArtifactProcess:
    def __init__(self, fixture, env, name):
        self.log = (fixture.base / f"{name}.stderr").open("xb")
        self.child = subprocess.Popen(
            [str(fixture.artifact_binary), "--ignored", "--exact", "artifact_process",
             "--nocapture", "--test-threads=1"], env=dict(env, GIT_RS_TEST_ARTIFACT_PROCESS="1"), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.log, start_new_session=True)
        self.lines = queue.Queue()
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()
        fixture.processes.append(self)
        assert self.receive() == "ready"

    def read(self):
        for line in self.child.stdout:
            _, separator, value = line.decode().partition("ARTIFACT ")
            if separator:
                self.lines.put(value.strip())
        self.lines.put(None)

    def receive(self, seconds=35):
        value = self.lines.get(timeout=seconds)
        assert value is not None, f"artifact fixture exited: {self.child.poll()}"
        return value

    def send(self, command):
        self.child.stdin.write((command + "\n").encode())
        self.child.stdin.flush()

    def ask(self, command):
        self.send(command)
        return self.receive()

    def kill(self):
        if self.child.poll() is None:
            os.killpg(self.child.pid, signal.SIGKILL)
        self.child.wait(timeout=5)

    def close(self):
        if self.child.poll() is None:
            try:
                self.send("quit")
                self.child.wait(timeout=5)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                self.kill()
        self.child.wait(timeout=5)
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        self.child.stdin.close()
        self.child.stdout.close()
        self.log.close()


class SqlLocker:
    """An owned transaction with bounded startup/release and independent cleanup."""
    def __init__(self, fixture, env, name, lock_sql):
        self.log = (fixture.base / f"{name}.stderr").open("xb")
        self.child = subprocess.Popen(
            ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log,
            start_new_session=True)
        self.lines = queue.Queue()
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()
        fixture.processes.append(self)
        self.child.stdin.write(("BEGIN; " + lock_sql + "; SELECT 'locked';\n").encode())
        self.child.stdin.flush()
        assert self.lines.get(timeout=5) == b"locked"

    def read(self):
        for line in self.child.stdout:
            self.lines.put(line.strip())
        self.lines.put(None)

    def close(self):
        if self.child.poll() is None:
            try:
                self.child.stdin.write(b"COMMIT;\n\\q\n")
                self.child.stdin.flush()
                self.child.wait(timeout=5)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                os.killpg(self.child.pid, signal.SIGKILL)
        self.child.wait(timeout=5)
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        self.child.stdin.close()
        self.child.stdout.close()
        self.log.close()
