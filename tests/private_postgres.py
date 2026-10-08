"""Owned socket-only PostgreSQL fixtures for harness self-tests (stdlib only)."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


class PrivatePostgres:
    def __init__(self):
        configured = os.environ.get("PG_BINDIR")
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith("PG")}
        self.bin = Path(configured or subprocess.check_output(
            ["pg_config", "--bindir"], env=self.env, text=True).strip())
        self.root = Path(tempfile.mkdtemp(prefix="pgque-harness-private-"))
        self.data = self.root / "data"
        self.socket = self.root / "socket"
        self.socket.mkdir()
        self.port = 65431

    def __enter__(self):
        try:
            subprocess.run([str(self.bin / "initdb"), "-D", str(self.data),
                            "-A", "trust", "-U", "postgres", "--no-locale"],
                           env=self.env, check=True, capture_output=True)
            subprocess.run([
                str(self.bin / "pg_ctl"), "-D", str(self.data), "-w", "start",
                "-l", str(self.root / "server.log"), "-o",
                f"-p {self.port} -c listen_addresses='' "
                f"-c unix_socket_directories='{self.socket}'",
            ], env=self.env, check=True, capture_output=True)
            expected = f"{self.data}|{self.socket}|{self.port}|t"
            actual = self.query("select current_setting('data_directory'), "
                                "current_setting('unix_socket_directories'), "
                                "current_setting('port'), inet_server_addr() is null")
            if actual != expected:
                raise RuntimeError("private PostgreSQL identity mismatch: " + actual)
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, exc_type, exc, tb):
        # Only a successfully stopped owned cluster may have its files removed.
        if (self.data / "postmaster.pid").exists():
            subprocess.run([str(self.bin / "pg_ctl"), "-D", str(self.data),
                            "-m", "immediate", "-w", "stop"],
                           env=self.env, check=True, capture_output=True)
        shutil.rmtree(self.root)

    def dsn(self, database="postgres"):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", database):
            raise ValueError("unsafe test database name")
        return (f"host={self.socket} port={self.port} hostaddr='' "
                f"user=postgres dbname={database}")

    def command(self, database="postgres"):
        return [str(self.bin / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
                "--dbname=" + self.dsn(database)]

    def run(self, sql, database="postgres", *, check=True):
        return subprocess.run(self.command(database), input=sql, env=self.env,
                              text=True, capture_output=True, check=check, timeout=30)

    def query(self, sql, database="postgres"):
        return self.run(sql, database).stdout.strip()

    def install(self, source, database):
        result = subprocess.run(self.command(database) + ["--single-transaction", "-f", str(source)],
                                env=self.env, text=True, capture_output=True, timeout=30)
        if result.returncode:
            raise RuntimeError(result.stderr)

    def environment(self, database):
        return dict(self.env, PGQUE_TEST_DSN=self.dsn(database),
                    PATH=str(self.bin) + os.pathsep + self.env["PATH"])
