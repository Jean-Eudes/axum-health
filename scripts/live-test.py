#!/usr/bin/env python3
"""Run axum-health against local HTTP, DNS, and disk checks and report latencies.

Requires certs/localhost.crt.pem and certs/localhost.key.pem (the default
repository development certificate paths). The config and server log are kept
in a temporary directory; the HTTP target and Axum server bind to loopback.
"""

import argparse
import http.server
import json
import os
import socket
import ssl
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class HealthTargetHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"status":"ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def free_loopback_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def summarize(samples):
    ordered = sorted(samples)
    return {
        "n": len(samples),
        "median_ms": round(statistics.median(samples), 3),
        "min_ms": round(ordered[0], 3),
        "max_ms": round(ordered[-1], 3),
        "p95_ms": round(ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))], 3),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", action="store_true", help="test target/release instead of target/debug")
    parser.add_argument("--skip-build", action="store_true", help="use an already-built binary")
    parser.add_argument("--cert", type=Path, default=ROOT / "certs/localhost.crt.pem")
    parser.add_argument("--key", type=Path, default=ROOT / "certs/localhost.key.pem")
    args = parser.parse_args()

    profile_args = ["--release"] if args.release else []
    profile = "release" if args.release else "debug"
    if not args.skip_build:
        subprocess.run(["cargo", "build", *profile_args], cwd=ROOT, check=True)

    binary = ROOT / "target" / profile / "axum-health"
    if not binary.is_file():
        raise SystemExit(f"binary not found: {binary}; build it or omit --skip-build")
    if not args.cert.is_file() or not args.key.is_file():
        raise SystemExit(f"TLS certificate/key not found: {args.cert}, {args.key}")

    upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), HealthTargetHandler)
    upstream_port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()

    server_port = free_loopback_port()
    temp_dir = Path(tempfile.mkdtemp(prefix="axum-health-live-"))
    config = temp_dir / "config.toml"
    config.write_text(
        f'''[server]
port = {server_port}
[server.tls]
cert_path = "{args.cert.resolve()}"
key_path = "{args.key.resolve()}"
[health.config]
cache_ttl_seconds = 1
http_timeout_seconds = 3
ldap_timeout_seconds = 3
[[health.checks.http]]
url = "http://internal.invalid:{upstream_port}/health"
resolve = "localhost"
[[health.checks.dns]]
host = "localhost"
[[health.checks.disk]]
path = "/tmp"
threshold = 5
''',
        encoding="utf-8",
    )

    log = (temp_dir / "server.log").open("w", encoding="utf-8")
    env = os.environ.copy()
    env["AXUM_HEALTH_CONFIG"] = str(config)
    process = subprocess.Popen([str(binary)], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    base_url = f"https://127.0.0.1:{server_port}"
    tls_context = ssl._create_unverified_context()

    def get(path):
        request = urllib.request.Request(base_url + path)
        with urllib.request.urlopen(request, context=tls_context, timeout=5) as response:
            return response.status, response.read()

    try:
        startup_start = time.perf_counter()
        while True:
            if process.poll() is not None:
                raise RuntimeError(f"server exited with code {process.returncode}")
            try:
                status, root_body = get("/actuator")
                break
            except (OSError, urllib.error.URLError):
                if time.perf_counter() - startup_start > 15:
                    raise RuntimeError("server did not become ready within 15 seconds")
                time.sleep(0.002)
        time_to_actuator_ms = (time.perf_counter() - startup_start) * 1000
        assert status == 200 and "_links" in json.loads(root_body)

        endpoint_statuses = {"/actuator": "200 links validated"}
        for path in ("/actuator/health/liveness", "/actuator/health/readiness"):
            status, body = get(path)
            response = json.loads(body)
            assert status == 200 and response["status"] == "UP", (path, status, response)
            endpoint_statuses[path] = f"{status} {response['status']}"

        cold_start = time.perf_counter()
        status, body = get("/actuator/health")
        cold_health_ms = (time.perf_counter() - cold_start) * 1000
        health = json.loads(body)
        assert status == 200 and health["status"] == "UP", (status, health)
        assert all(component["status"] == "UP" for component in health["components"].values())
        assert health["components"]["http"]["details"]["urls"][0]["status"] == "UP"
        endpoint_statuses["/actuator/health"] = "200 UP; http,dns,disk,ping all UP"

        cached_samples = []
        for _ in range(10):
            started = time.perf_counter()
            status, _ = get("/actuator/health")
            cached_samples.append((time.perf_counter() - started) * 1000)
            assert status == 200

        expired_cache_samples = []
        for _ in range(10):
            time.sleep(1.1)
            started = time.perf_counter()
            status, body = get("/actuator/health")
            expired_cache_samples.append((time.perf_counter() - started) * 1000)
            assert status == 200 and json.loads(body)["status"] == "UP"

        print(
            json.dumps(
                {
                    "result": "PASS",
                    "profile": profile,
                    "temporary_config": str(config),
                    "time_to_first_actuator_response_ms": round(time_to_actuator_ms, 3),
                    "first_health_request_ms": round(cold_health_ms, 3),
                    "rapid_health_cache_hits": summarize(cached_samples),
                    "requests_after_cache_expiry": summarize(expired_cache_samples),
                    "endpoints": endpoint_statuses,
                    "http_override": f"internal.invalid -> localhost -> 127.0.0.1:{upstream_port}",
                },
                indent=2,
            )
        )
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        upstream.shutdown()
        upstream.server_close()
        log.close()
        if process.returncode not in (0, -15):
            print((temp_dir / "server.log").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
