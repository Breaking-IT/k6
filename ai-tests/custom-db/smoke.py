#!/usr/bin/env python3
"""Isolated k6 -> Rust protocol test with the existing CSV writer as an oracle.

Requires Docker, Python 3, a built k6 fork and a built Custom DB service image.
Creates a temporary service/container and directory; never connects to customer DBs.
"""
import argparse
import csv
import email.parser
import gzip
import http.server
import io
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import threading
import time
import urllib.request
import uuid
from datetime import datetime


def run(args):
    name = "k6-custom-writer-smoke-" + uuid.uuid4().hex[:12]
    admin, producer = secrets.token_hex(32), secrets.token_hex(32)
    oracle = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            self.send_response(503 if self.path == "/failure" else 200)
            self.end_headers()
            self.wfile.write(b"fixture response")

        def do_POST(self):
            if self.path != "/ingest/batch":
                self.send_error(404)
                return
            data = self.rfile.read(int(self.headers["Content-Length"]))
            if self.headers.get("Content-Encoding") == "gzip":
                data = gzip.decompress(data)
            msg = email.parser.BytesParser().parsebytes(
                b"MIME-Version: 1.0\r\nContent-Type: "
                + self.headers["Content-Type"].encode() + b"\r\n\r\n" + data)
            for part in msg.get_payload():
                table = part["X-Table"].removeprefix("public.")
                rows = csv.reader(io.StringIO(part.get_payload(decode=True).decode()))
                oracle.setdefault(table, []).extend(rows)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

    target = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=target.serve_forever, daemon=True)
    thread.start()
    with tempfile.TemporaryDirectory(prefix=name) as temp:
        root = Path(temp)
        data_dir = root / "data"
        data_dir.mkdir()
        env = {**os.environ, "CUSTOM_STORAGE_ADMIN_TOKEN": admin}
        try:
            subprocess.run(["docker", "run", "-d", "--name", name,
                "--label", "breaktest.k6-writer-smoke=true", "--memory", "512m",
                "-e", "CUSTOM_STORAGE_ADMIN_TOKEN", "-p", "127.0.0.1::8099",
                "-v", f"{data_dir}:/data", args.image,
                "v3", "service", "/data", "0.0.0.0:8099"], env=env, check=True, capture_output=True)
            port = subprocess.check_output(["docker", "port", name, "8099/tcp"], text=True).strip().rsplit(":", 1)[1]
            service = f"http://127.0.0.1:{port}"

            def post(path, payload):
                request = urllib.request.Request(service + path, data=json.dumps(payload).encode(),
                    headers={"Authorization": "Bearer " + admin, "Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.load(response)

            for _ in range(100):
                try:
                    post("/health", {})
                    break
                except OSError:
                    time.sleep(.05)
            origin = int(time.time() - 946684800 - 10) * 1_000_000
            post("/runs/create", {"run_id": "k6-smoke", "source_tokens": [producer],
                "origin_us": origin, "customer_id": "isolated-k6-smoke"})
            script = root / "smoke.js"
            script.write_text('''import http from 'k6/http';
import { group, check, sleep } from 'k6';
export const options = { vus: 2, iterations: 20 };
export default function () {
  group('Checkout', function () {
    http.get(__ENV.TARGET + '/ok');
    const r = http.get(__ENV.TARGET + '/failure');
    check(r, { 'expected failure': x => x.status === 200 });
  });
  sleep(0.15);
}
''')
            target_url = f"http://127.0.0.1:{target.server_port}"
            writer_env = {**os.environ, "CUSTOM_STORAGE_TOKEN": producer, "runId": "k6-smoke",
                "nodeName": "k6-probe", "location": "Local", "PG_PROXY_TOKEN": "isolated-oracle",
                "TARGET": target_url, "K6_NO_USAGE_REPORT": "true", "K6_HTTP_ERROR_DATA": "true"}
            for key in ("syntheticMonitoring", "K6_BROWSER_SCREENSHOT_ON_ERROR", "CUSTOM_URL_PATTERNS", "BREAKINGIT_TRANSACTION_SOURCE"):
                writer_env.pop(key, None)
            result = subprocess.run([str(Path(args.k6).resolve()), "run", "--quiet",
                "--out", "custom-db-breakingit=" + service,
                "--out", "timescaledb-breakingit=" + target_url, str(script)],
                env=writer_env, text=True, capture_output=True, timeout=90)
            assert result.returncode == 0, result.stdout + result.stderr
            end = int(time.time() - 946684800 + 10) * 1_000_000
            final = post("/runs/k6-smoke/finalize", {})
            assert final["sealed"] and final["run_complete"], final
            expected_counts = {"requests_raw": 40, "transactions": 20, "requests_error": 40}
            counts = {}
            for table in ("requests_raw", "transactions", "requests_error", "virtual_users"):
                actual = post("/runs/k6-smoke/query", {"table": table, "start_us": origin, "end_us": end})
                counts[table] = len(actual["rows"])
                assert counts[table] == len(oracle[table]), (table, counts[table], len(oracle[table]))
                if table in expected_counts:
                    assert counts[table] == expected_counts[table], (table, counts)
                else:
                    assert counts[table] > 0
                # Match all fields including timestamp, except independently-generated UUIDs.
                def normalized(row, csv_row=False):
                    values = []
                    for i, value in enumerate(row):
                        if table == "requests_error" and i == 15:
                            continue
                        if csv_row and i == 0:
                            value = round((datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() - 946684800) * 1_000_000)
                        if value is None or value == "":
                            value = None
                        elif isinstance(value, bool):
                            value = "true" if value else "false"
                        else:
                            value = str(value)
                        values.append(value)
                    return json.dumps(values, ensure_ascii=False)
                left = sorted(normalized(row) for row in actual["rows"])
                right = sorted(normalized(row, True) for row in oracle[table])
                assert left == right, (table, "row mismatch", left[:1], right[:1], actual.get("columns"))
            subprocess.run(["docker", "restart", name], check=True, capture_output=True)
            port = subprocess.check_output(["docker", "port", name, "8099/tcp"], text=True).strip().rsplit(":", 1)[1]
            service = f"http://127.0.0.1:{port}"
            for _ in range(100):
                try:
                    post("/health", {})
                    break
                except OSError:
                    time.sleep(.05)
            for table, count in counts.items():
                actual = post("/runs/k6-smoke/query", {"table": table, "start_us": origin, "end_us": end})
                assert len(actual["rows"]) == count
            print(json.dumps({"status": "passed", "counts": counts, "parity": "all columns except generated error UUIDs", "sealed": True, "restart_reads": True}, indent=2))
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
            target.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--k6", required=True)
    parser.add_argument("--image", default="breaktest-storage-lab:local")
    run(parser.parse_args())
