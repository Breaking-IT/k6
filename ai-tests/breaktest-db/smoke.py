#!/usr/bin/env python3
"""Isolated k6 -> Rust protocol test with the pg-proxy binary COPY writer as an oracle.

Requires Docker, Python 3, a built k6 fork and a built BreakTest DB service image.
Creates a temporary service/container and directory; never connects to customer DBs.
"""
import argparse
import email.parser
import gzip
import http.server
import io
import json
import os
from pathlib import Path
import secrets
import struct
import socket
import html
import subprocess
import tempfile
import threading
import time
import urllib.request
import uuid



def copy_rows(table, payload):
    schemas = {
        "requests_raw": "tssssbiisiii",
        "transactions": "tsssbiii",
        "requests_error": "tssssssiissssssu",
        "virtual_users": "tsssiii",
    }
    schema = schemas[table]
    stream = io.BytesIO(payload)
    if stream.read(19) != b"PGCOPY\n\xff\r\n\x00" + bytes(8):
        raise ValueError("Invalid binary COPY header")
    while True:
        count, = struct.unpack(">h", stream.read(2))
        if count == -1:
            if stream.read(1):
                raise ValueError("Trailing bytes after COPY data")
            return
        if count != len(schema):
            raise ValueError(f"Unexpected {table} row width {count}")
        row = []
        for kind in schema:
            size, = struct.unpack(">i", stream.read(4))
            if size == -1:
                row.append(None)
                continue
            if size < 0:
                raise ValueError("Invalid COPY field length")
            field = stream.read(size)
            if len(field) != size:
                raise ValueError("Truncated COPY field")
            if kind == "s":
                value = field.decode()
            elif kind == "u":
                value = str(uuid.UUID(bytes=field))
            else:
                value, = struct.unpack({"i": ">i", "t": ">q", "b": ">?"}[kind], field)
            row.append(value)
        yield row


def run(args):
    name = "k6-breaktest-db-smoke-" + uuid.uuid4().hex[:12]
    admin = secrets.token_hex(32)
    producers = [secrets.token_hex(32) for _ in range(args.lanes)]
    producer = ",".join(producers)
    oracle = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path == '/redirect':
                self.send_response(302);self.send_header('Location','/ok');self.end_headers();return
            if self.path == '/reset':
                self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
            body=b'<html><body>fixture<iframe src="/frame"></iframe></body></html>' if self.path=='/page' else b'fixture response'
            self.send_response(503 if self.path in ('/failure','/slowfailure') else 200)
            self.send_header('Content-Type','text/html')
            self.send_header('Content-Length',str(len(body)))
            self.send_header('Cache-Control','max-age=3600' if self.path=='/cached' else 'no-store')
            self.end_headers()
            if self.path=='/slowfailure':
                self.wfile.write(body[:3]);self.wfile.flush();time.sleep(1.2);body=body[3:]
            self.wfile.write(body)

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
                if part["X-Format"] != "binary":
                    raise ValueError(f"Expected binary COPY for {table}")
                rows = copy_rows(table, part.get_payload(decode=True))
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
        env = {**os.environ, "BREAKTEST_DB_ADMIN_TOKEN": admin}
        with socket.socket() as port_socket:
            port_socket.bind(('127.0.0.1',0))
            fixed_port=port_socket.getsockname()[1]
        try:
            subprocess.run(["docker", "run", "-d", "--name", name,
                "--label", "breaktest.k6-writer-smoke=true", "--memory", "512m",
                "-e", "BREAKTEST_DB_ADMIN_TOKEN", "-p", f"127.0.0.1:{fixed_port}:8099",
                "-v", f"{data_dir}:/data", args.image,
                "serve", "/data", "0.0.0.0:8099"], env=env, check=True, capture_output=True)
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
            post("/runs/create", {"run_id": "k6-smoke", "source_tokens": producers,
                "origin_us": origin, "customer_id": "isolated-k6-smoke"})
            script = root / "smoke.js"
            script.write_text('''import http from 'k6/http';
import { group, check, sleep } from 'k6';
import { Trend } from 'k6/metrics';
const customLatency = new Trend('custom_latency');
export const options = { vus: 2, iterations: Number(__ENV.FIXTURE_ITERATIONS) };
export default function () {
  group('Checkout', function () {
    http.get(__ENV.TARGET + '/ok');
    const r = http.get(__ENV.TARGET + '/failure');
    check(r, { 'expected failure': x => x.status === 200 });
  });
  if (__ENV.FIXTURE_CUSTOM === 'true') customLatency.add(37);
  sleep(0.15);
}
''')
            if args.browser:
                script.write_text("""import { browser } from 'k6/browser';
import { group, check, sleep } from 'k6';
export const options = { scenarios: { browser: {executor:'per-vu-iterations',vus:1,iterations:Number(__ENV.FIXTURE_ITERATIONS),options:{browser:{type:'chromium'}}} } };
export default async function(){
 const page=await browser.newPage();
 try { await group('Checkout',async function(){
  await page.goto(__ENV.TARGET+'/page');
  const status=await page.evaluate(async()=>{
   await fetch('/redirect');await fetch('/cached');await fetch('/cached');
   try {await fetch('/reset')}catch(e){}
   return (await fetch('/slowfailure').then(async r=>{await r.text();return r.status}));
  });
  check(status,{'expected failure': s=>s===200});
 }); sleep(.3); } finally {await page.close();}
}
""")
            target_url = f"http://127.0.0.1:{target.server_port}"
            writer_env = {**os.environ, "BREAKTEST_DB_TOKEN": producer, "runId": "k6-smoke",
                "nodeName": "k6-probe", "location": "Local", "PG_PROXY_TOKEN": "isolated-oracle",
                "TARGET": target_url, "FIXTURE_ITERATIONS": str(args.iterations), "FIXTURE_CUSTOM": str(args.custom_metrics).lower(), "K6_NO_USAGE_REPORT": "true", "K6_HTTP_ERROR_DATA": "true"}
            for key in ("syntheticMonitoring", "K6_BROWSER_SCREENSHOT_ON_ERROR", "CUSTOM_URL_PATTERNS", "BREAKINGIT_TRANSACTION_SOURCE"):
                writer_env.pop(key, None)
            command = [str(Path(args.k6).resolve()), "run", "--quiet",
                "--out", "breaktest-db-breakingit=" + service,
                "--out", "timescaledb-breakingit=" + target_url, str(script)]
            if args.container_image:
                root.chmod(0o755); script.chmod(0o644)
                for key in ('TARGET',): writer_env[key]=writer_env[key].replace('127.0.0.1','host.docker.internal')
                command=['docker','run','--rm','--shm-size','512m','--entrypoint','/usr/bin/k6',
                    '-v',str(Path(args.k6).resolve())+':/usr/bin/k6:ro','-v',str(script)+':/fixture.js:ro',
                    '-e','K6_BROWSER_EXECUTABLE_PATH=/usr/bin/chromium','-e','K6_BROWSER_ARGS=no-sandbox',
                    '-e','K6_BROWSER_HEADLESS=true',
                    *[item for key in ('BREAKTEST_DB_TOKEN','runId','nodeName','location','PG_PROXY_TOKEN','TARGET','K6_NO_USAGE_REPORT','K6_HTTP_ERROR_DATA','FIXTURE_ITERATIONS','FIXTURE_CUSTOM') for item in ('-e',key)],
                    args.container_image,'run','--quiet','--out','breaktest-db-breakingit='+service.replace('127.0.0.1','host.docker.internal'),
                    '--out','timescaledb-breakingit='+target_url.replace('127.0.0.1','host.docker.internal'),'/fixture.js']
            observations=[]
            with (root/'k6.log').open('w+') as log:
                process=subprocess.Popen(command,env=writer_env,text=True,stdout=log,stderr=log)
                began=time.monotonic(); restarted=False
                try:
                    while process.poll() is None:
                        if time.monotonic()-began>90:raise TimeoutError('k6 smoke deadline')
                        try:
                            observation=post('/runs/k6-smoke/snapshot',{})
                            if not observation['sealed']: observations.append(observation)
                        except OSError: pass
                        if args.restart_live and not restarted and time.monotonic()-began>=2:
                            subprocess.run(['docker','kill','--signal=KILL',name],check=True,capture_output=True)
                            subprocess.run(['docker','start',name],check=True,capture_output=True)
                            restarted=True
                        time.sleep(.1)
                    log.seek(0)
                    text=log.read()
                    assert process.returncode==0 and 'BREAKTEST_DB_FAILED' not in text,text
                finally:
                    if process.poll() is None: process.kill(); process.wait()
            live=[s for s in observations if all(not x['finished'] for x in s['sources']) and min(x['watermark'] for x in s['sources'])>origin and any(r['complete'] for r in s['rows'])]
            assert live, 'No live watermark-completed aggregates observed'
            end = int(time.time() - 946684800 + 10) * 1_000_000
            final = post("/runs/k6-smoke/finalize", {})
            assert final["sealed"], final
            if args.restart_live:
                assert restarted and final['recovered'],final
                assert all(g.get('max_second') is not None for g in final['gaps']),final
                assert final['run_complete'] == (not final['gaps'] and not final['unknown_coverage']),final
            else: assert final['run_complete'] and not final['gaps'],final
            expected_counts = {"requests_raw": args.iterations*(3 if args.custom_metrics else 2), "transactions": args.iterations, "requests_error": args.iterations*2}
            counts = {}
            for table in ("requests_raw", "transactions", "requests_error", "virtual_users"):
                actual = post("/runs/k6-smoke/query", {"table": table, "start_us": origin, "end_us": end})
                counts[table] = len(actual["rows"])
                if not args.restart_live: assert counts[table] == len(oracle[table]), (table, counts[table], len(oracle[table]))
                else: assert counts[table] <= len(oracle[table]), (table, counts)
                if table in expected_counts and not args.restart_live and not args.browser:
                    assert counts[table] == expected_counts[table], (table, counts)
                else:
                    assert counts[table] > 0
                # Match all fields including timestamp, except independently-generated UUIDs.
                def normalized(row):
                    values = []
                    for i, value in enumerate(row):
                        if table == "requests_error" and i == 15:
                            continue
                        if value is None or value == "":
                            value = None
                        elif isinstance(value, bool):
                            value = "true" if value else "false"
                        else:
                            value = str(value)
                        values.append(value)
                    return json.dumps(values, ensure_ascii=False)
                left = sorted(normalized(row) for row in actual["rows"])
                right = sorted(normalized(row) for row in oracle[table])
                if args.restart_live:
                    from collections import Counter
                    assert not (Counter(left)-Counter(right)), (table,'duplicate or corrupted recovered rows')
                    if len(left)<len(right):assert final['gaps'] and not final['run_complete'],final
                else: assert left == right, (table, "row mismatch", left[:1], right[:1], actual.get("columns"))
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
            print(json.dumps({"status": "passed", "counts": counts, "lanes":args.lanes, "live_watermark_observations":len(live), "crash_recovery":args.restart_live, "browser":args.browser, "coverage_gaps":final["gaps"], "parity": "retained rows are a duplicate-free subset of oracle" if args.restart_live else "all columns except generated error UUIDs", "sealed": True, "restart_reads": True}, indent=2))
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
            target.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--k6", required=True)
    parser.add_argument("--image", default="breaktest-db:local")
    parser.add_argument('--lanes',type=int,choices=range(1,5),default=1)
    parser.add_argument('--iterations',type=int,default=80)
    parser.add_argument('--restart-live',action='store_true')
    parser.add_argument('--custom-metrics',action='store_true')
    parser.add_argument('--browser',action='store_true')
    parser.add_argument('--container-image')
    run(parser.parse_args())
