"""Minimal stand-in for the Prometheus query API, used by scripts/e2e-rollback.sh.

GET /api/v1/query answers the controller's analysis queries. Every revision
looks healthy (1000 requests, 0 errors, p95 50ms) until a healthy template
hash is set with POST /admin/healthy?hash=<hash>; after that, any other
pod_template_hash reports a 20% error rate.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import json
import re
import threading

HASH = re.compile(r'pod_template_hash="([^"]+)"')
STATE = {"healthy": None}
LOCK = threading.Lock()


def sample(query):
    match = HASH.search(query)
    with LOCK:
        healthy = STATE["healthy"]
    failing = healthy is not None and (match is None or match.group(1) != healthy)
    if "histogram_quantile" in query:
        return 50.0
    if 'status=~"5.."' in query:
        return 200.0 if failing else 0.0
    return 1000.0


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        if url.path != "/api/v1/query":
            self.send_error(404)
            return
        query = parse_qs(url.query).get("query", [""])[0]
        body = json.dumps({
            "status": "success",
            "data": {"resultType": "vector", "result": [{"metric": {}, "value": [0, str(sample(query))]}]},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        url = urlparse(self.path)
        if url.path != "/admin/healthy":
            self.send_error(404)
            return
        with LOCK:
            STATE["healthy"] = parse_qs(url.query).get("hash", [None])[0]
        self.send_response(204)
        self.end_headers()

    def log_message(self, fmt, *args):
        pass


ThreadingHTTPServer(("", 9090), Handler).serve_forever()
