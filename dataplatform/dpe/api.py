"""Serving layer: a small read-only JSON API over the data product catalog.

GET /health
GET /products
GET /products/<name>[?version=N]
GET /products/<name>/data?limit=100&offset=0[&version=N]
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .registry import Catalog


def _int(qs, key, default, lo, hi):
    try:
        return max(lo, min(hi, int(qs.get(key, [default])[0])))
    except ValueError:
        return default


def make_handler(catalog: Catalog):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, payload) -> None:
            body = json.dumps(payload, indent=2).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802 (http.server naming)
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            qs = parse_qs(url.query)
            version = _int(qs, "version", None, 1, 10**6) if "version" in qs else None

            if parts == ["health"]:
                return self._send(200, {"status": "ok"})
            if parts == ["products"]:
                return self._send(200, catalog.list_products())
            if len(parts) == 2 and parts[0] == "products":
                product = catalog.get_product(parts[1], version)
                return self._send(200, product) if product else \
                    self._send(404, {"error": "product not found"})
            if len(parts) == 3 and parts[0] == "products" and parts[2] == "data":
                rows = catalog.read_data(parts[1],
                                         _int(qs, "limit", 100, 1, 1000),
                                         _int(qs, "offset", 0, 0, 10**9), version)
                return self._send(200, rows) if rows is not None else \
                    self._send(404, {"error": "product not found"})
            self._send(404, {"error": "not found"})

        def log_message(self, fmt, *args):  # quieter logs
            print(f"[api] {self.address_string()} {fmt % args}")

    return Handler


def serve(catalog: Catalog, host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(catalog))
    print(f"Serving data products on http://{host}:{port}/products  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
