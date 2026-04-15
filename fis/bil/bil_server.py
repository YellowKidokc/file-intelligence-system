"""Lightweight HTTP server to receive BIL events from the browser extension."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from fis.bil.bil_api import BIL
from fis.bil.bil_features import extract_web_features


class BILHandler(BaseHTTPRequestHandler):
    bil = BIL()

    def do_POST(self):
        if self.path == "/bil/web":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)

            features, signal = extract_web_features(
                url=data.get("url", ""),
                text="",  # Content script doesn't send full text
                time_on_page=data.get("time_on_page", 0),
                scrolled_bottom=data.get("scrolledBottom", False),
                bookmarked=data.get("bookmarked", False),
                copied=data.get("copied", False),
            )

            self.bil.learn("web", features, signal)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default logging


def start_bil_server(port=8420):
    from fis.log import get_logger
    log = get_logger("bil.server")
    server = HTTPServer(("localhost", port), BILHandler)
    log.info("BIL server listening on http://localhost:%d", port)
    server.serve_forever()


if __name__ == "__main__":
    start_bil_server()
