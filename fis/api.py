"""FIS REST API — exposes all core functions over HTTP.

Any system can pipe data in: clipboard, browser, n8n, other repos, scripts.

Start: python -m fis api
Runs on: http://localhost:8420

Endpoints:
    POST /classify          — classify a file by path
    POST /classify-text     — classify raw text (no file needed)
    GET  /pending           — get pending rename queue
    POST /approve           — approve a pending rename
    POST /search            — search files by concept
    GET  /codes             — list all subject codes
    POST /tag               — add tags to a file
    POST /bil/learn         — feed behavioral signal
    POST /bil/predict       — get relevance prediction
    POST /bil/web           — browser extension endpoint
    POST /bil/clipboard     — clipboard event
    GET  /bil/export        — get today's daily digest
    GET  /health            — health check
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse


class FISAPIHandler(BaseHTTPRequestHandler):
    _pipeline = None
    _bil = None
    _api_token = None

    @classmethod
    def get_pipeline(cls):
        if cls._pipeline is None:
            from fis.pipeline import FISPipeline
            cls._pipeline = FISPipeline()
        return cls._pipeline

    @classmethod
    def get_bil(cls):
        if cls._bil is None:
            from fis.bil.bil_api import BIL
            cls._bil = BIL()
        return cls._bil

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/health":
            self._json_response({"status": "ok", "service": "fis"})

        elif path == "/pending":
            from fis.db.models import get_pending_files
            files = get_pending_files(limit=100)
            self._json_response({"files": files, "count": len(files)})

        elif path == "/codes":
            from fis.db.models import get_subject_codes
            query = parse_qs(urlparse(self.path).query)
            domain = query.get("domain", [None])[0]
            codes = get_subject_codes(domain)
            self._json_response({"codes": codes})

        elif path == "/bil/export":
            bil = self.get_bil()
            export_path = bil.export_daily()
            with open(export_path, "r") as f:
                digest = json.load(f)
            self._json_response(digest)

        else:
            self._json_response({"error": "not found"}, 404)

    def do_POST(self):
        if not self._authorized():
            self._json_response({"error": "unauthorized"}, 401)
            return

        path = urlparse(self.path).path
        body = self._read_body()

        if path == "/classify":
            # Classify a file by path
            file_path = body.get("path")
            if not file_path:
                self._json_response({"error": "path required"}, 400)
                return
            result = self.get_pipeline().process(file_path)
            self._json_response(result)

        elif path == "/classify-text":
            # Classify raw text without a file
            text = body.get("text", "")
            if not text:
                self._json_response({"error": "text required"}, 400)
                return
            from fis.nlp.engines import YakeEngine, text_to_slug
            from fis.nlp.classifier import FISClassifier
            yake = YakeEngine()
            keywords = yake.extract(text)
            classifier = FISClassifier()
            result = classifier.classify(text, keywords, [])
            slug = text_to_slug(keywords, 20)
            result["slug"] = slug
            result["keywords"] = [k["keyword"] for k in keywords]
            self._json_response(result)

        elif path == "/approve":
            file_id = body.get("file_id")
            if not file_id:
                self._json_response({"error": "file_id required"}, 400)
                return
            from fis.renamer import rename_file
            # Find the file
            from fis.db.models import _db
            with _db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM files WHERE file_id = %s", (file_id,))
                    f = cur.fetchone()
            if f and f["proposed_name"]:
                rename_file(f["file_path"], f["proposed_name"], f["file_id"])
                self._json_response({"status": "approved", "new_name": f["proposed_name"]})
            else:
                self._json_response({"error": "file not found or no proposed name"}, 404)

        elif path == "/search":
            query = body.get("query", "")
            from fis.db.models import search_files
            results = search_files(query, limit=body.get("limit", 20))
            self._json_response({"results": results, "count": len(results)})

        elif path == "/tag":
            file_id = body.get("file_id")
            tags = body.get("tags", [])
            if not file_id or not tags:
                self._json_response({"error": "file_id and tags required"}, 400)
                return
            from fis.db.models import insert_tags
            tag_records = [{"tag": t, "source": "manual"} for t in tags]
            insert_tags(file_id, tag_records)
            self._json_response({"status": "tagged", "count": len(tags)})

        elif path == "/bil/learn":
            model = body.get("model")
            features = body.get("features", {})
            signal = body.get("signal", 0)
            self.get_bil().learn(model, features, signal)
            self._json_response({"status": "learned"})

        elif path == "/bil/predict":
            model = body.get("model")
            features = body.get("features", {})
            score = self.get_bil().predict(model, features)
            self._json_response({"model": model, "score": score})

        elif path == "/bil/web":
            from fis.bil.bil_features import extract_web_features
            features, signal = extract_web_features(
                url=body.get("url", ""),
                text=body.get("text", ""),
                time_on_page=body.get("time_on_page", 0),
                scrolled_bottom=body.get("scrolledBottom", False),
                bookmarked=body.get("bookmarked", False),
                copied=body.get("copied", False),
            )
            self.get_bil().learn("web", features, signal)
            self._json_response({"status": "ok"})

        elif path == "/bil/clipboard":
            from fis.bil.bil_features import extract_clipboard_features
            text = body.get("text", "")
            app = body.get("app", "unknown")
            used = body.get("used", False)
            features = extract_clipboard_features(text, app)
            signal = 1.0 if used else 0.0
            self.get_bil().learn("clipboard", features, signal)
            self._json_response({"status": "ok"})

        else:
            self._json_response({"error": "not found"}, 404)

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def _json_response(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self._cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-FIS-Token")

    def _authorized(self) -> bool:
        if self._api_token is None:
            from fis.db.connection import get_config
            config = get_config()
            token = config.get("api", "shared_secret", fallback="").strip()
            self.__class__._api_token = token
        if not self._api_token:
            return True
        return self.headers.get("X-FIS-Token", "") == self._api_token

    def log_message(self, format, *args):
        from fis.log import get_logger
        get_logger("api").info(args[0] if args else "")


def start_api(port=8420):
    from fis.db.connection import get_config
    from fis.log import get_logger
    log = get_logger("api")
    config = get_config()
    host = config.get("api", "host", fallback="127.0.0.1")
    server = HTTPServer((host, port), FISAPIHandler)
    log.info("FIS API running on http://%s:%d", host, port)
    server.serve_forever()


if __name__ == "__main__":
    start_api()
