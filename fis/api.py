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

    --- Preference Engine ---
    POST /pref/ingest       — ingest URLs (bulk or single)
    POST /pref/like         — like a link or file
    POST /pref/dislike      — dislike a link or file
    POST /pref/rate         — rate a link or file (1-10)
    GET  /pref/links        — list ingested links
    GET  /pref/unrated      — get unrated links queue
    POST /pref/train        — run preference feedback loop
    GET  /pref/taste        — get computed taste profile
    GET  /pref/suggestions  — BIL-powered suggestions
    POST /pref/score        — score a specific link
    GET  /pref/stats        — preference + link stats
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse


class FISAPIHandler(BaseHTTPRequestHandler):
    _pipeline = None
    _bil = None
    _pref_engine = None
    _link_intel = None
    _feedback_loop = None

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

    @classmethod
    def get_pref_engine(cls):
        if cls._pref_engine is None:
            from fis.preference.engine import PreferenceEngine
            cls._pref_engine = PreferenceEngine()
        return cls._pref_engine

    @classmethod
    def get_link_intel(cls):
        if cls._link_intel is None:
            from fis.preference.links import LinkIntelligence
            cls._link_intel = LinkIntelligence()
        return cls._link_intel

    @classmethod
    def get_feedback_loop(cls):
        if cls._feedback_loop is None:
            from fis.preference.feedback import FeedbackLoop
            cls._feedback_loop = FeedbackLoop()
        return cls._feedback_loop

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

        # --- Preference Engine GET routes ---

        elif path == "/pref/links":
            query = parse_qs(urlparse(self.path).query)
            li = self.get_link_intel()
            links = li.list_links(
                limit=int(query.get("limit", [50])[0]),
                offset=int(query.get("offset", [0])[0]),
                source=query.get("source", [None])[0],
                fis_domain=query.get("domain", [None])[0],
                unclassified_only=query.get("unclassified", [""])[0] == "1",
            )
            self._json_response({"links": links, "count": len(links)})

        elif path == "/pref/unrated":
            query = parse_qs(urlparse(self.path).query)
            pe = self.get_pref_engine()
            unrated = pe.get_unrated(limit=int(query.get("limit", [20])[0]))
            self._json_response({"links": unrated, "count": len(unrated)})

        elif path == "/pref/taste":
            pe = self.get_pref_engine()
            profile = pe.taste_profile()
            self._json_response(profile)

        elif path == "/pref/suggestions":
            query = parse_qs(urlparse(self.path).query)
            fl = self.get_feedback_loop()
            suggestions = fl.suggest(limit=int(query.get("limit", [10])[0]))
            self._json_response({"suggestions": suggestions, "count": len(suggestions)})

        elif path == "/pref/stats":
            li = self.get_link_intel()
            pe = self.get_pref_engine()
            self._json_response({
                "links": li.stats(),
                "preferences": pe.stats(),
            })

        else:
            self._json_response({"error": "not found"}, 404)

    def do_POST(self):
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

        # --- Preference Engine POST routes ---

        elif path == "/pref/ingest":
            li = self.get_link_intel()
            urls = body.get("urls", [])
            text = body.get("text", "")
            source = body.get("source", "api")

            if urls:
                # Direct URL list (strings or objects)
                result = li.ingest_batch(urls, source=source)
            elif text:
                # Free text with URLs to extract
                from fis.preference.ingest import BulkIngestor
                bi = BulkIngestor()
                result = bi.from_text(text, source=source)
            else:
                self._json_response({"error": "urls (array) or text (string) required"}, 400)
                return
            self._json_response(result)

        elif path == "/pref/like":
            pe = self.get_pref_engine()
            pref = pe.like(
                link_id=body.get("link_id"),
                file_id=body.get("file_id"),
                tags=body.get("tags"),
                note=body.get("note"),
            )
            self._json_response({"status": "liked", "pref": pref})

        elif path == "/pref/dislike":
            pe = self.get_pref_engine()
            pref = pe.dislike(
                link_id=body.get("link_id"),
                file_id=body.get("file_id"),
                tags=body.get("tags"),
                note=body.get("note"),
            )
            self._json_response({"status": "disliked", "pref": pref})

        elif path == "/pref/rate":
            pe = self.get_pref_engine()
            score = body.get("score")
            if not score:
                self._json_response({"error": "score (1-10) required"}, 400)
                return
            pref = pe.rate(
                score=int(score),
                link_id=body.get("link_id"),
                file_id=body.get("file_id"),
                tags=body.get("tags"),
                note=body.get("note"),
            )
            self._json_response({"status": "rated", "pref": pref})

        elif path == "/pref/train":
            fl = self.get_feedback_loop()
            result = fl.train_cycle()
            self._json_response(result)

        elif path == "/pref/score":
            link_id = body.get("link_id")
            if not link_id:
                self._json_response({"error": "link_id required"}, 400)
                return
            fl = self.get_feedback_loop()
            result = fl.score_link(int(link_id))
            self._json_response(result)

        elif path == "/pref/search":
            li = self.get_link_intel()
            query = body.get("query", "")
            if not query:
                self._json_response({"error": "query required"}, 400)
                return
            results = li.search(
                query, limit=body.get("limit", 20),
                fis_domain=body.get("domain"),
            )
            self._json_response({"results": results, "count": len(results)})

        elif path == "/pref/enrich":
            li = self.get_link_intel()
            link_id = body.get("link_id")
            if not link_id:
                self._json_response({"error": "link_id required"}, 400)
                return
            link = li.enrich(
                link_id=int(link_id),
                title=body.get("title"),
                description=body.get("description"),
                content_text=body.get("content_text"),
            )
            self._json_response(link)

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
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def log_message(self, format, *args):
        from fis.log import get_logger
        get_logger("api").info(args[0] if args else "")


def start_api(port=8420):
    from fis.log import get_logger
    log = get_logger("api")
    server = HTTPServer(("0.0.0.0", port), FISAPIHandler)
    log.info("FIS API running on http://localhost:%d", port)
    server.serve_forever()


if __name__ == "__main__":
    start_api()
