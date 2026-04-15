"""Feedback Loop — bridges explicit preferences into BIL training.

This is the core "loop" that makes the system learn from your taste:

    1. You like/dislike/rate links and files
    2. FeedbackLoop extracts features from those preferences
    3. Features are fed to BIL's PreferenceModel (and optionally web/content models)
    4. BIL predictions improve, reflecting your explicit taste
    5. Suggestions and relevance scores update accordingly

Usage:
    from fis.preference.feedback import FeedbackLoop

    loop = FeedbackLoop()
    result = loop.train_cycle()     # Process all unfed preferences
    score = loop.score_link(42)     # Score a link using trained model
    suggestions = loop.suggest(10)  # Get top suggestions from unrated links
"""

from datetime import datetime

from fis.log import get_logger

log = get_logger("feedback")


class FeedbackLoop:
    """Converts explicit preferences into BIL training signals."""

    def __init__(self):
        from fis.bil.bil_api import BIL
        from fis.preference.engine import PreferenceEngine

        self.bil = BIL()
        self.engine = PreferenceEngine()

    def train_cycle(self) -> dict:
        """Process all unfed preferences through BIL.

        This is the main training entry point. Call it after a batch of
        likes/dislikes, or on a schedule.

        Returns summary of the training cycle.
        """
        unfed = self.engine.get_unfed(limit=500)

        if not unfed:
            log.info("No unfed preferences to process")
            return {"processed": 0, "message": "no new preferences"}

        processed = 0
        pref_ids = []

        for pref in unfed:
            features = self._extract_features(pref)
            if not features:
                continue

            signal = pref["score"]

            # Feed to preference model (primary)
            self.bil.learn("preference", features, signal)

            # Cross-feed to web model if this is a link preference
            if pref.get("link_id") and pref.get("url_domain"):
                web_features = self._to_web_features(pref, features)
                self.bil.learn("web", web_features, signal)

            # Cross-feed to content model if we have content
            if features.get("has_content"):
                content_features = self._to_content_features(features)
                self.bil.learn("content", content_features, signal * 10)  # Content uses 0-10

            processed += 1
            pref_ids.append(pref["pref_id"])

        # Mark all as fed
        self.engine.mark_fed(pref_ids)

        # Refresh taste profile (recompute after training)
        taste = self.engine.taste_vector()

        log.info("Training cycle complete: %d preferences processed", processed)
        return {
            "processed": processed,
            "taste_vector": taste,
            "timestamp": datetime.now().isoformat(),
        }

    def score_link(self, link_id: int) -> dict:
        """Score a single link using the trained preference model.

        Returns prediction from the preference model plus cross-model scores.
        """
        from fis.preference.links import LinkIntelligence

        li = LinkIntelligence()
        link = li.get_by_id(link_id)
        if not link:
            raise ValueError(f"Link {link_id} not found")

        features = self._link_to_features(link)
        pref_score = self.bil.predict("preference", features)

        # Also get web model opinion if available
        web_features = self._link_to_web_features(link)
        web_score = self.bil.predict("web", web_features)

        # Blend: 70% preference model, 30% web model
        blended = 0.7 * pref_score + 0.3 * web_score

        return {
            "link_id": link_id,
            "url": link["url"],
            "preference_score": round(pref_score, 4),
            "web_score": round(web_score, 4),
            "blended_score": round(blended, 4),
            "fis_domain": link.get("fis_domain"),
            "confidence": link.get("confidence"),
        }

    def score_batch(self, link_ids: list[int]) -> list[dict]:
        """Score multiple links. Returns sorted by blended score descending."""
        scores = [self.score_link(lid) for lid in link_ids]
        scores.sort(key=lambda s: s["blended_score"], reverse=True)
        return scores

    def suggest(self, limit: int = 10) -> list[dict]:
        """Suggest unrated links that BIL predicts you'll like.

        Scores all unrated links and returns the top N by blended score.
        """
        unrated = self.engine.get_unrated(limit=100)
        if not unrated:
            return []

        scored = []
        for link in unrated:
            features = self._link_to_features(link)
            pref_score = self.bil.predict("preference", features)

            web_features = self._link_to_web_features(link)
            web_score = self.bil.predict("web", web_features)

            blended = 0.7 * pref_score + 0.3 * web_score

            scored.append({
                "link_id": link["link_id"],
                "url": link["url"],
                "title": link.get("title"),
                "domain": link.get("domain"),
                "fis_domain": link.get("fis_domain"),
                "blended_score": round(blended, 4),
                "preference_score": round(pref_score, 4),
            })

        scored.sort(key=lambda s: s["blended_score"], reverse=True)
        return scored[:limit]

    # --- Feature extraction ---

    def _extract_features(self, pref: dict) -> dict:
        """Extract BIL-ready features from a preference record with joined data."""
        features = {}

        # URL domain (e.g., arxiv.org)
        if pref.get("url_domain"):
            features[f"dom_{pref['url_domain']}"] = 1

        # FIS domain (e.g., TP, DT)
        if pref.get("fis_domain"):
            features[f"fis_{pref['fis_domain']}"] = 1

        # Subject codes
        for sc in (pref.get("subject_codes") or []):
            features[f"sub_{sc}"] = 1

        # Keywords
        for kw in (pref.get("keywords") or [])[:10]:
            features[f"kw_{kw.lower()}"] = 1

        # FIS confidence as numeric feature
        if pref.get("link_confidence"):
            features["fis_confidence"] = pref["link_confidence"] / 100.0

        # Has content
        features["has_content"] = 1 if pref.get("link_slug") else 0

        # Inject taste vector as context features
        taste = self.engine.taste_vector()
        for domain, score in taste.get("top_domains", {}).items():
            features[f"taste_dom_{domain}"] = score
        for subj, score in taste.get("top_subjects", {}).items():
            features[f"taste_sub_{subj}"] = score

        # Time features
        now = datetime.now()
        features["hour"] = now.hour
        features["day_of_week"] = now.weekday()

        return features

    def _link_to_features(self, link: dict) -> dict:
        """Convert a link record to preference model features."""
        features = {}

        if link.get("domain"):
            features[f"dom_{link['domain']}"] = 1
        if link.get("fis_domain"):
            features[f"fis_{link['fis_domain']}"] = 1
        for sc in (link.get("subject_codes") or []):
            features[f"sub_{sc}"] = 1
        for kw in (link.get("keywords") or [])[:10]:
            features[f"kw_{kw.lower()}"] = 1
        if link.get("confidence"):
            features["fis_confidence"] = link["confidence"] / 100.0
        features["has_content"] = 1 if link.get("content_text") else 0

        now = datetime.now()
        features["hour"] = now.hour
        features["day_of_week"] = now.weekday()

        return features

    def _link_to_web_features(self, link: dict) -> dict:
        """Convert a link to web model features (for cross-model scoring)."""
        features = {}
        if link.get("domain"):
            features["domain"] = link["domain"]
        if link.get("keywords"):
            features["top_keywords"] = link["keywords"][:5]
        features["word_count"] = len((link.get("content_text") or "").split())
        features["has_equations"] = any(
            c in (link.get("content_text") or "")
            for c in "\u222b\u2211\u220f\u2202\u2207\u03c7\u03c8\u03c6="
        )
        features["time_of_day"] = datetime.now().hour
        features["time_on_page"] = 0
        return features

    def _to_web_features(self, pref: dict, features: dict) -> dict:
        """Convert preference features to web model format for cross-training."""
        web = {}
        if pref.get("url_domain"):
            web["domain"] = pref["url_domain"]
        if pref.get("keywords"):
            web["top_keywords"] = pref["keywords"][:5]
        web["word_count"] = 0
        web["time_of_day"] = datetime.now().hour
        web["time_on_page"] = 0
        return web

    def _to_content_features(self, features: dict) -> dict:
        """Convert preference features to content model format."""
        return {k: v for k, v in features.items()
                if not k.startswith("taste_")}
