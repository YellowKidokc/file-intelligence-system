"""Behavioral Intelligence Layer — clean interface for all systems.

Usage:
    from fis.bil.bil_api import BIL

    bil = BIL()
    bil.learn("web", features={...}, signal=1)
    prediction = bil.predict("web", features={...})
    bil.export_daily()
"""

import json
from datetime import datetime
from pathlib import Path

from fis.bil.bil_models import (
    ClipboardModel,
    ContentModel,
    FileModel,
    WebModel,
)
from fis.db.connection import get_config


class BIL:
    """Behavioral Intelligence Layer — learns from your behavior without labels."""

    def __init__(self):
        self.models = {
            "web": WebModel(),
            "clipboard": ClipboardModel(),
            "files": FileModel(),
            "content": ContentModel(),
        }
        config = get_config()
        self.export_path = Path(config.get("bil", "export_path", fallback="exports"))
        self.export_path.mkdir(parents=True, exist_ok=True)

    def learn(self, model_name: str, features: dict, signal: float):
        """Feed a behavioral signal into a model.

        Args:
            model_name: Which model to update (web, clipboard, files, content)
            features: Feature dict appropriate for the model
            signal: 0-1 for binary, 0-10 for gradient
        """
        if model_name not in self.models:
            raise ValueError(f"Unknown model: {model_name}. Options: {list(self.models.keys())}")

        model = self.models[model_name]
        model.learn(features, signal)

        # Log to Postgres
        self._log_event(model_name, features, signal)

    def predict(self, model_name: str, features: dict) -> float:
        """Get a relevance prediction for given features.

        Returns predicted signal value (0-1 or 0-10 depending on model).
        """
        if model_name not in self.models:
            raise ValueError(f"Unknown model: {model_name}")

        return self.models[model_name].predict(features)

    def predict_batch(self, model_name: str, feature_list: list[dict]) -> list[float]:
        """Predict relevance for a batch of items. Returns sorted scores."""
        return [self.predict(model_name, f) for f in feature_list]

    def export_daily(self) -> str:
        """Generate a daily digest for AI session context.

        Returns path to the export file.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        export_file = self.export_path / f"bil_digest_{today}.json"

        digest = {
            "date": today,
            "models": {},
        }

        for name, model in self.models.items():
            digest["models"][name] = model.get_summary()

        # Pull recent high-signal events from Postgres
        try:
            from fis.db.connection import get_connection
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_name, features, signal
                    FROM bil_events
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                      AND signal > 0.5
                    ORDER BY signal DESC
                    LIMIT 50
                """)
                high_signal = cur.fetchall()
            conn.close()

            digest["high_signal_events"] = [
                {"model": e["model_name"], "features": e["features"], "signal": e["signal"]}
                for e in high_signal
            ]
        except Exception:
            digest["high_signal_events"] = []

        export_file.write_text(json.dumps(digest, indent=2, default=str), encoding="utf-8")
        from fis.log import get_logger
        get_logger("bil").info("Daily digest exported to %s", export_file)
        return str(export_file)

    def _log_event(self, model_name: str, features: dict, signal: float):
        """Log event to Postgres for history and analysis."""
        try:
            from fis.db.connection import get_connection
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bil_events (model_name, features, signal)
                    VALUES (%s, %s, %s)
                    """,
                    (model_name, json.dumps(features, default=str), signal),
                )
            conn.commit()
            conn.close()
        except Exception:
            pass  # Don't let logging failures break the pipeline
