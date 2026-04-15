"""River-based online learning models for behavioral intelligence."""

from river import compose, linear_model, preprocessing


class BaseModel:
    """Base class for BIL models using River online learning."""

    def __init__(self):
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            linear_model.LogisticRegression(),
        )
        self.event_count = 0

    def learn(self, features: dict, signal: float):
        """Update model with a single observation."""
        # Convert signal to binary for logistic regression
        y = 1 if signal > 0.5 else 0
        self.model.learn_one(features, y)
        self.event_count += 1

    def predict(self, features: dict) -> float:
        """Predict probability of positive signal."""
        return self.model.predict_proba_one(features).get(True, 0.5)

    def get_summary(self) -> dict:
        """Return model summary for daily export."""
        return {
            "event_count": self.event_count,
            "type": self.__class__.__name__,
        }


class WebModel(BaseModel):
    """Learns which web pages you engage with vs skip.

    Features: domain, word_count, has_equations, top_keywords,
              time_of_day, session_position, days_since_domain
    Signal: 0 = closed quickly, 1 = bookmarked/copied
    """

    def learn(self, features: dict, signal: float):
        # Flatten keyword features for River
        flat = self._flatten(features)
        super().learn(flat, signal)

    def predict(self, features: dict) -> float:
        flat = self._flatten(features)
        return super().predict(flat)

    def _flatten(self, features: dict) -> dict:
        flat = {}
        for k, v in features.items():
            if k == "top_keywords":
                # Convert keyword list to binary features
                if isinstance(v, list):
                    for kw in v:
                        flat[f"kw_{kw}"] = 1
            elif isinstance(v, bool):
                flat[k] = 1 if v else 0
            elif isinstance(v, (int, float)):
                flat[k] = v
            elif isinstance(v, str):
                flat[f"{k}_{v}"] = 1
        return flat


class ClipboardModel(BaseModel):
    """Learns clipboard usage patterns — what you copy in sequence.

    Features: text_keywords, app, hour, sequence_position
    Signal: 1 = you used/pasted it, 0 = copied but never used
    """

    def learn(self, features: dict, signal: float):
        flat = self._flatten(features)
        super().learn(flat, signal)

    def predict(self, features: dict) -> float:
        flat = self._flatten(features)
        return super().predict(flat)

    def _flatten(self, features: dict) -> dict:
        flat = {}
        for k, v in features.items():
            if k == "text_keywords" and isinstance(v, list):
                for kw in v:
                    flat[f"kw_{kw}"] = 1
            elif isinstance(v, (int, float)):
                flat[k] = v
            elif isinstance(v, str):
                flat[f"{k}_{v}"] = 1
        return flat


class FileModel(BaseModel):
    """Learns file access patterns — which files you actually use.

    Features: domain, subject, confidence, hour, day_of_week
    Signal: 1 = you opened/approved it, 0 = sat in queue
    """
    pass


class ContentModel(BaseModel):
    """Learns content relevance — what content you agree with.

    Features: TF-IDF vector of text + domain + subject
    Signal: your agreement score (0-10 gradient)
    """

    def learn(self, features: dict, signal: float):
        # Content model uses 0-10 gradient, normalize to 0-1
        normalized = signal / 10.0 if signal > 1 else signal
        super().learn(features, normalized)
