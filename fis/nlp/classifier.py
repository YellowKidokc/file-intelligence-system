"""Classifier that maps NLP output to domain/subject codes."""

import json
import pickle
from pathlib import Path

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import LabelEncoder


class FISClassifier:
    """Maps NLP-extracted features to domain and subject codes.

    Uses a lightweight SGDClassifier that updates incrementally
    from user corrections.
    """

    def __init__(self, model_dir: str = "models/saved"):
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)

        self.vectorizer = TfidfVectorizer(max_features=5000, ngram_range=(1, 2))
        self.domain_clf = SGDClassifier(loss="log_loss", random_state=42)
        self.subject_clf = SGDClassifier(loss="log_loss", random_state=42)
        self.domain_encoder = LabelEncoder()
        self.subject_encoder = LabelEncoder()

        self._fitted = False
        self._load_if_exists()

    def _load_if_exists(self):
        model_path = self.model_dir / "classifier.pkl"
        if model_path.exists():
            with open(model_path, "rb") as f:
                data = pickle.load(f)
            self.vectorizer = data["vectorizer"]
            self.domain_clf = data["domain_clf"]
            self.subject_clf = data["subject_clf"]
            self.domain_encoder = data["domain_encoder"]
            self.subject_encoder = data["subject_encoder"]
            self._fitted = True

    def save(self):
        model_path = self.model_dir / "classifier.pkl"
        with open(model_path, "wb") as f:
            pickle.dump({
                "vectorizer": self.vectorizer,
                "domain_clf": self.domain_clf,
                "subject_clf": self.subject_clf,
                "domain_encoder": self.domain_encoder,
                "subject_encoder": self.subject_encoder,
            }, f)

    def classify(self, text: str, keywords: list[dict], entities: list[dict]) -> dict:
        """Classify text into domain and subject codes.

        Returns dict with domain, subjects, confidence.
        """
        # First try rule-based matching against subject codes
        rule_result = self._rule_based_match(text, keywords, entities)

        # If learning layer is trained, blend with ML prediction
        if self._fitted:
            ml_result = self._ml_predict(text, keywords)
            return self._blend_results(rule_result, ml_result)

        return rule_result

    def _rule_based_match(self, text: str, keywords: list[dict], entities: list[dict]) -> dict:
        """Match against subject code trigger words from the database."""
        from fis.db.models import get_subject_codes

        codes = get_subject_codes()
        scores = {}

        text_lower = text.lower()
        kw_text = " ".join(k["keyword"].lower() for k in keywords)
        ent_labels = [e.get("label", "") for e in entities]

        for code in codes:
            score = 0
            code_name = code["code"]

            # Check trigger words
            if code.get("trigger_words"):
                for word in code["trigger_words"]:
                    if word.lower() in text_lower:
                        score += 1
                    if word.lower() in kw_text:
                        score += 2  # Keywords weighted higher

            # Check if spaCy found entities matching this code
            if code_name in ent_labels:
                score += 3

            # Check aliases
            if code.get("aliases"):
                for alias in code["aliases"]:
                    if alias.lower() in text_lower:
                        score += 2

            if score > 0:
                scores[code_name] = {
                    "score": score,
                    "domain": code["domain"],
                }

        if not scores:
            return {"domain": "--", "subjects": ["GN"], "confidence": 10.0}

        # Sort by score, take top 3
        sorted_codes = sorted(scores.items(), key=lambda x: x[1]["score"], reverse=True)
        top_codes = sorted_codes[:3]

        # Determine domain from highest scoring code
        domain = top_codes[0][1]["domain"]
        subjects = [c[0] for c in top_codes]

        # Confidence based on top score relative to total possible
        max_score = top_codes[0][1]["score"]
        confidence = min(max_score * 10, 100.0)

        return {"domain": domain, "subjects": subjects, "confidence": confidence}

    def _ml_predict(self, text: str, keywords: list[dict]) -> dict:
        """Use trained ML model to predict domain and subject."""
        combined = text[:2000] + " " + " ".join(k["keyword"] for k in keywords)
        X = self.vectorizer.transform([combined])

        domain_proba = self.domain_clf.predict_proba(X)[0]
        domain_idx = np.argmax(domain_proba)
        domain = self.domain_encoder.inverse_transform([domain_idx])[0]
        domain_conf = domain_proba[domain_idx] * 100

        subject_proba = self.subject_clf.predict_proba(X)[0]
        top_3_idx = np.argsort(subject_proba)[-3:][::-1]
        subjects = self.subject_encoder.inverse_transform(top_3_idx).tolist()
        subject_conf = subject_proba[top_3_idx[0]] * 100

        confidence = (domain_conf + subject_conf) / 2

        return {"domain": domain, "subjects": subjects, "confidence": confidence}

    def _blend_results(self, rule: dict, ml: dict) -> dict:
        """Blend rule-based and ML results, preferring higher confidence."""
        if rule["confidence"] >= ml["confidence"]:
            return rule
        return ml

    def learn(self, texts: list[str], keywords_list: list[list[dict]],
              domains: list[str], subjects: list[str]):
        """Update the classifier from a batch of corrections."""
        combined = []
        for text, kws in zip(texts, keywords_list):
            combined.append(text[:2000] + " " + " ".join(k["keyword"] for k in kws))

        if not self._fitted:
            # First time — full fit
            self.domain_encoder.fit(domains)
            self.subject_encoder.fit(subjects)
            X = self.vectorizer.fit_transform(combined)
            y_domain = self.domain_encoder.transform(domains)
            y_subject = self.subject_encoder.transform(subjects)
            self.domain_clf.fit(X, y_domain)
            self.subject_clf.fit(X, y_subject)
            self._fitted = True
        else:
            # Incremental update
            X = self.vectorizer.transform(combined)
            y_domain = self.domain_encoder.transform(domains)
            y_subject = self.subject_encoder.transform(subjects)
            self.domain_clf.partial_fit(X, y_domain)
            self.subject_clf.partial_fit(X, y_subject)

        self.save()
