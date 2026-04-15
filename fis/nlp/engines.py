"""NLP engines for keyword extraction and entity recognition."""

import re


class YakeEngine:
    """Fast statistical keyword extraction. Zero model overhead."""

    def __init__(self, language="en", max_ngram=3, top_n=5):
        import yake

        self.extractor = yake.KeywordExtractor(
            lan=language,
            n=max_ngram,
            top=top_n,
            dedupLim=0.9,
            dedupFunc="seqm",
        )

    def extract(self, text: str) -> list[dict]:
        if not text.strip():
            return []
        keywords = self.extractor.extract_keywords(text)
        # YAKE returns (keyword, score) where lower score = more relevant
        return [
            {"keyword": kw, "score": round(1 - score, 3), "source": "yake"}
            for kw, score in keywords
        ]


class SpacyEngine:
    """Named entity recognition using spaCy."""

    def __init__(self, model_name="en_core_web_sm", custom_terms=None):
        import spacy

        self.nlp = spacy.load(model_name)

        # Add custom entity ruler for domain-specific terms
        if custom_terms:
            ruler = self.nlp.add_pipe("entity_ruler", before="ner")
            patterns = [
                {"label": term["label"], "pattern": term["pattern"]}
                for term in custom_terms
            ]
            ruler.add_patterns(patterns)

    def extract(self, text: str) -> list[dict]:
        if not text.strip():
            return []
        doc = self.nlp(text[:100000])  # Limit to 100k chars for performance
        entities = []
        seen = set()
        for ent in doc.ents:
            key = (ent.text.lower(), ent.label_)
            if key not in seen:
                seen.add(key)
                entities.append({
                    "entity": ent.text,
                    "label": ent.label_,
                    "source": "spacy",
                })
        return entities


class KeyBERTEngine:
    """Semantic keyword extraction using KeyBERT + Model2Vec."""

    def __init__(self, top_n=5):
        self.top_n = top_n
        self._model = None

    @property
    def model(self):
        if self._model is None:
            from keybert import KeyBERT

            self._model = KeyBERT("distilbert-base-nli-mean-tokens")
        return self._model

    def extract(self, text: str) -> list[dict]:
        if not text.strip():
            return []
        keywords = self.model.extract_keywords(
            text[:10000],  # KeyBERT is slower, limit input
            keyphrase_ngram_range=(1, 3),
            stop_words="english",
            top_n=self.top_n,
            use_maxsum=True,
            nr_candidates=20,
        )
        return [
            {"keyword": kw, "score": round(score, 3), "source": "keybert"}
            for kw, score in keywords
        ]


def build_custom_terms_from_db() -> list[dict]:
    """Load subject codes from Postgres and build spaCy entity patterns."""
    from fis.db.models import get_subject_codes

    codes = get_subject_codes()
    patterns = []
    for code in codes:
        # Add the label itself
        patterns.append({"label": code["code"], "pattern": code["label"]})
        # Add aliases
        if code.get("aliases"):
            for alias in code["aliases"]:
                patterns.append({"label": code["code"], "pattern": alias})
        # Add trigger words as individual patterns
        if code.get("trigger_words"):
            for word in code["trigger_words"]:
                if len(word) > 3:  # Skip very short trigger words
                    patterns.append({"label": code["code"], "pattern": word})
    return patterns


def text_to_slug(keywords: list[dict], max_chars: int = 20) -> str:
    """Convert top keywords into a kebab-case slug."""
    if not keywords:
        return "untitled"

    # Sort by score descending, take top phrases
    sorted_kw = sorted(keywords, key=lambda x: x.get("score", 0), reverse=True)

    slug_parts = []
    char_count = 0
    for kw in sorted_kw:
        word = kw["keyword"].lower()
        word = re.sub(r"[^a-z0-9\s]", "", word)
        word = re.sub(r"\s+", "-", word.strip())
        if not word:
            continue
        if char_count + len(word) + 1 > max_chars:
            break
        slug_parts.append(word)
        char_count += len(word) + 1  # +1 for hyphen

    return "-".join(slug_parts) if slug_parts else "untitled"
