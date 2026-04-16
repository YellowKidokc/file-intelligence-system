"""Core FIS pipeline — reads file, runs NLP engines, classifies, proposes name."""

from pathlib import Path

from fis.db.codes import resolve_domain, resolve_subject
from fis.db.connection import get_config
from fis.db.models import (
    compute_sha256,
    file_exists_by_hash,
    get_next_sequence_id,
    insert_file,
    insert_tags,
)
from fis.nlp.classifier import FISClassifier
from fis.nlp.engines import YakeEngine, SpacyEngine, KeyBERTEngine, text_to_slug
from fis.nlp.extractor import extract_text


class FISPipeline:
    """The main processing pipeline.

    1. Extract text from file
    2. Compute SHA-256 hash (skip if duplicate)
    3. Run YAKE (always)
    4. Run spaCy (always)
    5. Run classifier
    6. If confidence < threshold, run KeyBERT
    7. Generate slug and proposed filename
    8. Store in Postgres
    """

    def __init__(self):
        config = get_config()
        self.slug_max = int(config.get("pipeline", "slug_max_chars", fallback="20"))
        self.auto_threshold = float(config.get("pipeline", "auto_rename_threshold", fallback="85"))
        self.propose_threshold = float(config.get("pipeline", "propose_threshold", fallback="50"))
        self.yake_top_n = int(config.get("pipeline", "yake_top_n", fallback="5"))

        # Initialize engines (lazy load expensive ones)
        self.yake = YakeEngine(top_n=self.yake_top_n)
        self.spacy = None  # Loaded on first use
        self.keybert = None  # Only loaded when needed
        self.classifier = FISClassifier()

    def _get_spacy(self):
        if self.spacy is None:
            from fis.nlp.engines import build_custom_terms_from_db
            try:
                terms = build_custom_terms_from_db()
            except Exception:
                terms = None
            self.spacy = SpacyEngine(custom_terms=terms)
        return self.spacy

    def _get_keybert(self):
        if self.keybert is None:
            self.keybert = KeyBERTEngine()
        return self.keybert

    def process(self, file_path: str) -> dict:
        """Process a single file through the full pipeline.

        Returns dict with: file_id, proposed_name, domain, subjects,
                          slug, confidence, status, tags
        """
        path = Path(file_path)
        if not path.exists():
            return {"error": f"File not found: {file_path}"}

        # 1. Hash check — skip duplicates
        sha256 = compute_sha256(file_path)
        existing = file_exists_by_hash(sha256)
        if existing:
            return {
                "status": "duplicate",
                "existing_id": existing["sequence_id"],
                "message": f"Duplicate of {existing['final_name'] or existing['original_name']}",
            }

        # 2. Extract text
        text = extract_text(file_path)
        if not text.strip():
            # Can't classify empty files — still register them
            result = insert_file(
                original_name=path.name,
                file_path=str(path.resolve()),
                sha256=sha256,
                status="kickout",
                confidence=0.0,
            )
            return {
                "status": "kickout",
                "reason": "no_text",
                "file_id": result["file_id"],
                "sequence_id": result["sequence_id"],
            }

        # 3. YAKE — always runs
        yake_keywords = self.yake.extract(text)

        # 4. spaCy — always runs
        spacy_entities = self._get_spacy().extract(text)

        # 5. Classify
        classification = self.classifier.classify(text, yake_keywords, spacy_entities)
        confidence = classification["confidence"]

        # 6. If low confidence, run KeyBERT for better keywords
        all_keywords = yake_keywords
        if confidence < self.propose_threshold:
            keybert_keywords = self._get_keybert().extract(text)
            all_keywords = yake_keywords + keybert_keywords
            # Re-classify with enriched keywords
            classification = self.classifier.classify(text, all_keywords, spacy_entities)
            confidence = classification["confidence"]

        # 7. Generate slug and proposed filename (resolve through code layer)
        slug = text_to_slug(all_keywords, self.slug_max)
        domain = resolve_domain(classification["domain"])
        subjects = [resolve_subject(s) for s in classification["subjects"]]
        subject_str = "-".join(subjects[:3])

        # Determine status
        if confidence >= self.auto_threshold:
            status = "auto"
        elif confidence >= self.propose_threshold:
            status = "pending"
        else:
            status = "kickout"

        # Build proposed name: [slug]_[DOMAIN].[SUBJECTS]_[ID].ext
        ext = path.suffix

        # Get sequence ID first so proposed_name is set atomically with insert
        seq_id = get_next_sequence_id()
        proposed_name = f"{slug}_{domain}.{subject_str}_{seq_id}{ext}"

        # 8. Store in Postgres (proposed_name and sequence_id included in initial insert)
        result = insert_file(
            original_name=path.name,
            file_path=str(path.resolve()),
            sha256=sha256,
            domain=domain,
            subject_codes=subjects,
            slug=slug,
            proposed_name=proposed_name,
            confidence=confidence,
            status=status,
            sequence_id=seq_id,
        )

        # Store tags
        tags = [{"tag": kw["keyword"], "source": kw["source"], "confidence": kw.get("score")}
                for kw in all_keywords]
        for ent in spacy_entities:
            tags.append({"tag": ent["entity"], "source": "spacy"})
        insert_tags(result["file_id"], tags)

        return {
            "status": status,
            "file_id": result["file_id"],
            "sequence_id": seq_id,
            "original_name": path.name,
            "proposed_name": proposed_name,
            "domain": domain,
            "subjects": subjects,
            "slug": slug,
            "confidence": confidence,
            "keywords": [k["keyword"] for k in all_keywords],
            "entities": [e["entity"] for e in spacy_entities],
        }
