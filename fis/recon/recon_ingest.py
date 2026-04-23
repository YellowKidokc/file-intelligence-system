"""Recon/Extractor -> FIS integration.

Watches for .md files with YAML frontmatter containing pre-classified metadata
(tags, domain, subject, source_url, analysis_type, concept_mapping, confidence).
Uses frontmatter labels as high-confidence training data instead of classifying
from scratch — solves the cold-start problem.
"""

import re
from pathlib import Path

from fis.db.codes import resolve_domain, resolve_subject
from fis.db.connection import get_config
from fis.db.models import (
    compute_sha256,
    file_exists_by_hash,
    file_exists_by_path,
    get_next_sequence_id,
    insert_file_with_tags,
)
from fis.log import get_logger
from fis.nlp.engines import YakeEngine, SpacyEngine, text_to_slug

log = get_logger("recon")

# Map analysis_type tags from the extractor to subject codes
ANALYSIS_TAG_MAP = {
    "BRIDGE": "BR",
    "DATA": "DT",
    "CONFIRMS": "CF",
    "CHALLENGES": "CH",
    "EXTENDS": "EX",
    "BURIED": "BD",
}

# Reuse shared engine instances
_yake = None
_spacy = None
_classifier = None
_training_buffer = []
_training_batch_size = None


def _get_yake():
    global _yake
    if _yake is None:
        config = get_config()
        top_n = int(config.get("pipeline", "yake_top_n", fallback="5"))
        _yake = YakeEngine(top_n=top_n)
    return _yake


def _get_spacy():
    global _spacy
    if _spacy is None:
        from fis.nlp.engines import build_custom_terms_from_db
        try:
            terms = build_custom_terms_from_db()
        except Exception:
            terms = None
        _spacy = SpacyEngine(custom_terms=terms)
    return _spacy


def _get_classifier():
    global _classifier
    if _classifier is None:
        from fis.nlp.classifier import FISClassifier
        _classifier = FISClassifier()
    return _classifier


def _get_training_batch_size() -> int:
    global _training_batch_size
    if _training_batch_size is None:
        config = get_config()
        _training_batch_size = int(config.get("recon", "training_batch_size", fallback="25"))
    return _training_batch_size


def _buffer_training_example(text: str, keywords: list[dict], domain: str, subject: str):
    _training_buffer.append({
        "text": text,
        "keywords": keywords,
        "domain": domain,
        "subject": subject,
    })

    batch_size = _get_training_batch_size()
    if len(_training_buffer) >= batch_size:
        _flush_training_buffer()


def _flush_training_buffer():
    if not _training_buffer:
        return

    batch = list(_training_buffer)
    _training_buffer.clear()
    classifier = _get_classifier()
    classifier.learn(
        texts=[item["text"] for item in batch],
        keywords_list=[item["keywords"] for item in batch],
        domains=[item["domain"] for item in batch],
        subjects=[item["subject"] for item in batch],
    )


def has_frontmatter(file_path: str) -> bool:
    """Quick check: does this .md file start with YAML frontmatter?"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
            return first_line == "---"
    except (OSError, UnicodeDecodeError):
        return False


def parse_frontmatter(text: str) -> tuple[dict, str]:
    """Parse YAML frontmatter from markdown text.

    Returns (metadata_dict, body_text).
    """
    fm_pattern = re.compile(r"\A---\r?\n(.*?\r?\n)---\r?\n", re.DOTALL)
    match = fm_pattern.match(text)

    if not match:
        return {}, text

    fm_raw = match.group(1)
    body = text[match.end():]

    # Lightweight YAML parsing (avoids PyYAML dependency)
    metadata = {}
    for line in fm_raw.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            # Handle YAML lists: [item1, item2] or bare comma-separated
            if value.startswith("[") and value.endswith("]"):
                items = [v.strip().strip('"').strip("'")
                         for v in value[1:-1].split(",") if v.strip()]
                metadata[key] = items
            else:
                metadata[key] = value

    return metadata, body


def _map_analysis_tags(tags: list[str]) -> list[str]:
    """Map analysis type tags to subject codes."""
    mapped = []
    for tag in tags:
        tag_upper = tag.upper().strip()
        if tag_upper in ANALYSIS_TAG_MAP:
            mapped.append(ANALYSIS_TAG_MAP[tag_upper])
    return mapped


def ingest(file_path: str) -> dict:
    """Process an .md file with frontmatter through the recon pipeline.

    If frontmatter has domain/subject -> use directly (high confidence).
    Still runs YAKE/spaCy on body text for additional keywords and tags.
    Feeds results as training labels to the classifier.
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}"}
    resolved_path = str(path.resolve())

    existing_by_path = file_exists_by_path(resolved_path)
    if existing_by_path:
        return {
            "status": "already_tracked",
            "existing_id": existing_by_path["sequence_id"],
            "message": f"Already tracked: {existing_by_path['final_name'] or existing_by_path['original_name']}",
        }

    # Hash check
    sha256 = compute_sha256(file_path)
    existing = file_exists_by_hash(sha256)
    if existing:
        return {
            "status": "duplicate",
            "existing_id": existing["sequence_id"],
            "message": f"Duplicate of {existing['final_name'] or existing['original_name']}",
        }

    # Read and parse
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="latin-1")

    metadata, body = parse_frontmatter(text)

    if not body.strip() and not metadata:
        return {"status": "kickout", "reason": "empty"}

    # Extract domain and subjects from frontmatter
    fm_domain = metadata.get("domain", "").upper() or None
    fm_subject = metadata.get("subject", "")
    fm_tags = metadata.get("tags", [])
    if isinstance(fm_tags, str):
        fm_tags = [t.strip() for t in fm_tags.split(",")]
    fm_confidence = metadata.get("confidence", "")
    fm_source_url = metadata.get("source_url", "")
    fm_analysis_type = metadata.get("analysis_type", "")
    fm_concept_mapping = metadata.get("concept_mapping", "")

    # Build subjects from frontmatter
    subjects = []
    if fm_subject:
        if isinstance(fm_subject, list):
            subjects = [resolve_subject(s.strip()) for s in fm_subject]
        elif "," in fm_subject:
            subjects = [resolve_subject(s.strip()) for s in fm_subject.split(",")]
        else:
            subjects = [resolve_subject(fm_subject.strip())]

    # Map analysis tags to codes
    analysis_subjects = _map_analysis_tags(fm_tags)
    for s in analysis_subjects:
        if s not in subjects:
            subjects.append(s)

    # Map analysis_type directly
    if fm_analysis_type:
        at_upper = fm_analysis_type.upper().strip()
        if at_upper in ANALYSIS_TAG_MAP:
            mapped = ANALYSIS_TAG_MAP[at_upper]
            if mapped not in subjects:
                subjects.append(mapped)

    # Resolve domain
    if fm_domain:
        domain = resolve_domain(fm_domain)
    else:
        domain = "--"

    # Determine confidence
    if fm_confidence:
        try:
            confidence = float(fm_confidence)
        except (ValueError, TypeError):
            confidence = 90.0
    elif fm_domain and subjects:
        # Frontmatter-sourced labels get high confidence
        confidence = 92.0
    elif fm_domain or subjects:
        confidence = 75.0
    else:
        confidence = 50.0

    # Still run NLP on body text for keywords and tags
    config = get_config()
    slug_max = int(config.get("pipeline", "slug_max_chars", fallback="20"))

    yake_keywords = _get_yake().extract(body) if body.strip() else []
    spacy_entities = _get_spacy().extract(body) if body.strip() else []

    # If no domain/subject from frontmatter, fall back to classifier
    if domain == "--" and not subjects:
        classifier = _get_classifier()
        classification = classifier.classify(body, yake_keywords, spacy_entities)
        domain = resolve_domain(classification["domain"])
        subjects = [resolve_subject(s) for s in classification["subjects"]]
        confidence = classification["confidence"]

    if not subjects:
        subjects = ["GN"]

    # Generate slug and proposed name
    slug = text_to_slug(yake_keywords, slug_max) if yake_keywords else "recon-note"
    subject_str = "-".join(subjects[:3])

    auto_threshold = float(config.get("pipeline", "auto_rename_threshold", fallback="85"))
    propose_threshold = float(config.get("pipeline", "propose_threshold", fallback="50"))

    if confidence >= auto_threshold:
        status = "auto"
    elif confidence >= propose_threshold:
        status = "pending"
    else:
        status = "kickout"

    seq_id = get_next_sequence_id()
    ext = path.suffix
    proposed_name = f"{slug}_{domain}.{subject_str}_{seq_id}{ext}"

    # Store in Postgres
    tags = [{"tag": kw["keyword"], "source": kw["source"], "confidence": kw.get("score")}
            for kw in yake_keywords]
    for ent in spacy_entities:
        tags.append({"tag": ent["entity"], "source": "spacy"})
    for ft in fm_tags:
        tags.append({"tag": ft, "source": "frontmatter"})
    if fm_source_url:
        tags.append({"tag": fm_source_url, "source": "source_url"})
    if fm_concept_mapping:
        tags.append({"tag": fm_concept_mapping, "source": "concept_mapping"})

    result = insert_file_with_tags(
        original_name=path.name,
        file_path=resolved_path,
        sha256=sha256,
        domain=domain,
        subject_codes=subjects,
        slug=slug,
        proposed_name=proposed_name,
        confidence=confidence,
        status=status,
        sequence_id=seq_id,
        tags=tags,
    )

    # Feed as training label to classifier if high confidence
    if confidence >= 80.0 and domain != "--":
        try:
            _buffer_training_example(
                text=body,
                keywords=yake_keywords,
                domain=domain,
                subject=subjects[0] if subjects else "GN",
            )
            log.info("TRAIN buffered recon label: %s.%s (%.0f%%, batch=%d/%d)",
                     domain, subjects[0], confidence, len(_training_buffer), _get_training_batch_size())
        except Exception as e:
            log.error("Failed to feed training label: %s", e)

    log.info("RECON %s -> %s (confidence: %.0f%%, source: frontmatter)",
             path.name, proposed_name, confidence)

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
        "source": "recon_ingest",
        "frontmatter": metadata,
        "keywords": [k["keyword"] for k in yake_keywords],
        "entities": [e["entity"] for e in spacy_entities],
    }
