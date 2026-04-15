"""Feature extraction for BIL models."""

from datetime import datetime


def extract_web_features(url: str, text: str, time_on_page: int = 0,
                         scrolled_bottom: bool = False, bookmarked: bool = False,
                         copied: bool = False) -> tuple[dict, float]:
    """Extract features and implicit signal from a web page visit.

    Returns (features_dict, signal_score).
    """
    from urllib.parse import urlparse

    import yake

    parsed = urlparse(url)
    domain = parsed.netloc

    # Extract keywords from page text
    extractor = yake.KeywordExtractor(lan="en", n=2, top=5)
    keywords = [kw for kw, _ in extractor.extract_keywords(text[:5000])] if text else []

    features = {
        "domain": domain,
        "word_count": len(text.split()) if text else 0,
        "has_equations": any(c in text for c in "∫∑∏∂∇χψφ=") if text else False,
        "top_keywords": keywords,
        "time_of_day": datetime.now().hour,
        "time_on_page": time_on_page,
    }

    # Implicit signal from behavior
    signal = 0.0
    if time_on_page > 60:
        signal += 0.2
    if scrolled_bottom:
        signal += 0.2
    if copied:
        signal += 0.3
    if bookmarked:
        signal += 0.3

    return features, signal


def extract_file_features(domain: str, subject_codes: list, confidence: float,
                          slug: str) -> dict:
    """Extract features from a FIS classification event."""
    return {
        "domain": domain,
        "subject_primary": subject_codes[0] if subject_codes else "GN",
        "num_subjects": len(subject_codes),
        "confidence": confidence,
        "hour": datetime.now().hour,
        "day_of_week": datetime.now().weekday(),
    }


def extract_clipboard_features(text: str, app: str = "unknown") -> dict:
    """Extract features from a clipboard copy event."""
    import yake

    extractor = yake.KeywordExtractor(lan="en", n=2, top=3)
    keywords = [kw for kw, _ in extractor.extract_keywords(text[:2000])] if text else []

    return {
        "text_keywords": keywords,
        "app": app,
        "hour": datetime.now().hour,
        "text_length": len(text),
    }
