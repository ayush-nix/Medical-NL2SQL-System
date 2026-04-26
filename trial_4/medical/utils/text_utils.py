"""
Text Utilities — Pure-Python BM25, Tokenizer, Stopwords.
Zero external dependencies.
"""
import math
import re
from collections import Counter


# ── Stopwords (common English words that add no signal) ──────
STOPWORDS = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "in", "of", "to", "for", "and", "or", "at", "by", "from", "on",
    "with", "as", "it", "its", "this", "that", "which", "who", "whom",
    "what", "where", "when", "how", "do", "does", "did", "has", "have",
    "had", "will", "would", "could", "should", "may", "might", "shall",
    "not", "no", "nor", "but", "if", "then", "than", "so", "very",
    "can", "just", "about", "also", "into", "over", "such", "only",
    "own", "same", "too", "each", "every", "all", "any", "both", "few",
    "more", "most", "other", "some", "up", "out", "off", "down", "here",
    "there", "me", "my", "i", "we", "our", "you", "your", "he", "she",
    "they", "them", "his", "her", "us",
})


def tokenize(text: str) -> list[str]:
    """
    Tokenize text into lowercase alphanumeric tokens.
    Removes stopwords and single-character tokens.
    Splits on underscores and camelCase boundaries too.
    """
    if not text:
        return []
    # Split camelCase and underscores
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = text.replace("_", " ").replace("-", " ")
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return [t for t in tokens if t not in STOPWORDS and len(t) > 1]


def tokenize_keep_all(text: str) -> list[str]:
    """Tokenize but keep all tokens including short ones (for BM25 on column names)."""
    if not text:
        return []
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = text.replace("_", " ").replace("-", " ")
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return [t for t in tokens if t not in STOPWORDS]


def bm25_score(query_tokens: list[str], doc_tokens: list[str],
               k1: float = 1.5, b: float = 0.75, avg_dl: float = 5.0) -> float:
    """
    Okapi BM25 scoring for a single document against a query.
    
    Pure Python implementation — no numpy.
    
    Parameters:
        query_tokens: tokenized query
        doc_tokens: tokenized document (column name/description)
        k1: term frequency saturation parameter
        b: length normalization parameter
        avg_dl: average document length (for our column metadata, ~5 tokens)
    
    Returns:
        BM25 relevance score (higher = more relevant)
    """
    if not query_tokens or not doc_tokens:
        return 0.0

    dl = len(doc_tokens)
    doc_tf = Counter(doc_tokens)
    score = 0.0

    for qt in query_tokens:
        tf = doc_tf.get(qt, 0)
        if tf == 0:
            continue
        # Simplified IDF (since we're scoring a single doc, not a corpus)
        # Using a constant IDF boost for matching tokens
        idf = 1.0
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * (dl / avg_dl))
        score += idf * (numerator / denominator)

    return score


def multi_signal_score(query_tokens: list[str], column_meta: dict,
                       weights: dict = None) -> float:
    """
    Score a column against a query using multiple signals with weights.
    
    Signals:
    - name: column name (weight 10)
    - display: display/human name (weight 6)
    - synonyms: synonym list (weight 8 each)
    - group: column group (weight 4)
    - description: description text (weight 3)
    
    Returns weighted sum of BM25 scores across all signals.
    """
    if weights is None:
        weights = {
            "name": 10,
            "display": 6,
            "synonyms": 8,
            "group": 4,
            "description": 3,
        }

    score = 0.0

    # Name signal
    name_tokens = tokenize_keep_all(column_meta.get("name", ""))
    score += bm25_score(query_tokens, name_tokens) * weights["name"]

    # Display name signal
    display_tokens = tokenize(column_meta.get("display", ""))
    score += bm25_score(query_tokens, display_tokens) * weights["display"]

    # Synonyms signal (each synonym scored independently, take max * weight)
    syn_scores = []
    for syn in column_meta.get("synonyms", []):
        syn_tokens = tokenize(syn)
        s = bm25_score(query_tokens, syn_tokens)
        syn_scores.append(s)
    if syn_scores:
        # Use sum of top-2 synonym scores (not just max) for broader matching
        syn_scores.sort(reverse=True)
        top_syn = sum(syn_scores[:2])
        score += top_syn * weights["synonyms"]

    # Group signal
    group_tokens = tokenize(column_meta.get("group", ""))
    score += bm25_score(query_tokens, group_tokens) * weights["group"]

    # Description signal
    desc_tokens = tokenize(column_meta.get("description", ""))
    score += bm25_score(query_tokens, desc_tokens) * weights["description"]

    return score


def extract_numbers(text: str) -> list[int]:
    """Extract all integers from text."""
    return [int(n) for n in re.findall(r'\b(\d+)\b', text)]
