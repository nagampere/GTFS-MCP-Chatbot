from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


@dataclass(frozen=True)
class RagDoc:
    source: str
    kind: str
    text: str


_CJK_RE = re.compile(r"[\u3040-\u30ff\u3400-\u9fff]")


def _tokenize(text: str) -> List[str]:
    if not text:
        return []

    text = text.lower()
    raw_tokens = re.findall(r"[0-9a-z_]+|[\u3040-\u30ff\u3400-\u9fff]+", text)

    tokens: List[str] = []
    for tok in raw_tokens:
        if _CJK_RE.search(tok) and len(tok) >= 3:
            tokens.append(tok)
            # Add a few bigrams for partial matching (bounded)
            limit = min(len(tok) - 1, 20)
            for i in range(limit):
                tokens.append(tok[i : i + 2])
        else:
            tokens.append(tok)

    return tokens


def _dedent_answer_block(answer_block: str) -> str:
    lines = answer_block.splitlines()
    # Common indent in these YAML files is 8 spaces under answer: |
    trimmed: List[str] = []
    for line in lines:
        if line.startswith("        "):
            trimmed.append(line[8:])
        elif line.startswith("    "):
            trimmed.append(line[4:])
        else:
            trimmed.append(line)
    return "\n".join(trimmed).strip("\n")


def _parse_few_shot_yaml(path: Path, kind: str) -> List[RagDoc]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []

    # Split on top-level example_n: labels
    matches = list(re.finditer(r"(?m)^example_(\d+):\s*$", raw))
    if not matches:
        return []

    docs: List[RagDoc] = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(raw)
        block = raw[start:end].strip()

        feed = None
        q = None
        answer = None

        m_feed = re.search(r"(?m)^\s*feed:\s*(.+?)\s*$", block)
        if m_feed:
            feed = m_feed.group(1).strip().strip("\"")

        m_q = re.search(r"(?m)^\s*question:\s*(.+?)\s*$", block)
        if m_q:
            q = m_q.group(1).strip().strip("\"")

        m_ans = re.search(r"(?s)^\s*answer:\s*\|\s*\n(.*)$", block)
        if m_ans:
            answer = _dedent_answer_block(m_ans.group(1))

        # Fallback: keep the whole block if extraction fails
        if not q and not answer:
            text = block
        else:
            parts = []
            if feed:
                parts.append(f"feed: {feed}")
            if q:
                parts.append(f"question: {q}")
            if answer:
                parts.append("answer:\n" + answer)
            text = "\n".join(parts)

        example_id = f"example_{m.group(1)}"
        docs.append(
            RagDoc(
                source=f"{path.as_posix()}#{example_id}",
                kind=kind,
                text=text,
            )
        )

    return docs


def _parse_json_questions(path: Path, kind: str) -> List[RagDoc]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []

    try:
        data = json.loads(raw)
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    docs: List[RagDoc] = []
    for idx, item in enumerate(data):
        if not isinstance(item, str):
            continue
        text = item.strip()
        if not text:
            continue
        docs.append(
            RagDoc(
                source=f"{path.as_posix()}[{idx}]",
                kind=kind,
                text=text,
            )
        )

    return docs


@dataclass
class _Index:
    docs: List[RagDoc]
    idf: Dict[str, float]
    doc_vecs: List[Dict[str, float]]
    doc_norms: List[float]


def _build_idf(docs_tokens: List[List[str]]) -> Dict[str, float]:
    df: Dict[str, int] = {}
    for tokens in docs_tokens:
        for t in set(tokens):
            df[t] = df.get(t, 0) + 1

    n = max(len(docs_tokens), 1)
    idf: Dict[str, float] = {}
    for t, d in df.items():
        idf[t] = math.log((n + 1) / (d + 1)) + 1.0
    return idf


def _tf(tokens: Iterable[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for t in tokens:
        out[t] = out.get(t, 0) + 1
    return out


def _to_tfidf(tf: Dict[str, int], idf: Dict[str, float]) -> Dict[str, float]:
    vec: Dict[str, float] = {}
    for t, c in tf.items():
        w = idf.get(t)
        if w is None:
            continue
        vec[t] = float(c) * w
    return vec


def _norm(vec: Dict[str, float]) -> float:
    return math.sqrt(sum(v * v for v in vec.values()))


@lru_cache(maxsize=1)
def _get_index() -> _Index:
    base = Path(__file__).resolve().parent.parent / "examples"

    docs: List[RagDoc] = []
    docs.extend(_parse_few_shot_yaml(base / "few_shot.yaml", kind="few_shot"))
    docs.extend(_parse_few_shot_yaml(base / "few_shot_viz.yaml", kind="few_shot_viz"))
    docs.extend(_parse_json_questions(base / "sample_questions.json", kind="sample_en"))
    docs.extend(_parse_json_questions(base / "sample_questions_jp.json", kind="sample_ja"))
    docs.extend(_parse_json_questions(base / "sample_questions_with_parameters.json", kind="sample_en_params"))

    docs_tokens = [_tokenize(d.text) for d in docs]
    idf = _build_idf(docs_tokens)

    doc_vecs: List[Dict[str, float]] = []
    doc_norms: List[float] = []
    for tokens in docs_tokens:
        vec = _to_tfidf(_tf(tokens), idf)
        doc_vecs.append(vec)
        doc_norms.append(_norm(vec) or 1.0)

    return _Index(docs=docs, idf=idf, doc_vecs=doc_vecs, doc_norms=doc_norms)


def _cosine_score(q_vec: Dict[str, float], q_norm: float, d_vec: Dict[str, float], d_norm: float) -> float:
    dot = 0.0
    # iterate over smaller vector for speed
    if len(q_vec) <= len(d_vec):
        for t, w in q_vec.items():
            dot += w * d_vec.get(t, 0.0)
    else:
        for t, w in d_vec.items():
            dot += w * q_vec.get(t, 0.0)
    return dot / ((q_norm or 1.0) * (d_norm or 1.0))


def retrieve_examples(query: str, k: int = 4) -> List[Tuple[RagDoc, float]]:
    index = _get_index()
    if not index.docs:
        return []

    q_tokens = _tokenize(query)
    q_vec = _to_tfidf(_tf(q_tokens), index.idf)
    q_norm = _norm(q_vec) or 1.0

    scored: List[Tuple[int, float]] = []
    for i, (d_vec, d_norm) in enumerate(zip(index.doc_vecs, index.doc_norms)):
        score = _cosine_score(q_vec, q_norm, d_vec, d_norm)
        if score > 0:
            scored.append((i, score))

    scored.sort(key=lambda x: x[1], reverse=True)

    out: List[Tuple[RagDoc, float]] = []
    for i, score in scored[: max(k, 0)]:
        out.append((index.docs[i], score))
    return out


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 20)].rstrip() + "\n…(truncated)…"


def build_rag_system_message(query: str, k: int = 4, max_chars: int = 6000) -> str:
    """Build a system-message string with retrieved examples.

    Returns an empty string if nothing is found.
    """

    retrieved = retrieve_examples(query, k=k)
    if not retrieved:
        return ""

    parts: List[str] = []
    parts.append(
        "以下は、あなたの質問に近い『過去の質問例/回答スニペット』です。\n"
        "必要な部分だけ参考にし、データ列名や前提は必ず現行フィードに合わせてください。"
    )

    remaining = max_chars
    header = "\n\n".join(parts)
    if len(header) >= remaining:
        return _truncate(header, max_chars)
    remaining -= len(header)

    blocks: List[str] = [header]

    for doc, score in retrieved:
        snippet = doc.text
        # YAML few-shot answers can be huge; cap per-doc size
        per_doc_cap = 1800 if doc.kind.startswith("few_shot") else 400
        snippet = _truncate(snippet, per_doc_cap)
        block = f"\n\n---\nsource: {doc.source}\nscore: {score:.3f}\n{snippet}"

        if len(block) > remaining:
            break
        blocks.append(block)
        remaining -= len(block)

    return "".join(blocks).strip()
