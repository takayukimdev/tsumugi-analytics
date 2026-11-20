"""Prompt and response text, with optional injected PII (all values fictional).

PII values are deliberately fake: example.com emails, the unassigned 090-0000 phone block,
and a fictional ward (架空区 / Kasou-ku). Every injected value is returned as a label so
redaction recall can be measured against ground truth.
"""

from __future__ import annotations

import random

PiiItem = tuple[str, str]  # (pii_type, value)

TASKS = {
    "ja": (
        "次の議事録を3つの箇条書きで要約してください。",
        "プラン別の週次アクティブユーザー数を数えるSQLを書いてください。",
        "この段落を英語に翻訳してください。",
        "招待を丁寧に断るメールの返信を書いてください。",
        "LEFT JOINとINNER JOINの違いを説明してください。",
        "カフェの店名を5つ提案してください。",
        "このPython関数のバグを直してください。",
        "京都の3日間の旅行プランを作ってください。",
    ),
    "en": (
        "Summarize the following meeting notes in three bullet points.",
        "Write a SQL query that counts weekly active users by plan.",
        "Translate this paragraph into Japanese.",
        "Draft a polite reply declining the invitation.",
        "Explain the difference between a left join and an inner join.",
        "Suggest five names for a coffee shop.",
        "Fix the bug in this Python function.",
        "Give me a 3-day itinerary for Kyoto.",
    ),
}
RESPONSES = {
    "ja": (
        "承知しました。以下に要点をまとめます。",
        "こちらが回答です。必要に応じて調整してください。",
        "ご質問ありがとうございます。順を追って説明します。",
    ),
    "en": (
        "Sure. Here is a concise answer.",
        "Here is a draft you can adjust as needed.",
        "Good question. Let me walk through it step by step.",
    ),
}
NAMES = {
    "ja": ("山田 花子", "佐藤 太郎", "鈴木 一郎", "高橋 美咲", "田中 健太"),
    "en": ("Hanako Yamada", "Taro Sato", "Emily Carter", "Daniel Brooks"),
}
EMAIL_LOCALS = ("hanako", "taro", "emily", "dan", "user")
PII_TEMPLATES = {
    "ja": {
        "person_name": "私の名前は{v}です。",
        "email_address": "連絡先は{v}です。",
        "phone_number": "電話番号は{v}です。",
        "address": "住所は{postal} {street}です。",
    },
    "en": {
        "person_name": "My name is {v}.",
        "email_address": "You can reach me at {v}.",
        "phone_number": "Call me at {v}.",
        "address": "Please ship it to {street}, {postal}.",
    },
}
ECHO_TEMPLATES = {"ja": "{v}さん、", "en": "Hi {v}, "}


def _fake_value(rng: random.Random, pii_type: str, lang: str) -> str:
    if pii_type == "person_name":
        return rng.choice(NAMES[lang])
    if pii_type == "email_address":
        return f"{rng.choice(EMAIL_LOCALS)}{rng.randint(1, 999)}@example.com"
    if pii_type == "phone_number":
        return f"090-0000-{rng.randint(0, 9999):04d}"
    if pii_type == "postal_code":
        return f"〒{rng.randint(100, 999)}-{rng.randint(0, 9999):04d}"
    if pii_type == "street_address":
        block = f"{rng.randint(1, 9)}-{rng.randint(1, 30)}-{rng.randint(1, 20)}"
        return f"東京都架空区{block}" if lang == "ja" else f"{block} Kasou-ku, Tokyo"
    raise ValueError(f"unknown pii_type {pii_type!r}")


def _pii_sentence(rng: random.Random, kind: str, lang: str) -> tuple[str, list[PiiItem]]:
    template = PII_TEMPLATES[lang][kind]
    if kind == "address":
        postal = _fake_value(rng, "postal_code", lang)
        street = _fake_value(rng, "street_address", lang)
        sentence = template.format(postal=postal, street=street)
        return sentence, [("postal_code", postal), ("street_address", street)]
    value = _fake_value(rng, kind, lang)
    return template.format(v=value), [(kind, value)]


def make_prompt(rng: random.Random, lang: str, with_pii: bool) -> tuple[str, list[PiiItem]]:
    task = rng.choice(TASKS[lang])
    if not with_pii:
        return task, []
    kinds = rng.sample(sorted(PII_TEMPLATES[lang]), k=rng.randint(1, 2))
    sentences, items = [], []
    for kind in kinds:
        sentence, found = _pii_sentence(rng, kind, lang)
        sentences.append(sentence)
        items.extend(found)
    return " ".join([*sentences, task]), items


def make_response(rng: random.Random, lang: str, echo: PiiItem | None) -> tuple[str, list[PiiItem]]:
    """A model response. If `echo` is given, the model repeats that PII value back."""
    body = rng.choice(RESPONSES[lang])
    if echo is None:
        return body, []
    return ECHO_TEMPLATES[lang].format(v=echo[1]) + body, [echo]
