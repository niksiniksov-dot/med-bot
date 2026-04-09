#!/usr/bin/env python3
"""
Автоматичне оновлення розкладу:
1. Перевіряє сайт коледжу на новий PDF
2. Завантажує PDF → конвертує в зображення
3. Відправляє в Gemini Vision API → отримує JSON
4. Зберігає розклад у Supabase
"""

import os
import re
import sys
import json
import hashlib
import base64
import io
import logging
from typing import Dict, Any, Optional, List

import httpx
from pdf2image import convert_from_path
import tempfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GEMINI_KEY = os.environ["GEMINI_API_KEY"]

COLLEGE_URL = os.getenv(
    "COLLEGE_URL",
    "http://medcollege.mogpod.com/engine/index.php?option=com_content&task=view&id=30&Itemid=89",
)

GROUP_NAMES = [
    "1 м/с", "1 ф А", "1 ф Б", "1 11ф",
    "2 м/с", "2 ф А", "2 ф Б", "2 11ф",
    "3 м/с", "3 11ф", "3 ф А", "3 ф Б",
    "4 м/с", "4 ф А", "4 ф Б",
]

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# ── Supabase helpers ────────────────────────────────────────────────
_sb = httpx.Client(
    base_url=f"{SUPABASE_URL}/rest/v1",
    headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    },
    timeout=15,
)


def sb_get_meta(key: str) -> Optional[str]:
    """Отримати значення з schedule_meta."""
    r = _sb.get("/schedule_meta", params={"key": f"eq.{key}", "select": "value"})
    rows = r.json()
    return rows[0]["value"] if rows else None


def sb_set_meta(key: str, value: str):
    """Зберегти значення в schedule_meta."""
    _sb.post(
        "/schedule_meta",
        json={"key": key, "value": value},
        headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
    )


def sb_save_schedule(group_name: str, data: Dict):
    """Зберегти розклад групи в Supabase."""
    _sb.post(
        "/schedules",
        json={"group_name": group_name, "data": data},
        headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
    )


# ── Watcher: знайти PDF на сайті коледжу ───────────────────────────
def find_pdf_url() -> Optional[str]:
    """Парсить сторінку коледжу і знаходить посилання на PDF розкладу."""
    log.info("Перевіряю сайт коледжу: %s", COLLEGE_URL)
    try:
        r = httpx.get(COLLEGE_URL, timeout=15, follow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        log.error("Не вдалось завантажити сторінку: %s", e)
        return None

    html = r.text
    # Патерн 1: Google Drive
    gdrive = re.findall(r'href=["\']?(https://drive\.google\.com/[^"\'>\s]+)', html)
    if gdrive:
        url = gdrive[-1]
        m = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
        return url

    # Патерн 2: прямий PDF
    pdfs = re.findall(r'href=["\']?([^"\'>\s]+\.pdf)', html)
    if pdfs:
        pdf_url = pdfs[-1]
        if not pdf_url.startswith("http"):
            base = COLLEGE_URL.rsplit("/", 1)[0]
            pdf_url = f"{base}/{pdf_url}"
        return pdf_url

    log.warning("PDF не знайдено на сторінці")
    return None


def download_pdf(url: str) -> Optional[bytes]:
    """Завантажує PDF файл."""
    log.info("Завантажую PDF: %s", url)
    try:
        r = httpx.get(url, timeout=30, follow_redirects=True)
        r.raise_for_status()
        if len(r.content) < 1000:
            log.error("Файл занадто малий (%d bytes)", len(r.content))
            return None
        return r.content
    except Exception as e:
        log.error("Помилка завантаження: %s", e)
        return None


# ── Parser: Gemini Vision ───────────────────────────────────────────
def pdf_to_images(pdf_bytes: bytes) -> List[str]:
    """Конвертує PDF в base64 JPEG зображення."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(pdf_bytes)
        tmp_path = f.name

    try:
        images = convert_from_path(tmp_path, dpi=250)
    finally:
        os.unlink(tmp_path)

    result = []
    for img in images:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        result.append(base64.b64encode(buf.getvalue()).decode())
    log.info("Конвертовано %d сторінок", len(result))
    return result


def gemini_parse_page(image_b64: str, week_type: str) -> Dict:
    """Відправляє зображення в Gemini і отримує розклад."""
    groups_str = ", ".join(GROUP_NAMES)
    prompt = f"""Це сканований розклад занять медичного коледжу ({week_type} тиждень).
Таблиця: стовпці — групи (зліва направо): {groups_str}
Рядки — дні тижня (Понеділок-П'ятниця) з номерами пар (1-5).

Витягни розклад для КОЖНОЇ групи. Поверни JSON:
{{
  "group_name": {{
    "day_index": [
      {{"para": номер_пари, "subject": "назва предмету"}}
    ]
  }}
}}

Де day_index: 0=Понеділок, 1=Вівторок, 2=Середа, 3=Четвер, 4=П'ятниця.
Пропускай порожні клітинки та "---".
Назви предметів пиши українською, як в таблиці (скорочено).

Поверни ТІЛЬКИ валідний JSON без markdown та пояснень."""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_KEY}"

    resp = httpx.post(url, json={
        "contents": [{"parts": [
            {"text": prompt},
            {"inline_data": {"mime_type": "image/jpeg", "data": image_b64}},
        ]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 8192},
    }, timeout=120)

    if resp.status_code != 200:
        log.error("Gemini API error %d: %s", resp.status_code, resp.text[:300])
        return {}

    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    # Прибрати markdown обгортку
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error("Невалідний JSON від Gemini: %s", e)
        log.debug("Raw: %s", text[:500])
        return {}


def parse_schedule(pdf_bytes: bytes) -> Dict[str, Dict[str, Any]]:
    """Повний парсинг PDF: обидві сторінки → merged schedule."""
    images = pdf_to_images(pdf_bytes)

    if len(images) < 2:
        log.error("Очікується 2 сторінки (непарний + парний), знайдено %d", len(images))
        return {}

    log.info("Парсинг сторінки 1 (непарний тиждень)...")
    odd = gemini_parse_page(images[0], "НЕПАРНИЙ")
    log.info("Парсинг сторінки 2 (парний тиждень)...")
    even = gemini_parse_page(images[1], "ПАРНИЙ")

    # Merge: {"group": {"непарний": {...}, "парний": {...}}}
    merged: Dict[str, Dict[str, Any]] = {}
    all_groups = set(list(odd.keys()) + list(even.keys()))

    for group in all_groups:
        merged[group] = {
            "непарний": odd.get(group, {}),
            "парний": even.get(group, {}),
        }

    log.info("Розпізнано %d груп", len(merged))
    return merged


# ── Main ────────────────────────────────────────────────────────────
def main():
    # 1. Знайти PDF
    pdf_url = find_pdf_url()
    if not pdf_url:
        log.info("PDF не знайдено — завершення")
        return

    # 2. Завантажити
    pdf_bytes = download_pdf(pdf_url)
    if not pdf_bytes:
        return

    # 3. Перевірити чи новий
    pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()[:16]
    last_hash = sb_get_meta("last_pdf_hash")
    if last_hash == pdf_hash:
        log.info("PDF не змінився (hash=%s) — пропускаю", pdf_hash)
        return

    log.info("Новий PDF! hash=%s (попередній=%s)", pdf_hash, last_hash)

    # 4. Парсинг через Gemini
    schedule = parse_schedule(pdf_bytes)
    if not schedule:
        log.error("Парсинг не вдався")
        sys.exit(1)

    # 5. Завантаження в Supabase
    log.info("Завантаження в Supabase...")
    for group_name, data in schedule.items():
        total = sum(len(ls) for w in data.values() for ls in w.values())
        sb_save_schedule(group_name, data)
        log.info("  %s: %d пар", group_name, total)

    # 6. Зберегти метадані
    sb_set_meta("last_pdf_hash", pdf_hash)
    sb_set_meta("last_pdf_url", pdf_url)

    log.info("Готово! Оновлено %d груп", len(schedule))


if __name__ == "__main__":
    main()
