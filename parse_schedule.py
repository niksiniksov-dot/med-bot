"""Парсинг розкладу з тексту (.txt / .docx)."""

import re
import io
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

DAY_MAP = {
    "Понеділок": "0",
    "Вівторок": "1",
    "Середа": "2",
    "Четвер": "3",
    "П'ятниця": "4",
}


def parse_text(text: str) -> Dict[str, Dict[str, Any]]:
    """Парсить текстовий розклад і повертає {group_name: schedule_data}."""
    groups: Dict[str, Dict[str, Any]] = {}
    cur_g: Optional[str] = None
    cur_w: Optional[str] = None
    cur_d: Optional[str] = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("══") or line.startswith("РОЗКЛАД") or line.startswith("(Примітка"):
            continue

        m = re.match(r"^Група\s+(.+)$", line)
        if m:
            cur_g = m.group(1).strip()
            groups[cur_g] = {"непарний": {}, "парний": {}}
            cur_w = None
            cur_d = None
            continue

        if line == "НЕПАРНИЙ ТИЖДЕНЬ":
            if cur_g:
                cur_w = "непарний"
                groups[cur_g]["непарний"] = {}
            cur_d = None
            continue
        if line == "ПАРНИЙ ТИЖДЕНЬ":
            if cur_g:
                cur_w = "парний"
                groups[cur_g]["парний"] = {}
            cur_d = None
            continue

        if line in DAY_MAP:
            cur_d = DAY_MAP[line]
            if cur_g and cur_w:
                groups[cur_g][cur_w].setdefault(cur_d, [])
            continue

        m = re.match(r"^(\d)\s+(.+)$", line)
        if m and cur_g and cur_w and cur_d is not None:
            para = int(m.group(1))
            subj = m.group(2).strip()
            if subj == "---":
                continue
            groups[cur_g][cur_w][cur_d].append({
                "para": para,
                "subject": subj,
                "teacher": "",
            })

    return groups


def parse_docx_bytes(data: bytes) -> Dict[str, Dict[str, Any]]:
    """Парсить .docx файл і повертає {group_name: schedule_data}."""
    from docx import Document
    doc = Document(io.BytesIO(data))
    lines = [p.text for p in doc.paragraphs]
    return parse_text("\n".join(lines))


def parse_txt_bytes(data: bytes) -> Dict[str, Dict[str, Any]]:
    """Парсить .txt файл і повертає {group_name: schedule_data}."""
    text = data.decode("utf-8")
    return parse_text(text)
