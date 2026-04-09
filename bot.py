#!/usr/bin/env python3
"""
Медик Розклад Бот
Telegram-бот для відображення розкладу медичного навчального закладу.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import pytz
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    InlineQueryHandler,
    filters,
    ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import httpx
from parse_schedule import parse_docx_bytes, parse_txt_bytes

# ──────────────────────────────────────────────────────────────
# НАЛАШТУВАННЯ
# ──────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TZ = pytz.timezone(os.getenv("TZ", "Europe/Kiev"))

_admin_raw = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS: List[int] = [int(x) for x in _admin_raw.split(",") if x.strip()]

logging.basicConfig(
    format="%(asctime)s │ %(name)s │ %(levelname)s │ %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

GROUP_NAMES: List[str] = [
    "1 м/с", "1 ф А", "1 ф Б", "1 11ф",
    "2 м/с", "2 ф А", "2 ф Б", "2 11ф",
    "3 м/с", "3 11ф", "3 ф А", "3 ф Б",
    "4 м/с", "4 ф А", "4 ф Б",
]

PARA_TIMES = {
    1: "08:30–09:50",
    2: "10:00–11:20",
    3: "11:50–13:10",
    4: "13:20–14:40",
    5: "14:50–16:10",
    6: "16:20–17:40",
}

DAY_NAMES = {
    0: "Понеділок", 1: "Вівторок", 2: "Середа",
    3: "Четвер", 4: "П'ятниця", 5: "Субота", 6: "Неділя",
}

MONTH_NAMES = {
    1: "січня", 2: "лютого", 3: "березня", 4: "квітня",
    5: "травня", 6: "червня", 7: "липня", 8: "серпня",
    9: "вересня", 10: "жовтня", 11: "листопада", 12: "грудня",
}


# ──────────────────────────────────────────────────────────────
# КЛАВІАТУРА
# ──────────────────────────────────────────────────────────────
def get_main_kbd(chat_id: str = "") -> ReplyKeyboardMarkup:
    """Повертає головну клавіатуру з динамічним станом сповіщень."""
    if chat_id:
        data = load_data()
        user = data["users"].get(chat_id, {})
        is_active = user.get("active", True)
        wt = get_week_type_for_user(datetime.now(TZ), user)
        week_label = "Парний" if wt == "парний" else "Непарний"
    else:
        is_active = True
        week_label = "Парний" if get_week_type(datetime.now(TZ)) == "парний" else "Непарний"
    notify_btn = (
        "Вимкнути сповіщення" if is_active
        else "Увімкнути сповіщення"
    )
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Сьогодні"), KeyboardButton("Завтра")],
            [KeyboardButton("Тиждень"), KeyboardButton("Що зараз?")],
            [KeyboardButton("Змінити групу"), KeyboardButton(f"Тиждень: {week_label}")],
            [KeyboardButton(notify_btn)],
        ],
        resize_keyboard=True,
    )


# ──────────────────────────────────────────────────────────────
# SUPABASE CLIENT
# ──────────────────────────────────────────────────────────────
_http = httpx.Client(
    base_url=f"{SUPABASE_URL}/rest/v1",
    headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    },
    timeout=10,
)


# ──────────────────────────────────────────────────────────────
# LOAD / SAVE  (користувачі — Supabase: users)
# ──────────────────────────────────────────────────────────────
_DATA: Optional[Dict[str, Any]] = None


def load_data() -> Dict[str, Any]:
    global _DATA
    if _DATA is not None:
        return _DATA
    try:
        r = _http.get("/users", params={"select": "*"})
        r.raise_for_status()
        users = {}
        for row in r.json():
            users[str(row["chat_id"])] = row.get("data", {})
        _DATA = {"users": users}
        logger.info("Loaded %d users from Supabase", len(users))
    except Exception as e:
        logger.error("Supabase load error: %s", e)
        _DATA = {"users": {}}
    return _DATA


def save_data(data: Dict[str, Any]) -> None:
    global _DATA
    _DATA = data
    rows = [
        {"chat_id": cid, "data": u}
        for cid, u in data.get("users", {}).items()
    ]
    if not rows:
        return
    try:
        _http.post(
            "/users", json=rows,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
    except Exception as e:
        logger.error("Supabase save error: %s", e)


# ──────────────────────────────────────────────────────────────
# LOAD / SAVE  (розклади — Supabase: schedules)
# ──────────────────────────────────────────────────────────────
_SCHEDULE_CACHE: Dict[str, Dict[str, Any]] = {}


def load_schedule(group_name: str) -> Dict[str, Any]:
    """Завантажує розклад групи з Supabase (з кешуванням)."""
    if group_name in _SCHEDULE_CACHE:
        return _SCHEDULE_CACHE[group_name]
    try:
        r = _http.get("/schedules", params={
            "group_name": f"eq.{group_name}",
            "select": "data",
        })
        r.raise_for_status()
        rows = r.json()
        if rows:
            schedule = rows[0].get("data", {})
            _SCHEDULE_CACHE[group_name] = schedule
            return schedule
    except Exception as e:
        logger.error("Supabase load_schedule error for %s: %s", group_name, e)
    return {}


def save_schedule(group_name: str, schedule: Dict[str, Any]) -> None:
    """Зберігає розклад групи у Supabase."""
    _SCHEDULE_CACHE[group_name] = schedule
    try:
        _http.post(
            "/schedules",
            json={"group_name": group_name, "data": schedule},
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
    except Exception as e:
        logger.error("Supabase save_schedule error for %s: %s", group_name, e)


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
MAX_MSG_LEN = 4000


def h(t: str) -> str:
    """HTML-escape."""
    return (
        t.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def split_long_message(text: str, limit: int = MAX_MSG_LEN) -> List[str]:
    """Розбиває повідомлення на частини по подвійному переносу."""
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    current = ""
    for block in text.split("\n\n"):
        if len(block) > limit:
            if current:
                chunks.append(current)
                current = ""
            sub = ""
            for line in block.split("\n"):
                cand = (sub + "\n" + line) if sub else line
                if len(cand) > limit and sub:
                    chunks.append(sub)
                    sub = line[:limit]
                else:
                    sub = cand
            if sub:
                current = sub
            continue
        candidate = (current + "\n\n" + block) if current else block
        if len(candidate) > limit and current:
            chunks.append(current)
            current = block
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


async def send_long_message(target, text: str, **kwargs) -> None:
    """Надсилає повідомлення, розбиваючи якщо > MAX_MSG_LEN."""
    for chunk in split_long_message(text):
        await target.reply_text(chunk, **kwargs)


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def get_user_group(data: Dict[str, Any], chat_id: str) -> str:
    """Повертає назву групи користувача або порожній рядок."""
    return data.get("users", {}).get(chat_id, {}).get("group", "")


# ──────────────────────────────────────────────────────────────
# WEEK TYPE (парний / непарний)
# ──────────────────────────────────────────────────────────────
def get_week_type(target: datetime) -> str:
    """Повертає 'парний' або 'непарний' для заданої дати."""
    week_num = target.isocalendar()[1]
    return "парний" if week_num % 2 == 0 else "непарний"


def get_week_type_for_user(
    target: datetime, user_data: Dict[str, Any]
) -> str:
    """Визначає тип тижня з урахуванням калібрування користувача."""
    base = get_week_type(target)
    if user_data.get("swap_weeks", False):
        return "непарний" if base == "парний" else "парний"
    return base


# ──────────────────────────────────────────────────────────────
# RENDER
# ──────────────────────────────────────────────────────────────
def render_para_block(entry: Dict[str, Any]) -> str:
    """Рендерить одну пару у blockquote."""
    p = entry["para"]
    time_ = PARA_TIMES.get(p, "")
    subj = h(entry.get("subject", ""))
    teacher = h(entry.get("teacher", ""))
    lines = [f"<blockquote><b>{p} пара: {subj}</b>", time_]
    if teacher and teacher != "—":
        lines.append(teacher)
    return "\n".join(lines) + "</blockquote>"


def build_schedule_message(
    schedule: Dict[str, Any],
    target: datetime,
    week_type: str,
    group_name: str = "",
) -> str:
    """Будує повідомлення з розкладом на один день."""
    weekday = target.weekday()
    day_name = DAY_NAMES[weekday]
    month_n = MONTH_NAMES[target.month]
    day_num = target.day
    is_today = target.date() == datetime.now(TZ).date()
    date_label = " — сьогодні" if is_today else ""

    week_label = "Парний" if week_type == "парний" else "Непарний"
    header = (
        f"<b>{day_name}, {day_num} {month_n}{date_label}</b>\n"
        f"(Тиждень: {week_label})"
    )

    # Отримуємо пари для поточного дня і типу тижня
    day_key = str(weekday)
    day_classes = schedule.get(week_type, {}).get(day_key, [])

    if not day_classes:
        return (
            f"{header}\n\n"
            "<b>Вільний день!</b>\nПар немає — відпочивай"
        )

    # Сортуємо по номеру пари
    sorted_classes = sorted(day_classes, key=lambda x: x.get("para", 0))

    parts = [header]
    for entry in sorted_classes:
        parts.append(render_para_block(entry))

    return "\n\n".join(parts)


# ──────────────────────────────────────────────────────────────
# GROUP KEYBOARD
# ──────────────────────────────────────────────────────────────
def build_group_keyboard() -> InlineKeyboardMarkup:
    """Створює інлайн-клавіатуру для вибору групи."""
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i, name in enumerate(GROUP_NAMES):
        row.append(InlineKeyboardButton(
            name, callback_data=f"setgroup:{name}"
        ))
        if len(row) == 3 or i == len(GROUP_NAMES) - 1:
            rows.append(row)
            row = []
    return InlineKeyboardMarkup(rows)


def _parity_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура вибору парності тижня."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "Парний тиждень", callback_data="parity:парний"
        )],
        [InlineKeyboardButton(
            "Непарний тиждень", callback_data="parity:непарний"
        )],
    ])


def build_week_keyboard(target_date: datetime) -> InlineKeyboardMarkup:
    """Створює клавіатуру навігації по днях тижня."""
    start_of_week = target_date - timedelta(days=target_date.weekday())
    today = datetime.now(TZ).date()
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i in range(5):
        d = start_of_week + timedelta(days=i)
        day_str = ["Пн", "Вт", "Ср", "Чт", "Пт"][i]
        label = f"{day_str} {d.strftime('%d.%m')}"
        if d.date() == today:
            label = f"• {label}"
        row.append(InlineKeyboardButton(
            label, callback_data=f"week:{d.strftime('%Y-%m-%d')}"
        ))
        if len(row) == 3 or i == 4:
            rows.append(row)
            row = []
    return InlineKeyboardMarkup(rows)


# ──────────────────────────────────────────────────────────────
# REQUIRE GROUP
# ──────────────────────────────────────────────────────────────
async def _require_group(update: Update) -> Optional[str]:
    """Повертає групу або просить обрати. None = група не обрана."""
    if not update.effective_chat:
        return None
    chat_id = str(update.effective_chat.id)
    data = load_data()
    group = get_user_group(data, chat_id)
    if not group:
        msg = update.message or (
            update.callback_query and update.callback_query.message
        )
        if msg and hasattr(msg, "reply_text"):
            await msg.reply_text(
                "Спочатку оберіть групу:",
                parse_mode="HTML",
                reply_markup=build_group_keyboard(),
            )
        return None
    return group


def _get_user_data(chat_id: str) -> Dict[str, Any]:
    """Повертає дані користувача."""
    data = load_data()
    return data.get("users", {}).get(chat_id, {})


# ──────────────────────────────────────────────────────────────
# COMMANDS
# ──────────────────────────────────────────────────────────────
async def cmd_start(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.effective_chat or not update.effective_user:
        return
    chat_id = str(update.effective_chat.id)
    data = load_data()

    if chat_id not in data["users"]:
        user_name = update.effective_user.first_name or "Студент"
        data["users"][chat_id] = {
            "name": user_name,
            "notify_time": "07:30",
            "active": True,
            "group": "",
            "swap_weeks": False,
        }
        save_data(data)

    if not update.message:
        return

    user_group = get_user_group(data, chat_id)

    if not user_group:
        await update.message.reply_text(
            "<b>Привіт! Я бот-розклад</b>\n\n"
            "Для початку, <b>обери свою групу</b>:",
            parse_mode="HTML",
            reply_markup=build_group_keyboard(),
        )
    else:
        # Група вже обрана — перевіримо чи є калібрування тижня
        user_data = data["users"].get(chat_id, {})
        if "parity_set" not in user_data:
            await update.message.reply_text(
                "Який зараз тиждень — парний чи непарний?",
                parse_mode="HTML",
                reply_markup=_parity_keyboard(),
            )
            return

        time_str = user_data.get("notify_time", "07:30")
        await update.message.reply_text(
            f"<b>Привіт! Я бот-розклад</b>\n\n"
            f"Ваша група: <b>{h(user_group)}</b>\n"
            f"Час сповіщень: <b>{time_str}</b>\n\n"
            f"Скористайтеся клавіатурою знизу",
            parse_mode="HTML",
            reply_markup=get_main_kbd(chat_id),
        )


async def callback_set_group(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Обробник вибору групи."""
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()

    group_name = query.data.replace("setgroup:", "")
    if group_name not in GROUP_NAMES:
        await query.edit_message_text("Невідома група.")
        return

    if not query.message:
        return
    chat_id = str(query.message.chat.id)
    data = load_data()
    data["users"].setdefault(chat_id, {
        "notify_time": "07:30",
        "active": True,
        "swap_weeks": False,
    })
    data["users"][chat_id]["group"] = group_name
    save_data(data)

    await query.edit_message_text(
        f"Групу встановлено: <b>{h(group_name)}</b>",
        parse_mode="HTML",
    )

    # Запитуємо парність тижня якщо ще не налаштовано
    user_data = data["users"].get(chat_id, {})
    if "parity_set" not in user_data:
        await query.message.reply_text(
            "Який зараз тиждень — парний чи непарний?",
            parse_mode="HTML",
            reply_markup=_parity_keyboard(),
        )
    else:
        await query.message.reply_text(
            "Скористайтеся клавіатурою знизу",
            reply_markup=get_main_kbd(chat_id),
        )


async def callback_parity(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Обробка вибору парності тижня."""
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()

    choice = query.data.replace("parity:", "")  # "парний" або "непарний"
    now = datetime.now(TZ)
    system_type = get_week_type(now)

    # Якщо система каже інше ніж юзер — треба swap
    swap = (system_type != choice)

    chat_id = str(query.message.chat.id) if query.message else ""
    if not chat_id:
        return

    data = load_data()
    data["users"].setdefault(chat_id, {
        "notify_time": "07:30",
        "active": True,
        "group": "",
    })
    data["users"][chat_id]["swap_weeks"] = swap
    data["users"][chat_id]["parity_set"] = True
    save_data(data)

    label = "Парний" if choice == "парний" else "Непарний"
    await query.edit_message_text(
        f"Поточний тиждень: <b>{label}</b>",
        parse_mode="HTML",
    )
    if query.message:
        await query.message.reply_text(
            "Скористайтеся клавіатурою знизу",
            reply_markup=get_main_kbd(chat_id),
        )


async def cmd_setgroup(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Зміна групи."""
    if not update.message:
        return
    await update.message.reply_text(
        "<b>Оберіть свою групу:</b>",
        parse_mode="HTML",
        reply_markup=build_group_keyboard(),
    )


async def cmd_setweek(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Зміна типу поточного тижня."""
    if not update.message or not update.effective_chat:
        return
    chat_id = str(update.effective_chat.id)
    data = load_data()
    user_data = data["users"].get(chat_id, {})
    wt = get_week_type_for_user(datetime.now(TZ), user_data)
    current_label = "Парний" if wt == "парний" else "Непарний"
    await update.message.reply_text(
        f"Поточний тиждень: <b>{current_label}</b>\n\n"
        "Оберіть правильний тип тижня:",
        parse_mode="HTML",
        reply_markup=_parity_keyboard(),
    )


async def cmd_today(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.message or not update.effective_chat:
        return
    group = await _require_group(update)
    if not group:
        return

    chat_id = str(update.effective_chat.id)
    user_data = _get_user_data(chat_id)
    schedule = load_schedule(group)
    now = datetime.now(TZ)

    if now.weekday() in (5, 6):
        monday = now + timedelta(days=(7 - now.weekday()))
        wt = get_week_type_for_user(monday, user_data)
        msg = build_schedule_message(schedule, monday, wt, group)
        prefix = "<i>Вихідний! Ось розклад на понеділок:</i>\n\n"
        await send_long_message(
            update.message, prefix + msg, parse_mode="HTML"
        )
    elif (now.hour == 17 and now.minute >= 40) or now.hour >= 18:
        target = now + timedelta(days=1)
        if target.weekday() in (5, 6):
            target += timedelta(days=(7 - target.weekday()))
        wt = get_week_type_for_user(target, user_data)
        msg = build_schedule_message(schedule, target, wt, group)
        prefix = (
            "<i>Пари на сьогодні закінчились. "
            "Ось розклад на завтра:</i>\n\n"
        )
        await send_long_message(
            update.message, prefix + msg, parse_mode="HTML"
        )
    else:
        wt = get_week_type_for_user(now, user_data)
        msg = build_schedule_message(schedule, now, wt, group)
        await send_long_message(
            update.message, msg, parse_mode="HTML"
        )


async def cmd_tomorrow(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.message or not update.effective_chat:
        return
    group = await _require_group(update)
    if not group:
        return

    chat_id = str(update.effective_chat.id)
    user_data = _get_user_data(chat_id)
    schedule = load_schedule(group)
    target = datetime.now(TZ) + timedelta(days=1)
    if target.weekday() in (5, 6):
        target += timedelta(days=(7 - target.weekday()))
    wt = get_week_type_for_user(target, user_data)
    msg = build_schedule_message(schedule, target, wt, group)
    await send_long_message(update.message, msg, parse_mode="HTML")


async def cmd_week(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.message or not update.effective_chat:
        return
    group = await _require_group(update)
    if not group:
        return

    chat_id = str(update.effective_chat.id)
    user_data = _get_user_data(chat_id)
    schedule = load_schedule(group)
    now = datetime.now(TZ)
    target = now
    if target.weekday() in (5, 6):
        target = target + timedelta(days=(7 - target.weekday()))

    wt = get_week_type_for_user(target, user_data)
    msg = build_schedule_message(schedule, target, wt, group)
    kbd = build_week_keyboard(target)
    await update.message.reply_text(
        msg, parse_mode="HTML", reply_markup=kbd
    )


async def callback_week(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Навігація по днях тижня."""
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()

    date_str = query.data.replace("week:", "")
    try:
        target = TZ.localize(datetime.strptime(date_str, "%Y-%m-%d"))
    except ValueError:
        return

    chat_id = str(query.message.chat.id) if query.message else ""
    if not chat_id:
        return

    data = load_data()
    group = get_user_group(data, chat_id)
    if not group:
        await query.edit_message_text(
            "Спочатку оберіть групу: /setgroup"
        )
        return

    user_data = data["users"].get(chat_id, {})
    schedule = load_schedule(group)
    wt = get_week_type_for_user(target, user_data)
    msg = build_schedule_message(schedule, target, wt, group)
    kbd = build_week_keyboard(target)

    try:
        await query.edit_message_text(
            msg, parse_mode="HTML", reply_markup=kbd
        )
    except Exception as exc:
        logger.debug("callback_week edit failed: %s", exc)


async def cmd_now(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Що зараз? — поточна / наступна пара."""
    if not update.message:
        return
    group = await _require_group(update)
    if not group:
        return

    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    user_data = _get_user_data(chat_id)
    schedule = load_schedule(group)
    target = datetime.now(TZ)

    if target.weekday() in (5, 6):
        monday = target + timedelta(days=(7 - target.weekday()))
        wt = get_week_type_for_user(monday, user_data)
        msg = build_schedule_message(schedule, monday, wt, group)
        await send_long_message(
            update.message,
            "<i>Вихідний! Ось розклад на понеділок:</i>\n\n" + msg,
            parse_mode="HTML",
        )
        return

    wt = get_week_type_for_user(target, user_data)
    day_key = str(target.weekday())
    day_classes = schedule.get(wt, {}).get(day_key, [])
    day_classes = sorted(day_classes, key=lambda x: x.get("para", 0))

    if not day_classes:
        await update.message.reply_text(
            "Сьогодні пар немає — відпочивай!\n"
            "Натисніть 'Завтра', щоб переглянути розклад."
        )
        return

    current_time = target.time()

    def parse_times(time_str: str):
        ts = time_str.split("–")
        start_t = datetime.strptime(ts[0].strip(), "%H:%M").time()
        end_t = datetime.strptime(ts[1].strip(), "%H:%M").time()
        return start_t, end_t

    active_class = None
    next_class = None

    for c in day_classes:
        p = c.get("para", 0)
        t_str = PARA_TIMES.get(p, "")
        if not t_str:
            continue
        try:
            st_t, en_t = parse_times(t_str)
        except Exception:
            continue
        if st_t <= current_time <= en_t:
            active_class = (c, en_t)
            break
        elif current_time < st_t and next_class is None:
            next_class = (c, st_t)

    if active_class:
        for c in day_classes:
            if c.get("para", 0) > active_class[0].get("para", 0):
                t_str = PARA_TIMES.get(c["para"], "")
                if t_str:
                    next_class = (c, parse_times(t_str)[0])
                break

    lines: List[str] = []
    if active_class:
        c, en_t = active_class
        dt_end = TZ.localize(datetime.combine(target.date(), en_t))
        remains = int((dt_end - target).total_seconds() / 60)
        subj = h(c.get("subject", ""))
        lines.append(f"<b>Зараз іде {c['para']} пара:</b>")
        lines.append(f"   {subj} (залишилось {remains} хв)")
    else:
        lines.append("Зараз пар немає.")

    if next_class:
        c, st_t = next_class
        dt_start = TZ.localize(datetime.combine(target.date(), st_t))
        wait_time = int((dt_start - target).total_seconds() / 60)
        subj = h(c.get("subject", ""))
        if wait_time > 0:
            lines.append(f"\n<b>Наступна {c['para']} пара:</b>")
            lines.append(
                f"   {subj} (через {wait_time} хв, "
                f"о {st_t.strftime('%H:%M')})"
            )
    else:
        if active_class:
            lines.append("\nЦе остання пара на сьогодні!")
        else:
            lines.append("\nВсі пари на сьогодні закінчились!")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML"
    )


async def cmd_settime(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Встановити час щоденного сповіщення."""
    if not update.effective_chat or not update.message:
        return
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await update.message.reply_text("Формат: /settime 07:30")
        return
    time_str = context.args[0]
    if not re.match(r"\d{2}:\d{2}$", time_str):
        await update.message.reply_text("Невірно. Приклад: 07:30")
        return
    hh, mm = int(time_str[:2]), int(time_str[3:])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        await update.message.reply_text(
            "Невірний час. Години: 00–23, хвилини: 00–59."
        )
        return
    data = load_data()
    data["users"].setdefault(chat_id, {
        "active": True, "group": "", "swap_weeks": False
    })
    data["users"][chat_id]["notify_time"] = time_str
    save_data(data)
    await update.message.reply_text(
        f"Час сповіщень оновлено: <b>{time_str}</b>",
        parse_mode="HTML",
    )


async def cmd_help(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.message:
        return
    text = (
        "<b>Команди бота:</b>\n\n"
        "/today — розклад на сьогодні\n"
        "/tomorrow — розклад на завтра\n"
        "/week — розклад на тиждень\n"
        "/now — що зараз?\n"
        "/setgroup — змінити групу\n"
        "/setweek — змінити тип тижня\n"
        "/settime HH:MM — час сповіщень\n"
        "/status — статус бота\n"
        "/pause — вимкнути сповіщення\n"
        "/resume — увімкнути сповіщення\n"
        "/id — дізнатися свій Telegram ID"
    )
    if update.effective_user and _is_admin(update.effective_user.id):
        text += (
            "\n\n<b>Адмін-команди:</b>\n"
            "Надішліть JSON-файл або JSON-текст для завантаження розкладу\n"
            "/broadcast текст — розсилка\n"
            "/reload — перечитати дані\n"
            "/stats — статистика"
        )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_id(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.message or not update.effective_user:
        return
    await update.message.reply_text(
        f"Ваш Telegram ID: <code>{update.effective_user.id}</code>",
        parse_mode="HTML",
    )


async def cmd_status(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.effective_chat or not update.message:
        return
    chat_id = str(update.effective_chat.id)
    data = load_data()
    user_data = data["users"].get(chat_id, {})
    active_str = (
        "активні" if user_data.get("active", True) else "вимкнені"
    )
    time_str = user_data.get("notify_time", "07:30")
    group = user_data.get("group", "не обрана")
    wt = get_week_type_for_user(datetime.now(TZ), user_data)
    wt_label = "Парний" if wt == "парний" else "Непарний"

    lines = [
        f"<b>Статус</b>",
        f"Група: <b>{h(group or 'не обрана')}</b>",
        f"Тиждень: <b>{wt_label}</b>",
        f"Сповіщення: {active_str}",
        f"Час: <b>{time_str}</b>",
    ]
    if update.effective_user and _is_admin(update.effective_user.id):
        total = len(data["users"])
        active = sum(
            1 for u in data["users"].values() if u.get("active", True)
        )
        lines.append(f"\n<b>Адмін-статистика</b>")
        lines.append(f"Всього: {total} | Активних: {active}")
    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML"
    )


async def cmd_pause(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.effective_chat or not update.message:
        return
    chat_id = str(update.effective_chat.id)
    data = load_data()
    if chat_id in data["users"]:
        data["users"][chat_id]["active"] = False
        save_data(data)
    await update.message.reply_text(
        "Сповіщення вимкнені.",
        reply_markup=get_main_kbd(chat_id),
    )


async def cmd_resume(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.effective_chat or not update.message:
        return
    chat_id = str(update.effective_chat.id)
    data = load_data()
    data["users"].setdefault(chat_id, {
        "notify_time": "07:30", "group": "", "swap_weeks": False
    })
    data["users"][chat_id]["active"] = True
    save_data(data)
    await update.message.reply_text(
        "Сповіщення увімкнені.",
        reply_markup=get_main_kbd(chat_id),
    )


# ──────────────────────────────────────────────────────────────
# ADMIN: ЗАВАНТАЖЕННЯ РОЗКЛАДУ (JSON)
# ──────────────────────────────────────────────────────────────
def _validate_schedule_json(obj: Any) -> Optional[str]:
    """
    Перевіряє структуру JSON розкладу.
    Повертає повідомлення про помилку або None якщо все ок.

    Очікуваний формат:
    {
      "group": "1 м/с",
      "парний": {
        "0": [{"para": 1, "subject": "...", "teacher": "..."}],
        ...
      },
      "непарний": { ... }
    }
    """
    if not isinstance(obj, dict):
        return "JSON має бути об'єктом (dict)"

    group = obj.get("group")
    if not group:
        return "Відсутнє поле 'group'"
    if group not in GROUP_NAMES:
        return f"Невідома група: '{group}'"

    for week_key in ("парний", "непарний"):
        week_data = obj.get(week_key)
        if week_data is None:
            continue  # Необов'язково мати обидва тижні
        if not isinstance(week_data, dict):
            return f"'{week_key}' має бути об'єктом"
        for day_key, lessons in week_data.items():
            if day_key not in ("0", "1", "2", "3", "4", "5", "6"):
                return f"Невірний ключ дня: '{day_key}'"
            if not isinstance(lessons, list):
                return f"'{week_key}.{day_key}' має бути масивом"
            for i, lesson in enumerate(lessons):
                if not isinstance(lesson, dict):
                    return (
                        f"'{week_key}.{day_key}[{i}]' "
                        "має бути об'єктом"
                    )
                if "para" not in lesson:
                    return (
                        f"'{week_key}.{day_key}[{i}]' "
                        "відсутнє поле 'para'"
                    )
                if "subject" not in lesson:
                    return (
                        f"'{week_key}.{day_key}[{i}]' "
                        "відсутнє поле 'subject'"
                    )
    return None


async def _process_schedule_json(
    update: Update, json_text: str
) -> None:
    """Обробляє JSON-текст розкладу від адміна."""
    if not update.message:
        return

    try:
        obj = json.loads(json_text)
    except json.JSONDecodeError as exc:
        await update.message.reply_text(
            f"Невалідний JSON:\n<code>{h(str(exc))}</code>",
            parse_mode="HTML",
        )
        return

    # Підтримка масиву (кілька груп одразу)
    if isinstance(obj, list):
        results: List[str] = []
        for item in obj:
            err = _validate_schedule_json(item)
            if err:
                results.append(f"[!] {item.get('group', '?')}: {h(err)}")
                continue
            group_name = item["group"]
            schedule_data = {
                "парний": item.get("парний", {}),
                "непарний": item.get("непарний", {}),
            }
            save_schedule(group_name, schedule_data)
            results.append(f"[+] {h(group_name)}")
        await update.message.reply_text(
            "<b>Результат завантаження:</b>\n" + "\n".join(results),
            parse_mode="HTML",
        )
        return

    # Один об'єкт
    err = _validate_schedule_json(obj)
    if err:
        await update.message.reply_text(
            f"Помилка: {h(err)}", parse_mode="HTML"
        )
        return

    group_name = obj["group"]
    schedule_data = {
        "парний": obj.get("парний", {}),
        "непарний": obj.get("непарний", {}),
    }
    save_schedule(group_name, schedule_data)

    # Рахуємо кількість пар
    total = 0
    for wk in ("парний", "непарний"):
        for day_lessons in schedule_data.get(wk, {}).values():
            total += len(day_lessons)

    await update.message.reply_text(
        f"Розклад для <b>{h(group_name)}</b> завантажено!\n"
        f"Загалом пар: {total}",
        parse_mode="HTML",
    )


async def handle_document(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Обробка файлу розкладу від адміна (.json / .docx / .txt)."""
    if not update.message or not update.effective_user:
        return
    if not _is_admin(update.effective_user.id):
        return

    doc = update.message.document
    if not doc or not doc.file_name:
        return

    fname = doc.file_name.lower()

    try:
        file = await doc.get_file()
        data_bytes = bytes(await file.download_as_bytearray())
    except Exception as exc:
        await update.message.reply_text(
            f"Помилка завантаження файлу: {h(str(exc))}",
            parse_mode="HTML",
        )
        return

    # JSON — як раніше
    if fname.endswith(".json"):
        json_text = data_bytes.decode("utf-8")
        await _process_schedule_json(update, json_text)
        return

    # DOCX або TXT — парсинг розкладу
    if fname.endswith(".docx") or fname.endswith(".txt"):
        await update.message.reply_text("Обробляю файл...")
        try:
            if fname.endswith(".docx"):
                groups = parse_docx_bytes(data_bytes)
            else:
                groups = parse_txt_bytes(data_bytes)
        except Exception as exc:
            await update.message.reply_text(
                f"Помилка парсингу: {h(str(exc))}",
                parse_mode="HTML",
            )
            return

        if not groups:
            await update.message.reply_text(
                "Не вдалося знайти жодної групи у файлі.\n"
                "Перевірте формат: кожна група має починатися з 'Група ...'."
            )
            return

        results: List[str] = []
        for gname, sched in groups.items():
            total = sum(
                len(ls) for w in sched.values() for ls in w.values()
            )
            save_schedule(gname, sched)
            results.append(f"[+] {h(gname)} ({total} пар)")

        await update.message.reply_text(
            f"<b>Розклад оновлено з файлу:</b>\n"
            + "\n".join(results)
            + f"\n\nВсього: <b>{len(groups)}</b> груп",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        "Підтримувані формати: .json, .docx, .txt"
    )


async def cmd_broadcast(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Розсилка всім активним користувачам."""
    if not update.message or not update.effective_user:
        return
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("Тільки для адмінів.")
        return
    if not context.args:
        await update.message.reply_text(
            "Формат: /broadcast текст повідомлення"
        )
        return
    text = h(" ".join(context.args))
    data = load_data()
    sent, failed = 0, 0
    for chat_id, u in data["users"].items():
        if not u.get("active", True):
            continue
        try:
            await update.get_bot().send_message(
                int(chat_id), text, parse_mode="HTML"
            )
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(
        f"Розіслано: {sent} | Помилок: {failed}"
    )


async def cmd_reload(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Скидає кеш data.json."""
    if not update.message or not update.effective_user:
        return
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("Тільки для адмінів.")
        return
    global _DATA
    _DATA = None
    _SCHEDULE_CACHE.clear()
    load_data()
    await update.message.reply_text(
        "Кеш скинуто. Дані перечитано з Supabase."
    )


async def cmd_stats(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Статистика."""
    if not update.message or not update.effective_user:
        return
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("Тільки для адмінів.")
        return

    data = load_data()
    total = len(data["users"])
    active = sum(
        1 for u in data["users"].values() if u.get("active", True)
    )

    # Групи з розкладами
    groups_with_schedule: List[str] = []
    for g in GROUP_NAMES:
        s = load_schedule(g)
        if s:
            groups_with_schedule.append(g)

    # Розподіл по групах
    group_counts: Dict[str, int] = {}
    for u in data["users"].values():
        g = u.get("group", "")
        if g:
            group_counts[g] = group_counts.get(g, 0) + 1

    lines = [
        "<b>Статистика бота</b>\n",
        f"Всього користувачів: <b>{total}</b>",
        f"Активних: <b>{active}</b>",
        f"Груп з розкладом: <b>{len(groups_with_schedule)}</b>",
    ]
    if group_counts:
        lines.append("\n<b>Розподіл по групах:</b>")
        for g in sorted(group_counts.keys()):
            lines.append(f"  {h(g)}: {group_counts[g]}")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML"
    )


# ──────────────────────────────────────────────────────────────
# TEXT MESSAGE HANDLER
# ──────────────────────────────────────────────────────────────
async def handle_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not update.message or not update.effective_user:
        return

    text = update.message.text or ""

    # Меню з кнопок
    if text == "Сьогодні":
        await cmd_today(update, context)
        return
    elif text == "Завтра":
        await cmd_tomorrow(update, context)
        return
    elif text == "Тиждень":
        await cmd_week(update, context)
        return
    elif text == "Що зараз?":
        await cmd_now(update, context)
        return
    elif text == "Змінити групу":
        await cmd_setgroup(update, context)
        return
    elif text.startswith("Тиждень:"):
        await cmd_setweek(update, context)
        return
    elif text in (
        "Вимкнути сповіщення",
        "Увімкнути сповіщення",
    ):
        if not update.effective_chat:
            return
        chat_id = str(update.effective_chat.id)
        data = load_data()
        user = data["users"].get(chat_id, {})
        if user.get("active", True):
            await cmd_pause(update, context)
        else:
            await cmd_resume(update, context)
        return

    # Адмін: JSON-текст (якщо починається з { або [)
    stripped = text.strip()
    if (stripped.startswith("{") or stripped.startswith("[")) and \
       _is_admin(update.effective_user.id):
        await _process_schedule_json(update, stripped)
        return


# ──────────────────────────────────────────────────────────────
# INLINE MODE
# ──────────────────────────────────────────────────────────────
async def inline_query(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Обробка inline-запитів: @botname [група]."""
    query = update.inline_query
    if not query:
        return

    user_id = query.from_user.id if query.from_user else 0
    chat_id = str(user_id)
    data = load_data()
    user_data = data.get("users", {}).get(chat_id, {})
    user_group = user_data.get("group", "")
    search = (query.query or "").strip()

    now = datetime.now(TZ)
    results: list = []

    # Визначаємо групи для відображення
    if search:
        groups = [g for g in GROUP_NAMES if search.lower() in g.lower()]
    elif user_group:
        groups = [user_group]
    else:
        groups = GROUP_NAMES

    for group in groups[:10]:
        schedule = load_schedule(group)
        wt = get_week_type_for_user(now, user_data) if user_group == group else get_week_type(now)

        # Сьогодні
        if now.weekday() < 5:
            today_msg = build_schedule_message(schedule, now, wt, group)
        else:
            monday = now + timedelta(days=(7 - now.weekday()))
            wt_mon = get_week_type_for_user(monday, user_data) if user_group == group else get_week_type(monday)
            today_msg = build_schedule_message(schedule, monday, wt_mon, group)

        # Завтра
        tmr = now + timedelta(days=1)
        if tmr.weekday() in (5, 6):
            tmr += timedelta(days=(7 - tmr.weekday()))
        wt_tmr = get_week_type_for_user(tmr, user_data) if user_group == group else get_week_type(tmr)
        tmr_msg = build_schedule_message(schedule, tmr, wt_tmr, group)

        day_name = DAY_NAMES[now.weekday()] if now.weekday() < 5 else "Понеділок"
        results.append(InlineQueryResultArticle(
            id=f"today_{group}",
            title=f"{group} — {day_name}",
            description="Розклад на сьогодні",
            input_message_content=InputTextMessageContent(
                today_msg[:4096], parse_mode="HTML"
            ),
        ))
        results.append(InlineQueryResultArticle(
            id=f"tomorrow_{group}",
            title=f"{group} — Завтра",
            description="Розклад на завтра",
            input_message_content=InputTextMessageContent(
                tmr_msg[:4096], parse_mode="HTML"
            ),
        ))

    try:
        await query.answer(results[:50], cache_time=60)
    except Exception as exc:
        logger.error("inline_query error: %s", exc)


# ──────────────────────────────────────────────────────────────
# DAILY NOTIFICATION
# ──────────────────────────────────────────────────────────────
async def send_daily(app: Application) -> None:
    """Щоденне сповіщення — перевіряється щохвилини."""
    now = datetime.now(TZ)
    if now.weekday() in (5, 6):
        return

    data = load_data()
    hhmm = now.strftime("%H:%M")

    for chat_id, u in data["users"].items():
        if not u.get("active", True):
            continue
        if u.get("notify_time", "07:30") != hhmm:
            continue
        group = u.get("group", "")
        if not group:
            continue
        try:
            schedule = load_schedule(group)
            wt = get_week_type_for_user(now, u)
            msg = build_schedule_message(schedule, now, wt, group)
            for chunk in split_long_message(msg):
                await app.bot.send_message(
                    int(chat_id), chunk, parse_mode="HTML"
                )
        except Exception as e:
            logger.error("Fail send_daily %s: %s", chat_id, e)


# ──────────────────────────────────────────────────────────────
# ERROR HANDLER
# ──────────────────────────────────────────────────────────────
async def error_handler(
    update: object, context: ContextTypes.DEFAULT_TYPE
) -> None:
    logger.error("Необроблений виняток:", exc_info=context.error)


# ──────────────────────────────────────────────────────────────
# LIFECYCLE
# ──────────────────────────────────────────────────────────────
async def post_init(app: Application) -> None:
    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(
        send_daily, "interval", minutes=1, args=[app]
    )
    scheduler.start()


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
def main() -> None:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Команди
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("tomorrow", cmd_tomorrow))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("now", cmd_now))
    app.add_handler(CommandHandler("settime", cmd_settime))
    app.add_handler(CommandHandler("setgroup", cmd_setgroup))
    app.add_handler(CommandHandler("setweek", cmd_setweek))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("reload", cmd_reload))
    app.add_handler(CommandHandler("stats", cmd_stats))

    # Callbacks
    app.add_handler(CallbackQueryHandler(
        callback_set_group, pattern=r"^setgroup:"
    ))
    app.add_handler(CallbackQueryHandler(
        callback_parity, pattern=r"^parity:"
    ))
    app.add_handler(CallbackQueryHandler(
        callback_week, pattern=r"^week:"
    ))

    # Документи (JSON-файли від адміна)
    app.add_handler(MessageHandler(
        filters.Document.ALL, handle_document
    ))

    # Inline mode
    app.add_handler(InlineQueryHandler(inline_query))

    # Текстові повідомлення
    text_filter = filters.TEXT & ~filters.COMMAND
    app.add_handler(MessageHandler(text_filter, handle_text))

    app.add_error_handler(error_handler)

    logger.info("Bot started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
