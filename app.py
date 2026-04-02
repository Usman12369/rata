# -*- coding: utf-8 -*-
import html
import logging
import os
import re
import sqlite3
import threading
import time
from typing import Optional

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


TOKEN = os.getenv("BOT_TOKEN", "")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")

ADMIN_ID = int(os.getenv("ADMIN_ID", "7019136722"))
HELP_HANDLE = os.getenv("HELP_HANDLE", "@U5M4H")
HELP_URL = os.getenv("HELP_URL", "https://t.me/U5M4H")
DATA_DIR = os.getenv("BOT_DATA_DIR", "./data")
DB_PATH = os.path.join(DATA_DIR, "peer_reviews.db")
ENABLE_HEALTH_SERVER = os.getenv("ENABLE_HEALTH_SERVER", "0").lower() in {"1", "true", "yes", "on"}
INSTANCE_NAME = os.getenv("BOT_INSTANCE_NAME", "bot2")
LOGIN_BLOCK_SECONDS = 180
MIN_REVIEW_LEN = 10
MAX_REVIEW_LEN = 60

if ENABLE_HEALTH_SERVER:
    try:
        from health import start_health_server

        start_health_server()
    except Exception as exc:  # pragma: no cover - best effort only
        print(f"Health server error: {exc}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(TOKEN, parse_mode="HTML")
db_lock = threading.Lock()
db_connection = None


def now_ts() -> int:
    return int(time.time())


def escape(value: Optional[str]) -> str:
    return html.escape(value or "")


def score_bar(score: int) -> str:
    score = max(0, min(10, int(score)))
    return "■" * score + "□" * (10 - score)


def get_db():
    global db_connection
    with db_lock:
        if db_connection is None:
            os.makedirs(DATA_DIR, exist_ok=True)
            db_connection = sqlite3.connect(DB_PATH, check_same_thread=False)
            db_connection.row_factory = sqlite3.Row
        return db_connection


def execute(query, params=(), fetchone=False, fetchall=False, commit=False):
    conn = get_db()
    with db_lock:
        cursor = conn.cursor()
        cursor.execute(query, params)
        if commit:
            conn.commit()
        if fetchone:
            return cursor.fetchone()
        if fetchall:
            return cursor.fetchall()
        return cursor


def init_db():
    execute(
        """
        CREATE TABLE IF NOT EXISTS participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            login TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            telegram_user_id INTEGER,
            is_active INTEGER NOT NULL DEFAULT 0,
            blocked_until INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """,
        commit=True,
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS ratings (
            reviewer_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            score INTEGER NOT NULL,
            review_text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (reviewer_id, target_id)
        )
        """,
        commit=True,
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        commit=True,
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS chat_state (
            chat_id INTEGER PRIMARY KEY,
            telegram_user_id INTEGER NOT NULL,
            admin_mode INTEGER NOT NULL DEFAULT 0,
            active_login TEXT,
            state TEXT,
            pending_target_id INTEGER,
            pending_score INTEGER,
            pending_mode TEXT
        )
        """,
        commit=True,
    )
    set_setting_default("review_open", "0")
    set_setting_default("info_text", "")
    logger.info("Bot DB ready: %s", DB_PATH)


def set_setting_default(key: str, value: str):
    execute(
        "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
        (key, value),
        commit=True,
    )


def get_setting(key: str, default: str = "") -> str:
    row = execute(
        "SELECT value FROM settings WHERE key = ?",
        (key,),
        fetchone=True,
    )
    return row["value"] if row else default


def set_setting(key: str, value: str):
    execute(
        """
        INSERT INTO settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
        commit=True,
    )


def reviews_open() -> bool:
    return get_setting("review_open", "0") == "1"


def get_info_text() -> str:
    text = get_setting("info_text", "").strip()
    if text:
        return text
    return "Пока информации нет."


def ensure_chat_state(chat_id: int, telegram_user_id: int):
    execute(
        """
        INSERT INTO chat_state (chat_id, telegram_user_id, admin_mode, active_login, state, pending_target_id, pending_score, pending_mode)
        VALUES (?, ?, 0, NULL, NULL, NULL, NULL, NULL)
        ON CONFLICT(chat_id) DO UPDATE SET telegram_user_id = excluded.telegram_user_id
        """,
        (chat_id, telegram_user_id),
        commit=True,
    )


def get_chat_state(chat_id: int, telegram_user_id: int):
    ensure_chat_state(chat_id, telegram_user_id)
    return execute(
        "SELECT * FROM chat_state WHERE chat_id = ?",
        (chat_id,),
        fetchone=True,
    )


def update_chat_state(chat_id: int, **fields):
    if not fields:
        return
    set_clause = ", ".join(f"{key} = ?" for key in fields.keys())
    params = list(fields.values()) + [chat_id]
    execute(f"UPDATE chat_state SET {set_clause} WHERE chat_id = ?", params, commit=True)


def clear_pending(chat_id: int):
    update_chat_state(
        chat_id,
        state=None,
        pending_target_id=None,
        pending_score=None,
        pending_mode=None,
    )


def is_admin_user(user_id: int) -> bool:
    return user_id == ADMIN_ID


def admin_mode_enabled(chat_id: int, user_id: int) -> bool:
    state = get_chat_state(chat_id, user_id)
    return bool(state["admin_mode"])


def set_admin_mode(chat_id: int, telegram_user_id: int, enabled: bool):
    ensure_chat_state(chat_id, telegram_user_id)
    update_chat_state(chat_id, admin_mode=1 if enabled else 0)


def get_participant_by_login(login: str):
    return execute(
        "SELECT * FROM participants WHERE lower(login) = lower(?)",
        (login,),
        fetchone=True,
    )


def get_participant_by_id(participant_id: int):
    return execute(
        "SELECT * FROM participants WHERE id = ?",
        (participant_id,),
        fetchone=True,
    )


def get_participants_by_password(password: str):
    return execute(
        "SELECT * FROM participants WHERE password = ? ORDER BY id",
        (password,),
        fetchall=True,
    )


def get_active_participant_for_chat(chat_id: int, telegram_user_id: int):
    state = get_chat_state(chat_id, telegram_user_id)
    active_login = state["active_login"]
    if not active_login:
        return None
    participant = get_participant_by_login(active_login)
    if not participant:
        update_chat_state(chat_id, active_login=None)
        return None
    if participant["telegram_user_id"] != telegram_user_id or not participant["is_active"]:
        update_chat_state(chat_id, active_login=None)
        return None
    return participant


def list_participants():
    return execute(
        "SELECT * FROM participants ORDER BY full_name COLLATE NOCASE, id",
        fetchall=True,
    )


def add_participant(full_name: str, login: str):
    ts = now_ts()
    execute(
        """
        INSERT INTO participants (full_name, login, password, telegram_user_id, is_active, blocked_until, created_at, updated_at)
        VALUES (?, ?, ?, NULL, 0, 0, ?, ?)
        """,
        (full_name, login, login, ts, ts),
        commit=True,
    )


def set_participant_password(login: str, password: str):
    execute(
        "UPDATE participants SET password = ?, updated_at = ? WHERE lower(login) = lower(?)",
        (password, now_ts(), login),
        commit=True,
    )


def logout_participant(login: str, block_seconds: int = 0):
    blocked_until = now_ts() + block_seconds if block_seconds else 0
    execute(
        """
        UPDATE participants
        SET telegram_user_id = NULL, is_active = 0, blocked_until = ?, updated_at = ?
        WHERE lower(login) = lower(?)
        """,
        (blocked_until, now_ts(), login),
        commit=True,
    )
    execute(
        """
        UPDATE chat_state
        SET active_login = NULL, state = NULL, pending_target_id = NULL, pending_score = NULL, pending_mode = NULL
        WHERE lower(active_login) = lower(?)
        """,
        (login,),
        commit=True,
    )


def bind_participant(participant_id: int, telegram_user_id: int):
    execute(
        """
        UPDATE participants
        SET telegram_user_id = ?, is_active = 1, blocked_until = 0, updated_at = ?
        WHERE id = ?
        """,
        (telegram_user_id, now_ts(), participant_id),
        commit=True,
    )


def set_active_login(chat_id: int, telegram_user_id: int, login: Optional[str]):
    ensure_chat_state(chat_id, telegram_user_id)
    update_chat_state(chat_id, active_login=login)


def participant_total_score(participant_id: int) -> int:
    row = execute(
        "SELECT COALESCE(SUM(score), 0) AS total FROM ratings WHERE target_id = ?",
        (participant_id,),
        fetchone=True,
    )
    return int(row["total"] if row else 0)


def participant_received_count(participant_id: int) -> int:
    row = execute(
        "SELECT COUNT(*) AS total FROM ratings WHERE target_id = ?",
        (participant_id,),
        fetchone=True,
    )
    return int(row["total"] if row else 0)


def get_scoreboard():
    return execute(
        """
        SELECT
            p.id,
            p.full_name,
            p.login,
            COALESCE(SUM(r.score), 0) AS total_score,
            COUNT(r.reviewer_id) AS votes
        FROM participants p
        LEFT JOIN ratings r ON r.target_id = p.id
        GROUP BY p.id
        ORDER BY total_score DESC, p.full_name COLLATE NOCASE ASC, p.id ASC
        """,
        fetchall=True,
    )


def get_rank_for_participant(participant_id: int) -> Optional[int]:
    scoreboard = get_scoreboard()
    for index, row in enumerate(scoreboard, start=1):
        if row["id"] == participant_id:
            return index
    return None


def get_given_ratings(reviewer_id: int):
    return execute(
        """
        SELECT r.target_id, r.score, r.review_text, p.full_name, p.login
        FROM ratings r
        JOIN participants p ON p.id = r.target_id
        WHERE r.reviewer_id = ?
        ORDER BY p.full_name COLLATE NOCASE, p.id
        """,
        (reviewer_id,),
        fetchall=True,
    )


def get_received_ratings(target_id: int):
    return execute(
        """
        SELECT r.score, r.review_text, p.full_name, p.login
        FROM ratings r
        JOIN participants p ON p.id = r.reviewer_id
        WHERE r.target_id = ?
        ORDER BY r.updated_at DESC, p.full_name COLLATE NOCASE
        """,
        (target_id,),
        fetchall=True,
    )


def get_rating(reviewer_id: int, target_id: int):
    return execute(
        """
        SELECT reviewer_id, target_id, score, review_text, created_at, updated_at
        FROM ratings
        WHERE reviewer_id = ? AND target_id = ?
        """,
        (reviewer_id, target_id),
        fetchone=True,
    )


def upsert_rating(reviewer_id: int, target_id: int, score: int, review_text: str):
    ts = now_ts()
    execute(
        """
        INSERT INTO ratings (reviewer_id, target_id, score, review_text, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(reviewer_id, target_id) DO UPDATE SET
            score = excluded.score,
            review_text = excluded.review_text,
            updated_at = excluded.updated_at
        """,
        (reviewer_id, target_id, score, review_text, ts, ts),
        commit=True,
    )


def get_next_unrated_target(reviewer_id: int):
    return execute(
        """
        SELECT p.*
        FROM participants p
        WHERE p.id != ?
          AND NOT EXISTS (
              SELECT 1
              FROM ratings r
              WHERE r.reviewer_id = ? AND r.target_id = p.id
          )
        ORDER BY p.full_name COLLATE NOCASE, p.id
        LIMIT 1
        """,
        (reviewer_id, reviewer_id),
        fetchone=True,
    )


def participant_status_text(participant) -> str:
    return "актив" if participant["is_active"] else "неактив"


def format_profile_text(participant) -> str:
    total_score = participant_total_score(participant["id"])
    received = participant_received_count(participant["id"])
    rank = get_rank_for_participant(participant["id"])
    rank_text = str(rank) if rank is not None else "-"
    return (
        f"<b>Профиль</b>\n\n"
        f"Имя: <b>{escape(participant['full_name'])}</b>\n"
        f"Логин: <code>{escape(participant['login'])}</code>\n"
        f"Итоговый рейтинг: <b>{total_score}</b>\n"
        f"Получено оценок: <b>{received}</b>\n"
        f"Место в рейтинге: <b>{rank_text}</b>"
    )


def main_menu_markup():
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("Моя оценка", callback_data="menu:my"),
        InlineKeyboardButton("Оценка других", callback_data="menu:received"),
    )
    markup.row(
        InlineKeyboardButton("Профиль", callback_data="menu:profile"),
        InlineKeyboardButton("Помощь", url=HELP_URL),
    )
    return markup


def my_ratings_markup():
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton("Оценить студентов", callback_data="menu:rate"))
    markup.row(InlineKeyboardButton("Профиль", callback_data="menu:profile"))
    return markup


def send_message(chat_id: int, text: str, markup=None):
    bot.send_message(
        chat_id,
        text,
        reply_markup=markup,
        disable_web_page_preview=True,
    )


def send_login_prompt(chat_id: int):
    send_message(
        chat_id,
        (
            "<b>Привет.</b>\n\n"
            "Чтобы войти, отправь свой пароль в этот чат.\n"
            f"Если пароля нет, обратись к {escape(HELP_HANDLE)}."
        ),
    )


def show_profile(chat_id: int, telegram_user_id: int):
    participant = get_active_participant_for_chat(chat_id, telegram_user_id)
    if not participant:
        send_login_prompt(chat_id)
        return
    send_message(chat_id, format_profile_text(participant), main_menu_markup())


def show_my_ratings(chat_id: int, telegram_user_id: int):
    participant = get_active_participant_for_chat(chat_id, telegram_user_id)
    if not participant:
        send_login_prompt(chat_id)
        return
    ratings = get_given_ratings(participant["id"])
    lines = ["<b>Моя оценка</b>", ""]
    if not ratings:
        lines.append("Ты пока никого не оценил.")
    else:
        for index, row in enumerate(ratings, start=1):
            lines.append(
                f"{index}. {escape(row['full_name'])} - <b>{row['score']}/10</b> /red{index}"
            )
            lines.append(f"   Отзыв: {escape(row['review_text'])}")
    send_message(chat_id, "\n".join(lines), my_ratings_markup())


def show_received_ratings(chat_id: int, telegram_user_id: int):
    participant = get_active_participant_for_chat(chat_id, telegram_user_id)
    if not participant:
        send_login_prompt(chat_id)
        return
    ratings = get_received_ratings(participant["id"])
    total_score = participant_total_score(participant["id"])
    lines = [
        "<b>Оценка других</b>",
        "",
        f"Итоговый рейтинг: <b>{total_score}</b>",
        "",
    ]
    if not ratings:
        lines.append("Пока никто не оставил тебе отзыв.")
    else:
        for row in ratings:
            lines.append(f"{row['score']}/10: {escape(row['review_text'])}")
    send_message(chat_id, "\n".join(lines), main_menu_markup())


def show_info(chat_id: int):
    send_message(chat_id, f"<b>Инфа</b>\n\n{escape(get_info_text())}")


def send_help(chat_id: int):
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton("Написать Уcману", url=HELP_URL))
    send_message(
        chat_id,
        (
            "<b>Помощь</b>\n\n"
            f"Если нужен пароль или помощь, напиши {escape(HELP_HANDLE)}."
        ),
        markup,
    )


def review_score_markup(target_id: int, mode: str):
    markup = InlineKeyboardMarkup(row_width=5)
    buttons = [
        InlineKeyboardButton(str(score), callback_data=f"score:{target_id}:{score}:{mode}")
        for score in range(1, 11)
    ]
    markup.add(*buttons)
    markup.row(InlineKeyboardButton("Профиль", callback_data="menu:profile"))
    return markup


def prompt_for_rating(chat_id: int, telegram_user_id: int, target_id: int, mode: str):
    reviewer = get_active_participant_for_chat(chat_id, telegram_user_id)
    if not reviewer:
        send_login_prompt(chat_id)
        return
    target = get_participant_by_id(target_id)
    if not target:
        send_message(chat_id, "Студент не найден.")
        return
    if reviewer["id"] == target["id"]:
        send_message(chat_id, "Себя оценивать нельзя.")
        return
    old_rating = get_rating(reviewer["id"], target_id)
    lines = [f"<b>Оцени студента:</b> {escape(target['full_name'])}", ""]
    if old_rating:
        lines.append(
            f"Текущая оценка: <b>{old_rating['score']}/10</b> {score_bar(old_rating['score'])}"
        )
        lines.append(f"Текущий отзыв: {escape(old_rating['review_text'])}")
        lines.append("")
    lines.append("Выбери оценку от 1 до 10.")
    send_message(chat_id, "\n".join(lines), review_score_markup(target_id, mode))


def prompt_next_student(chat_id: int, telegram_user_id: int):
    reviewer = get_active_participant_for_chat(chat_id, telegram_user_id)
    if not reviewer:
        send_login_prompt(chat_id)
        return
    if not reviews_open():
        send_message(chat_id, "Сейчас период оценивания закрыт.")
        return
    target = get_next_unrated_target(reviewer["id"])
    if not target:
        send_message(chat_id, "Ты уже оценил всех студентов.", my_ratings_markup())
        return
    prompt_for_rating(chat_id, telegram_user_id, target["id"], "new")


def parse_red_command(text: str) -> Optional[int]:
    match = re.fullmatch(r"/red(\d+)", text.strip(), flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def format_admin_help() -> str:
    return (
        "<b>Команды админа</b>\n\n"
        "join admin - войти в режим админа\n"
        "exit admin - выйти из режима админа\n"
        "addnew &lt;Имя&gt; &lt;логин&gt; - добавить участника\n"
        "reset &lt;логин&gt; - выкинуть участника и блок на 3 минуты\n"
        "cp &lt;логин&gt; &lt;новый_пароль&gt; - сменить пароль\n"
        "marsh - открыть период оценивания\n"
        "stop - закрыть период оценивания\n"
        "рейтинг - общий топ\n"
        "актив - активность участников\n"
        "otziv &lt;логин&gt; - отзывы по человеку с авторами\n"
        "+инфа &lt;текст&gt; - установить инфо-текст\n"
        "helpadmin - показать это сообщение"
    )


def format_active_table() -> str:
    participants = list_participants()
    lines = ["<b>Актив</b>", ""]
    if not participants:
        lines.append("Пока участников нет.")
    else:
        for participant in participants:
            lines.append(
                f"{escape(participant['full_name'])}/{escape(participant['login'])}/{participant_status_text(participant)}"
            )
    return "\n".join(lines)


def format_ranking_table() -> str:
    scoreboard = get_scoreboard()
    lines = ["<b>Рейтинг</b>", ""]
    if not scoreboard:
        lines.append("Пока участников нет.")
    else:
        for index, row in enumerate(scoreboard, start=1):
            lines.append(f"{index}. {escape(row['full_name'])} - {row['total_score']}")
    return "\n".join(lines)


def format_admin_reviews(login: str) -> str:
    participant = get_participant_by_login(login)
    if not participant:
        return "Пользователь не найден."
    rows = execute(
        """
        SELECT r.score, r.review_text, p.full_name
        FROM ratings r
        JOIN participants p ON p.id = r.reviewer_id
        WHERE r.target_id = ?
        ORDER BY r.updated_at DESC, p.full_name COLLATE NOCASE
        """,
        (participant["id"],),
        fetchall=True,
    )
    lines = [f"<b>Отзывы о {escape(participant['full_name'])}</b>", ""]
    if not rows:
        lines.append("Пока отзывов нет.")
    else:
        for row in rows:
            lines.append(
                f"{escape(row['full_name'])} {row['score']}/10: {escape(row['review_text'])}"
            )
    return "\n".join(lines)


def notify_admin_about_rating(reviewer, target, score: int, review_text: str, updated: bool):
    action = "обновил отзыв о" if updated else "оценил"
    text = (
        f"{escape(reviewer['full_name'])} {action} {escape(target['full_name'])} "
        f'на {score}/10 с отзывом "{escape(review_text)}"'
    )
    try:
        send_message(ADMIN_ID, text)
    except Exception as exc:  # pragma: no cover - network side effect
        logger.warning("Admin notify failed: %s", exc)


def notify_target_about_rating(target, score: int, review_text: str, updated: bool):
    if not target["is_active"] or not target["telegram_user_id"]:
        return
    title = "Оценка о тебе обновлена" if updated else "Ты получил новую оценку"
    text = (
        f"<b>{title}</b>\n\n"
        f"{score_bar(score)} <b>{score}/10</b>\n"
        f"Отзыв: {escape(review_text)}\n\n"
        "Имя автора скрыто."
    )
    try:
        send_message(target["telegram_user_id"], text)
    except Exception as exc:  # pragma: no cover - network side effect
        logger.warning("Target notify failed: %s", exc)


def complete_review(chat_id: int, telegram_user_id: int, review_text: str):
    state = get_chat_state(chat_id, telegram_user_id)
    participant = get_active_participant_for_chat(chat_id, telegram_user_id)
    if not participant:
        clear_pending(chat_id)
        send_login_prompt(chat_id)
        return
    if state["state"] != "awaiting_review" or not state["pending_target_id"] or not state["pending_score"]:
        clear_pending(chat_id)
        send_message(chat_id, "Сначала выбери студента и оценку.")
        return
    target = get_participant_by_id(int(state["pending_target_id"]))
    if not target:
        clear_pending(chat_id)
        send_message(chat_id, "Студент не найден.")
        return
    existing = get_rating(participant["id"], target["id"])
    updated = existing is not None
    upsert_rating(participant["id"], target["id"], int(state["pending_score"]), review_text)
    clear_pending(chat_id)
    notify_target_about_rating(target, int(state["pending_score"]), review_text, updated)
    notify_admin_about_rating(participant, target, int(state["pending_score"]), review_text, updated)
    send_message(
        chat_id,
        (
            f"Отзыв для <b>{escape(target['full_name'])}</b> сохранен.\n"
            f"Оценка: <b>{state['pending_score']}/10</b>\n"
            f"Отзыв: {escape(review_text)}"
        ),
    )
    if state["pending_mode"] == "new":
        prompt_next_student(chat_id, telegram_user_id)
    else:
        show_my_ratings(chat_id, telegram_user_id)


def try_login_with_password(chat_id: int, telegram_user_id: int, raw_text: str) -> bool:
    password = raw_text.strip()
    if not password:
        return False
    current = get_active_participant_for_chat(chat_id, telegram_user_id)
    if current:
        if password == current["password"]:
            show_profile(chat_id, telegram_user_id)
            return True
        return False
    matches = get_participants_by_password(password)
    if not matches:
        return False
    if len(matches) > 1:
        send_message(
            chat_id,
            "Этот пароль совпал у нескольких участников. Попроси админа выдать уникальный пароль.",
        )
        return True
    participant = matches[0]
    if participant["blocked_until"] > now_ts():
        wait_seconds = participant["blocked_until"] - now_ts()
        send_message(
            chat_id,
            f"Этот аккаунт временно заблокирован. Попробуй снова через {wait_seconds} сек.",
        )
        return True
    if participant["is_active"] and participant["telegram_user_id"] not in (None, telegram_user_id):
        send_message(
            chat_id,
            "Этот аккаунт уже открыт на другом устройстве. Попроси админа сделать reset.",
        )
        return True
    bind_participant(participant["id"], telegram_user_id)
    set_active_login(chat_id, telegram_user_id, participant["login"])
    send_message(
        chat_id,
        (
            "Вход выполнен.\n\n"
            f"Привет, <b>{escape(participant['full_name'])}</b>."
        ),
        main_menu_markup(),
    )
    show_profile(chat_id, telegram_user_id)
    return True


def handle_exit_login(chat_id: int, telegram_user_id: int, login: str):
    participant = get_active_participant_for_chat(chat_id, telegram_user_id)
    if not participant:
        send_login_prompt(chat_id)
        return
    if participant["login"].lower() != login.lower():
        send_message(chat_id, "Сейчас у тебя открыт другой профиль.")
        return
    logout_participant(login, block_seconds=0)
    clear_pending(chat_id)
    send_message(chat_id, "Ты вышел из профиля.")
    send_login_prompt(chat_id)


def handle_addnew(chat_id: int, text: str):
    payload = text[7:].strip()
    if not payload:
        send_message(chat_id, "Формат: addnew <Имя> <логин>")
        return
    try:
        full_name, login = payload.rsplit(" ", 1)
    except ValueError:
        send_message(chat_id, "Формат: addnew <Имя> <логин>")
        return
    full_name = full_name.strip()
    login = login.strip()
    if not full_name or not login:
        send_message(chat_id, "Имя и логин должны быть заполнены.")
        return
    if not re.fullmatch(r"[A-Za-z0-9_]{3,32}", login):
        send_message(chat_id, "Логин должен быть от 3 до 32 символов: буквы, цифры, _.")
        return
    if get_participant_by_login(login):
        send_message(chat_id, "Такой логин уже существует.")
        return
    add_participant(full_name, login)
    send_message(
        chat_id,
        (
            "Новый участник добавлен.\n\n"
            f"Имя: <b>{escape(full_name)}</b>\n"
            f"Логин: <code>{escape(login)}</code>\n"
            f"Стартовый пароль: <code>{escape(login)}</code>"
        ),
    )


def handle_cp(chat_id: int, text: str):
    parts = text.strip().split(maxsplit=2)
    if len(parts) != 3:
        send_message(chat_id, "Формат: cp <логин> <новый_пароль>")
        return
    _, login, new_password = parts
    participant = get_participant_by_login(login)
    if not participant:
        send_message(chat_id, "Пользователь не найден.")
        return
    if len(new_password) < 3:
        send_message(chat_id, "Пароль должен быть минимум 3 символа.")
        return
    set_participant_password(login, new_password)
    send_message(
        chat_id,
        f"Пароль для <code>{escape(login)}</code> обновлен на <code>{escape(new_password)}</code>.",
    )


def handle_reset(chat_id: int, login: str):
    participant = get_participant_by_login(login)
    if not participant:
        send_message(chat_id, "Пользователь не найден.")
        return
    was_active = participant["is_active"] and participant["telegram_user_id"]
    target_chat_id = participant["telegram_user_id"]
    logout_participant(login, block_seconds=LOGIN_BLOCK_SECONDS)
    send_message(
        chat_id,
        f"<code>{escape(login)}</code> сброшен. Повторный вход будет доступен через 3 минуты.",
    )
    if was_active:
        try:
            send_message(
                target_chat_id,
                "Админ завершил твою сессию. Повторный вход будет доступен через 3 минуты.",
            )
        except Exception as exc:  # pragma: no cover - network side effect
            logger.warning("Reset notify failed: %s", exc)


def handle_admin_text(message) -> bool:
    text = (message.text or "").strip()
    lowered = text.casefold()
    chat_id = message.chat.id
    user_id = message.from_user.id

    if lowered == "join admin":
        if not is_admin_user(user_id):
            return False
        set_admin_mode(chat_id, user_id, True)
        send_message(chat_id, "Режим админа включен.\n\n" + format_admin_help())
        return True

    if lowered == "exit admin":
        if not is_admin_user(user_id):
            return False
        set_admin_mode(chat_id, user_id, False)
        send_message(chat_id, "Режим админа выключен.")
        return True

    if lowered == "helpadmin":
        if not is_admin_user(user_id):
            return False
        send_message(chat_id, format_admin_help())
        return True

    if not is_admin_user(user_id) or not admin_mode_enabled(chat_id, user_id):
        return False

    if lowered.startswith("addnew "):
        handle_addnew(chat_id, text)
        return True
    if lowered.startswith("cp "):
        handle_cp(chat_id, text)
        return True
    if lowered.startswith("reset "):
        login = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) == 2 else ""
        if not login:
            send_message(chat_id, "Формат: reset <логин>")
        else:
            handle_reset(chat_id, login)
        return True
    if lowered in {"marsh", "марш"}:
        set_setting("review_open", "1")
        send_message(chat_id, "Период оценивания открыт.")
        return True
    if lowered == "stop":
        set_setting("review_open", "0")
        send_message(chat_id, "Период оценивания закрыт.")
        return True
    if lowered == "актив":
        send_message(chat_id, format_active_table())
        return True
    if lowered == "рейтинг":
        send_message(chat_id, format_ranking_table())
        return True
    if lowered.startswith("otziv "):
        login = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) == 2 else ""
        send_message(chat_id, format_admin_reviews(login))
        return True
    if lowered.startswith("+инфа"):
        payload = text[5:].strip()
        set_setting("info_text", payload)
        send_message(chat_id, "Инфа обновлена.")
        return True
    return False


def handle_student_commands(message) -> bool:
    text = (message.text or "").strip()
    lowered = text.casefold()
    chat_id = message.chat.id
    user_id = message.from_user.id
    state = get_chat_state(chat_id, user_id)

    if state["state"] == "awaiting_review":
        if lowered in {"отмена", "cancel"}:
            clear_pending(chat_id)
            send_message(chat_id, "Редактирование отзыва отменено.")
            return True
        review_text = text.strip()
        if len(review_text) < MIN_REVIEW_LEN or len(review_text) > MAX_REVIEW_LEN:
            send_message(
                chat_id,
                (
                    f"Отзыв должен быть от {MIN_REVIEW_LEN} до {MAX_REVIEW_LEN} символов.\n\n"
                    "Напиши текст заново."
                ),
            )
            return True
        complete_review(chat_id, user_id, review_text)
        return True

    if lowered in {"/start", "start"}:
        participant = get_active_participant_for_chat(chat_id, user_id)
        if participant:
            show_profile(chat_id, user_id)
        else:
            send_login_prompt(chat_id)
        return True
    if lowered in {"профиль", "profile"}:
        show_profile(chat_id, user_id)
        return True
    if lowered == "моя оценка":
        show_my_ratings(chat_id, user_id)
        return True
    if lowered == "оценка других":
        show_received_ratings(chat_id, user_id)
        return True
    if lowered in {"помощь", "help"}:
        send_help(chat_id)
        return True
    if lowered == "инфа":
        show_info(chat_id)
        return True
    if lowered == "оценить студентов":
        prompt_next_student(chat_id, user_id)
        return True

    red_number = parse_red_command(text)
    if red_number is not None:
        participant = get_active_participant_for_chat(chat_id, user_id)
        if not participant:
            send_login_prompt(chat_id)
            return True
        if not reviews_open():
            send_message(chat_id, "Сейчас редактирование отзывов закрыто.")
            return True
        ratings = get_given_ratings(participant["id"])
        if red_number < 1 or red_number > len(ratings):
            send_message(chat_id, "Такого номера редактирования нет.")
            return True
        target = ratings[red_number - 1]
        prompt_for_rating(chat_id, user_id, target["target_id"], "edit")
        return True

    if lowered.startswith("exit "):
        payload = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) == 2 else ""
        if payload.casefold() == "admin":
            return False
        handle_exit_login(chat_id, user_id, payload)
        return True
    return False


@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    data = call.data or ""
    try:
        if data == "menu:profile":
            show_profile(chat_id, user_id)
        elif data == "menu:my":
            show_my_ratings(chat_id, user_id)
        elif data == "menu:received":
            show_received_ratings(chat_id, user_id)
        elif data == "menu:rate":
            prompt_next_student(chat_id, user_id)
        elif data.startswith("score:"):
            parts = data.split(":")
            if len(parts) != 4:
                bot.answer_callback_query(call.id, "Некорректная кнопка.")
                return
            _, target_id, score, mode = parts
            participant = get_active_participant_for_chat(chat_id, user_id)
            if not participant:
                bot.answer_callback_query(call.id, "Сначала войди в профиль.")
                send_login_prompt(chat_id)
                return
            if not reviews_open():
                bot.answer_callback_query(call.id, "Оценивание сейчас закрыто.")
                return
            target = get_participant_by_id(int(target_id))
            if not target:
                bot.answer_callback_query(call.id, "Студент не найден.")
                return
            update_chat_state(
                chat_id,
                state="awaiting_review",
                pending_target_id=int(target_id),
                pending_score=int(score),
                pending_mode=mode,
            )
            send_message(
                chat_id,
                (
                    f"Оценка для <b>{escape(target['full_name'])}</b>: <b>{score}/10</b> {score_bar(int(score))}\n\n"
                    "<i>Отзыв полностью анонимен. Только студент увидит сам отзыв, но не увидит твое имя.</i>\n\n"
                    f"Теперь напиши отзыв от {MIN_REVIEW_LEN} до {MAX_REVIEW_LEN} символов."
                ),
            )
            bot.answer_callback_query(call.id, "Оценка выбрана.")
            return
        bot.answer_callback_query(call.id)
    except Exception as exc:
        logger.exception("Callback error")
        bot.answer_callback_query(call.id, "Произошла ошибка.")
        send_message(chat_id, f"Ошибка: {escape(str(exc))}")


@bot.message_handler(content_types=["text"])
def handle_text(message):
    if message.chat.type != "private":
        return
    chat_id = message.chat.id
    user_id = message.from_user.id
    ensure_chat_state(chat_id, user_id)
    text = (message.text or "").strip()

    try:
        if handle_admin_text(message):
            return
        if handle_student_commands(message):
            return
        if try_login_with_password(chat_id, user_id, text):
            return
        participant = get_active_participant_for_chat(chat_id, user_id)
        if participant:
            send_message(
                chat_id,
                "Не понял команду. Используй кнопки меню или команды профиль / моя оценка / оценка других / инфа.",
                main_menu_markup(),
            )
        else:
            send_login_prompt(chat_id)
    except Exception as exc:
        logger.exception("Message handler error")
        send_message(chat_id, f"Ошибка: {escape(str(exc))}")


def notify_startup():
    try:
        bot_info = bot.get_me()
        send_message(
            ADMIN_ID,
            (
                f"Бот запущен: <b>{escape(bot_info.first_name)}</b> (@{escape(bot_info.username)})\n"
                f"Инстанс: <code>{escape(INSTANCE_NAME)}</code>\n"
                f"База: <code>{escape(DB_PATH)}</code>"
            ),
        )
    except Exception as exc:  # pragma: no cover - network side effect
        logger.warning("Startup notify failed: %s", exc)


def main():
    logger.info("Starting peer review bot instance %s", INSTANCE_NAME)
    init_db()
    notify_startup()
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as exc:  # pragma: no cover - runtime resilience
            logger.exception("Polling crashed: %s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
