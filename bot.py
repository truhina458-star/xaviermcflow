# ===== НАЧАЛО ЧАСТИ 1 =====
# -*- coding: utf-8 -*-
import sys
import os
import random
import sqlite3
import re
import time
import threading
import json
import logging
import datetime

# ======================== ЛОГГЕР (ДО КОНФИГА) ============================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ======================== НАСТРОЙКА ПУТЕЙ ДЛЯ ХОСТИНГА ============================
DATA_DIR = os.environ.get('DATA_DIR', '/app/data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

MAIN_DB = os.path.join(DATA_DIR, "assistant.db")
AUDIENCE_DB_PREFIX = "audience_"
RESTART_PEER_FILE = os.path.join(DATA_DIR, "restart_peer_id.txt")
# =================================================================================

# ======================== ЗАГРУЗКА КОНФИГА ИЗ config.json ============================
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def load_config():
    """Загружает конфиг из config.json. Если файла нет — берёт из переменных окружения."""
    cfg = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            logger.info(f"✅ Конфиг загружен из {CONFIG_FILE}")
        except Exception as e:
            logger.error(f"Ошибка чтения config.json: {e}")
            cfg = {}
    else:
        logger.warning(f"⚠️ Файл {CONFIG_FILE} не найден, используются переменные окружения")
    cfg.setdefault('GROUP_TOKEN', os.environ.get('GROUP_TOKEN'))
    cfg.setdefault('GROUP_ID', os.environ.get('GROUP_ID'))
    cfg.setdefault('OWNER_ID', os.environ.get('OWNER_ID'))
    return cfg

_config = load_config()
GROUP_TOKEN = _config.get('GROUP_TOKEN')
GROUP_ID = _config.get('GROUP_ID')
OWNER_ID = _config.get('OWNER_ID')

if not GROUP_TOKEN or not GROUP_ID or not OWNER_ID:
    print("❌ Ошибка: не заданы GROUP_TOKEN, GROUP_ID, OWNER_ID!")
    print(f"Проверьте файл {CONFIG_FILE} или переменные окружения.")
    sys.exit(1)

# GROUP_ID должен быть ЦЕЛЫМ ЧИСЛОМ для VkBotLongPoll
try:
    GROUP_ID = int(str(GROUP_ID).strip())
except (TypeError, ValueError):
    print(f"❌ GROUP_ID должен быть числом, получено: {GROUP_ID!r}")
    sys.exit(1)

# OWNER_ID остаётся строкой, потому что сравнивается со строковым sender_id
OWNER_ID = str(OWNER_ID).strip()
# =====================================================================================

import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor

# ======================== ОТПРАВКА ОШИБОК ВЛАДЕЛЬЦУ ============================
class VkErrorHandler(logging.Handler):
    def emit(self, record):
        if record.levelno >= logging.ERROR:
            try:
                import traceback
                msg = self.format(record)
                if record.exc_info:
                    tb = traceback.format_exception(*record.exc_info)
                    msg += "\n" + "".join(tb)
                if 'vk' in globals() and vk:
                    vk.messages.send(
                        peer_id=int(OWNER_ID),
                        message=msg,
                        random_id=random.getrandbits(31)
                    )
            except:
                pass

def send_to_owner(text):
    try:
        if 'vk' in globals() and vk:
            vk.messages.send(
                peer_id=int(OWNER_ID),
                message=text,
                random_id=random.getrandbits(31)
            )
    except:
        pass

# ======================== РАБОТА С БАЗАМИ ДАННЫХ ============================

DB_LOCK = threading.RLock()

def get_db_path(peer_id):
    if peer_id is None or peer_id == 0:
        return MAIN_DB
    return os.path.join(DATA_DIR, f"{AUDIENCE_DB_PREFIX}{peer_id}.db")

def create_audience_schema(conn):
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS creative (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            variant INTEGER,
            task_text TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT,
            template TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS test_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            variant INTEGER NOT NULL,
            question_text TEXT NOT NULL,
            correct_option_index INTEGER NOT NULL,
            order_num INTEGER NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS test_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question_id INTEGER NOT NULL,
            option_label TEXT NOT NULL,
            option_text TEXT NOT NULL,
            FOREIGN KEY (question_id) REFERENCES test_questions(id) ON DELETE CASCADE
        )
    ''')
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('st1_text', '')")
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('exam_info_text', '')")
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('report_template', '')")
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('test_time_limit', '30')")
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('test_fail_threshold', '5')")
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('welcome_message', '')")
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('welcome_enabled', '1')")
    conn.commit()

def get_db_connection(peer_id=None):
    try:
        if peer_id is not None:
            dc = get_datacenter_peer_id()
            if dc is not None and peer_id == dc:
                peer_id = None

        if peer_id is None:
            conn = sqlite3.connect(MAIN_DB, timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            return conn

        db_file = get_db_path(peer_id)
        is_new = not os.path.exists(db_file)
        conn = sqlite3.connect(db_file, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        if is_new:
            create_audience_schema(conn)
        return conn
    except Exception as e:
        logger.error(f"Ошибка в get_db_connection(peer_id={peer_id}): {e}")
        raise

def init_main_db():
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id TEXT PRIMARY KEY,
                added_by TEXT,
                added_at INTEGER,
                role TEXT DEFAULT 'admin'
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS co_owners (
                user_id TEXT PRIMARY KEY,
                added_by TEXT,
                added_at INTEGER
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS audiences (
                peer_id INTEGER PRIMARY KEY,
                owner_id TEXT,
                confirmed INTEGER DEFAULT 0,
                request_time INTEGER,
                request_message_id INTEGER,
                is_datacenter INTEGER DEFAULT 0,
                last_activity INTEGER
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS creative (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT,
                variant INTEGER,
                task_text TEXT
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS topics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT,
                template TEXT
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS test_questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                variant INTEGER NOT NULL,
                question_text TEXT NOT NULL,
                correct_option_index INTEGER NOT NULL,
                order_num INTEGER NOT NULL
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS test_options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER NOT NULL,
                option_label TEXT NOT NULL,
                option_text TEXT NOT NULL,
                FOREIGN KEY (question_id) REFERENCES test_questions(id) ON DELETE CASCADE
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS user_nicknames (
                peer_id INTEGER NOT NULL,
                user_id TEXT NOT NULL,
                nickname TEXT NOT NULL,
                set_by TEXT,
                set_at INTEGER,
                PRIMARY KEY (peer_id, user_id)
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS audience_students (
                peer_id INTEGER NOT NULL,
                user_id TEXT NOT NULL,
                added_by TEXT NOT NULL,
                added_at INTEGER,
                status TEXT DEFAULT 'active',
                finished_at INTEGER,
                result TEXT,
                PRIMARY KEY (peer_id, user_id)
            )
        ''')
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('datacenter_peer_id', '')")
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('st1_text', '')")
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('exam_info_text', '')")
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('report_template', '')")
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('test_time_limit', '30')")
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('test_fail_threshold', '5')")
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('notification_chat_id', '')")
        conn.commit()
    finally:
        conn.close()

def cleanup_audience_dbs():
    expected_tables = {'creative', 'topics', 'settings', 'test_questions', 'test_options'}
    removed_count = 0
    for filename in os.listdir(DATA_DIR):
        if not filename.startswith(AUDIENCE_DB_PREFIX) or not filename.endswith('.db'):
            continue
        filepath = os.path.join(DATA_DIR, filename)
        try:
            conn = sqlite3.connect(filepath, timeout=30)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row['name'] for row in cur.fetchall()]
            for table in tables:
                if table.startswith('sqlite_'):
                    continue
                if table not in expected_tables:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")
                    logger.info(f"Удалена лишняя таблица '{table}' из {filename}")
                    removed_count += 1
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка при очистке {filename}: {e}")
    logger.info(f"Очистка завершена. Удалено {removed_count} лишних таблиц.")

def auto_repair_audiences():
    conn = get_db_connection(None)
    repaired = 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT peer_id, confirmed, is_datacenter FROM audiences")
        rows = cur.fetchall()
        for row in rows:
            peer_id = row['peer_id']
            confirmed = row['confirmed']
            is_dc = row['is_datacenter']
            db_file = get_db_path(peer_id)
            db_exists = os.path.exists(db_file)

            if db_exists and confirmed == 0:
                cur.execute("UPDATE audiences SET confirmed = 1 WHERE peer_id = ?", (peer_id,))
                repaired += 1
                logger.info(f"Аудитория {peer_id} восстановлена: confirmed = 1 (БД существует)")
            elif not db_exists and confirmed == 1 and not is_dc:
                cur.execute("DELETE FROM audiences WHERE peer_id = ?", (peer_id,))
                cur.execute("DELETE FROM audience_students WHERE peer_id = ?", (peer_id,))
                cur.execute("DELETE FROM user_nicknames WHERE peer_id = ?", (peer_id,))
                repaired += 1
                logger.info(f"Удалена запись аудитории {peer_id} (БД отсутствует)")
            elif not db_exists and is_dc:
                logger.warning(f"Датацентр {peer_id} не имеет файла БД, но запись существует. Рекомендуется проверить.")

        dc_id = get_datacenter_peer_id()
        if dc_id:
            cur.execute("SELECT is_datacenter FROM audiences WHERE peer_id = ?", (dc_id,))
            row = cur.fetchone()
            if not row or row['is_datacenter'] != 1:
                set_datacenter_peer_id(None)
                logger.warning(f"Сброшен datacenter_peer_id = {dc_id} (не соответствует записи)")
                repaired += 1

        cur.execute("DELETE FROM audience_students WHERE peer_id NOT IN (SELECT peer_id FROM audiences)")
        if cur.rowcount > 0:
            logger.info(f"Удалено {cur.rowcount} сирот из audience_students")
        cur.execute("DELETE FROM user_nicknames WHERE peer_id NOT IN (SELECT peer_id FROM audiences)")
        if cur.rowcount > 0:
            logger.info(f"Удалено {cur.rowcount} сирот из user_nicknames")

        conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при автоматическом восстановлении аудиторий: {e}")
    finally:
        conn.close()
    if repaired:
        logger.info(f"Автовосстановление завершено. Исправлено {repaired} проблем.")
    else:
        logger.info("Автовосстановление не потребовалось.")

def delete_audience_db(peer_id):
    if is_datacenter(peer_id):
        return True
    db_file = get_db_path(peer_id)
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
            for ext in ['-wal', '-shm']:
                wal_file = db_file + ext
                if os.path.exists(wal_file):
                    try:
                        os.remove(wal_file)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить {wal_file}: {e}")
            logger.info(f"База данных аудитории {peer_id} удалена.")
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления БД {peer_id}: {e}")
            return False
    return True

# -------------------- ФУНКЦИИ ДОСТУПА К ДАННЫМ --------------------

def get_setting(key, default=None, peer_id=None):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = cur.fetchone()
        return row['value'] if row else default
    finally:
        conn.close()

def set_setting(key, value, peer_id=None):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
    finally:
        conn.close()

def get_report_template(peer_id=None):
    return get_setting("report_template", "", peer_id)

def set_report_template(text, peer_id=None):
    set_setting("report_template", text, peer_id)

def get_creative_text(ctype, variant, peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT task_text FROM creative WHERE type=? AND variant=?", (ctype, variant))
        return cur.fetchone()
    finally:
        conn.close()

def set_creative_text(ctype, variant, text, peer_id):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("DELETE FROM creative WHERE type=? AND variant=?", (ctype, variant))
        conn.execute("INSERT INTO creative (type, variant, task_text) VALUES (?, ?, ?)", (ctype, variant, text))
        conn.commit()
    finally:
        conn.close()

def delete_creative_text(ctype, variant, peer_id):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("DELETE FROM creative WHERE type=? AND variant=?", (ctype, variant))
        conn.commit()
    finally:
        conn.close()

def get_topic_by_id(topic_id, peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, text, template FROM topics WHERE id=?", (topic_id,))
        return cur.fetchone()
    finally:
        conn.close()

def get_all_topics(peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, text, template FROM topics ORDER BY id")
        return cur.fetchall()
    finally:
        conn.close()

def add_topic(text, template, peer_id):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("INSERT INTO topics (text, template) VALUES (?, ?)", (text, template))
        conn.commit()
    finally:
        conn.close()

def delete_topic(topic_id, peer_id):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("DELETE FROM topics WHERE id=?", (topic_id,))
        conn.commit()
    finally:
        conn.close()

def get_creative_text_fallback(ctype, variant, peer_id):
    row = get_creative_text(ctype, variant, peer_id)
    if row is not None:
        return row
    if ctype == "Обращения":
        src_variant_map = {1: 1, 3: 2}
        src_variant = src_variant_map.get(variant)
        if src_variant is not None:
            src_row = get_creative_text("Прокуратура", src_variant, peer_id)
            if src_row is not None:
                set_creative_text(ctype, variant, src_row['task_text'], peer_id)
                return get_creative_text(ctype, variant, peer_id)
    return None

def delete_all_topics(peer_id):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("DELETE FROM topics")
        conn.commit()
    finally:
        conn.close()

# -------------------- ФУНКЦИИ ДЛЯ ТЕСТОВ ПО ОДНОМУ --------------------

def get_test_questions(topic, variant, peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, question_text, correct_option_index, order_num FROM test_questions WHERE topic=? AND variant=? ORDER BY order_num", (topic, variant))
        return cur.fetchall()
    finally:
        conn.close()

def get_test_options(question_id, peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, option_label, option_text FROM test_options WHERE question_id=? ORDER BY id", (question_id,))
        return cur.fetchall()
    finally:
        conn.close()

def add_test_question(peer_id, topic, variant, question_text, correct_option_index, order_num, options):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        if order_num is None:
            cur.execute("SELECT COALESCE(MAX(order_num), 0) + 1 FROM test_questions WHERE topic=? AND variant=?", (topic, variant))
            order_num = cur.fetchone()[0]
        cur.execute("INSERT INTO test_questions (topic, variant, question_text, correct_option_index, order_num) VALUES (?, ?, ?, ?, ?)",
                    (topic, variant, question_text, correct_option_index, order_num))
        qid = cur.lastrowid
        for label, text in options:
            cur.execute("INSERT INTO test_options (question_id, option_label, option_text) VALUES (?, ?, ?)", (qid, label, text))
        conn.commit()
        return qid
    finally:
        conn.close()

def update_test_question(question_id, new_question_text, new_correct_index, new_options, peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE test_questions SET question_text=?, correct_option_index=? WHERE id=?", (new_question_text, new_correct_index, question_id))
        cur.execute("DELETE FROM test_options WHERE question_id=?", (question_id,))
        for label, text in new_options:
            cur.execute("INSERT INTO test_options (question_id, option_label, option_text) VALUES (?, ?, ?)", (question_id, label, text))
        conn.commit()
    finally:
        conn.close()

def delete_test_question(question_id, peer_id):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("DELETE FROM test_questions WHERE id=?", (question_id,))
        conn.commit()
    finally:
        conn.close()

def delete_test_questions(peer_id, topic, variant):
    conn = get_db_connection(peer_id)
    try:
        conn.execute("DELETE FROM test_questions WHERE topic=? AND variant=?", (topic, variant))
        conn.commit()
    finally:
        conn.close()

def get_test_time_limit(peer_id):
    val = get_setting("test_time_limit", "30", peer_id)
    try:
        return int(val)
    except:
        return 30

def set_test_time_limit(peer_id, seconds):
    set_setting("test_time_limit", str(seconds), peer_id)

def get_test_fail_threshold(peer_id):
    val = get_setting("test_fail_threshold", "5", peer_id)
    try:
        return int(val)
    except:
        return 5

def set_test_fail_threshold(peer_id, threshold):
    set_setting("test_fail_threshold", str(threshold), peer_id)

def has_one_by_one_test(topic, variant, peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM test_questions WHERE topic=? AND variant=? LIMIT 1", (topic, variant))
        return cur.fetchone() is not None
    finally:
        conn.close()

# -------------------- ФУНКЦИИ ДЛЯ НИКНЕЙМОВ, СТУДЕНТОВ, УВЕДОМЛЕНИЙ --------------------

def set_user_nickname(user_id, nickname, peer_id, set_by=None):
    conn = get_db_connection(None)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO user_nicknames (peer_id, user_id, nickname, set_by, set_at) VALUES (?, ?, ?, ?, ?)",
            (peer_id, str(user_id), nickname, str(set_by) if set_by else None, int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()

def get_user_nickname(user_id, peer_id):
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT nickname FROM user_nicknames WHERE peer_id=? AND user_id=?", (peer_id, str(user_id)))
        row = cur.fetchone()
        return row['nickname'] if row else None
    finally:
        conn.close()

def get_user_mention(user_id, peer_id):
    nickname = get_user_nickname(user_id, peer_id)
    if nickname:
        return f"[id{user_id}|{nickname}]"
    else:
        try:
            user = vk.users.get(user_ids=user_id)[0]
            name = f"{user['first_name']} {user['last_name']}"
        except:
            name = f"id{user_id}"
        return f"[id{user_id}|{name}]"

def add_student(peer_id, user_id, added_by):
    conn = get_db_connection(None)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO audience_students (peer_id, user_id, added_by, added_at, status) VALUES (?, ?, ?, ?, 'active')",
            (peer_id, str(user_id), str(added_by), int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()

def remove_student(peer_id, user_id):
    conn = get_db_connection(None)
    try:
        conn.execute("DELETE FROM audience_students WHERE peer_id=? AND user_id=?", (peer_id, str(user_id)))
        conn.commit()
    finally:
        conn.close()

def finish_student(peer_id, user_id, result):
    conn = get_db_connection(None)
    try:
        conn.execute(
            "UPDATE audience_students SET status='finished', finished_at=?, result=? WHERE peer_id=? AND user_id=?",
            (int(time.time()), result, peer_id, str(user_id))
        )
        conn.commit()
    finally:
        conn.close()

def get_audience_students(peer_id):
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id, added_by, added_at, status, result FROM audience_students WHERE peer_id=? AND status='active'", (peer_id,))
        return cur.fetchall()
    finally:
        conn.close()

def get_student(peer_id, user_id):
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM audience_students WHERE peer_id=? AND user_id=?", (peer_id, str(user_id)))
        return cur.fetchone()
    finally:
        conn.close()

# ==================== ФУНКЦИИ ДЛЯ ПРИВЕТСТВИЯ ====================

def get_welcome_message(peer_id):
    return get_setting("welcome_message", "", peer_id)

def set_welcome_message(peer_id, text):
    set_setting("welcome_message", text, peer_id)

def is_welcome_enabled(peer_id):
    val = get_setting("welcome_enabled", "1", peer_id)
    return val == "1"

def set_welcome_enabled(peer_id, enabled):
    set_setting("welcome_enabled", "1" if enabled else "0", peer_id)

def get_notification_chat():
    val = get_setting("notification_chat_id", None, None)
    if val is None or val == '' or val == 'None':
        return None
    try:
        return int(val)
    except ValueError:
        return None

def set_notification_chat(peer_id):
    set_setting("notification_chat_id", str(peer_id), None)

def send_notification(text):
    chat = get_notification_chat()
    if chat:
        send_message(chat, text)

# -------------------- УПРАВЛЕНИЕ ДАТАЦЕНТРОМ И КОПИРОВАНИЕ --------------------

def get_datacenter_peer_id():
    """Читаем напрямую из MAIN_DB, чтобы избежать рекурсии."""
    try:
        conn = sqlite3.connect(MAIN_DB, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key='datacenter_peer_id'")
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        val = row['value']
        if val is None or val == '' or val == 'None':
            return None
        return int(val)
    except Exception:
        return None

def set_datacenter_peer_id(peer_id):
    set_setting("datacenter_peer_id", str(peer_id) if peer_id else "")

def copy_global_to_audience(target_peer):
    source_conn = get_db_connection(None)
    target_conn = get_db_connection(target_peer)
    try:
        target_conn.execute("PRAGMA foreign_keys=OFF")
        tables = ['creative', 'topics', 'test_questions']
        for table in tables:
            target_conn.execute(f"DELETE FROM {table}")
            cur = source_conn.cursor()
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            if not rows:
                logger.info(f"Таблица {table} пуста, пропускаем")
                continue
            cur.execute(f"PRAGMA table_info({table})")
            cols = [row['name'] for row in cur.fetchall() if row['name'] != 'id']
            placeholders = ', '.join(['?' for _ in cols])
            insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
            if table == 'test_questions':
                qid_map = {}
                for row in rows:
                    cur2 = target_conn.cursor()
                    cur2.execute(insert_sql, [row[col] for col in cols])
                    new_id = cur2.lastrowid
                    qid_map[row['id']] = new_id
                    cur2.close()
                cur_opts = source_conn.cursor()
                cur_opts.execute("SELECT * FROM test_options")
                opts = cur_opts.fetchall()
                if opts:
                    target_conn.execute("DELETE FROM test_options")
                    for opt in opts:
                        new_qid = qid_map.get(opt['question_id'])
                        if new_qid:
                            target_conn.execute(
                                "INSERT INTO test_options (question_id, option_label, option_text) VALUES (?, ?, ?)",
                                (new_qid, opt['option_label'], opt['option_text'])
                            )
                    logger.info(f"Скопировано {len(opts)} вариантов ответов")
                cur_opts.close()
            else:
                for row in rows:
                    target_conn.execute(insert_sql, [row[col] for col in cols])
            logger.info(f"Скопировано {len(rows)} записей из {table}")
        source_cur = source_conn.cursor()
        source_cur.execute("SELECT key, value FROM settings")
        settings = source_cur.fetchall()
        target_conn.execute("DELETE FROM settings")
        exclude_keys = {'datacenter_peer_id', 'notification_chat_id'}
        for s in settings:
            if s['key'] not in exclude_keys:
                target_conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (s['key'], s['value']))
        logger.info(f"Скопировано {len(settings)} настроек (исключены служебные)")
        source_cur.close()
        target_conn.execute("PRAGMA foreign_keys=ON")
        target_conn.commit()
        logger.info("Копирование данных в аудиторию завершено успешно")
    except Exception as e:
        logger.error(f"Ошибка при копировании данных: {e}")
        raise
    finally:
        source_conn.close()
        target_conn.close()

def copy_datacenter_to_audience(target_peer_id):
    dc = get_datacenter_peer_id()
    if dc is None:
        return False
    copy_global_to_audience(target_peer_id)
    return True

def ensure_audience_initialized(peer_id):
    conn = get_db_connection(peer_id)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM creative LIMIT 1")
        if cur.fetchone() is not None:
            return True
    finally:
        conn.close()
    return copy_datacenter_to_audience(peer_id)

# -------------------- УПРАВЛЕНИЕ АУДИТОРИЯМИ --------------------

def is_datacenter(peer_id):
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT is_datacenter FROM audiences WHERE peer_id=? AND confirmed=1", (peer_id,))
        row = cur.fetchone()
        return row is not None and row['is_datacenter'] == 1
    finally:
        conn.close()

def is_audience_confirmed(peer_id):
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT confirmed FROM audiences WHERE peer_id=?", (peer_id,))
        row = cur.fetchone()
        return row is not None and row['confirmed'] == 1
    finally:
        conn.close()

def get_audience_owner(peer_id):
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT owner_id FROM audiences WHERE peer_id=?", (peer_id,))
        row = cur.fetchone()
        return row['owner_id'] if row else None
    finally:
        conn.close()

def set_audience_request(peer_id, owner_id=None, request_msg_id=None):
    conn = get_db_connection(None)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO audiences (peer_id, owner_id, confirmed, request_time, request_message_id, is_datacenter, last_activity) VALUES (?, ?, 0, ?, ?, 0, ?)",
            (peer_id, str(owner_id) if owner_id else None, int(time.time()), request_msg_id, int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()

def update_audience_activity(peer_id):
    conn = get_db_connection(None)
    try:
        conn.execute("UPDATE audiences SET last_activity=? WHERE peer_id=?", (int(time.time()), peer_id))
        conn.commit()
    finally:
        conn.close()

def get_all_audiences():
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT peer_id, owner_id, last_activity FROM audiences WHERE confirmed=1 AND is_datacenter=0 ORDER BY last_activity DESC")
        return cur.fetchall()
    finally:
        conn.close()

# -------------------- ОЧИСТКА СОСТОЯНИЯ БЕСЕДЫ --------------------

def cleanup_peer_state(peer_id):
    if peer_id in active_tests:
        test = active_tests.pop(peer_id)
        if test.get('timer'):
            try:
                test['timer'].cancel()
            except:
                pass
        if test.get('start_timer'):
            try:
                test['start_timer'].cancel()
            except:
                pass
        logger.info(f"Принудительно завершён тест в беседе {peer_id}")
    keys_to_remove = []
    for key in list(menu_state.keys()):
        if isinstance(key, tuple) and len(key) == 2 and key[0] == peer_id:
            keys_to_remove.append(key)
    for key in keys_to_remove:
        menu_state.pop(key, None)
    if keys_to_remove:
        logger.info(f"Удалено {len(keys_to_remove)} состояний меню для беседы {peer_id}")

def delete_audience_by_owner(peer_id):
    if is_datacenter(peer_id):
        return False, "Это датацентр, его нельзя удалить этой командой."
    cleanup_peer_state(peer_id)
    delete_audience_db(peer_id)
    conn = get_db_connection(None)
    try:
        conn.execute("DELETE FROM audiences WHERE peer_id=?", (peer_id,))
        conn.execute("DELETE FROM audience_students WHERE peer_id=?", (peer_id,))
        conn.execute("DELETE FROM user_nicknames WHERE peer_id=?", (peer_id,))
        conn.commit()
    finally:
        conn.close()
    return True, "Аудитория удалена."

# ===== КОНЕЦ ЧАСТИ 1 =====
# ===== НАЧАЛО ЧАСТИ 2 =====
# -------------------- СОЗДАНИЕ И УДАЛЕНИЕ АУДИТОРИЙ --------------------

def init_global_materials():
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM topics")
        if cur.fetchone()[0] == 0:
            default_topics = [
                "Понятие преступления и пределы уголовной ответственности",
                "Роль адвоката в системе правосудия",
                "Презумпция невиновности как основа судебного процесса",
                "Соотношение уголовной и административной ответственности",
                "Юридическая квалификация преступлений: ошибки следствия",
                "Права обвиняемого при задержании и допросе",
                "Законность доказательств в уголовном процессе",
                "Тактика защиты подозреваемого на стадии расследования",
                "Работа адвоката при избрании меры пресечения",
                "Оспаривание незаконного задержания",
                "Стратегия защиты при обвинении в тяжких преступлениях",
                "Адвокатская линия защиты при соучастии",
                "Переговоры с прокуратурой и досудебное урегулирование",
                "Роль адвоката при заключении процессуальных соглашений",
                "Границы полномочий правоохранительных органов",
                "Типичные процессуальные нарушения сотрудников LSPD/FBI",
                "Защита граждан от превышения должностных полномочий",
                "Правомерность применения силы сотрудниками государства",
                "Обжалование действий государственных служащих",
                "Подготовка адвоката к судебному заседанию",
                "Искусство перекрёстного допроса",
                "Оценка доказательств судом",
                "Построение убедительной защитительной речи",
                "Тактика поведения адвоката в суде",
                "Судебные ошибки и основания для апелляции"
            ]
            for topic in default_topics:
                conn.execute("INSERT INTO topics (text, template) VALUES (?, ?)", (topic, ""))
            conn.commit()
            logger.info("Добавлены темы для докладов.")

        cur.execute("SELECT COUNT(*) FROM test_questions")
        if cur.fetchone()[0] == 0:
            exam_topics = ["Экзамен_1", "Экзамен_2", "Экзамен_3"]
            demo_questions = [
                {
                    "question": "Что такое презумпция невиновности?",
                    "options": [("А", "Обвиняемый считается виновным, пока не докажет обратное"),
                                ("Б", "Обвиняемый считается невиновным, пока его вина не будет доказана в установленном порядке"),
                                ("В", "Судья всегда на стороне обвинения"),
                                ("Г", "Адвокат обязан доказывать невиновность")],
                    "correct": "Б"
                },
                {
                    "question": "Какое из перечисленных действий является процессуальным нарушением?",
                    "options": [("А", "Отказ в предоставлении адвоката при задержании"),
                                ("Б", "Проведение допроса без свидетелей"),
                                ("В", "Изъятие личных вещей без протокола"),
                                ("Г", "Все варианты верны")],
                    "correct": "Г"
                },
                {
                    "question": "В какой срок адвокат должен подать апелляцию после вынесения приговора?",
                    "options": [("А", "В течение 10 дней"),
                                ("Б", "В течение 1 месяца"),
                                ("В", "В течение 3 месяцев"),
                                ("Г", "Срок не ограничен")],
                    "correct": "А"
                }
            ]
            for topic in exam_topics:
                for variant in [1, 2, 3]:
                    for idx, q in enumerate(demo_questions, start=1):
                        correct_index = None
                        for i, (label, text) in enumerate(q["options"]):
                            if label == q["correct"]:
                                correct_index = i
                                break
                        if correct_index is None:
                            correct_index = 0
                        cur.execute(
                            "INSERT INTO test_questions (topic, variant, question_text, correct_option_index, order_num) VALUES (?, ?, ?, ?, ?)",
                            (topic, variant, q["question"], correct_index, idx)
                        )
                        qid = cur.lastrowid
                        for label, text in q["options"]:
                            cur.execute(
                                "INSERT INTO test_options (question_id, option_label, option_text) VALUES (?, ?, ?)",
                                (qid, label, text)
                            )
            conn.commit()
            logger.info("Добавлены демо-вопросы для экзаменационных тем (3 темы × 3 варианта × 3 вопроса).")
    except Exception as e:
        logger.error(f"Ошибка в init_global_materials: {e}")
    finally:
        conn.close()

def create_datacenter(peer_id, owner_id, request_msg_id=None):
    old_dc = get_datacenter_peer_id()
    if old_dc is not None and old_dc != peer_id:
        conn = get_db_connection(None)
        try:
            conn.execute("UPDATE audiences SET is_datacenter=0 WHERE peer_id=?", (old_dc,))
            conn.commit()
        finally:
            conn.close()
        logger.info(f"Старый датацентр {old_dc} стал аудиторией")
    init_global_materials()
    set_datacenter_peer_id(peer_id)
    conn = get_db_connection(None)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO audiences (peer_id, owner_id, confirmed, request_time, request_message_id, is_datacenter, last_activity) VALUES (?, ?, 1, ?, ?, 1, ?)",
            (peer_id, str(owner_id), int(time.time()), request_msg_id, int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()
    logger.info(f"✅ Датацентр создан: {peer_id}")

def create_audience(peer_id, owner_id, request_msg_id=None):
    dc = get_datacenter_peer_id()
    if dc is None:
        raise Exception("Нет активного датацентра. Сначала создайте датацентр.")
    get_db_connection(peer_id)
    copy_global_to_audience(peer_id)
    conn = get_db_connection(None)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO audiences (peer_id, owner_id, confirmed, request_time, request_message_id, is_datacenter, last_activity) VALUES (?, ?, 1, ?, ?, 0, ?)",
            (peer_id, str(owner_id), int(time.time()), request_msg_id, int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()
    logger.info(f"✅ Аудитория создана: {peer_id}")

def delete_audience(peer_id):
    if is_datacenter(peer_id):
        conn = get_db_connection(None)
        try:
            conn.execute("UPDATE audiences SET is_datacenter=0 WHERE peer_id=?", (peer_id,))
            conn.commit()
        finally:
            conn.close()
        dc = get_datacenter_peer_id()
        if dc == peer_id:
            set_datacenter_peer_id(None)
        logger.info(f"🗑 Датацентр {peer_id} стал обычной аудиторией (данные сохранены в глобальной БД).")
        return
    delete_audience_db(peer_id)
    conn = get_db_connection(None)
    try:
        conn.execute("DELETE FROM audiences WHERE peer_id=?", (peer_id,))
        conn.commit()
    finally:
        conn.close()
    logger.info(f"🗑 Аудитория {peer_id} удалена полностью.")

# ======================== ГЛОБАЛЬНЫЕ СЛОВАРИ ============================
menu_messages = {}
menu_state = {}
menu_state_locks = {}
active_tests = {}
test_timers = {}
notification_messages = {}

# -------------------- ЗАПРОС ПОДТВЕРЖДЕНИЯ --------------------

def request_audience_confirmation(peer_id):
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        label="✅ Создать аудиторию",
        color=VkKeyboardColor.POSITIVE,
        payload={"cmd": "confirm_audience"}
    )
    keyboard.add_callback_button(
        label="⭐ Создать датацентр",
        color=VkKeyboardColor.PRIMARY,
        payload={"cmd": "confirm_datacenter"}
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        label="📢 Назначить беседу оповещений",
        color=VkKeyboardColor.SECONDARY,
        payload={"cmd": "set_notification_chat"}
    )
    resp = send_message(peer_id,
                 "📢 Управление беседой:\n\n"
                 "• «Создать аудиторию» – обычная группа со своей копией базы (требуется наличие датацентра и прав).\n"
                 "• «Создать датацентр» – центральная база (доступно только владельцу или совладельцу).\n"
                 "• «Назначить беседу оповещений» – все уведомления будут приходить сюда (только владелец).",
                 keyboard=keyboard)
    if resp and resp.get('conversation_message_id'):
        menu_messages[peer_id] = resp['conversation_message_id']

# -------------------- ПРАВА ДОСТУПА --------------------

def is_owner(vk_id):
    return str(vk_id) == str(OWNER_ID)

def is_co_owner(vk_id):
    if is_owner(vk_id):
        return True
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM co_owners WHERE user_id=?", (str(vk_id),))
        return cur.fetchone() is not None
    finally:
        conn.close()

def is_full_access(vk_id):
    return is_owner(vk_id) or is_co_owner(vk_id)

def is_allowed(vk_id):
    if is_full_access(vk_id):
        return True
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM allowed_users WHERE user_id=?", (str(vk_id),))
        return cur.fetchone() is not None
    finally:
        conn.close()

def can_create_audience(vk_id):
    return is_full_access(vk_id) or is_allowed(vk_id)

def can_manage_materials(vk_id, peer_id):
    if is_full_access(vk_id):
        return True
    owner = get_audience_owner(peer_id)
    return owner == str(vk_id)

def add_allowed_user(vk_id, added_by):
    conn = get_db_connection(None)
    try:
        conn.execute("INSERT OR REPLACE INTO allowed_users (user_id, added_by, added_at, role) VALUES (?, ?, ?, 'admin')",
                     (str(vk_id), str(added_by), int(time.time())))
        conn.commit()
    finally:
        conn.close()

def remove_allowed_user(vk_id):
    conn = get_db_connection(None)
    try:
        conn.execute("DELETE FROM allowed_users WHERE user_id=?", (str(vk_id),))
        conn.commit()
    finally:
        conn.close()

def get_allowed_users():
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id, added_by, added_at FROM allowed_users ORDER BY added_at")
        return cur.fetchall()
    finally:
        conn.close()

def add_co_owner(user_id, added_by):
    conn = get_db_connection(None)
    try:
        conn.execute("INSERT OR REPLACE INTO co_owners (user_id, added_by, added_at) VALUES (?, ?, ?)",
                     (str(user_id), str(added_by), int(time.time())))
        conn.commit()
    finally:
        conn.close()

def remove_co_owner(user_id):
    conn = get_db_connection(None)
    try:
        conn.execute("DELETE FROM co_owners WHERE user_id=?", (str(user_id),))
        conn.commit()
    finally:
        conn.close()

def get_co_owners():
    conn = get_db_connection(None)
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id, added_by, added_at FROM co_owners ORDER BY added_at")
        return cur.fetchall()
    finally:
        conn.close()

# -------------------- ПРОВЕРКИ VK --------------------

def bot_is_admin_in_chat(peer_id):
    try:
        members = vk.messages.getConversationMembers(peer_id=peer_id)
        bot_id = -int(GROUP_ID)
        for item in members.get("items", []):
            if item.get("member_id") == bot_id:
                return item.get("is_admin", False)
        return False
    except Exception as e:
        print(f"⚠️ Ошибка проверки прав администратора в беседе {peer_id}: {e}")
        return False

def get_chat_name(peer_id):
    if peer_id < 2000000000:
        return None
    try:
        info = vk.messages.getConversationsById(peer_ids=[peer_id])
        items = info.get("items", [])
        if items:
            settings = items[0].get("chat_settings")
            if settings:
                return settings.get("title")
        return None
    except Exception as e:
        print(f"⚠️ Ошибка получения названия беседы {peer_id}: {e}")
        return None

# -------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ --------------------

def read_text_file(filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(script_dir, filename)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except:
        return None

def write_text_file(filename, text):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(script_dir, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(text)
        return True
    except:
        return False

def send_message(peer_id, text, attachment=None, keyboard=None, retries=2):
    if not isinstance(peer_id, int) or peer_id <= 0:
        logger.error(f"Некорректный peer_id: {peer_id}")
        return None
    for attempt in range(retries):
        try:
            params = {"peer_id": peer_id, "message": text, "random_id": random.getrandbits(31)}
            if attachment:
                params["attachment"] = attachment
            if keyboard:
                if hasattr(keyboard, 'get_keyboard'):
                    params["keyboard"] = keyboard.get_keyboard()
                else:
                    params["keyboard"] = keyboard
            response = vk.messages.send(**params)
            if isinstance(response, dict):
                conv_id = response.get('conversation_message_id')
                msg_id = response.get('response') or response.get('message_id')
                if msg_id is None and conv_id is not None:
                    msg_id = conv_id
                return {'message_id': msg_id, 'conversation_message_id': conv_id}
            else:
                return {'message_id': response, 'conversation_message_id': None}
        except Exception as e:
            logger.warning(f"Ошибка отправки (попытка {attempt+1}): {e}")
            if "[15]" in str(e):
                logger.error(f"Access denied, прекращаем попытки")
                return None
            if attempt == retries-1:
                logger.error(f"Не удалось отправить сообщение в {peer_id}: {e}")
                return None
            time.sleep(1)
    return None

def edit_message(peer_id, cmid, text, keyboard=None):
    if cmid is None:
        return False
    try:
        params = {"peer_id": peer_id, "message": text, "conversation_message_id": cmid}
        if keyboard:
            if hasattr(keyboard, 'get_keyboard'):
                params["keyboard"] = keyboard.get_keyboard()
            else:
                params["keyboard"] = keyboard
        vk.messages.edit(**params)
        return True
    except Exception as e:
        logger.error(f"Ошибка редактирования сообщения {cmid}: {e}")
        return False

def delete_message(peer_id, conversation_message_id, retries=2, force=False):
    if conversation_message_id is None:
        return False
    if not force and peer_id >= 2000000000 and not bot_is_admin_in_chat(peer_id):
        logger.warning(f"Бот не администратор в беседе {peer_id}, удаление пропущено")
        return False
    for attempt in range(retries):
        try:
            vk.messages.delete(
                peer_id=peer_id,
                conversation_message_ids=[conversation_message_id],
                delete_for_all=1
            )
            return True
        except Exception as e:
            logger.warning(f"Ошибка удаления сообщения {conversation_message_id} (попытка {attempt+1}): {e}")
            if "[15]" in str(e):
                return False
            if attempt == retries-1:
                return False
            time.sleep(0.5)
    return False

def send_long_message(peer_id, text, keyboard=None):
    MAX_LEN = 4000
    msg_ids = []
    if not text:
        resp = send_message(peer_id, "📭 Содержимое отсутствует.", keyboard=keyboard)
        if resp and isinstance(resp, dict) and resp.get('conversation_message_id'):
            msg_ids.append(resp['conversation_message_id'])
        return msg_ids
    if len(text) <= MAX_LEN:
        resp = send_message(peer_id, text, keyboard=keyboard)
        if resp and isinstance(resp, dict) and resp.get('conversation_message_id'):
            msg_ids.append(resp['conversation_message_id'])
        return msg_ids
    parts = []
    current = ""
    for line in text.splitlines(True):
        if len(current) + len(line) > MAX_LEN:
            if current:
                parts.append(current)
            current = line
        else:
            current += line
    if current:
        parts.append(current)
    for i, part in enumerate(parts):
        kb = keyboard if i == len(parts)-1 else None
        resp = send_message(peer_id, part, keyboard=kb)
        if resp and isinstance(resp, dict) and resp.get('conversation_message_id'):
            msg_ids.append(resp['conversation_message_id'])
    return msg_ids

def kick_from_chat(peer_id, user_id):
    try:
        vk.messages.removeChatUser(chat_id=peer_id - 2000000000, user_id=user_id)
    except Exception as e:
        print(f"⚠️ Ошибка удаления {user_id}: {e}")

def add_user_to_chat(peer_id, user_id):
    try:
        vk.messages.addChatUser(chat_id=peer_id - 2000000000, user_id=user_id)
        return True
    except Exception as e:
        print(f"⚠️ Ошибка добавления пользователя {user_id}: {e}")
        return False

def delete_message_later(peer_id, msg_id, delay=1):
    if msg_id is None:
        return
    def _delete():
        time.sleep(delay)
        delete_message(peer_id, msg_id)
    threading.Thread(target=_delete, daemon=True).start()

# ======================== ПРОВЕРКА ПРАВ ДЛЯ УПРАВЛЕНИЯ ТЕСТОМ ============================

def can_control_test(user_id, peer_id):
    if is_full_access(user_id):
        return True
    owner = get_audience_owner(peer_id)
    if owner and str(owner) == str(user_id):
        return True
    return False

# ======================== КЛАВИАТУРЫ ============================

HODAITSTVA_NAMES = {
    1: "Вызов эксперта",
    2: "Истребование",
    3: "Отвод",
    4: "Отложение",
    5: "Привлечение специалиста",
    6: "Приобщение"
}

def get_main_menu_keyboard(has_full_access=False, can_manage=False, is_datacenter=False):
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("📚 1 этап (ознакомление)", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("📖 2 этап (теория)", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("📝 3 этап (практика)", color=VkKeyboardColor.SECONDARY)
    keyboard.add_button("🎯 4 этап (экзаменационный)", color=VkKeyboardColor.SECONDARY)
    if can_manage:
        keyboard.add_line()
        keyboard.add_button("🛠 Управление материалами", color=VkKeyboardColor.PRIMARY)
        keyboard.add_button("👨‍🎓 Студент", color=VkKeyboardColor.PRIMARY)
    if is_datacenter:
        keyboard.add_line()
        keyboard.add_button("⭐ Датацентр", color=VkKeyboardColor.POSITIVE)
    keyboard.add_line()
    keyboard.add_button("🔒 Скрыть панель", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_stage2_theory_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    topics = ["Конституция", "ФКЗ О прокуратуре", "Уголовный кодекс", "Федеральное постановление", "Процессуальный кодекс"]
    for topic in topics:
        keyboard.add_button(topic, color=VkKeyboardColor.SECONDARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_obrasheniya_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("Уведомление о принятии", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("Ответ на обращение", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("Извещение по делу", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_stage2_variants_keyboard(topic):
    keyboard = VkKeyboard(one_time=False, inline=False)
    display_topic = topic.replace('_', ' ')
    for v in [1, 2, 3]:
        keyboard.add_button(f"{display_topic} вариант {v}", color=VkKeyboardColor.PRIMARY)
        if v % 2 == 0:
            keyboard.add_line()
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_practice_types_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("Суды", color=VkKeyboardColor.SECONDARY)
    keyboard.add_button("Прокуратура", color=VkKeyboardColor.SECONDARY)
    keyboard.add_button("Доклады", color=VkKeyboardColor.SECONDARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_practice_variants_keyboard(practice_type):
    keyboard = VkKeyboard(one_time=False, inline=False)
    if practice_type == "Суды":
        variants = {1: "Исковое заявление", 2: "Ходатайство прокурора", 3: "Прокурорское представление"}
    elif practice_type == "Прокуратура":
        variants = {
            1: "Обращения",
            2: "ПОСТАНОВЛЕНИЯ",
            3: "Запрос материалов",
            4: "Решение по делу ФБР",
            5: "Возбуждение УД"
        }
    elif practice_type == "Доклады":
        variants = {1: "Доклад"}
    else:
        variants = {}
    for v, label in variants.items():
        keyboard.add_button(label, color=VkKeyboardColor.PRIMARY)
        if v % 2 == 0 and v < len(variants):
            keyboard.add_line()
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_exam_menu_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("📄 Информация", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("❓ Тесты", color=VkKeyboardColor.SECONDARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_exam_topics_keyboard(peer_id):
    keyboard = VkKeyboard(one_time=False, inline=False)
    topics = ["Экзамен 1", "Экзамен 2", "Экзамен 3"]
    for display in topics:
        keyboard.add_button(display, color=VkKeyboardColor.SECONDARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_exam_variants_keyboard(topic):
    keyboard = VkKeyboard(one_time=False, inline=False)
    for v in [1, 2, 3]:
        keyboard.add_button(str(v), color=VkKeyboardColor.PRIMARY)
        if v % 2 == 0:
            keyboard.add_line()
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_empty_keyboard():
    return VkKeyboard.get_empty_keyboard()

def get_manage_main_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("📚 1 этап (ознакомление)", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("📖 2 этап (теория)", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("📝 3 этап (практика)", color=VkKeyboardColor.SECONDARY)
    keyboard.add_button("🎯 4 этап (экзаменационный)", color=VkKeyboardColor.SECONDARY)
    keyboard.add_line()
    keyboard.add_button("👋 Приветствие", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("⚙️ Настройки тестирования", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("🏛 Главное меню", color=VkKeyboardColor.PRIMARY)
    return keyboard.get_keyboard()

def get_manage_simple_action_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("➕ Изменить текст", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_manage_action_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("🔍 Посмотреть", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("➕ Добавить/Заменить", color=VkKeyboardColor.POSITIVE)
    keyboard.add_button("🗑 Удалить", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_buffer_keyboard(next_step=False):
    keyboard = VkKeyboard(one_time=False, inline=False)
    if next_step:
        keyboard.add_button("➡️ Далее", color=VkKeyboardColor.POSITIVE)
    else:
        keyboard.add_button("💾 Сохранить", color=VkKeyboardColor.POSITIVE)
    return keyboard.get_keyboard()

def get_creative_topics_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("➕ Добавить тему", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("✏️ Изменить форму доклада", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("🗑 Очистить все темы", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_creative_topic_action_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("✏️ Изменить шаблон", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("🗑 Удалить тему", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_test_question_keyboard(options, labels):
    keyboard = VkKeyboard(inline=True)
    for i, (label, text) in enumerate(zip(labels, options)):
        if i % 2 == 0 and i > 0:
            keyboard.add_line()
        keyboard.add_callback_button(label, color=VkKeyboardColor.PRIMARY, payload={"cmd": "test_answer", "index": i})
    keyboard.add_line()
    keyboard.add_callback_button("⏸ Остановить", color=VkKeyboardColor.NEGATIVE, payload={"cmd": "test_pause"})
    return keyboard.get_keyboard()

def get_test_pause_keyboard():
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button("▶️ Продолжить", color=VkKeyboardColor.POSITIVE, payload={"cmd": "test_resume"})
    keyboard.add_callback_button("⏹ Завершить", color=VkKeyboardColor.NEGATIVE, payload={"cmd": "test_end"})
    return keyboard.get_keyboard()

def get_test_start_keyboard():
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button("✅ Готов", color=VkKeyboardColor.POSITIVE, payload={"cmd": "test_ready"})
    keyboard.add_callback_button("❌ Отмена", color=VkKeyboardColor.NEGATIVE, payload={"cmd": "test_cancel"})
    return keyboard.get_keyboard()

def get_test_settings_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("⏱ Время на вопрос", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("❌ Порог ошибок", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_manage_test_questions_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("➕ Добавить вопрос", color=VkKeyboardColor.POSITIVE)
    keyboard.add_button("✏️ Редактировать вопрос", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("🗑 Удалить вопрос", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🗑 Удалить все вопросы", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_add_option_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("➕ Ещё вариант", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("✅ Готово", color=VkKeyboardColor.POSITIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_question_list_keyboard(questions):
    keyboard = VkKeyboard(one_time=False, inline=False)
    for i, q in enumerate(questions, 1):
        keyboard.add_button(str(i), color=VkKeyboardColor.SECONDARY)
        if i % 5 == 0:
            keyboard.add_line()
    if len(questions) % 5 != 0:
        keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()

def get_edit_question_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("✏️ Редактировать вопрос", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("✏️ Редактировать варианты", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("🗑 Удалить вопрос", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_edit_options_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("➕ Добавить вариант", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("🗑 Удалить вариант", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("✏️ Изменить вариант", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("✅ Готово", color=VkKeyboardColor.POSITIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_welcome_management_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("📝 Изменить текст", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("🔕 Отключить", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_button("🔊 Включить", color=VkKeyboardColor.POSITIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_student_management_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("➕ Добавить студента", color=VkKeyboardColor.POSITIVE)
    keyboard.add_button("🗑 Удалить студента", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🎓 Завершить обучение", color=VkKeyboardColor.PRIMARY)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

def get_student_result_keyboard():
    keyboard = VkKeyboard(one_time=False, inline=False)
    keyboard.add_button("✅ Успешно (1)", color=VkKeyboardColor.POSITIVE)
    keyboard.add_button("❌ Не прошёл (2)", color=VkKeyboardColor.NEGATIVE)
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY)
    return keyboard.get_keyboard()

# ===== КОНЕЦ ЧАСТИ 2 =====
# ===== НАЧАЛО ЧАСТИ 3 =====

# ======================== ОЧИСТКА ТЕКСТА ============================

def clean_text_from_mentions(text):
    cleaned = re.sub(r'\[[^\]]+\]', '', text)
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)
    return cleaned.strip()

def is_panel_command(text):
    clean = clean_text_from_mentions(text)
    if not clean:
        return False
    panel_texts = [
        "🔙 Назад", "📚 1 этап (ознакомление)", "📖 2 этап (теория)",
        "📝 3 этап (практика)", "🎯 4 этап (экзаменационный)",
        "🔒 Скрыть панель", "🛠 Управление материалами", "👨‍🎓 Студент",
        "➕ Добавить студента", "🗑 Удалить студента", "🎓 Завершить обучение",
        "✅ Успешно (1)", "❌ Не прошёл (2)",
        "Конституция", "ФКЗ О прокуратуре", "Уголовный кодекс",
        "Федеральное постановление", "Процессуальный кодекс",
        "Конституция вариант 1", "Конституция вариант 2", "Конституция вариант 3",
        "ФКЗ О прокуратуре вариант 1", "ФКЗ О прокуратуре вариант 2", "ФКЗ О прокуратуре вариант 3",
        "Уголовный кодекс вариант 1", "Уголовный кодекс вариант 2", "Уголовный кодекс вариант 3",
        "Федеральное постановление вариант 1", "Федеральное постановление вариант 2", "Федеральное постановление вариант 3",
        "Процессуальный кодекс вариант 1", "Процессуальный кодекс вариант 2", "Процессуальный кодекс вариант 3",
        "Суды", "Прокуратура", "Доклады",
        "📄 Информация", "❓ Тесты",
        "Экзамен 1", "Экзамен 2", "Экзамен 3",
        "Экзамен 1 вариант 1", "Экзамен 1 вариант 2", "Экзамен 1 вариант 3",
        "Экзамен 2 вариант 1", "Экзамен 2 вариант 2", "Экзамен 2 вариант 3",
        "Экзамен 3 вариант 1", "Экзамен 3 вариант 2", "Экзамен 3 вариант 3",
        "🏛 Главное меню", "✏️ Изменить форму доклада",
        "⚙️ Настройки тестирования", "⏱ Время на вопрос", "❌ Порог ошибок",
        "➕ Ещё вариант", "✅ Готово",
        "✏️ Редактировать вопрос", "✏️ Редактировать варианты",
        "🗑 Удалить вопрос", "➕ Добавить вариант", "🗑 Удалить вариант",
        "✏️ Изменить вариант", "📝 Изменить текст",
        "🔕 Отключить", "🔊 Включить", "👋 Приветствие",
        "Исковое заявление", "Ходатайство прокурора", "Прокурорское представление",
        "Обращение (принятие)", "Обращение (извещение)",
        "ПОСТАНОВЛЕНИЯ", "Запрос материалов", "Решение по делу ФБР", "Возбуждение УД",
        "Уведомление о принятии", "Ответ на обращение", "Извещение по делу",
        "Обращения", "Доклад"
    ]
    return clean in panel_texts

def send_menu(peer_id, user_id, text, keyboard):
    send_message(peer_id, text, keyboard=keyboard)

# ======================== ГЛОБАЛЬНОЕ СОСТОЯНИЕ ============================

def get_menu_state_lock(key):
    if key not in menu_state_locks:
        menu_state_locks[key] = threading.Lock()
    return menu_state_locks[key]

def safe_menu_state_set(key, value):
    with get_menu_state_lock(key):
        menu_state[key] = value

def safe_menu_state_get(key):
    with get_menu_state_lock(key):
        return menu_state.get(key)

def safe_menu_state_pop(key):
    with get_menu_state_lock(key):
        return menu_state.pop(key, None)

def delete_notification_message(peer_id, cmid):
    if peer_id in notification_messages and notification_messages[peer_id].get('cmid') == cmid:
        delete_message(peer_id, cmid, force=True)
        notification_messages.pop(peer_id, None)

def schedule_daily_restart():
    now = datetime.datetime.now()
    target = now.replace(hour=5, minute=0, second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    delay = (target - now).total_seconds()
    logger.info(f"Запланирован перезапуск бота в {target.strftime('%Y-%m-%d %H:%M')} МСК (через {int(delay//3600)}ч {int((delay%3600)//60)}м)")

    def restart_bot():
        logger.info("🔄 Выполняется плановый перезапуск бота в 5:00 МСК")
        os.execv(sys.executable, [sys.executable] + sys.argv)

    timer = threading.Timer(delay, restart_bot)
    timer.daemon = True
    timer.start()
    return timer

# ======================== ОБРАБОТЧИК ГЛАВНОГО МЕНЮ ============================

def handle_main_menu(text, peer_id, sender_id, conversation_message_id, can_manage=False):
    clean_text = clean_text_from_mentions(text)
    key = (peer_id, sender_id)

    state_data = safe_menu_state_get(key)
    if state_data and isinstance(state_data, dict) and state_data.get('mode') == 'manage':
        return False

    if not state_data or not isinstance(state_data, dict):
        state_data = {'mode': 'main', 'state': 'main'}
        safe_menu_state_set(key, state_data)

    state = state_data.get('state', 'main')
    is_dc = is_datacenter(peer_id)
    has_full = is_full_access(sender_id)

    def delete_original():
        if conversation_message_id:
            delete_message(peer_id, conversation_message_id)

    if clean_text == "🔙 Назад":
        delete_original()
        if state.startswith('stage2_variants_'):
            state_data['state'] = 'stage2_theory'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тему теории:", get_stage2_theory_keyboard())
        elif state == 'stage2_theory':
            state_data['state'] = 'main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(has_full, can_manage, is_dc))
        elif state.startswith('practice_variants_'):
            state_data['state'] = 'practice_types'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тип практики:", get_practice_types_keyboard())
        elif state == 'practice_types':
            state_data['state'] = 'main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(has_full, can_manage, is_dc))
        elif state.startswith('exam_variants_'):
            state_data['state'] = 'exam_topics'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тему экзамена:", get_exam_topics_keyboard(peer_id))
        elif state == 'exam_topics':
            state_data['state'] = 'exam_menu'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🎯 Экзаменационный этап:", get_exam_menu_keyboard())
        elif state == 'exam_menu':
            state_data['state'] = 'main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(has_full, can_manage, is_dc))
        elif state == 'student_menu':
            state_data['state'] = 'main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(has_full, can_manage, is_dc))
        else:
            state_data['state'] = 'main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(has_full, can_manage, is_dc))
        return True

    if clean_text == "🔒 Скрыть панель":
        delete_original()
        safe_menu_state_pop(key)
        send_message(peer_id, "🔒 Панель скрыта.", keyboard=get_empty_keyboard())
        return True

    # ======= ОБРАБОТКА ЭТАПОВ =======
    if clean_text in ["📚 1 этап (ознакомление)", "📖 2 этап (теория)", "📝 3 этап (практика)", "🎯 4 этап (экзаменационный)"]:
        delete_original()
        if clean_text == "📚 1 этап (ознакомление)":
            st1_text = get_setting("st1_text", "📝 Текст ознакомления не задан.", peer_id)
            send_long_message(peer_id, st1_text)
            students = get_audience_students(peer_id)
            if students:
                keyboard = VkKeyboard(inline=True)
                keyboard.add_callback_button("📨 Отправить уведомление", color=VkKeyboardColor.POSITIVE, payload={"cmd": "notify_stage", "stage": 1})
                keyboard.add_callback_button("❌ Пропустить", color=VkKeyboardColor.NEGATIVE, payload={"cmd": "skip_notification", "stage": 1})
                resp = send_message(peer_id, "📤 В аудитории есть студенты. Отправить уведомление о начале этапа 1 в коллегию?", keyboard=keyboard)
                if resp and resp.get('conversation_message_id'):
                    cmid = resp['conversation_message_id']
                    timer = threading.Timer(60.0, lambda: delete_notification_message(peer_id, cmid))
                    timer.daemon = True
                    timer.start()
                    notification_messages[peer_id] = {'cmid': cmid, 'timer': timer}
            return True
        elif clean_text == "📖 2 этап (теория)":
            state_data['state'] = 'stage2_theory'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тему теории:", get_stage2_theory_keyboard())
            return True
        elif clean_text == "📝 3 этап (практика)":
            state_data['state'] = 'practice_types'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тип практики:", get_practice_types_keyboard())
            return True
        elif clean_text == "🎯 4 этап (экзаменационный)":
            state_data['state'] = 'exam_menu'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🎯 Экзаменационный этап:", get_exam_menu_keyboard())
            return True

    if clean_text == "🛠 Управление материалами" and can_manage:
        delete_original()
        state_data = {
            'mode': 'manage',
            'state': 'manage_main',
            'buffer': ''
        }
        safe_menu_state_set(key, state_data)
        send_menu(peer_id, sender_id, "🛠 Панель управления материалами:", get_manage_main_keyboard())
        return True

    # ====== УПРАВЛЕНИЕ СТУДЕНТОМ ======
    if clean_text == "👨‍🎓 Студент" and can_manage:
        delete_original()
        state_data['state'] = 'student_menu'
        safe_menu_state_set(key, state_data)
        show_student_menu(peer_id, sender_id, key)
        return True

    if state == 'student_menu':
        if clean_text == "➕ Добавить студента":
            state_data['state'] = 'student_add_wait'
            safe_menu_state_set(key, state_data)
            send_message(peer_id, "👤 Отправьте @упоминание пользователя, которого хотите добавить как студента.\nИли нажмите «🔙 Назад» для отмены.")
            return True
        elif clean_text == "🗑 Удалить студента":
            student = get_audience_students(peer_id)
            if not student:
                send_message(peer_id, "❌ В аудитории нет студентов для удаления.")
                return True
            user_id = student[0]['user_id']
            remove_student(peer_id, user_id)
            send_message(peer_id, f"✅ Студент {get_user_mention(user_id, peer_id)} удалён.")
            show_student_menu(peer_id, sender_id, key)
            return True
        elif clean_text == "🎓 Завершить обучение":
            student = get_audience_students(peer_id)
            if not student:
                send_message(peer_id, "❌ В аудитории нет студентов для завершения.")
                return True
            state_data['state'] = 'student_result_wait'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите результат обучения:", get_student_result_keyboard())
            return True
        else:
            return True

    if state == 'student_add_wait':
        if clean_text == "🔙 Назад":
            state_data['state'] = 'student_menu'
            safe_menu_state_set(key, state_data)
            show_student_menu(peer_id, sender_id, key)
            return True
        match = re.search(r'\[id(\d+)\|', text)
        if not match:
            send_message(peer_id, "⚠️ Не удалось распознать пользователя. Используйте @упоминание.")
            return True
        user_id = match.group(1)
        existing = get_student(peer_id, user_id)
        if existing:
            send_message(peer_id, f"⚠️ Пользователь {get_user_mention(user_id, peer_id)} уже является студентом.")
            return True
        current = get_audience_students(peer_id)
        if current:
            send_message(peer_id, f"⚠️ В аудитории уже есть студент: {get_user_mention(current[0]['user_id'], peer_id)}. Сначала удалите его.")
            return True
        add_student(peer_id, user_id, sender_id)
        send_message(peer_id, f"✅ Студент {get_user_mention(user_id, peer_id)} добавлен.")
        state_data['state'] = 'student_menu'
        safe_menu_state_set(key, state_data)
        show_student_menu(peer_id, sender_id, key)
        return True

    if state == 'student_result_wait':
        if clean_text == "🔙 Назад":
            state_data['state'] = 'student_menu'
            safe_menu_state_set(key, state_data)
            show_student_menu(peer_id, sender_id, key)
            return True
        result = None
        if clean_text == "✅ Успешно (1)":
            result = "1"
        elif clean_text == "❌ Не прошёл (2)":
            result = "2"
        else:
            send_message(peer_id, "⚠️ Выберите один из вариантов на клавиатуре.")
            return True

        student = get_audience_students(peer_id)
        if not student:
            send_message(peer_id, "❌ Студент не найден.")
            state_data['state'] = 'student_menu'
            safe_menu_state_set(key, state_data)
            show_student_menu(peer_id, sender_id, key)
            return True
        user_id = student[0]['user_id']
        chat_name = get_chat_name(peer_id) or f"Беседа {peer_id}"
        owner_id = get_audience_owner(peer_id)
        owner_mention = get_user_mention(owner_id, peer_id) if owner_id else "Неизвестно"
        student_mention = get_user_mention(user_id, peer_id)
        result_text = "✅ прошёл" if result == "1" else "❌ не прошёл"
        notif_msg = f"📢 Аудитория: {chat_name}\nРектор: {owner_mention}\nСтудент: {student_mention} {result_text} университет."
        send_notification(notif_msg)

        finish_student(peer_id, user_id, result)
        remove_student(peer_id, user_id)

        if result == "1":
            text = read_text_file("graduation.txt")
            if text is None:
                text = "🎉 Поздравляем! Вы успешно окончили университет!"
            send_message(peer_id, text)
        else:
            send_message(peer_id, "❌ Студент не прошёл университет.")
        kick_from_chat(peer_id, int(user_id))

        state_data['state'] = 'student_menu'
        safe_menu_state_set(key, state_data)
        show_student_menu(peer_id, sender_id, key)
        return True

    # ===== ЭТАП 2 (ТЕОРИЯ) =====
    if state == 'stage2_theory':
        topics_map = {
            "Конституция": "Конституция",
            "ФКЗ О прокуратуре": "ФКЗ_О_прокуратуре",
            "Уголовный кодекс": "Уголовный_кодекс",
            "Федеральное постановление": "Федеральное_постановление",
            "Процессуальный кодекс": "Процессуальный_кодекс"
        }
        if clean_text in topics_map:
            delete_original()
            topic = topics_map[clean_text]
            state_data['state'] = f'stage2_variants_{topic}'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {clean_text}:", get_stage2_variants_keyboard(topic))
            return True

    if state.startswith('stage2_variants_'):
        topic = state.replace('stage2_variants_', '')
        display_topic = topic.replace('_', ' ')
        for v in [1, 2, 3]:
            if clean_text == f"{display_topic} вариант {v}":
                delete_original()
                has_one = has_one_by_one_test(topic, v, peer_id)
                if has_one:
                    start_one_by_one_test(peer_id, topic, v, sender_id)
                else:
                    send_message(peer_id, f"❓ Для {display_topic} вариант {v} нет вопросов. Добавьте их в управлении материалами.")
                return True

    # ===== ЭТАП 3 (ПРАКТИКА) =====
    if state == 'practice_types':
        if clean_text == "Суды":
            delete_original()
            state_data['state'] = 'practice_variants_Суды'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите вариант для Суды:", get_practice_variants_keyboard("Суды"))
            return True
        elif clean_text == "Прокуратура":
            delete_original()
            state_data['state'] = 'practice_variants_Прокуратура'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите вариант для Прокуратуры:", get_practice_variants_keyboard("Прокуратура"))
            return True
        elif clean_text == "Доклады":
            delete_original()
            topics = get_all_topics(peer_id)
            if not topics:
                send_message(peer_id, "📭 Нет тем для докладов. Добавьте их в управлении материалами.")
                return True
            import random
            topic = random.choice(topics)
            template = get_report_template(peer_id)
            if not template or template.strip() == '':
                template = "(форма доклада не задана)"
            logger.info(f"Доклад: тема='{topic['text']}', шаблон='{template}'")
            output = f"📎 Тема: {topic['text']}\n\n{template}"
            keyboard = VkKeyboard(one_time=False, inline=False)
            keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
            keyboard.add_button("🎲 Ещё доклад", color=VkKeyboardColor.PRIMARY)
            state_data['state'] = 'practice_topics'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, output, keyboard.get_keyboard())
            return True

    if state.startswith('practice_variants_'):
        practice_type = state.replace('practice_variants_', '')
        if practice_type == "Суды":
            variants = {1: "Исковое заявление", 2: "Ходатайство прокурора", 3: "Прокурорское представление"}
        elif practice_type == "Прокуратура":
            variants = {
                1: "Обращения",
                2: "ПОСТАНОВЛЕНИЯ",
                3: "Запрос материалов",
                4: "Решение по делу ФБР",
                5: "Возбуждение УД"
            }
        else:
            variants = {}

        if practice_type == "Прокуратура" and clean_text == "Обращения":
            delete_original()
            state_data['state'] = 'practice_obrasheniya'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тип обращения:", get_obrasheniya_keyboard())
            return True

        for v, label in variants.items():
            if clean_text == label:
                delete_original()
                row = get_creative_text(practice_type, v, peer_id)
                if row and row['task_text']:
                    send_long_message(peer_id, f"📎 {label}\n\n{row['task_text']}")
                else:
                    send_message(peer_id, f"📂 Документ «{label}» не найден или пуст.")
                return True

    if state == 'practice_topics':
        if clean_text == "🔙 Назад":
            delete_original()
            state_data['state'] = 'practice_types'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тип практики:", get_practice_types_keyboard())
            return True
        elif clean_text == "🎲 Ещё доклад":
            delete_original()
            topics = get_all_topics(peer_id)
            if not topics:
                send_message(peer_id, "📭 Тем больше нет.")
                state_data['state'] = 'practice_types'
                safe_menu_state_set(key, state_data)
                send_menu(peer_id, sender_id, "Выберите тип практики:", get_practice_types_keyboard())
                return True
            import random
            topic = random.choice(topics)
            template = get_report_template(peer_id)
            if not template or template.strip() == '':
                template = "(форма доклада не задана)"
            logger.info(f"Ещё доклад: тема='{topic['text']}', шаблон='{template}'")
            output = f"📎 Тема: {topic['text']}\n\n{template}"
            keyboard = VkKeyboard(one_time=False, inline=False)
            keyboard.add_button("🔙 Назад", color=VkKeyboardColor.NEGATIVE)
            keyboard.add_button("🎲 Ещё доклад", color=VkKeyboardColor.PRIMARY)
            send_menu(peer_id, sender_id, output, keyboard.get_keyboard())
            return True

    if state == 'practice_obrasheniya':
        if clean_text in ["Уведомление о принятии", "Ответ на обращение", "Извещение по делу"]:
            delete_original()
            variant_map = {"Уведомление о принятии": 1, "Ответ на обращение": 2, "Извещение по делу": 3}
            v = variant_map.get(clean_text)
            if v:
                row = get_creative_text_fallback("Обращения", v, peer_id)
                if row and row['task_text']:
                    send_long_message(peer_id, f"📎 {clean_text}\n\n{row['task_text']}")
                else:
                    send_message(peer_id, f"📂 Шаблон «{clean_text}» не найден или пуст.")
            return True
        elif clean_text == "🔙 Назад":
            delete_original()
            state_data['state'] = 'practice_variants_Прокуратура'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тип документа в прокуратуре:", get_practice_variants_keyboard("Прокуратура"))
            return True

    # ===== ЭТАП 4 (ЭКЗАМЕН) =====
    if state == 'exam_menu':
        if clean_text == "📄 Информация":
            delete_original()
            info_text = get_setting("exam_info_text", "📝 Информация для экзамена не задана.", peer_id)
            send_long_message(peer_id, info_text)
            return True
        elif clean_text == "❓ Тесты":
            delete_original()
            state_data['state'] = 'exam_topics'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тему экзамена:", get_exam_topics_keyboard(peer_id))
            return True

    if state == 'exam_topics':
        exam_topics = {
            "Экзамен 1": "Экзамен_1",
            "Экзамен 2": "Экзамен_2",
            "Экзамен 3": "Экзамен_3"
        }
        if clean_text in exam_topics:
            topic = exam_topics[clean_text]
            state_data['state'] = f'exam_variants_{topic}'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {clean_text}:", get_exam_variants_keyboard(topic))
            return True

    if state.startswith('exam_variants_'):
        topic = state.replace('exam_variants_', '')
        if clean_text.isdigit():
            v = int(clean_text)
            if 1 <= v <= 3:
                delete_original()
                has_one = has_one_by_one_test(topic, v, peer_id)
                if has_one:
                    start_one_by_one_test(peer_id, topic, v, sender_id)
                else:
                    send_message(peer_id, f"❓ Для темы {topic.replace('_', ' ')} вариант {v} нет вопросов. Добавьте их в управлении материалами.")
                return True

    return False

# ===== МЕНЮ СТУДЕНТА =====
def show_student_menu(peer_id, sender_id, key):
    students = get_audience_students(peer_id)
    if students:
        student_id = students[0]['user_id']
        mention = get_user_mention(student_id, peer_id)
        added_by = get_user_mention(students[0]['added_by'], peer_id)
        added_at = time.strftime("%d.%m.%Y %H:%M", time.localtime(students[0]['added_at']))
        info = f"👨‍🎓 Текущий студент: {mention}\nДобавлен: {added_by} {added_at}\n"
    else:
        info = "👨‍🎓 В аудитории нет студентов.\n"
    msg = info + "\nВыберите действие:"
    send_menu(peer_id, sender_id, msg, get_student_management_keyboard())

# ======================== ТЕСТИРОВАНИЕ ============================

def start_one_by_one_test(peer_id, topic, variant, sender_id):
    if peer_id in active_tests:
        send_message(peer_id, "⏳ В этой беседе уже запущен тест. Дождитесь его завершения.")
        return

    questions = get_test_questions(topic, variant, peer_id)
    if not questions:
        send_message(peer_id, "❓ Нет вопросов для этого варианта в режиме по одному.")
        return

    questions_with_options = []
    for q in questions:
        options = get_test_options(q['id'], peer_id)
        questions_with_options.append({
            'id': q['id'],
            'question_text': q['question_text'],
            'correct_option_index': q['correct_option_index'],
            'order_num': q['order_num'],
            'options': options
        })

    start_text = f"📝 Тест по теме: {topic} (вариант {variant})\n\nНажмите «Готов», чтобы начать, или «Отмена», чтобы отменить."
    keyboard = get_test_start_keyboard()
    send_message(peer_id, start_text, keyboard=keyboard)

    active_tests[peer_id] = {
        'topic': topic,
        'variant': variant,
        'questions': questions_with_options,
        'current_index': 0,
        'errors': 0,
        'results': [],
        'answers': [],
        'total': len(questions_with_options),
        'finished': False,
        'timer': None,
        'start_timer': None,
        'cmid': None,
        'started': False,
        'paused': False,
        'awaiting_answer': False,
        'initiator': sender_id
    }

def begin_test(peer_id, cmid):
    test = active_tests.get(peer_id)
    if not test or test.get('started', False):
        return
    test['cmid'] = cmid
    test['started'] = True

    edit_message(peer_id, cmid, "📝 Тест начат! Подождите 5 секунд...", keyboard=get_empty_keyboard())

    def start_questions():
        if test.get('finished', False):
            return
        send_next_question(peer_id)

    timer = threading.Timer(5.0, start_questions)
    timer.daemon = True
    timer.start()
    test['start_timer'] = timer

def cancel_test(peer_id, cmid):
    test = active_tests.pop(peer_id, None)
    if not test:
        return
    if test.get('start_timer'):
        test['start_timer'].cancel()
    if cmid:
        edit_message(peer_id, cmid, "❌ Тест отменён.", keyboard=get_empty_keyboard())

def send_next_question(peer_id):
    test = active_tests.get(peer_id)
    if not test or test.get('finished', False):
        return
    if not test.get('started', False):
        return
    if test.get('paused', False):
        return
    idx = test['current_index']
    questions = test['questions']
    if idx >= len(questions):
        finish_test(peer_id, success=True)
        return
    q = questions[idx]
    options = q['options']
    if not options:
        test['results'].append(False)
        test['answers'].append({
            'question_text': q['question_text'],
            'chosen_text': 'Нет вариантов',
            'correct_text': 'Нет вариантов',
            'correct': False
        })
        test['errors'] += 1
        test['current_index'] += 1
        if not check_fail(peer_id):
            send_next_question(peer_id)
        return
    question_text = f"❓ {idx+1}. {q['question_text']}"
    option_labels = [opt['option_label'] for opt in options]
    option_texts = [opt['option_text'] for opt in options]
    for label, text in zip(option_labels, option_texts):
        question_text += f"\n{label}). {text}"
    keyboard = get_test_question_keyboard(option_texts, option_labels)
    cmid = test.get('cmid')
    if cmid:
        ok = edit_message(peer_id, cmid, question_text, keyboard=keyboard)
        if not ok:
            resp = send_message(peer_id, question_text, keyboard=keyboard)
            if resp and resp.get('conversation_message_id'):
                test['cmid'] = resp['conversation_message_id']
    else:
        resp = send_message(peer_id, question_text, keyboard=keyboard)
        if resp and resp.get('conversation_message_id'):
            test['cmid'] = resp['conversation_message_id']
    test['awaiting_answer'] = True
    time_limit = get_test_time_limit(peer_id)
    if test.get('timer'):
        test['timer'].cancel()
    timer = threading.Timer(time_limit, on_test_timeout, args=[peer_id])
    timer.daemon = True
    timer.start()
    test['timer'] = timer

def pause_test(peer_id, cmid):
    test = active_tests.get(peer_id)
    if not test or test.get('finished', False):
        return
    if test.get('timer'):
        test['timer'].cancel()
        test['timer'] = None
    test['paused'] = True
    test['awaiting_answer'] = False
    text = "⏸ Тест приостановлен. Выберите действие:"
    keyboard = get_test_pause_keyboard()
    if cmid:
        edit_message(peer_id, cmid, text, keyboard=keyboard)
    else:
        send_message(peer_id, text, keyboard=keyboard)

def resume_test(peer_id, cmid):
    test = active_tests.get(peer_id)
    if not test or test.get('finished', False):
        return
    if not test.get('paused', False):
        return
    test['paused'] = False
    send_next_question(peer_id)

def end_test_early(peer_id, cmid):
    test = active_tests.get(peer_id)
    if not test or test.get('finished', False):
        return
    if test.get('timer'):
        test['timer'].cancel()
        test['timer'] = None
    finish_test(peer_id, success=False, reason='user_cancelled')

def on_test_timeout(peer_id):
    test = active_tests.get(peer_id)
    if not test or test.get('finished', False):
        return
    if test['current_index'] >= len(test['questions']):
        return
    if not test.get('awaiting_answer', False):
        return
    test['awaiting_answer'] = False
    q = test['questions'][test['current_index']]
    test['results'].append(False)
    test['answers'].append({
        'question_text': q['question_text'],
        'chosen_text': 'Время вышло',
        'correct_text': q['options'][q['correct_option_index']]['option_text'] if q['options'] else 'Нет вариантов',
        'correct': False
    })
    test['errors'] += 1
    test['current_index'] += 1
    if check_fail(peer_id):
        return
    cmid = test.get('cmid')
    if cmid:
        edit_message(peer_id, cmid, "⏰ Время вышло! Засчитано как ошибка.", keyboard=get_empty_keyboard())
        def show_next():
            time.sleep(1.5)
            if test.get('finished', False):
                return
            if cmid:
                edit_message(peer_id, cmid, "⏳ Следующий вопрос...", keyboard=get_empty_keyboard())
            time.sleep(1.0)
            if test.get('finished', False):
                return
            send_next_question(peer_id)
        threading.Thread(target=show_next, daemon=True).start()
    else:
        send_next_question(peer_id)

def check_fail(peer_id):
    test = active_tests.get(peer_id)
    if not test:
        return True
    threshold = get_test_fail_threshold(peer_id)
    if test['errors'] >= threshold:
        finish_test(peer_id, success=False)
        return True
    return False

def finish_test(peer_id, success, reason=None):
    test = active_tests.pop(peer_id, None)
    if not test:
        return
    if test.get('start_timer'):
        test['start_timer'].cancel()
    if test.get('timer'):
        test['timer'].cancel()
    total = test['total']
    results = test['results']
    correct = sum(results)
    errors = test['errors']
    report_lines = []
    for i, res in enumerate(results, 1):
        report_lines.append(f"{i}. {'+' if res else '-'}")
    report = "\n".join(report_lines)
    if reason == 'user_cancelled':
        msg = f"⏹ Тест завершён досрочно.\nПравильных: {correct}/{total}\nОшибок: {errors}\n\n{report}"
    elif success:
        msg = f"✅ Тест пройден!\nПравильных: {correct}/{total}\nОшибок: {errors}\n\n{report}"
    else:
        msg = f"❌ Тест провален! Превышен порог ошибок ({get_test_fail_threshold(peer_id)}).\nПравильных: {correct}/{total}\nОшибок: {errors}\n\n{report}"
    cmid = test.get('cmid')
    if cmid:
        edit_message(peer_id, cmid, msg, keyboard=get_empty_keyboard())
    else:
        send_message(peer_id, msg, keyboard=get_empty_keyboard())
    initiator = test.get('initiator')
    if initiator:
        key = (peer_id, initiator)
        safe_menu_state_set(key, {'mode': 'main', 'state': 'main'})

    datacenter = get_datacenter_peer_id()
    if datacenter and datacenter != peer_id:
        audience_name = get_chat_name(peer_id) or f"Беседа {peer_id}"
        students = get_audience_students(peer_id)
        student_info = ""
        if students:
            student_id = students[0]['user_id']
            student_mention = get_user_mention(student_id, peer_id)
            student_info = f"Студент: {student_mention}\n"
        header = f"📝 ДЕТАЛЬНЫЙ ОТЧЁТ по тесту от аудитории: {audience_name}\n{student_info}Тема: {test['topic']} (вариант {test['variant']})\n\n"
        detail_lines = []
        for i, ans in enumerate(test.get('answers', []), 1):
            status = "✅" if ans.get('correct', False) else "❌"
            question = ans.get('question_text', 'Вопрос')
            chosen = ans.get('chosen_text', 'Нет ответа')
            correct_ans = ans.get('correct_text', 'Нет правильного')
            detail_lines.append(f"{i}. {question}\n   Выбран: {chosen}\n   Правильный: {correct_ans}\n   {status}\n")
        if not detail_lines:
            detail_lines.append("Нет данных по ответам.")
        detail_report = header + "\n".join(detail_lines)
        send_long_message(int(datacenter), detail_report)

def handle_test_answer_callback(event):
    peer_id = event.object.peer_id
    test = active_tests.get(peer_id)
    if not test or test.get('finished', False):
        return
    if not test.get('awaiting_answer', False):
        return
    test['awaiting_answer'] = False
    if test.get('timer'):
        test['timer'].cancel()
        test['timer'] = None
    payload = event.object.payload
    if not payload or 'index' not in payload:
        return
    chosen_index = payload['index']
    q = test['questions'][test['current_index']]
    options = q.get('options', [])
    correct_index = q.get('correct_option_index', 0)
    correct_text = options[correct_index]['option_text'] if options else 'Нет вариантов'
    chosen_text = options[chosen_index]['option_text'] if options and chosen_index < len(options) else 'Неизвестно'
    correct = (chosen_index == correct_index)
    test['results'].append(correct)
    test['answers'].append({
        'question_text': q['question_text'],
        'chosen_text': chosen_text,
        'correct_text': correct_text,
        'correct': correct
    })
    if not correct:
        test['errors'] += 1
    test['current_index'] += 1
    if check_fail(peer_id):
        return
    cmid = test.get('cmid')
    if cmid:
        prev_text = f'Был дан ответ: "{chosen_text}"\n⏳ Следующий вопрос...'
        edit_message(peer_id, cmid, prev_text, keyboard=get_empty_keyboard())
        def show_next():
            time.sleep(1.5)
            if test.get('finished', False):
                return
            send_next_question(peer_id)
        threading.Thread(target=show_next, daemon=True).start()
    else:
        send_next_question(peer_id)

# ===== КОНЕЦ ЧАСТИ 3 =====
# ===== НАЧАЛО ЧАСТИ 4 =====

# ======================== ОБРАБОТЧИК КОМАНД ============================

def handle_command(text, peer_id, sender_id):
    if not text.startswith('/'):
        return
    parts = text.split()
    cmd = parts[0].lower()
    args = parts[1:] if len(parts) > 1 else []

    if peer_id >= 2000000000 and not is_audience_confirmed(peer_id):
        if cmd not in ['/init', '/help', '/setnotifchat']:
            send_message(peer_id, "❌ Беседа не активирована. Используйте /init для активации.")
            return

    owner_only_commands = ["/addcoowner", "/removecoowner", "/listcoowners", "/listaudiences", "/deleteaudience", "/settext", "/settime", "/setthreshold", "/setnotifchat"]
    if cmd in owner_only_commands and not is_owner(sender_id):
        send_message(peer_id, "❌ Эта команда доступна только владельцу.")
        return

    if cmd == "/init":
        if peer_id < 2000000000:
            send_message(peer_id, "❌ /init работает только в беседах.")
            return
        request_audience_confirmation(peer_id)
        return

    if not is_owner(sender_id) and not is_allowed(sender_id):
        send_message(peer_id, "❌ У вас нет прав для использования бота.")
        return

    if cmd == "/nick":
        if len(args) < 1:
            send_message(peer_id, "⚠️ Использование: /nick [@user] <ник>\nЕсли @user указан, устанавливает ник ему (только для владельца/совладельца или владельца аудитории), иначе – себе.")
            return

        mention = None
        nickname_parts = []
        for i, arg in enumerate(args):
            if re.search(r'\[id\d+\|', arg) or arg.startswith('@'):
                mention = arg
                nickname_parts = args[i+1:]
                break

        if mention:
            if not (is_full_access(sender_id) or can_manage_materials(sender_id, peer_id)):
                send_message(peer_id, "❌ Установка ника другому пользователю доступна только владельцу/совладельцу бота или владельцу аудитории.")
                return

            user_id = None
            match = re.search(r'\[id(\d+)\|', mention)
            if match:
                user_id = match.group(1)
            else:
                name = mention[1:].lower()
                try:
                    members = vk.messages.getConversationMembers(peer_id=peer_id)
                    for item in members.get('items', []):
                        member_id = item.get('member_id')
                        if member_id and member_id > 0:
                            try:
                                user_info = vk.users.get(user_ids=member_id)[0]
                                full_name = f"{user_info['first_name']} {user_info['last_name']}".lower()
                                screen_name = user_info.get('screen_name', '').lower()
                                if name in full_name or name == screen_name:
                                    user_id = str(member_id)
                                    break
                            except:
                                continue
                except Exception as e:
                    logger.error(f"Ошибка поиска участников: {e}")

            if not user_id:
                send_message(peer_id, "⚠️ Не удалось распознать пользователя. Используйте @упоминание из списка (кликните по имени) или проверьте имя.")
                return

            if not nickname_parts:
                send_message(peer_id, "⚠️ Укажите ник после @упоминания.")
                return

            nickname = ' '.join(nickname_parts)
            set_user_nickname(user_id, nickname, peer_id, sender_id)
            send_message(peer_id, f"✅ Пользователю {get_user_mention(user_id, peer_id)} установлен ник: {nickname}")
        else:
            nickname = ' '.join(args)
            set_user_nickname(sender_id, nickname, peer_id, sender_id)
            send_message(peer_id, f"✅ Ваш ник в этой аудитории установлен: {nickname}")
        return

    if cmd == "/setnotifchat":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Команда доступна только владельцу бота.")
            return
        if peer_id < 2000000000:
            send_message(peer_id, "❌ Команда работает только в беседах.")
            return
        set_notification_chat(peer_id)
        send_message(peer_id, "✅ Эта беседа назначена как беседа оповещений (коллегия).")
        return

    if cmd == "/menu":
        if not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на управление этой аудиторией.")
            return
        key = (peer_id, sender_id)
        if key in menu_state:
            safe_menu_state_pop(key)
        is_dc = is_datacenter(peer_id)
        can_manage = can_manage_materials(sender_id, peer_id)
        state_data = {'mode': 'main', 'state': 'main'}
        safe_menu_state_set(key, state_data)
        send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(is_full_access(sender_id), can_manage, is_dc))
        return

    if cmd == "/panel":
        if not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на управление этой аудиторией.")
            return
        key = (peer_id, sender_id)
        if key in menu_state:
            safe_menu_state_pop(key)
        is_dc = is_datacenter(peer_id)
        can_manage = can_manage_materials(sender_id, peer_id)
        state_data = {'mode': 'main', 'state': 'main'}
        safe_menu_state_set(key, state_data)
        send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(is_full_access(sender_id), can_manage, is_dc))
        return

    if cmd == "/manage":
        if not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на управление материалами в этой аудитории.")
            return
        key = (peer_id, sender_id)
        state_data = {
            'mode': 'manage',
            'state': 'manage_main',
            'buffer': ''
        }
        safe_menu_state_set(key, state_data)
        send_menu(peer_id, sender_id, "🛠 Панель управления материалами:", get_manage_main_keyboard())
        return

    if cmd == "/clearmenu":
        key = (peer_id, sender_id)
        safe_menu_state_pop(key)
        send_message(peer_id, "✅ Меню сброшено.")
        return

    if cmd == "/restart":
        if not is_full_access(sender_id) and not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ Команда доступна только владельцу/совладельцу бота или владельцу аудитории.")
            return
        send_message(peer_id, "🔄 Перезапуск бота...")
        logger.info(f"Бот перезапущен пользователем {sender_id} из беседы {peer_id}")
        try:
            with open(RESTART_PEER_FILE, 'w') as f:
                f.write(str(peer_id))
        except:
            pass
        sys.exit(0)

    if cmd == "/help":
        help_text = (
            "⚙️ УПРАВЛЕНИЕ БОТОМ\n\n"
            "📌 ОСНОВНЫЕ КОМАНДЫ\n"
            "/menu — открыть главное меню\n"
            "/manage — открыть панель управления материалами (доступно владельцу аудитории)\n"
            "/clearmenu — сбросить состояние меню\n"
            "/mypeer — показать ID текущей беседы\n"
            "/help — показать эту справку\n\n"
            "🔒 ПРАВА ДОСТУПА (только владелец бота)\n"
            "/allow @user — выдать право на создание аудиторий\n"
            "/disallow @user — забрать право\n"
            "/listallowed — список пользователей с правом создания аудиторий\n"
            "/addcoowner @user — добавить совладельца (полный доступ)\n"
            "/removecoowner @user — убрать совладельца\n"
            "/listcoowners — список совладельцев\n"
            "/setnotifchat — назначить текущую беседу как беседу оповещений (коллегия)\n\n"
            "📝 ШАБЛОНЫ ТЕКСТОВ (только владелец бота)\n"
            "/settext st1|exam_info|graduation <текст> — установить текст для этапов\n"
            "   st1 — ознакомление, exam_info — информация в экзаменационном этапе, graduation — поздравление\n\n"
            "🔧 НАСТРОЙКИ ТЕСТИРОВАНИЯ (по одному) (только владелец бота)\n"
            "/settime <сек> — время на вопрос\n"
            "/setthreshold <число> — порог ошибок\n\n"
            "👥 УПРАВЛЕНИЕ АУДИТОРИЯМИ\n"
            "/init — запросить подтверждение аудитории (доступно тем, у кого есть право создавать)\n"
            "/sync — синхронизировать данные с датацентром (владелец аудитории)\n"
            "/setowner @user — сменить владельца аудитории (владелец аудитории)\n"
            "/listaudiences — список всех аудиторий (только владелец бота)\n"
            "/deleteaudience <peer_id> — удалить аудиторию (только владелец бота)\n\n"
            "👤 УПРАВЛЕНИЕ НИКАМИ\n"
            "/nick [@user] <ник> — установить ник (если @user указан, то ему, иначе себе; для установки другому нужны права владельца/совладельца или владельца аудитории)\n\n"
            "👨‍🎓 УПРАВЛЕНИЕ СТУДЕНТОМ (владелец аудитории)\n"
            "Используйте кнопку «👨‍🎓 Студент» в главном меню.\n"
            "Там можно добавить, удалить студента или завершить его обучение.\n\n"
            "🔄 ПРОЧЕЕ\n"
            "/restart — перезапустить бота (только владелец/совладелец)\n"
            "/addto @user — добавить пользователя в беседу (требуются права бота)"
        )
        send_message(peer_id, help_text)
        return

    if cmd == "/mypeer":
        if not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на использование этой команды в данной беседе.")
            return
        send_message(peer_id, f"📌 Peer ID: {peer_id}")
        return

    if cmd == "/addto":
        if not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на добавление пользователей в эту беседу.")
            return
        if not args:
            send_message(peer_id, "⚠️ /addto @user")
            return
        mention = args[0]
        match = re.search(r'\[id(\d+)\|', mention)
        if not match:
            send_message(peer_id, "⚠️ Не удалось распознать пользователя.")
            return
        user_id = match.group(1)
        if add_user_to_chat(peer_id, int(user_id)):
            send_message(peer_id, f"✅ Пользователь {get_user_mention(user_id, peer_id)} добавлен в беседу.")
        else:
            send_message(peer_id, "❌ Не удалось добавить пользователя. Проверьте права бота.")
        return

    if cmd == "/allow":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Только владелец может выдавать права.")
            return
        if not args:
            send_message(peer_id, "⚠️ /allow @user")
            return
        mention = args[0]
        match = re.search(r'\[id(\d+)\|', mention)
        if not match:
            send_message(peer_id, "⚠️ Не удалось распознать пользователя.")
            return
        user_id = match.group(1)
        add_allowed_user(user_id, sender_id)
        send_message(peer_id, f"✅ Права на создание аудиторий выданы пользователю {get_user_mention(user_id, peer_id)}.")
        return

    if cmd == "/disallow":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Только владелец может забирать права.")
            return
        if not args:
            send_message(peer_id, "⚠️ /disallow @user")
            return
        mention = args[0]
        match = re.search(r'\[id(\d+)\|', mention)
        if not match:
            send_message(peer_id, "⚠️ Не удалось распознать пользователя.")
            return
        user_id = match.group(1)
        remove_allowed_user(user_id)
        send_message(peer_id, f"✅ Права на создание аудиторий отозваны у {get_user_mention(user_id, peer_id)}.")
        return

    if cmd == "/listallowed":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Только владелец может просматривать список.")
            return
        rows = get_allowed_users()
        if not rows:
            send_message(peer_id, "📭 Список пользователей с правом создания аудиторий пуст.")
            return
        text = "📋 СПИСОК АДМИНИСТРАТОРОВ (могут создавать аудитории)\n\n"
        for row in rows:
            nick = get_user_mention(row['user_id'], peer_id)
            added_by = get_user_mention(row['added_by'], peer_id)
            date = time.strftime("%d.%m.%Y", time.localtime(row['added_at']))
            text += f"• {nick} — добавлен {added_by} {date}\n"
        send_message(peer_id, text)
        return

    if cmd == "/addcoowner":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Только владелец может назначать совладельцев.")
            return
        if not args:
            send_message(peer_id, "⚠️ /addcoowner @user")
            return
        mention = args[0]
        match = re.search(r'\[id(\d+)\|', mention)
        if not match:
            send_message(peer_id, "⚠️ Не удалось распознать пользователя.")
            return
        user_id = match.group(1)
        add_co_owner(user_id, sender_id)
        send_message(peer_id, f"✅ Пользователь {get_user_mention(user_id, peer_id)} назначен совладельцем.")
        return

    if cmd == "/removecoowner":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Только владелец может снимать совладельцев.")
            return
        if not args:
            send_message(peer_id, "⚠️ /removecoowner @user")
            return
        mention = args[0]
        match = re.search(r'\[id(\d+)\|', mention)
        if not match:
            send_message(peer_id, "⚠️ Не удалось распознать пользователя.")
            return
        user_id = match.group(1)
        remove_co_owner(user_id)
        send_message(peer_id, f"✅ Пользователь {get_user_mention(user_id, peer_id)} больше не совладелец.")
        return

    if cmd == "/listcoowners":
        rows = get_co_owners()
        if not rows:
            send_message(peer_id, "📭 Список совладельцев пуст.")
            return
        text = "📋 СПИСОК СОВЛАДЕЛЬЦЕВ (полный доступ)\n\n"
        for row in rows:
            nick = get_user_mention(row['user_id'], peer_id)
            added_by = get_user_mention(row['added_by'], peer_id)
            date = time.strftime("%d.%m.%Y", time.localtime(row['added_at']))
            text += f"• {nick} — добавлен {added_by} {date}\n"
        send_message(peer_id, text)
        return

    if cmd == "/listaudiences":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Только владелец может просматривать список аудиторий.")
            return
        rows = get_all_audiences()
        text = "📋 СПИСОК АУДИТОРИЙ\n\n"

        notif_chat = get_notification_chat()
        if notif_chat:
            chat_name = get_chat_name(notif_chat) or f"Беседа {notif_chat}"
            text += f"⭐ КОЛЛЕГИЯ (беседа оповещений): {chat_name}\n   ID: {notif_chat}\n\n"

        if not rows and not notif_chat:
            send_message(peer_id, "📭 Список аудиторий пуст, коллегия не назначена.")
            return

        for row in rows:
            peer = row['peer_id']
            owner = row['owner_id']
            last_activity = row['last_activity']
            owner_mention = get_user_mention(owner, peer) if owner else "Неизвестно"
            chat_name = get_chat_name(peer) or f"Беседа {peer}"
            last_time = time.strftime("%d.%m.%Y %H:%M", time.localtime(last_activity))
            students = get_audience_students(peer)
            students_text = ", ".join([get_user_mention(s['user_id'], peer) for s in students]) if students else "нет"
            text += f"• {chat_name}\n   ID: {peer}\n   Владелец: {owner_mention}\n   Студенты: {students_text}\n   Последняя активность: {last_time}\n\n"
        send_message(peer_id, text)
        return

    if cmd == "/deleteaudience":
        if not is_owner(sender_id):
            send_message(peer_id, "❌ Только владелец может удалять аудитории.")
            return
        if len(args) < 1:
            send_message(peer_id, "⚠️ /deleteaudience <peer_id>")
            return
        try:
            target_peer = int(args[0])
        except ValueError:
            send_message(peer_id, "❌ peer_id должен быть числом.")
            return
        if target_peer == peer_id:
            send_message(peer_id, "❌ Нельзя удалить текущую беседу.")
            return
        success, msg = delete_audience_by_owner(target_peer)
        send_message(peer_id, f"{'✅' if success else '❌'} {msg}")
        return

    if cmd == "/sync":
        if peer_id < 2000000000:
            send_message(peer_id, "❌ /sync работает только в беседах.")
            return
        if not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на синхронизацию этой аудитории.")
            return
        if not is_audience_confirmed(peer_id):
            send_message(peer_id, "❌ Аудитория не подтверждена. Используйте /init.")
            return
        if is_datacenter(peer_id):
            send_message(peer_id, "⚠️ Эта беседа является датацентром. Синхронизация не требуется.")
            return
        try:
            if copy_datacenter_to_audience(peer_id):
                send_message(peer_id, "✅ Данные аудитории синхронизированы с датацентром.")
            else:
                send_message(peer_id, "❌ Не удалось синхронизировать: датацентр не найден.")
        except Exception as e:
            send_message(peer_id, f"❌ Ошибка синхронизации: {e}")
        return

    if cmd == "/setowner":
        if peer_id < 2000000000:
            send_message(peer_id, "❌ /setowner работает только в беседах.")
            return
        if not can_manage_materials(sender_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на смену владельца этой аудитории.")
            return
        if not args:
            send_message(peer_id, "⚠️ /setowner @user")
            return
        mention = args[0]
        match = re.search(r'\[id(\d+)\|', mention)
        if not match:
            send_message(peer_id, "⚠️ Не удалось распознать пользователя.")
            return
        new_owner = match.group(1)
        conn = get_db_connection(None)
        try:
            conn.execute("UPDATE audiences SET owner_id=? WHERE peer_id=?", (new_owner, peer_id))
            conn.commit()
        finally:
            conn.close()
        send_message(peer_id, f"✅ Владельцем аудитории теперь является {get_user_mention(new_owner, peer_id)}.")
        return

    if cmd == "/settext":
        if len(args) < 2:
            send_message(peer_id, "⚠️ /settext st1|exam_info|graduation <текст>")
            return
        name = args[0]
        if name not in ("st1", "exam_info", "graduation"):
            send_message(peer_id, "❌ Имя должно быть st1, exam_info или graduation.")
            return
        new_text = ' '.join(args[1:])
        if name == "exam_info":
            set_setting("exam_info_text", new_text, peer_id)
        else:
            set_setting(name, new_text, peer_id)
        send_message(peer_id, f"✅ Шаблон «{name}» обновлён.")
        return

    if cmd == "/settime":
        if not args:
            send_message(peer_id, "⚠️ /settime <секунды>")
            return
        try:
            raw = args[0].strip()
            seconds = int(raw)
            if seconds < 1:
                raise ValueError
            set_test_time_limit(peer_id, seconds)
            send_message(peer_id, f"✅ Время на вопрос установлено: {seconds} сек.")
        except ValueError as e:
            logger.error(f"Ошибка /settime: args={args}, raw='{raw}', error={e}")
            send_message(peer_id, "❌ Введите положительное целое число (например, 30).")
        except Exception as e:
            logger.error(f"Неизвестная ошибка /settime: {e}")
            send_message(peer_id, "❌ Произошла ошибка. Попробуйте снова.")
        return

    if cmd == "/setthreshold":
        if not args:
            send_message(peer_id, "⚠️ /setthreshold <число>")
            return
        try:
            raw = args[0].strip()
            threshold = int(raw)
            if threshold < 0:
                raise ValueError
            set_test_fail_threshold(peer_id, threshold)
            send_message(peer_id, f"✅ Порог ошибок установлен: {threshold}.")
        except ValueError as e:
            logger.error(f"Ошибка /setthreshold: args={args}, raw='{raw}', error={e}")
            send_message(peer_id, "❌ Введите неотрицательное целое число (например, 5).")
        except Exception as e:
            logger.error(f"Неизвестная ошибка /setthreshold: {e}")
            send_message(peer_id, "❌ Произошла ошибка. Попробуйте снова.")
        return

    send_message(peer_id, "⚠️ Неизвестная команда. Введите /help для списка.")

# ======================== ПАНЕЛЬ УПРАВЛЕНИЯ МАТЕРИАЛАМИ ============================

def handle_manage_message(text, peer_id, sender_id, conversation_message_id):
    clean_text = clean_text_from_mentions(text)
    key = (peer_id, sender_id)
    state_data = safe_menu_state_get(key)
    if not state_data or not isinstance(state_data, dict) or state_data.get('mode') != 'manage':
        return False

    current_state = state_data.get('state', 'manage_main')

    def delete_original():
        if conversation_message_id:
            delete_message(peer_id, conversation_message_id)

    # ===== ПРИВЕТСТВИЕ =====
    if current_state == 'wait_welcome_text':
        if clean_text == "💾 Сохранить":
            final_text = state_data.get('buffer', '').strip()
            set_welcome_message(peer_id, final_text)
            state_data['state'] = 'manage_welcome'
            safe_menu_state_set(key, state_data)
            send_message(peer_id, f"✅ Приветствие сохранено!\n\n{final_text if final_text else '(пусто)'}")
            delete_message_later(peer_id, conversation_message_id)
            show_welcome_status(peer_id, sender_id, key)
            return True
        elif clean_text == "🔙 Назад":
            state_data['state'] = 'manage_welcome'
            safe_menu_state_set(key, state_data)
            send_message(peer_id, "❌ Редактирование отменено.")
            delete_message_later(peer_id, conversation_message_id)
            show_welcome_status(peer_id, sender_id, key)
            return True
        else:
            if 'buffer' not in state_data:
                state_data['buffer'] = ""
            if state_data['buffer']:
                state_data['buffer'] += "\n"
            state_data['buffer'] += clean_text
            safe_menu_state_set(key, state_data)
            return True

    if current_state == 'manage_welcome':
        if clean_text == "📝 Изменить текст":
            state_data['state'] = 'wait_welcome_text'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📥 Отправьте новое приветствие (можно несколькими сообщениями).\nПо окончании нажмите «💾 Сохранить».", get_buffer_keyboard())
            delete_message_later(peer_id, conversation_message_id)
            return True
        elif clean_text == "🔕 Отключить":
            set_welcome_enabled(peer_id, False)
            show_welcome_status(peer_id, sender_id, key)
            delete_message_later(peer_id, conversation_message_id)
            return True
        elif clean_text == "🔊 Включить":
            set_welcome_enabled(peer_id, True)
            show_welcome_status(peer_id, sender_id, key)
            delete_message_later(peer_id, conversation_message_id)
            return True
        elif clean_text == "🔙 Назад":
            state_data['state'] = 'manage_main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🛠 Панель управления материалами:", get_manage_main_keyboard())
            delete_message_later(peer_id, conversation_message_id)
            return True
        else:
            return True

    # ===== БУФЕРНЫЕ СОСТОЯНИЯ =====
    if current_state in ['wait_st1_text', 'wait_exam_info_text', 'wait_creative_text', 'wait_new_topic', 'wait_template_text', 'wait_report_template',
                         'wait_edit_question_text', 'wait_edit_option_text', 'wait_add_question_text', 'wait_enter_options_text', 'wait_enter_correct',
                         'manage_set_time', 'manage_set_threshold', 'manage_edit_options_change',
                         'manage_add_question', 'manage_enter_options_type', 'manage_edit_options_text',
                         'manage_st4_exam_add_question',
                         'manage_st4_exam_enter_options_type',
                         'manage_st4_exam_enter_correct',
                         'manage_st4_exam_edit_options_text',
                         'manage_st4_exam_edit_options_change']:
        if clean_text not in ["💾 Сохранить", "➡️ Далее", "🔙 Назад", "✅ Готово", "➕ Ещё вариант"]:
            if 'buffer' not in state_data:
                state_data['buffer'] = ""
            if state_data['buffer']:
                state_data['buffer'] += "\n"
            state_data['buffer'] += clean_text
            safe_menu_state_set(key, state_data)
            return True

    if clean_text == "🔙 Назад":
        delete_original()
        if current_state in ['manage_st1', 'manage_st2_theory', 'manage_st3_practice', 'manage_st4_exam']:
            state_data['state'] = 'manage_main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🛠 Панель управления материалами:", get_manage_main_keyboard())
        elif current_state == 'manage_st2_variants':
            state_data['state'] = 'manage_st2_theory'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📖 Управление теорией. Выберите тему:", get_stage2_theory_keyboard())
        elif current_state == 'manage_st2_action':
            state_data['state'] = 'manage_st2_variants'
            topic = state_data.get('selected_topic')
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {topic}:", get_stage2_variants_keyboard(topic))
        elif current_state == 'manage_edit_one_by_one':
            state_data['state'] = 'manage_st2_variants'
            topic = state_data.get('selected_topic')
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {topic}:", get_stage2_variants_keyboard(topic))
        elif current_state in ['manage_add_question', 'manage_enter_options_type', 'manage_enter_options_text', 'manage_enter_correct', 'manage_edit_question', 'manage_edit_options', 'manage_edit_options_text',
                               'manage_select_question_to_edit', 'manage_select_question_to_delete']:
            state_data['state'] = 'manage_edit_one_by_one'
            topic = state_data['selected_topic']
            variant = state_data['selected_variant']
            safe_menu_state_set(key, state_data)
            questions = get_test_questions(topic, variant, peer_id)
            if questions:
                msg = f"❓ Режим по одному. Вопросов: {len(questions)}\n\n"
                for q in questions:
                    msg += f"{q['order_num']}. {q['question_text']}\n"
                msg += "\nВыберите действие:"
                send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
            else:
                send_menu(peer_id, sender_id, "❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
        elif current_state == 'manage_st3_variants':
            state_data['state'] = 'manage_st3_practice'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📝 Управление практикой. Выберите тип:", get_practice_types_keyboard())
        elif current_state == 'manage_st3_action':
            state_data['state'] = 'manage_st3_variants'
            practice_type = state_data.get('selected_practice_type')
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {practice_type}:", get_practice_variants_keyboard(practice_type))
        elif current_state == 'manage_st3_topics':
            state_data['state'] = 'manage_st3_practice'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📝 Управление практикой. Выберите тип:", get_practice_types_keyboard())
        elif current_state == 'manage_st3_topic_action':
            state_data['state'] = 'manage_st3_topics'
            safe_menu_state_set(key, state_data)
            topics = get_all_topics(peer_id)
            topics_list = "\n".join([f"{t['id']}. {t['text']} (шаблон: {'есть' if t['template'] else 'нет'})" for t in topics]) if topics else "Список тем пуст."
            send_menu(peer_id, sender_id, f"📋 ТЕМЫ ДОКЛАДОВ:\n\n{topics_list}", get_creative_topics_keyboard())
            return True
        elif current_state == 'manage_st4_exam_info':
            state_data['state'] = 'manage_st4_exam'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🎯 Управление экзаменационным этапом:", get_exam_menu_keyboard())
        elif current_state == 'manage_st4_exam_tests':
            state_data['state'] = 'manage_st4_exam'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🎯 Управление экзаменационным этапом:", get_exam_menu_keyboard())
        elif current_state == 'manage_st4_exam_variants':
            state_data['state'] = 'manage_st4_exam_tests'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📝 Управление экзаменационными тестами. Выберите тему:", get_exam_topics_keyboard(peer_id))
        elif current_state == 'manage_st4_exam_action':
            state_data['state'] = 'manage_st4_exam_variants'
            topic = state_data.get('selected_topic')
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {topic}:", get_exam_variants_keyboard(topic))
        elif current_state == 'manage_st4_exam_edit_one_by_one':
            state_data['state'] = 'manage_st4_exam_variants'
            topic = state_data.get('selected_topic')
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {topic}:", get_exam_variants_keyboard(topic))
        elif current_state in ['manage_st4_exam_add_question', 'manage_st4_exam_enter_options_type', 'manage_st4_exam_enter_options_text', 'manage_st4_exam_enter_correct', 'manage_st4_exam_edit_question', 'manage_st4_exam_edit_options', 'manage_st4_exam_edit_options_text',
                               'manage_st4_exam_select_question_to_edit', 'manage_st4_exam_select_question_to_delete']:
            state_data['state'] = 'manage_st4_exam_edit_one_by_one'
            topic = state_data['selected_topic']
            variant = state_data['selected_variant']
            safe_menu_state_set(key, state_data)
            questions = get_test_questions(topic, variant, peer_id)
            if questions:
                msg = f"❓ Режим по одному. Вопросов: {len(questions)}\n\n"
                for q in questions:
                    msg += f"{q['order_num']}. {q['question_text']}\n"
                msg += "\nВыберите действие:"
                send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
            else:
                send_menu(peer_id, sender_id, "❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
        elif current_state == 'manage_test_settings':
            state_data['state'] = 'manage_main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🛠 Панель управления материалами:", get_manage_main_keyboard())
        elif current_state in ['manage_set_time', 'manage_set_threshold']:
            state_data['state'] = 'manage_test_settings'
            safe_menu_state_set(key, state_data)
            time_limit = get_test_time_limit(peer_id)
            threshold = get_test_fail_threshold(peer_id)
            msg = f"⚙️ НАСТРОЙКИ ТЕСТИРОВАНИЯ (по одному)\n\n⏱ Время на вопрос: {time_limit} сек\n❌ Порог ошибок: {threshold}\n\nИспользуйте команды для изменения:\n/settime <сек>\n/setthreshold <число>"
            send_menu(peer_id, sender_id, msg, get_test_settings_keyboard())
        else:
            state_data['state'] = 'manage_main'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🛠 Панель управления материалами:", get_manage_main_keyboard())
        return True

    if current_state == 'manage_main':
        if clean_text == "👋 Приветствие":
            state_data['state'] = 'manage_welcome'
            safe_menu_state_set(key, state_data)
            show_welcome_status(peer_id, sender_id, key)
            delete_message_later(peer_id, conversation_message_id)
            return True
        elif clean_text == "📚 1 этап (ознакомление)":
            state_data['state'] = 'manage_st1'
            safe_menu_state_set(key, state_data)
            current_txt = get_setting("st1_text", None, peer_id)
            if not current_txt:
                current_txt = "Текст не задан."
            send_long_message(peer_id, f"📝 ТЕКУЩИЙ ТЕКСТ ОЗНАКОМЛЕНИЯ:\n\n{current_txt}", keyboard=get_manage_simple_action_keyboard())
        elif clean_text == "📖 2 этап (теория)":
            state_data['state'] = 'manage_st2_theory'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📖 Управление теорией. Выберите тему:", get_stage2_theory_keyboard())
        elif clean_text == "📝 3 этап (практика)":
            state_data['state'] = 'manage_st3_practice'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📝 Управление практикой. Выберите тип:", get_practice_types_keyboard())
        elif clean_text == "🎯 4 этап (экзаменационный)":
            state_data['state'] = 'manage_st4_exam'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🎯 Управление экзаменационным этапом:", get_exam_menu_keyboard())
        elif clean_text == "⚙️ Настройки тестирования":
            state_data['state'] = 'manage_test_settings'
            safe_menu_state_set(key, state_data)
            time_limit = get_test_time_limit(peer_id)
            threshold = get_test_fail_threshold(peer_id)
            msg = f"⚙️ НАСТРОЙКИ ТЕСТИРОВАНИЯ (по одному)\n\n⏱ Время на вопрос: {time_limit} сек\n❌ Порог ошибок: {threshold}\n\nИспользуйте команды для изменения:\n/settime <сек>\n/setthreshold <число>"
            send_menu(peer_id, sender_id, msg, get_test_settings_keyboard())
        elif clean_text == "🏛 Главное меню":
            safe_menu_state_pop(key)
            is_dc = is_datacenter(peer_id)
            can_manage = can_manage_materials(sender_id, peer_id)
            state_data = {'mode': 'main', 'state': 'main'}
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "🏛 Главное меню:", get_main_menu_keyboard(is_full_access(sender_id), can_manage, is_dc))
        return True

    # --- 1 этап ---
    if current_state == 'manage_st1':
        if clean_text == "➕ Изменить текст":
            state_data['state'] = 'wait_st1_text'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📥 Отправьте текст ОЗНАКОМЛЕНИЯ (можно несколькими сообщениями).\nПо окончании нажмите «💾 Сохранить».", get_buffer_keyboard())
        return True

    if current_state == 'wait_st1_text' and clean_text == "💾 Сохранить":
        final_text = state_data.get('buffer', '').strip()
        set_setting("st1_text", final_text, peer_id)
        state_data['state'] = 'manage_st1'
        safe_menu_state_set(key, state_data)
        send_long_message(peer_id, f"✅ Текст ознакомления обновлён!\n\n{final_text if final_text else '(пусто)'}", keyboard=get_manage_simple_action_keyboard())
        delete_message_later(peer_id, conversation_message_id)
        return True

    # --- 2 этап (теория) ---
    if current_state == 'manage_st2_theory':
        topics_map = {
            "Конституция": "Конституция",
            "ФКЗ О прокуратуре": "ФКЗ_О_прокуратуре",
            "Уголовный кодекс": "Уголовный_кодекс",
            "Федеральное постановление": "Федеральное_постановление",
            "Процессуальный кодекс": "Процессуальный_кодекс"
        }
        if clean_text in topics_map:
            topic = topics_map[clean_text]
            state_data['state'] = 'manage_st2_variants'
            state_data['selected_topic'] = topic
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {clean_text}:", get_stage2_variants_keyboard(topic))
        return True

    if current_state == 'manage_st2_variants':
        topic = state_data.get('selected_topic')
        display_topic = topic.replace('_', ' ')
        for v in [1, 2, 3]:
            if clean_text == f"{display_topic} вариант {v}":
                state_data['selected_variant'] = v
                state_data['state'] = 'manage_edit_one_by_one'
                safe_menu_state_set(key, state_data)
                questions = get_test_questions(topic, v, peer_id)
                if questions:
                    msg = f"❓ Режим по одному. Тема: {display_topic}, вариант {v}\n\nВопросов: {len(questions)}\n\n"
                    for q in questions:
                        msg += f"{q['order_num']}. {q['question_text']}\n"
                    msg += "\nВыберите действие:"
                    send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
                else:
                    send_menu(peer_id, sender_id, f"❓ Режим по одному. Тема: {display_topic}, вариант {v}\n\nВопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
                return True

    # --- 3 этап (практика) ---
    if current_state == 'manage_st3_practice':
        if clean_text == "Суды":
            state_data['state'] = 'manage_st3_variants'
            state_data['selected_practice_type'] = "Суды"
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите вариант для Суды:", get_practice_variants_keyboard("Суды"))
            return True
        elif clean_text == "Прокуратура":
            state_data['state'] = 'manage_st3_variants'
            state_data['selected_practice_type'] = "Прокуратура"
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите вариант для Прокуратуры:", get_practice_variants_keyboard("Прокуратура"))
            return True
        elif clean_text == "Доклады":
            state_data['state'] = 'manage_st3_topics'
            safe_menu_state_set(key, state_data)
            topics = get_all_topics(peer_id)
            topics_list = "\n".join([f"{t['id']}. {t['text']} (шаблон: {'есть' if t['template'] else 'нет'})" for t in topics]) if topics else "Список тем пуст."
            send_menu(peer_id, sender_id, f"📋 ТЕМЫ ДОКЛАДОВ:\n\n{topics_list}", get_creative_topics_keyboard())
            return True
        return True

    if current_state == 'manage_st3_variants':
        practice_type = state_data.get('selected_practice_type')
        if practice_type == "Суды":
            variants = {1: "Исковое заявление", 2: "Ходатайство прокурора", 3: "Прокурорское представление"}
        elif practice_type == "Прокуратура":
            variants = {
                1: "Обращения",
                2: "ПОСТАНОВЛЕНИЯ",
                3: "Запрос материалов",
                4: "Решение по делу ФБР",
                5: "Возбуждение УД"
            }
        elif practice_type == "Обращения":
            variants = {1: "Уведомление о принятии", 2: "Ответ на обращение", 3: "Извещение по делу"}
        else:
            variants = {}

        for v, label in variants.items():
            if clean_text == label:
                if practice_type == "Прокуратура" and label == "Обращения":
                    state_data['state'] = 'manage_obrasheniya'
                    safe_menu_state_set(key, state_data)
                    send_menu(peer_id, sender_id, "Выберите шаблон обращения для редактирования:", get_obrasheniya_keyboard())
                    return True
                else:
                    state_data['selected_variant'] = v
                    state_data['state'] = 'manage_st3_action'
                    safe_menu_state_set(key, state_data)
                    row = get_creative_text(practice_type, v, peer_id)
                    current_text = row['task_text'] if row else "Текст не задан."
                    send_menu(peer_id, sender_id, f"📎 {label}\n\n{current_text}", get_manage_action_keyboard())
                    return True

    if current_state == 'manage_obrasheniya':
        if clean_text in ["Уведомление о принятии", "Ответ на обращение", "Извещение по делу"]:
            variant_map = {"Уведомление о принятии": 1, "Ответ на обращение": 2, "Извещение по делу": 3}
            v = variant_map.get(clean_text)
            state_data['state'] = 'manage_st3_action'
            state_data['selected_practice_type'] = "Обращения"
            state_data['selected_variant'] = v
            safe_menu_state_set(key, state_data)
            row = get_creative_text_fallback("Обращения", v, peer_id)
            current_text = row['task_text'] if row else "Текст не задан."
            send_menu(peer_id, sender_id, f"📎 {clean_text}\n\n{current_text}", get_manage_action_keyboard())
            return True
        elif clean_text == "🔙 Назад":
            state_data['state'] = 'manage_st3_variants'
            state_data['selected_practice_type'] = "Прокуратура"
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Выберите тип документа в прокуратуре:", get_practice_variants_keyboard("Прокуратура"))
            return True

    if current_state == 'manage_st3_action':
        if clean_text == "🔍 Посмотреть":
            practice_type = state_data.get('selected_practice_type')
            variant = state_data.get('selected_variant')
            if practice_type == "Обращения":
                row = get_creative_text_fallback(practice_type, variant, peer_id)
            else:
                row = get_creative_text(practice_type, variant, peer_id)
            current_text = row['task_text'] if row else "Текст не задан."
            send_long_message(peer_id, f"📎 ТЕКСТ ЗАДАНИЯ:\n\n{current_text}")
            return True
        elif clean_text == "➕ Добавить/Заменить":
            practice_type = state_data.get('selected_practice_type')
            variant = state_data.get('selected_variant')
            state_data['state'] = 'wait_creative_text'
            state_data['buffer'] = ""
            state_data['ctype'] = practice_type
            state_data['variant'] = variant
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📥 Отправьте текст практического задания (можно несколькими сообщениями).\nПо окончании нажмите «💾 Сохранить».", get_buffer_keyboard())
            return True
        elif clean_text == "🗑 Удалить":
            practice_type = state_data.get('selected_practice_type')
            variant = state_data.get('selected_variant')
            delete_creative_text(practice_type, variant, peer_id)
            send_message(peer_id, "🗑 Задание удалено.")
            state_data['state'] = 'manage_st3_variants'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {practice_type}:", get_practice_variants_keyboard(practice_type))
            return True

    if current_state == 'manage_st3_topics':
        if clean_text == "➕ Добавить тему":
            state_data['state'] = 'wait_new_topic'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Введите текст новой темы:", get_buffer_keyboard())
            return True
        elif clean_text == "✏️ Изменить форму доклада":
            state_data['state'] = 'wait_report_template'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            current_template = get_report_template(peer_id)
            send_menu(peer_id, sender_id, f"📝 Текущая форма доклада:\n{current_template if current_template else '(не задана)'}\n\nВведите новый шаблон (можно несколькими сообщениями). По окончании нажмите «💾 Сохранить».", get_buffer_keyboard())
            return True
        elif clean_text == "🗑 Очистить все темы":
            delete_all_topics(peer_id)
            send_message(peer_id, "🗑 Все темы удалены.")
            state_data['state'] = 'manage_st3_topics'
            safe_menu_state_set(key, state_data)
            topics = get_all_topics(peer_id)
            topics_list = "\n".join([f"{t['id']}. {t['text']} (шаблон: {'есть' if t['template'] else 'нет'})" for t in topics]) if topics else "Список тем пуст."
            send_menu(peer_id, sender_id, f"📋 ТЕМЫ ДОКЛАДОВ:\n\n{topics_list}", get_creative_topics_keyboard())
            return True
        elif clean_text.isdigit():
            topic_id = int(clean_text)
            topic = get_topic_by_id(topic_id, peer_id)
            if topic:
                state_data['selected_topic_id'] = topic_id
                state_data['state'] = 'manage_st3_topic_action'
                safe_menu_state_set(key, state_data)
                send_menu(peer_id, sender_id, f"Тема: {topic['text']}\nШаблон: {topic['template'] if topic['template'] else 'нет'}\n\nЧто сделать?", get_creative_topic_action_keyboard())
            else:
                send_message(peer_id, "❌ Тема не найдена.")
        return True

    if current_state == 'wait_new_topic' and clean_text == "💾 Сохранить":
        new_topic = state_data.get('buffer', '').strip()
        if new_topic:
            add_topic(new_topic, "", peer_id)
            send_message(peer_id, "✅ Тема добавлена.")
        else:
            send_message(peer_id, "❌ Тема не может быть пустой.")
        state_data['state'] = 'manage_st3_topics'
        safe_menu_state_set(key, state_data)
        topics = get_all_topics(peer_id)
        topics_list = "\n".join([f"{t['id']}. {t['text']} (шаблон: {'есть' if t['template'] else 'нет'})" for t in topics]) if topics else "Список тем пуст."
        send_menu(peer_id, sender_id, f"📋 ТЕМЫ ДОКЛАДОВ:\n\n{topics_list}", get_creative_topics_keyboard())
        delete_message_later(peer_id, conversation_message_id)
        return True

    if current_state == 'wait_report_template' and clean_text == "💾 Сохранить":
        template = state_data.get('buffer', '').strip()
        set_report_template(template, peer_id)
        send_message(peer_id, "✅ Форма доклада обновлена.")
        state_data['state'] = 'manage_st3_topics'
        safe_menu_state_set(key, state_data)
        topics = get_all_topics(peer_id)
        topics_list = "\n".join([f"{t['id']}. {t['text']} (шаблон: {'есть' if t['template'] else 'нет'})" for t in topics]) if topics else "Список тем пуст."
        send_menu(peer_id, sender_id, f"📋 ТЕМЫ ДОКЛАДОВ:\n\n{topics_list}", get_creative_topics_keyboard())
        delete_message_later(peer_id, conversation_message_id)
        return True

    if current_state == 'manage_st3_topic_action':
        if clean_text == "✏️ Изменить шаблон":
            topic_id = state_data.get('selected_topic_id')
            topic = get_topic_by_id(topic_id, peer_id)
            if not topic:
                send_message(peer_id, "❌ Тема не найдена.")
                return True
            state_data['state'] = 'wait_template_text'
            state_data['buffer'] = ""
            state_data['edit_topic_id'] = topic_id
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Текущий шаблон:\n{topic['template'] if topic['template'] else '(не задан)'}\n\nВведите новый шаблон для темы:", get_buffer_keyboard())
            return True
        elif clean_text == "🗑 Удалить тему":
            topic_id = state_data.get('selected_topic_id')
            delete_topic(topic_id, peer_id)
            send_message(peer_id, "🗑 Тема удалена.")
            state_data['state'] = 'manage_st3_topics'
            safe_menu_state_set(key, state_data)
            topics = get_all_topics(peer_id)
            topics_list = "\n".join([f"{t['id']}. {t['text']} (шаблон: {'есть' if t['template'] else 'нет'})" for t in topics]) if topics else "Список тем пуст."
            send_menu(peer_id, sender_id, f"📋 ТЕМЫ ДОКЛАДОВ:\n\n{topics_list}", get_creative_topics_keyboard())
            return True

    if current_state == 'wait_template_text' and clean_text == "💾 Сохранить":
        template = state_data.get('buffer', '').strip()
        topic_id = state_data.get('edit_topic_id')
        conn = get_db_connection(peer_id)
        try:
            conn.execute("UPDATE topics SET template=? WHERE id=?", (template, topic_id))
            conn.commit()
        finally:
            conn.close()
        send_message(peer_id, "✅ Шаблон темы обновлён.")
        state_data['state'] = 'manage_st3_topics'
        safe_menu_state_set(key, state_data)
        topics = get_all_topics(peer_id)
        topics_list = "\n".join([f"{t['id']}. {t['text']} (шаблон: {'есть' if t['template'] else 'нет'})" for t in topics]) if topics else "Список тем пуст."
        send_menu(peer_id, sender_id, f"📋 ТЕМЫ ДОКЛАДОВ:\n\n{topics_list}", get_creative_topics_keyboard())
        delete_message_later(peer_id, conversation_message_id)
        return True

    # --- 4 этап (экзамен) ---
    if current_state == 'manage_st4_exam':
        if clean_text == "📄 Информация":
            state_data['state'] = 'manage_st4_exam_info'
            safe_menu_state_set(key, state_data)
            current_txt = get_setting("exam_info_text", None, peer_id)
            if not current_txt:
                current_txt = "Текст не задан."
            send_menu(peer_id, sender_id, f"📝 ТЕКУЩИЙ ТЕКСТ ЭКЗАМЕНАЦИОННОЙ ИНФОРМАЦИИ:\n\n{current_txt}", get_manage_simple_action_keyboard())
            return True
        elif clean_text == "❓ Тесты":
            state_data['state'] = 'manage_st4_exam_tests'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📝 Управление экзаменационными тестами. Выберите тему:", get_exam_topics_keyboard(peer_id))
            return True
        else:
            return True

    if current_state == 'manage_st4_exam_info':
        if clean_text == "➕ Изменить текст":
            state_data['state'] = 'wait_exam_info_text'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📥 Отправьте текст ЭКЗАМЕНАЦИОННОЙ ИНФОРМАЦИИ (можно несколькими сообщениями).\nПо окончании нажмите «💾 Сохранить».", get_buffer_keyboard())
        return True

    if current_state == 'wait_exam_info_text' and clean_text == "💾 Сохранить":
        final_text = state_data.get('buffer', '').strip()
        set_setting("exam_info_text", final_text, peer_id)
        state_data['state'] = 'manage_st4_exam_info'
        safe_menu_state_set(key, state_data)
        send_menu(peer_id, sender_id, f"✅ Текст экзаменационной информации обновлён!\n\n{final_text if final_text else '(пусто)'}", get_manage_simple_action_keyboard())
        delete_message_later(peer_id, conversation_message_id)
        return True

    # ===== РЕДАКТИРОВАНИЕ ТЕКСТА ВОПРОСА =====
    if current_state == 'wait_edit_question_text' and clean_text == "💾 Сохранить":
        new_text = state_data.get('buffer', '').strip()
        if not new_text:
            send_message(peer_id, "❌ Вопрос не может быть пустым.")
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            return True

        qid = state_data.get('edit_question_id')
        if qid is None:
            send_message(peer_id, "❌ Ошибка: не найден ID вопроса.")
            return True

        conn = get_db_connection(peer_id)
        try:
            conn.execute("UPDATE test_questions SET question_text=? WHERE id=?", (new_text, qid))
            conn.commit()
        finally:
            conn.close()

        send_message(peer_id, "✅ Текст вопроса обновлён!")

        if state_data.get('from_exam'):
            state_data['state'] = 'manage_st4_exam_edit_question'
            state_data.pop('from_exam', None)
        else:
            state_data['state'] = 'manage_edit_question'

        state_data['buffer'] = ""
        safe_menu_state_set(key, state_data)

        conn = get_db_connection(peer_id)
        try:
            cur = conn.cursor()
            cur.execute("SELECT question_text FROM test_questions WHERE id=?", (qid,))
            row = cur.fetchone()
            question_text = row['question_text'] if row else "Вопрос не найден."
        finally:
            conn.close()

        send_menu(peer_id, sender_id, f"Редактируем вопрос:\n\n{question_text}\n\nЧто сделать?", get_edit_question_keyboard())
        delete_message_later(peer_id, conversation_message_id)
        return True

    # ===== СОХРАНЕНИЕ ТЕКСТА ПРАКТИКИ =====
    if current_state == 'wait_creative_text' and clean_text == "💾 Сохранить":
        final_text = state_data.get('buffer', '').strip()
        ctype = state_data.get('ctype')
        variant = state_data.get('variant')
        if ctype is not None and variant is not None:
            set_creative_text(ctype, variant, final_text, peer_id)
            state_data['state'] = 'manage_st3_action'
            safe_menu_state_set(key, state_data)
            send_menu(
                peer_id,
                sender_id,
                f"✅ Задание сохранено!\n\n{final_text if final_text else '(пусто)'}",
                get_manage_action_keyboard()
            )
            delete_message_later(peer_id, conversation_message_id)
        else:
            send_message(peer_id, "❌ Ошибка: не удалось определить тип задания.")
        return True

    # ===== ЭКЗАМЕНАЦИОННЫЕ ТЕСТЫ =====
    if current_state == 'manage_st4_exam_tests':
        exam_topics = {
            "Экзамен 1": "Экзамен_1",
            "Экзамен 2": "Экзамен_2",
            "Экзамен 3": "Экзамен_3"
        }
        if clean_text in exam_topics:
            topic = exam_topics[clean_text]
            state_data['selected_topic'] = topic
            state_data['state'] = 'manage_st4_exam_variants'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"Выберите вариант для {clean_text}:", get_exam_variants_keyboard(topic))
            return True

    if current_state == 'manage_st4_exam_variants':
        topic = state_data.get('selected_topic')
        if clean_text.isdigit():
            v = int(clean_text)
            if 1 <= v <= 3:
                state_data['selected_variant'] = v
                state_data['state'] = 'manage_st4_exam_edit_one_by_one'
                safe_menu_state_set(key, state_data)
                questions = get_test_questions(topic, v, peer_id)
                if questions:
                    msg = f"❓ Режим по одному. Тема: {topic.replace('_', ' ')}, вариант {v}\n\nВопросов: {len(questions)}\n\n"
                    for q in questions:
                        msg += f"{q['order_num']}. {q['question_text']}\n"
                    msg += "\nВыберите действие:"
                    send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
                else:
                    send_menu(peer_id, sender_id, f"❓ Режим по одному. Тема: {topic.replace('_', ' ')}, вариант {v}\n\nВопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
                return True

    if current_state == 'manage_st4_exam_edit_one_by_one':
        topic = state_data.get('selected_topic')
        variant = state_data.get('selected_variant')
        if clean_text == "➕ Добавить вопрос":
            state_data['state'] = 'manage_st4_exam_add_question'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📝 Введите текст вопроса:", get_buffer_keyboard())
            return True
        elif clean_text == "✏️ Редактировать вопрос":
            questions = get_test_questions(topic, variant, peer_id)
            if not questions:
                send_message(peer_id, "❌ Нет вопросов для редактирования.")
                return True
            state_data['state'] = 'manage_st4_exam_select_question_to_edit'
            safe_menu_state_set(key, state_data)
            kb = get_question_list_keyboard(questions)
            send_menu(peer_id, sender_id, "Выберите номер вопроса для редактирования:", kb)
            return True
        elif clean_text == "🗑 Удалить вопрос":
            questions = get_test_questions(topic, variant, peer_id)
            if not questions:
                send_message(peer_id, "❌ Нет вопросов для удаления.")
                return True
            state_data['state'] = 'manage_st4_exam_select_question_to_delete'
            safe_menu_state_set(key, state_data)
            kb = get_question_list_keyboard(questions)
            send_menu(peer_id, sender_id, "Выберите номер вопроса для удаления:", kb)
            return True
        elif clean_text == "🗑 Удалить все вопросы":
            delete_test_questions(peer_id, topic, variant)
            send_message(peer_id, "✅ Все вопросы удалены.")
            state_data['state'] = 'manage_st4_exam_edit_one_by_one'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
            return True

    if current_state == 'manage_st4_exam_add_question':
        if clean_text == "💾 Сохранить":
            question_text = state_data.get('buffer', '').strip()
            if not question_text:
                send_message(peer_id, "❌ Вопрос не может быть пустым.")
                return True
            state_data['question_text'] = question_text
            state_data['state'] = 'manage_st4_exam_enter_options_type'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Введите варианты ответов.\nКаждый вариант с новой строки в формате:\n<буква>. <текст>\nНапример:\nА. Вариант 1\nБ. Вариант 2\n\nПосле ввода всех вариантов нажмите «✅ Готово».", get_add_option_keyboard())
            return True

    if current_state == 'manage_st4_exam_enter_options_type':
        if clean_text == "✅ Готово":
            options_text = state_data.get('buffer', '').strip()
            if not options_text:
                send_message(peer_id, "❌ Нужно ввести хотя бы один вариант.")
                return True
            lines = options_text.splitlines()
            options = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                match = re.match(r'^([А-Яа-яA-Za-z])[\.\)]\s*(.+)$', line)
                if not match:
                    send_message(peer_id, f"❌ Неверный формат строки: {line}\nИспользуйте «А. текст» или «А) текст».")
                    return True
                label = match.group(1).upper()
                text_option = match.group(2).strip()
                options.append((label, text_option))
            if len(options) < 2:
                send_message(peer_id, "❌ Должно быть минимум 2 варианта.")
                return True
            state_data['options'] = options
            state_data['state'] = 'manage_st4_exam_enter_correct'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            options_list = "\n".join([f"{label}). {text}" for label, text in options])
            send_menu(peer_id, sender_id, f"Введите номер (букву) правильного варианта из списка:\n\n{options_list}", get_buffer_keyboard())
            return True
        elif clean_text == "➕ Ещё вариант":
            pass
        else:
            if 'buffer' not in state_data:
                state_data['buffer'] = ""
            if state_data['buffer']:
                state_data['buffer'] += "\n"
            state_data['buffer'] += clean_text
            safe_menu_state_set(key, state_data)
            return True

    if current_state == 'manage_st4_exam_enter_correct':
        if clean_text == "💾 Сохранить":
            correct_label = state_data.get('buffer', '').strip().upper()
            options = state_data.get('options', [])
            correct_index = None
            for i, (label, text) in enumerate(options):
                if label.upper() == correct_label:
                    correct_index = i
                    break
            if correct_index is None:
                send_message(peer_id, "❌ Неверная буква. Попробуйте снова.")
                state_data['buffer'] = ""
                safe_menu_state_set(key, state_data)
                return True
            topic = state_data.get('selected_topic')
            variant = state_data.get('selected_variant')
            question_text = state_data.get('question_text')
            order_num = len(get_test_questions(topic, variant, peer_id)) + 1
            add_test_question(peer_id, topic, variant, question_text, correct_index, order_num, options)
            send_message(peer_id, "✅ Вопрос добавлен!")
            state_data['state'] = 'manage_st4_exam_edit_one_by_one'
            safe_menu_state_set(key, state_data)
            questions = get_test_questions(topic, variant, peer_id)
            if questions:
                msg = f"❓ Режим по одному. Вопросов: {len(questions)}\n\n"
                for q in questions:
                    msg += f"{q['order_num']}. {q['question_text']}\n"
                msg += "\nВыберите действие:"
                send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
            else:
                send_menu(peer_id, sender_id, "❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
            return True
        else:
            state_data['buffer'] = clean_text
            safe_menu_state_set(key, state_data)
            return True

    # ===== ОБЩИЕ ВОПРОСЫ ТЕОРИИ =====
    if current_state == 'manage_edit_one_by_one':
        topic = state_data.get('selected_topic')
        variant = state_data.get('selected_variant')
        if clean_text == "➕ Добавить вопрос":
            state_data['state'] = 'manage_add_question'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "📝 Введите текст вопроса:", get_buffer_keyboard())
            return True
        elif clean_text == "✏️ Редактировать вопрос":
            questions = get_test_questions(topic, variant, peer_id)
            if not questions:
                send_message(peer_id, "❌ Нет вопросов для редактирования.")
                return True
            state_data['state'] = 'manage_select_question_to_edit'
            safe_menu_state_set(key, state_data)
            kb = get_question_list_keyboard(questions)
            send_menu(peer_id, sender_id, "Выберите номер вопроса для редактирования:", kb)
            return True
        elif clean_text == "🗑 Удалить вопрос":
            questions = get_test_questions(topic, variant, peer_id)
            if not questions:
                send_message(peer_id, "❌ Нет вопросов для удаления.")
                return True
            state_data['state'] = 'manage_select_question_to_delete'
            safe_menu_state_set(key, state_data)
            kb = get_question_list_keyboard(questions)
            send_menu(peer_id, sender_id, "Выберите номер вопроса для удаления:", kb)
            return True
        elif clean_text == "🗑 Удалить все вопросы":
            delete_test_questions(peer_id, topic, variant)
            send_message(peer_id, "✅ Все вопросы удалены.")
            state_data['state'] = 'manage_edit_one_by_one'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, f"❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
            return True

    if current_state == 'manage_add_question':
        if clean_text == "💾 Сохранить":
            question_text = state_data.get('buffer', '').strip()
            if not question_text:
                send_message(peer_id, "❌ Вопрос не может быть пустым.")
                return True
            state_data['question_text'] = question_text
            state_data['state'] = 'manage_enter_options_type'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Введите варианты ответов.\nКаждый вариант с новой строки в формате:\n<буква>. <текст>\nНапример:\nА. Вариант 1\nБ. Вариант 2\n\nПосле ввода всех вариантов нажмите «✅ Готово».", get_add_option_keyboard())
            return True
        else:
            if 'buffer' not in state_data:
                state_data['buffer'] = ""
            if state_data['buffer']:
                state_data['buffer'] += "\n"
            state_data['buffer'] += clean_text
            safe_menu_state_set(key, state_data)
            return True

    if current_state == 'manage_enter_options_type':
        if clean_text == "✅ Готово":
            options_text = state_data.get('buffer', '').strip()
            if not options_text:
                send_message(peer_id, "❌ Нужно ввести хотя бы один вариант.")
                return True
            lines = options_text.splitlines()
            options = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                match = re.match(r'^([А-Яа-яA-Za-z])[\.\)]\s*(.+)$', line)
                if not match:
                    send_message(peer_id, f"❌ Неверный формат строки: {line}\nИспользуйте «А. текст» или «А) текст».")
                    return True
                label = match.group(1).upper()
                text_option = match.group(2).strip()
                options.append((label, text_option))
            if len(options) < 2:
                send_message(peer_id, "❌ Должно быть минимум 2 варианта.")
                return True
            state_data['options'] = options
            state_data['state'] = 'manage_enter_correct'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            options_list = "\n".join([f"{label}). {text}" for label, text in options])
            send_menu(peer_id, sender_id, f"Введите номер (букву) правильного варианта из списка:\n\n{options_list}", get_buffer_keyboard())
            return True
        else:
            if 'buffer' not in state_data:
                state_data['buffer'] = ""
            if state_data['buffer']:
                state_data['buffer'] += "\n"
            state_data['buffer'] += clean_text
            safe_menu_state_set(key, state_data)
            return True

    if current_state == 'manage_enter_correct':
        if clean_text == "💾 Сохранить":
            correct_label = state_data.get('buffer', '').strip().upper()
            options = state_data.get('options', [])
            correct_index = None
            for i, (label, text) in enumerate(options):
                if label.upper() == correct_label:
                    correct_index = i
                    break
            if correct_index is None:
                send_message(peer_id, "❌ Неверная буква. Попробуйте снова.")
                state_data['buffer'] = ""
                safe_menu_state_set(key, state_data)
                return True
            topic = state_data.get('selected_topic')
            variant = state_data.get('selected_variant')
            question_text = state_data.get('question_text')
            order_num = len(get_test_questions(topic, variant, peer_id)) + 1
            add_test_question(peer_id, topic, variant, question_text, correct_index, order_num, options)
            send_message(peer_id, "✅ Вопрос добавлен!")
            state_data['state'] = 'manage_edit_one_by_one'
            safe_menu_state_set(key, state_data)
            questions = get_test_questions(topic, variant, peer_id)
            if questions:
                msg = f"❓ Режим по одному. Вопросов: {len(questions)}\n\n"
                for q in questions:
                    msg += f"{q['order_num']}. {q['question_text']}\n"
                msg += "\nВыберите действие:"
                send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
            else:
                send_menu(peer_id, sender_id, "❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
            return True
        else:
            state_data['buffer'] = clean_text
            safe_menu_state_set(key, state_data)
            return True

    if current_state == 'manage_select_question_to_edit':
        if clean_text.isdigit():
            qnum = int(clean_text)
            topic = state_data.get('selected_topic')
            variant = state_data.get('selected_variant')
            questions = get_test_questions(topic, variant, peer_id)
            if 1 <= qnum <= len(questions):
                q = questions[qnum-1]
                state_data['edit_question_id'] = q['id']
                state_data['state'] = 'manage_edit_question'
                safe_menu_state_set(key, state_data)
                send_menu(peer_id, sender_id, f"Редактируем вопрос {qnum}:\n\n{q['question_text']}\n\nЧто сделать?", get_edit_question_keyboard())
            else:
                send_message(peer_id, "❌ Неверный номер.")
        return True

    if current_state == 'manage_select_question_to_delete':
        if clean_text.isdigit():
            qnum = int(clean_text)
            topic = state_data.get('selected_topic')
            variant = state_data.get('selected_variant')
            questions = get_test_questions(topic, variant, peer_id)
            if 1 <= qnum <= len(questions):
                q = questions[qnum-1]
                delete_test_question(q['id'], peer_id)
                send_message(peer_id, "✅ Вопрос удалён.")
                state_data['state'] = 'manage_edit_one_by_one'
                safe_menu_state_set(key, state_data)
                questions = get_test_questions(topic, variant, peer_id)
                if questions:
                    msg = f"❓ Режим по одному. Вопросов: {len(questions)}\n\n"
                    for q in questions:
                        msg += f"{q['order_num']}. {q['question_text']}\n"
                    msg += "\nВыберите действие:"
                    send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
                else:
                    send_menu(peer_id, sender_id, "❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
            else:
                send_message(peer_id, "❌ Неверный номер.")
        return True

    if current_state == 'manage_edit_question':
        if clean_text == "✏️ Редактировать вопрос":
            state_data['state'] = 'wait_edit_question_text'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Введите новый текст вопроса:", get_buffer_keyboard())
            return True
        elif clean_text == "✏️ Редактировать варианты":
            state_data['state'] = 'manage_edit_options'
            safe_menu_state_set(key, state_data)
            qid = state_data.get('edit_question_id')
            options = get_test_options(qid, peer_id)
            if not options:
                send_message(peer_id, "❌ У вопроса нет вариантов.")
                return True
            options_list = "\n".join([f"{i+1}. {opt['option_label']}). {opt['option_text']}" for i, opt in enumerate(options)])
            send_menu(peer_id, sender_id, f"Текущие варианты:\n\n{options_list}\n\nВыберите действие:", get_edit_options_keyboard())
            return True
        elif clean_text == "🗑 Удалить вопрос":
            qid = state_data.get('edit_question_id')
            delete_test_question(qid, peer_id)
            send_message(peer_id, "✅ Вопрос удалён.")
            state_data['state'] = 'manage_edit_one_by_one'
            safe_menu_state_set(key, state_data)
            topic = state_data.get('selected_topic')
            variant = state_data.get('selected_variant')
            questions = get_test_questions(topic, variant, peer_id)
            if questions:
                msg = f"❓ Режим по одному. Вопросов: {len(questions)}\n\n"
                for q in questions:
                    msg += f"{q['order_num']}. {q['question_text']}\n"
                msg += "\nВыберите действие:"
                send_menu(peer_id, sender_id, msg, get_manage_test_questions_keyboard())
            else:
                send_menu(peer_id, sender_id, "❓ Режим по одному. Вопросов пока нет.\nДобавьте вопросы:", get_manage_test_questions_keyboard())
            return True

    if current_state == 'manage_edit_options':
        if clean_text == "➕ Добавить вариант":
            state_data['state'] = 'manage_edit_options_text'
            state_data['buffer'] = ""
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Введите новый вариант в формате:\n<буква>. <текст>", get_buffer_keyboard())
            return True
        elif clean_text == "🗑 Удалить вариант":
            qid = state_data.get('edit_question_id')
            options = get_test_options(qid, peer_id)
            if not options:
                send_message(peer_id, "❌ Нет вариантов для удаления.")
                return True
            state_data['state'] = 'manage_edit_options_delete'
            safe_menu_state_set(key, state_data)
            options_list = "\n".join([f"{i+1}. {opt['option_label']}). {opt['option_text']}" for i, opt in enumerate(options)])
            send_menu(peer_id, sender_id, f"Выберите номер варианта для удаления:\n\n{options_list}\n\n(введите число)", get_buffer_keyboard(next_step=True))
            return True
        elif clean_text == "✏️ Изменить вариант":
            qid = state_data.get('edit_question_id')
            options = get_test_options(qid, peer_id)
            if not options:
                send_message(peer_id, "❌ Нет вариантов для изменения.")
                return True
            state_data['state'] = 'manage_edit_options_select'
            safe_menu_state_set(key, state_data)
            options_list = "\n".join([f"{i+1}. {opt['option_label']}). {opt['option_text']}" for i, opt in enumerate(options)])
            send_menu(peer_id, sender_id, f"Выберите номер варианта для изменения:\n\n{options_list}\n\n(введите число)", get_buffer_keyboard(next_step=True))
            return True
        elif clean_text == "✅ Готово":
            state_data['state'] = 'manage_edit_question'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Возврат к редактированию вопроса.", get_edit_question_keyboard())
            return True

    if current_state == 'manage_edit_options_text':
        if clean_text == "💾 Сохранить":
            line = state_data.get('buffer', '').strip()
            match = re.match(r'^([А-Яа-яA-Za-z])[\.\)]\s*(.+)$', line)
            if not match:
                send_message(peer_id, "❌ Неверный формат. Используйте «А. текст» или «А) текст».")
                return True
            label = match.group(1).upper()
            text_option = match.group(2).strip()
            qid = state_data.get('edit_question_id')
            conn = get_db_connection(peer_id)
            try:
                cur = conn.cursor()
                cur.execute("SELECT MAX(id) FROM test_options WHERE question_id=?", (qid,))
                max_id = cur.fetchone()[0]
                if max_id is None:
                    max_id = 0
                new_order = max_id + 1
                cur.execute("INSERT INTO test_options (question_id, option_label, option_text) VALUES (?, ?, ?)", (qid, label, text_option))
                conn.commit()
            finally:
                conn.close()
            send_message(peer_id, "✅ Вариант добавлен.")
            state_data['state'] = 'manage_edit_options'
            safe_menu_state_set(key, state_data)
            qid = state_data.get('edit_question_id')
            options = get_test_options(qid, peer_id)
            options_list = "\n".join([f"{i+1}. {opt['option_label']}). {opt['option_text']}" for i, opt in enumerate(options)])
            send_menu(peer_id, sender_id, f"Текущие варианты:\n\n{options_list}\n\nВыберите действие:", get_edit_options_keyboard())
            return True

    if current_state == 'manage_edit_options_delete':
        if clean_text.isdigit():
            idx = int(clean_text) - 1
            qid = state_data.get('edit_question_id')
            options = get_test_options(qid, peer_id)
            if 0 <= idx < len(options):
                opt_id = options[idx]['id']
                conn = get_db_connection(peer_id)
                try:
                    conn.execute("DELETE FROM test_options WHERE id=?", (opt_id,))
                    conn.commit()
                finally:
                    conn.close()
                send_message(peer_id, "✅ Вариант удалён.")
                state_data['state'] = 'manage_edit_options'
                safe_menu_state_set(key, state_data)
                options = get_test_options(qid, peer_id)
                options_list = "\n".join([f"{i+1}. {opt['option_label']}). {opt['option_text']}" for i, opt in enumerate(options)])
                send_menu(peer_id, sender_id, f"Текущие варианты:\n\n{options_list}\n\nВыберите действие:", get_edit_options_keyboard())
            else:
                send_message(peer_id, "❌ Неверный номер.")
        return True

    if current_state == 'manage_edit_options_select':
        if clean_text.isdigit():
            idx = int(clean_text) - 1
            qid = state_data.get('edit_question_id')
            options = get_test_options(qid, peer_id)
            if 0 <= idx < len(options):
                state_data['edit_option_id'] = options[idx]['id']
                state_data['state'] = 'manage_edit_options_change'
                state_data['buffer'] = ""
                safe_menu_state_set(key, state_data)
                send_menu(peer_id, sender_id, f"Введите новый текст для варианта {options[idx]['option_label']}:\n(формат: <буква>. <текст>)", get_buffer_keyboard())
            else:
                send_message(peer_id, "❌ Неверный номер.")
        return True

    if current_state == 'manage_edit_options_change':
        if clean_text == "💾 Сохранить":
            line = state_data.get('buffer', '').strip()
            match = re.match(r'^([А-Яа-яA-Za-z])[\.\)]\s*(.+)$', line)
            if not match:
                send_message(peer_id, "❌ Неверный формат. Используйте «А. текст» или «А) текст».")
                return True
            label = match.group(1).upper()
            text_option = match.group(2).strip()
            opt_id = state_data.get('edit_option_id')
            conn = get_db_connection(peer_id)
            try:
                conn.execute("UPDATE test_options SET option_label=?, option_text=? WHERE id=?", (label, text_option, opt_id))
                conn.commit()
            finally:
                conn.close()
            send_message(peer_id, "✅ Вариант обновлён.")
            state_data['state'] = 'manage_edit_options'
            safe_menu_state_set(key, state_data)
            qid = state_data.get('edit_question_id')
            options = get_test_options(qid, peer_id)
            options_list = "\n".join([f"{i+1}. {opt['option_label']}). {opt['option_text']}" for i, opt in enumerate(options)])
            send_menu(peer_id, sender_id, f"Текущие варианты:\n\n{options_list}\n\nВыберите действие:", get_edit_options_keyboard())
            return True

    # ===== НАСТРОЙКИ =====
    if current_state == 'manage_test_settings':
        if clean_text == "⏱ Время на вопрос":
            state_data['state'] = 'manage_set_time'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Введите новое время на вопрос (в секундах):", get_buffer_keyboard())
        elif clean_text == "❌ Порог ошибок":
            state_data['state'] = 'manage_set_threshold'
            safe_menu_state_set(key, state_data)
            send_menu(peer_id, sender_id, "Введите новый порог ошибок (число):", get_buffer_keyboard())
        return True

    if current_state == 'manage_set_time':
        if clean_text == "💾 Сохранить":
            try:
                seconds = int(state_data.get('buffer', '').strip())
                if seconds < 1:
                    raise ValueError
                set_test_time_limit(peer_id, seconds)
                send_message(peer_id, f"✅ Время на вопрос установлено: {seconds} сек.")
                state_data['buffer'] = ""
                safe_menu_state_set(key, state_data)
            except:
                send_message(peer_id, "❌ Введите положительное целое число (например, 30).")
                state_data['buffer'] = ""
                safe_menu_state_set(key, state_data)
                return True
            state_data['state'] = 'manage_test_settings'
            safe_menu_state_set(key, state_data)
            time_limit = get_test_time_limit(peer_id)
            threshold = get_test_fail_threshold(peer_id)
            msg = f"⚙️ НАСТРОЙКИ ТЕСТИРОВАНИЯ (по одному)\n\n⏱ Время на вопрос: {time_limit} сек\n❌ Порог ошибок: {threshold}\n\nИспользуйте команды для изменения:\n/settime <сек>\n/setthreshold <число>"
            send_menu(peer_id, sender_id, msg, get_test_settings_keyboard())
            return True
        else:
            state_data['buffer'] = clean_text
            safe_menu_state_set(key, state_data)
            return True

    if current_state == 'manage_set_threshold':
        if clean_text == "💾 Сохранить":
            try:
                threshold = int(state_data.get('buffer', '').strip())
                if threshold < 0:
                    raise ValueError
                set_test_fail_threshold(peer_id, threshold)
                send_message(peer_id, f"✅ Порог ошибок установлен: {threshold}.")
                state_data['buffer'] = ""
                safe_menu_state_set(key, state_data)
            except:
                send_message(peer_id, "❌ Введите неотрицательное целое число (например, 5).")
                state_data['buffer'] = ""
                safe_menu_state_set(key, state_data)
                return True
            state_data['state'] = 'manage_test_settings'
            safe_menu_state_set(key, state_data)
            time_limit = get_test_time_limit(peer_id)
            threshold = get_test_fail_threshold(peer_id)
            msg = f"⚙️ НАСТРОЙКИ ТЕСТИРОВАНИЯ (по одному)\n\n⏱ Время на вопрос: {time_limit} сек\n❌ Порог ошибок: {threshold}\n\nИспользуйте команды для изменения:\n/settime <сек>\n/setthreshold <число>"
            send_menu(peer_id, sender_id, msg, get_test_settings_keyboard())
            return True
        else:
            state_data['buffer'] = clean_text
            safe_menu_state_set(key, state_data)
            return True

    return False

# ===== СТАТУС ПРИВЕТСТВИЯ =====
def show_welcome_status(peer_id, sender_id, key):
    current_text = get_welcome_message(peer_id)
    enabled = is_welcome_enabled(peer_id)
    status_text = "ВКЛЮЧЕНО ✅" if enabled else "ОТКЛЮЧЕНО ❌"
    msg = f"👋 УПРАВЛЕНИЕ ПРИВЕТСТВИЕМ\n\n"
    msg += f"Статус: {status_text}\n"
    msg += f"Текст:\n{current_text if current_text else '(не задан)'}\n\n"
    msg += "Выберите действие:"
    send_menu(peer_id, sender_id, msg, get_welcome_management_keyboard())

# ======================== ОБРАБОТЧИК CALLBACK ============================

def handle_callback(event):
    payload = event.object.payload
    if not payload:
        return
    cmd = payload.get('cmd')

    def safe_answer():
        try:
            vk.messages.sendMessageEventAnswer(
                event_id=event.object.event_id,
                user_id=event.object.user_id,
                peer_id=event.object.peer_id
            )
        except Exception:
            pass

    peer_id = event.object.peer_id
    cmid_to_delete = event.object.conversation_message_id

    if cmd in ("confirm_audience", "confirm_datacenter", "set_notification_chat"):
        menu_cmid = menu_messages.pop(peer_id, None)
        if menu_cmid:
            delete_message(peer_id, menu_cmid, force=True)
        if cmid_to_delete:
            delete_message(peer_id, cmid_to_delete, force=True)

    if cmd == "confirm_audience":
        if not can_create_audience(event.object.user_id):
            send_message(peer_id, "❌ У вас нет прав на создание аудиторий.")
            return
        if is_audience_confirmed(peer_id):
            send_message(peer_id, "⚠️ Эта беседа уже активирована.")
            return
        try:
            create_audience(peer_id, event.object.user_id, cmid_to_delete)
            send_message(peer_id, "✅ Аудитория создана! Теперь вы можете использовать бота.")
        except Exception as e:
            send_message(peer_id, f"❌ Ошибка создания аудитории: {e}")
        return

    elif cmd == "confirm_datacenter":
        if not is_full_access(event.object.user_id):
            send_message(peer_id, "❌ Только владелец или совладелец может создать датацентр.")
            return
        if is_audience_confirmed(peer_id):
            send_message(peer_id, "⚠️ Эта беседа уже активирована.")
            return
        try:
            create_datacenter(peer_id, event.object.user_id, cmid_to_delete)
            send_message(peer_id, "✅ Датацентр создан! Теперь можно создавать аудитории.")
        except Exception as e:
            send_message(peer_id, f"❌ Ошибка создания датацентра: {e}")
        return

    elif cmd == "set_notification_chat":
        if not is_owner(event.object.user_id):
            send_message(peer_id, "❌ Только владелец бота может назначить беседу оповещений.")
            return
        set_notification_chat(peer_id)
        send_message(peer_id, "✅ Эта беседа назначена как беседа оповещений (коллегия).")
        return

    elif cmd == "test_ready":
        test = active_tests.get(peer_id)
        if not test:
            return
        if not can_manage_materials(event.object.user_id, peer_id):
            send_message(peer_id, "❌ Только владелец аудитории может начать тест.")
            return
        begin_test(peer_id, cmid_to_delete)
        return

    elif cmd == "test_cancel":
        test = active_tests.get(peer_id)
        if not test:
            return
        if not can_manage_materials(event.object.user_id, peer_id):
            send_message(peer_id, "❌ Только владелец аудитории может отменить тест.")
            return
        cancel_test(peer_id, cmid_to_delete)
        return

    elif cmd == "test_answer":
        test = active_tests.get(peer_id)
        if not test:
            return
        students = get_audience_students(peer_id)
        if students:
            student_id = students[0]['user_id']
            if str(event.object.user_id) != str(student_id):
                send_message(peer_id, "❌ Только текущий студент может отвечать на вопросы.")
                return
        handle_test_answer_callback(event)
        return

    elif cmd == "test_pause":
        test = active_tests.get(peer_id)
        if not test:
            return
        if not can_manage_materials(event.object.user_id, peer_id):
            send_message(peer_id, "❌ Только владелец аудитории может управлять тестом.")
            return
        pause_test(peer_id, cmid_to_delete)
        return

    elif cmd == "test_resume":
        test = active_tests.get(peer_id)
        if not test:
            return
        if not can_manage_materials(event.object.user_id, peer_id):
            send_message(peer_id, "❌ Только владелец аудитории может управлять тестом.")
            return
        resume_test(peer_id, cmid_to_delete)
        return

    elif cmd == "test_end":
        test = active_tests.get(peer_id)
        if not test:
            return
        if not can_manage_materials(event.object.user_id, peer_id):
            send_message(peer_id, "❌ Только владелец аудитории может завершить тест.")
            return
        end_test_early(peer_id, cmid_to_delete)
        return

    elif cmd == "notify_stage":
        if not can_manage_materials(event.object.user_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на отправку уведомлений в этой аудитории.")
            return
        stage = payload.get('stage')
        chat = get_notification_chat()
        if chat:
            audience_name = get_chat_name(peer_id) or f"Беседа {peer_id}"
            stage_names = {1: "ознакомление", 2: "теория", 3: "практика", 4: "экзаменационный"}
            stage_name = stage_names.get(stage, "этап")
            owner_id = get_audience_owner(peer_id)
            owner_mention = get_user_mention(owner_id, peer_id) if owner_id else "Неизвестно"
            students = get_audience_students(peer_id)
            if students:
                student_mentions = ", ".join([get_user_mention(s['user_id'], peer_id) for s in students])
                notify_text = f"📢 Аудитория: {audience_name}\nРектор: {owner_mention}\nСтудент: {student_mentions}\nПриступил к {stage} этапу ({stage_name})"
            else:
                notify_text = f"📢 Аудитория: {audience_name}\nРектор: {owner_mention}\nСтудент: нет\n(этап {stage} не начат)"
            send_notification(notify_text)
        if cmid_to_delete:
            delete_message(peer_id, cmid_to_delete, force=True)
        return

    elif cmd == "skip_notification":
        if not can_manage_materials(event.object.user_id, peer_id):
            send_message(peer_id, "❌ У вас нет прав на пропуск уведомления.")
            return
        if cmid_to_delete:
            delete_message(peer_id, cmid_to_delete, force=True)
        return

# ======================== ОСНОВНОЙ ЦИКЛ ============================

def main():
    global vk, longpoll
    logger.info("🚀 Запуск бота...")
    logger.info(f"GROUP_ID = {GROUP_ID!r} (тип: {type(GROUP_ID).__name__})")
    logger.info(f"OWNER_ID = {OWNER_ID!r}")
    logger.info(f"GROUP_TOKEN задан: {bool(GROUP_TOKEN)}, длина: {len(GROUP_TOKEN) if GROUP_TOKEN else 0}")

    try:
        vk_session = vk_api.VkApi(token=GROUP_TOKEN)
        vk = vk_session.get_api()
        logger.info("✅ Авторизация VK API прошла успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка авторизации VK API: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    error_handler = VkErrorHandler()
    error_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    send_to_owner("✅ Бот успешно запущен и готов к работе!")

    try:
        if os.path.exists(RESTART_PEER_FILE):
            with open(RESTART_PEER_FILE, 'r') as f:
                peer_id = int(f.read().strip())
            send_message(peer_id, "✅ Бот успешно перезапущен и готов к работе!")
            os.remove(RESTART_PEER_FILE)
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о перезапуске в беседу: {e}")

    try:
        logger.info(f"Создаю VkBotLongPoll (group_id={GROUP_ID}, wait=45)...")
        longpoll = VkBotLongPoll(vk_session, GROUP_ID, wait=45)
        logger.info("✅ VkBotLongPoll создан успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка LongPoll: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    try:
        init_main_db()
        logger.info("✅ База данных инициализирована")
        cleanup_audience_dbs()
        auto_repair_audiences()
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    restart_timer = schedule_daily_restart()

    bot_id = -GROUP_ID
    logger.info(f"🤖 Бот запущен и готов к работе. bot_id = {bot_id}")

    while True:
        try:
            for event in longpoll.listen():
                if event.type == VkBotEventType.MESSAGE_EVENT:
                    handle_callback(event)
                    continue

                if event.type == VkBotEventType.MESSAGE_NEW:
                    msg = event.object.message
                    peer_id = msg['peer_id']
                    text = msg['text'].strip()
                    sender_id = str(msg['from_id'])

                    if peer_id == get_notification_chat():
                        continue

                    if peer_id >= 2000000000 and is_audience_confirmed(peer_id):
                        update_audience_activity(peer_id)

                    if peer_id >= 2000000000 and not is_audience_confirmed(peer_id):
                        if not text.startswith('/') or (text.startswith('/') and not text.lower().startswith(('/init', '/help'))):
                            continue

                    action = msg.get('action')
                    if action and peer_id >= 2000000000:
                        action_type = action.get('type')
                        member_id = action.get('member_id')
                        if action_type == 'chat_invite_user' and member_id == bot_id:
                            logger.info(f"✅ Бот добавлен в беседу {peer_id}")
                            if is_audience_confirmed(peer_id):
                                send_message(peer_id, "✅ Бот уже настроен для этой аудитории.")
                            else:
                                request_audience_confirmation(peer_id)
                            continue
                        elif action_type == 'chat_kick_user' and member_id == bot_id:
                            logger.info(f"❌ Бот удалён из беседы {peer_id}")
                        elif action_type == 'chat_invite_user' and member_id != bot_id:
                            if is_audience_confirmed(peer_id) and is_welcome_enabled(peer_id):
                                welcome = get_welcome_message(peer_id)
                                if welcome:
                                    send_message(peer_id, f"👋 {get_user_mention(member_id, peer_id)}, {welcome}")

                    if text.startswith('/'):
                        handle_command(text, peer_id, sender_id)
                        continue

                    key = (peer_id, sender_id)
                    can_manage = can_manage_materials(sender_id, peer_id)

                    state_data = safe_menu_state_get(key)
                    if state_data and isinstance(state_data, dict) and state_data.get('mode') == 'manage':
                        handled = handle_manage_message(text, peer_id, sender_id, msg.get('conversation_message_id'))
                        if handled:
                            continue

                    if can_manage:
                        handled = handle_main_menu(text, peer_id, sender_id, msg.get('conversation_message_id'), True)
                        if handled:
                            continue

                elif event.type == VkBotEventType.GROUP_JOIN:
                    peer_id = event.object.peer_id
                    user_id = event.object.user_id
                    if peer_id >= 2000000000 and user_id == bot_id:
                        logger.info(f"✅ (запасное) Бот добавлен в беседу {peer_id}")
                        if is_audience_confirmed(peer_id):
                            send_message(peer_id, "✅ Бот уже настроен для этой аудитории.")
                        else:
                            request_audience_confirmation(peer_id)

                elif event.type == VkBotEventType.GROUP_LEAVE:
                    peer_id = event.object.peer_id
                    user_id = event.object.user_id
                    if peer_id >= 2000000000 and user_id == bot_id:
                        logger.info(f"❌ (запасное) Бот удалён из беседы {peer_id}")
        except Exception as e:
            import traceback
            logger.error(f"Ошибка в основном цикле:\n{traceback.format_exc()}")
            time.sleep(5)
            try:
                longpoll = VkBotLongPoll(vk_session, GROUP_ID, wait=45)
            except:
                pass

# ===== КОНЕЦ ЧАСТИ 4 =====

if __name__ == "__main__":
    main()
