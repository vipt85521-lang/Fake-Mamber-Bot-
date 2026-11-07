# ============================================================
# 🚀 TS SAJOL VC JOIN BOT
# Telegram Voice Chat Manager
# Stable Edition
# ============================================================

import os
import sys
import time
import asyncio
import logging
import sqlite3
import shutil
import uuid
from urllib.parse import urlparse, parse_qs

try:
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = hard if hard > 0 else 65535
    if target > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
except Exception:
    pass

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from telegram.constants import ParseMode

from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from telethon import TelegramClient, utils as telethon_utils

from telethon.errors import (
    ApiIdInvalidError,
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    FloodWaitError,
    UserAlreadyParticipantError,
    InviteHashInvalidError,
    InviteHashExpiredError,
    RPCError,
)

from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    CheckChatInviteRequest,
    GetFullChatRequest,
)

from telethon.tl.functions.channels import (
    GetFullChannelRequest,
)

from telethon.tl.functions.phone import (
    GetGroupParticipantsRequest,
)

from telethon.tl.types import (
    InputChannel,
    InputPeerChannel,
    InputPeerChat,
    InputGroupCall,
    ChatInviteAlready,
    PeerChannel,
)

from concurrent.futures import ThreadPoolExecutor

from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality, GroupCallConfig, MediaStream
from pytgcalls.exceptions import NoActiveGroupCall
from ntgcalls import ConnectionState

PyTgCalls.WORKERS = 1
shared_tgcalls_executor = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="PyTgCallsShared",
)

# ⚡ DISABLE STREAM DOWNLOADS (Prevents 33s-44s GetFileRequest FloodWaits & bot freezing)
try:
    from pytgcalls.mtproto.telethon_client import TelethonClient

    async def _disable_stream_download(self, *args, **kwargs):
        return None

    TelethonClient.download_stream = _disable_stream_download
except Exception:
    pass

# Suppress spammy network logs that slow down console I/O
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("pytgcalls").setLevel(logging.WARNING)


# ============================================================
# CONFIG
# ============================================================

BOT_NAME = "TS SAJOL VC JOIN BOT"

MAX_ACCOUNTS = 100

SILENT_AUDIO_STREAM = (
    "https://raw.githubusercontent.com/tgcallsjs/"
    "tgcalls/main/example/input.raw"
)

BOT_TOKEN = "9259090018:AAHC5ckm_9w-uNoNgRWXsUl_UdhZ1SNDKGs"

API_ID = 31908044  # Set your own Telegram API ID

API_HASH = "12ca45ff9dc183c3deba775ae82e2a6e"

OWNER_ID = 6349914249  # Set your own Telegram user ID


# ============================================================
# CONFIG CHECK
# ============================================================

if not BOT_TOKEN or BOT_TOKEN == "PASTE_BOT_TOKEN_HERE":
    raise RuntimeError(
        "BOT_TOKEN সেট করুন।"
    )

if not API_ID or API_ID == 0:
    raise RuntimeError(
        "API_ID সেট করুন।"
    )

if not API_HASH or API_HASH == "PASTE_API_HASH_HERE":
    raise RuntimeError(
        "API_HASH সেট করুন।"
    )

if not OWNER_ID or OWNER_ID == 0:
    raise RuntimeError(
        "OWNER_ID সেট করুন।"
    )


# ============================================================
# STORAGE
# ============================================================

DATA_DIR = "data"

SESSION_DIR = os.path.join(
    DATA_DIR,
    "sessions",
)

DB_FILE = os.path.join(
    DATA_DIR,
    "bot.db",
)

os.makedirs(
    DATA_DIR,
    exist_ok=True,
)

os.makedirs(
    SESSION_DIR,
    exist_ok=True,
)

silence_path = os.path.join(DATA_DIR, "silence.wav")
if not os.path.exists(silence_path):
    try:
        import wave
        with wave.open(silence_path, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(48000)
            wf.writeframes(b"\x00" * (48000 * 2))
    except Exception:
        pass


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(
    "TS_SAJOL_VC_JOIN_BOT"
)


# ============================================================
# RUNTIME
# ============================================================

clients = {}

calls = {}

active_call_chats = {}

desired_live = {}

stop_requested = set()

pending = {}

operation_locks = {}

live_monitor_task = None

rejoin_locks = {}

missing_counts = {}

user_active_live = {}

pending_live_links = {}

live_batch_running = False


# ============================================================
# PERFORMANCE
# ============================================================

JOIN_CONCURRENCY = 4
JOIN_STAGGER = 0.15
JOIN_ACCOUNT_TIMEOUT = 45

LEAVE_CONCURRENCY = 6
LEAVE_ACCOUNT_TIMEOUT = 25

REJOIN_CONCURRENCY = 3
REJOIN_ACCOUNT_TIMEOUT = 35

LIVE_MONITOR_INTERVAL = 10.0

MISSING_REQUIRED_SCANS = 2
MISSING_CONFIRM_DELAY = 0.40

REJOIN_VERIFY_DELAY = 0.60
REJOIN_VERIFY_CHECKS = 2
REJOIN_VERIFY_GAP = 0.50

REJOIN_RETRY_DELAY = 1.0
REJOIN_MAX_ATTEMPTS = 3
REJOIN_STAGGER = 0.20


# ============================================================
# DATABASE CONNECT
# ============================================================

def db_connect():
    conn = sqlite3.connect(
        DB_FILE,
        timeout=30,
        check_same_thread=False,
    )

    conn.row_factory = sqlite3.Row

    conn.execute(
        "PRAGMA journal_mode=WAL"
    )

    conn.execute(
        "PRAGMA synchronous=NORMAL"
    )

    conn.execute(
        "PRAGMA busy_timeout=30000"
    )

    return conn


# ============================================================
# DATABASE INIT
# ============================================================

def db_init():
    conn = db_connect()

    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                created_at INTEGER
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                phone TEXT NOT NULL,
                telegram_user_id INTEGER,
                session_name TEXT NOT NULL UNIQUE,
                created_at INTEGER,
                active_chat_id TEXT,
                UNIQUE(owner_id, phone)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                added_by INTEGER,
                created_at INTEGER
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                account_id INTEGER,
                operation TEXT,
                chat_id TEXT,
                status TEXT,
                error TEXT,
                created_at INTEGER
            )
            """
        )

        columns = [
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(accounts)"
            ).fetchall()
        ]

        if "active_chat_id" not in columns:
            conn.execute(
                """
                ALTER TABLE accounts
                ADD COLUMN active_chat_id TEXT
                """
            )

        conn.execute(
            """
            INSERT OR IGNORE INTO admins
            (
                user_id,
                added_by,
                created_at
            )
            VALUES (?, ?, ?)
            """,
            (
                OWNER_ID,
                OWNER_ID,
                time.time()
            ),
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_accounts_owner ON accounts(owner_id)"
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_accounts_phone ON accounts(phone)"
        )

        conn.execute(
            "UPDATE accounts SET active_chat_id=NULL"
        )

        conn.commit()

    finally:
        conn.close()


# ============================================================
# REGISTER USER
# ============================================================

def register_user(user):
    if not user:
        return

    conn = db_connect()

    try:
        conn.execute(
            """
            INSERT INTO users
            (
                user_id,
                username,
                first_name,
                created_at
            )
            VALUES (?, ?, ?, ?)

            ON CONFLICT(user_id)
            DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name
            """,
            (
                user.id,
                user.username or "",
                user.first_name or "",
                int(time.time()),
            ),
        )

        conn.commit()

    finally:
        conn.close()


# ============================================================
# OWNER
# ============================================================

def is_owner(user_id):
    return int(user_id) == int(OWNER_ID)


# ============================================================
# ADMIN
# ============================================================

def is_admin(user_id):
    if is_owner(user_id):
        return True

    conn = db_connect()

    try:
        row = conn.execute(
            """
            SELECT user_id
            FROM admins
            WHERE user_id=?
            """,
            (user_id,),
        ).fetchone()

        return row is not None

    finally:
        conn.close()


# ============================================================
# ACCOUNT COUNT
# ============================================================

def account_count(owner_id):
    conn = db_connect()

    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM accounts
            WHERE owner_id=?
            """,
            (owner_id,),
        ).fetchone()

        return int(row["c"])

    finally:
        conn.close()


# ============================================================
# GET ACCOUNTS
# ============================================================

def get_accounts(owner_id):
    conn = db_connect()

    try:
        rows = conn.execute(
            """
            SELECT *
            FROM accounts
            WHERE owner_id=?
            ORDER BY id ASC
            """,
            (owner_id,),
        ).fetchall()

        return [dict(r) for r in rows]

    finally:
        conn.close()


# ============================================================
# GET ACCOUNT
# ============================================================

def get_account(
    account_id,
    owner_id=None,
):
    conn = db_connect()

    try:
        if owner_id is None:
            row = conn.execute(
                """
                SELECT *
                FROM accounts
                WHERE id=?
                """,
                (account_id,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT *
                FROM accounts
                WHERE id=? AND owner_id=?
                """,
                (
                    account_id,
                    owner_id,
                ),
            ).fetchone()

        return dict(row) if row else None

    finally:
        conn.close()


# ============================================================
# MASK PHONE
# ============================================================

def mask_phone(phone):
    if not phone:
        return "••••"

    phone = str(phone)

    if len(phone) <= 4:
        return "••••" + phone

    return "••••" + phone[-4:]


# ============================================================
# SESSION NAME
# ============================================================

def safe_session_name(account_id):
    return os.path.join(
        SESSION_DIR,
        f"account_{account_id}",
    )


# ============================================================
# LOG OPERATION
# ============================================================

def log_operation(
    owner_id,
    account_id,
    operation,
    chat_id,
    status,
    error="",
):
    conn = db_connect()

    try:
        conn.execute(
            """
            INSERT INTO operations
            (
                owner_id,
                account_id,
                operation,
                chat_id,
                status,
                error,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_id,
                account_id,
                operation,
                str(chat_id or ""),
                status,
                str(error)[:1000],
                int(time.time()),
            ),
        )

        conn.commit()

    finally:
        conn.close()


# ============================================================
# LOCK
# ============================================================

def lock_for(user_id):
    if user_id not in operation_locks:
        operation_locks[user_id] = asyncio.Lock()

    return operation_locks[user_id]


def rejoin_lock_for(account_id):
    if account_id not in rejoin_locks:
        rejoin_locks[account_id] = asyncio.Lock()

    return rejoin_locks[account_id]


rejoin_sem = None

def get_rejoin_semaphore():
    global rejoin_sem
    if rejoin_sem is None:
        rejoin_sem = asyncio.Semaphore(REJOIN_CONCURRENCY)
    return rejoin_sem


# ============================================================
# PHONE NORMALIZE
# ============================================================

def normalize_phone(phone):
    phone = phone.strip()
    phone = phone.replace(" ", "")
    phone = phone.replace("-", "")
    phone = phone.replace("(", "")
    phone = phone.replace(")", "")

    return phone


# ============================================================
# MAIN KEYBOARD
# ============================================================

def main_keyboard(user_id):
    buttons = [
        [
            InlineKeyboardButton(
                "👤 My Accounts",
                callback_data="accounts",
            )
        ],
        [
            InlineKeyboardButton(
                "🔴 Join Live",
                callback_data="join_live",
            ),
            InlineKeyboardButton(
                "🔵 Stop Live",
                callback_data="stop_live",
            ),
        ],
        [
            InlineKeyboardButton(
                "📊 Live Status",
                callback_data="live_status",
            ),
            InlineKeyboardButton(
                "⚡ Force Rejoin",
                callback_data="force_rejoin",
            ),
        ],
        [
            InlineKeyboardButton(
                "⚙️ Settings",
                callback_data="settings",
            )
        ],
    ]

    if is_admin(user_id):
        buttons.append(
            [
                InlineKeyboardButton(
                    "🛠 Admin Panel",
                    callback_data="admin_panel",
                )
            ]
        )

    return InlineKeyboardMarkup(
        buttons
    )


# ============================================================
# START
# ============================================================

async def start(update, context):
    user = update.effective_user

    register_user(user)

    count = account_count(
        user.id
    )

    text = (
        "✨ <b>𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢</b>\n\n"
        "<b>𝐓𝐒 𝐒𝐀𝐉𝐎𝐋 𝐕𝐂 𝐉𝐎𝐈𝐍 𝐁𝐎𝐓</b>\n\n"
        "🚀 Telegram Voice Chat Manager\n"
        f"📱 Accounts: {count}/{MAX_ACCOUNTS}\n"
        "🟢 Bot Status: Online"
    )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(
            user.id
        ),
    )


async def safe_answer_query(query, *args, **kwargs):
    try:
        await query.answer(*args, **kwargs)
    except Exception:
        pass


# ============================================================
# ACCOUNTS MENU
# ============================================================

async def accounts_menu(update, context):
    query = update.callback_query

    await safe_answer_query(query)

    user_id = query.from_user.id

    register_user(
        query.from_user
    )

    page = 1
    if query.data and ":" in query.data:
        try:
            page = int(query.data.split(":")[1])
        except (IndexError, ValueError):
            page = 1

    rows = get_accounts(
        user_id
    )

    total_acc = len(rows)
    page_size = 16
    total_pages = max(1, (total_acc + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))

    start_idx = (page - 1) * page_size
    page_rows = rows[start_idx:start_idx + page_size]

    buttons = []
    row_pair = []

    for row in page_rows:
        btn = InlineKeyboardButton(
            f"👤 {mask_phone(row['phone'])}",
            callback_data=f"account_info:{row['id']}:{page}",
        )
        row_pair.append(btn)
        if len(row_pair) == 2:
            buttons.append(row_pair)
            row_pair = []

    if row_pair:
        buttons.append(row_pair)

    if total_pages > 1:
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"accounts:{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data=f"accounts:{page}"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"accounts:{page + 1}"))
        buttons.append(nav_row)

    buttons.append(
        [
            InlineKeyboardButton(
                "➕ Add Account",
                callback_data="add_account",
            )
        ]
    )

    buttons.append(
        [
            InlineKeyboardButton(
                "⬅️ Main Menu",
                callback_data="back_main",
            )
        ]
    )

    text = (
        "👤 <b>My Accounts</b>\n\n"
        f"📱 Total: <b>{total_acc}/{MAX_ACCOUNTS}</b>\n"
        f"📄 Page: <b>{page}/{total_pages}</b>\n\n"
        "নতুন Telegram account যোগ করতে "
        "➕ Add Account চাপুন।"
    )

    try:
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                buttons
            ),
        )
    except Exception:
        pass


# ============================================================
# ADD ACCOUNT
# ============================================================

async def add_account_button(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    user_id = query.from_user.id

    if account_count(user_id) >= MAX_ACCOUNTS:
        await query.edit_message_text(
            "❌ <b>Account Limit Reached</b>\n\n"
            f"সর্বোচ্চ {MAX_ACCOUNTS}টি account যোগ করা যাবে।",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Back",
                            callback_data="accounts",
                        )
                    ]
                ]
            ),
        )

        return

    pending[user_id] = {
        "state": "phone"
    }

    await query.edit_message_text(
        "📱 <b>Add Telegram Account</b>\n\n"
        "আপনার Telegram account-এর phone number পাঠান।\n\n"
        "উদাহরণ:\n"
        "<code>+8801XXXXXXXXX</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data="back_main",
                    )
                ]
            ]
        ),
    )


# ============================================================
# PROCESS PHONE
# ============================================================

async def process_phone(
    update,
    context,
):
    user = update.effective_user
    user_id = user.id

    state = pending.get(
        user_id
    )

    if not state:
        return False

    if state.get("state") != "phone":
        return False

    phone = normalize_phone(
        update.message.text
    )

    if not phone.startswith("+"):
        await update.message.reply_text(
            "❌ Phone number country code সহ দিন।\n\n"
            "উদাহরণ: +8801XXXXXXXXX"
        )

        return True

    if account_count(user_id) >= MAX_ACCOUNTS:
        pending.pop(
            user_id,
            None,
        )

        await update.message.reply_text(
            f"❌ সর্বোচ্চ {MAX_ACCOUNTS}টি account যোগ করা যাবে।",
            reply_markup=main_keyboard(
                user_id
            ),
        )

        return True

    conn = db_connect()

    try:
        exists = conn.execute(
            """
            SELECT id
            FROM accounts
            WHERE owner_id=? AND phone=?
            """,
            (
                user_id,
                phone,
            ),
        ).fetchone()

    finally:
        conn.close()

    if exists:
        pending.pop(
            user_id,
            None,
        )

        await update.message.reply_text(
            "⚠️ এই phone number আগে থেকেই account list-এ আছে।",
            reply_markup=main_keyboard(
                user_id
            ),
        )

        return True

    conn = db_connect()

    try:
        cur = conn.execute(
            """
            INSERT INTO accounts
            (
                owner_id,
                phone,
                telegram_user_id,
                session_name,
                created_at,
                active_chat_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                phone,
                None,
                f"pending_{user_id}_{int(time.time())}",
                int(time.time()),
                None,
            ),
        )

        account_id = cur.lastrowid

        session_name = safe_session_name(
            account_id
        )

        conn.execute(
            """
            UPDATE accounts
            SET session_name=?
            WHERE id=?
            """,
            (
                session_name,
                account_id,
            ),
        )

        conn.commit()

    finally:
        conn.close()

    status_msg = await update.message.reply_text(
        "⏳ <b>Telegram সার্ভারের সাথে কানেক্ট হচ্ছে...</b>\n\n"
        f"📱 Number: <code>{phone}</code>\n"
        "কোড পাঠানোর অনুরোধ করা হচ্ছে, একটু অপেক্ষা করুন...",
        parse_mode=ParseMode.HTML,
    )

    client = TelegramClient(
        session_name,
        API_ID,
        API_HASH,
        flood_sleep_threshold=0,
        request_retries=2,
        connection_retries=2,
        timeout=10,
    )

    try:
        await asyncio.wait_for(
            client.connect(),
            timeout=20,
        )

        sent_code = await asyncio.wait_for(
            client.send_code_request(phone),
            timeout=25,
        )

        clients[account_id] = client

        pending[user_id] = {
            "state": "code",
            "account_id": account_id,
            "phone": phone,
            "phone_code_hash": getattr(sent_code, "phone_code_hash", None),
        }

        await status_msg.edit_text(
            "📩 <b>Telegram Code Sent!</b>\n\n"
            f"📱 Number: <code>{phone}</code>\n\n"
            "আপনার Telegram app-এ আসা login code পাঠান।\n\n"
            "💡 <i>Code যদি 12345 হয় তবে 12345 লিখে পাঠান।</i>",
            parse_mode=ParseMode.HTML,
        )

    except ApiIdInvalidError:
        try:
            await client.disconnect()
        except Exception:
            pass
        await delete_account(account_id)
        pending.pop(user_id, None)
        await status_msg.edit_text("❌ API_ID / API_HASH ভুল।")

    except PhoneNumberInvalidError:
        try:
            await client.disconnect()
        except Exception:
            pass
        await delete_account(account_id)
        pending.pop(user_id, None)
        await status_msg.edit_text("❌ Phone number invalid। সঠিক নাম্বার দিন।")

    except FloodWaitError as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        await delete_account(account_id)
        pending.pop(user_id, None)
        await status_msg.edit_text(
            f"⏳ Telegram FloodWait দিয়েছে।\n"
            f"{e.seconds} সেকেন্ড পরে আবার চেষ্টা করুন।"
        )

    except asyncio.TimeoutError:
        try:
            await client.disconnect()
        except Exception:
            pass
        await delete_account(account_id)
        pending.pop(user_id, None)
        await status_msg.edit_text(
            "⏱️ Telegram সার্ভার থেকে সময়মত রেসপন্স আসেনি (Timeout)। আবার চেষ্টা করুন।"
        )

    except Exception as e:
        logger.exception("Phone login error")
        try:
            await client.disconnect()
        except Exception:
            pass
        await delete_account(account_id)
        pending.pop(user_id, None)
        await status_msg.edit_text(
            f"❌ Account authorization শুরু করা যায়নি।\n\nError: {str(e)[:300]}"
        )

    return True


# ============================================================
# PROCESS CODE
# ============================================================

async def process_code(
    update,
    context,
):
    user_id = update.effective_user.id

    state = pending.get(
        user_id
    )

    if not state:
        return False

    if state.get("state") != "code":
        return False

    code = update.message.text.strip().replace(" ", "").replace("-", "")

    account_id = state["account_id"]
    phone = state["phone"]
    phone_code_hash = state.get("phone_code_hash")

    client = clients.get(
        account_id
    )

    if not client:
        pending.pop(
            user_id,
            None,
        )

        await update.message.reply_text(
            "❌ Login session পাওয়া যায়নি। আবার Add Account করুন।"
        )

        return True

    status_msg = await update.message.reply_text(
        "⏳ কোড যাচাই করা হচ্ছে..."
    )

    try:
        sign_kwargs = {"phone": phone, "code": code}
        if phone_code_hash:
            sign_kwargs["phone_code_hash"] = phone_code_hash

        await asyncio.wait_for(
            client.sign_in(**sign_kwargs),
            timeout=25,
        )

        me = await asyncio.wait_for(
            client.get_me(),
            timeout=15,
        )

        conn = db_connect()

        try:
            conn.execute(
                """
                UPDATE accounts
                SET telegram_user_id=?
                WHERE id=? AND owner_id=?
                """,
                (
                    me.id,
                    account_id,
                    user_id,
                ),
            )

            conn.commit()

        finally:
            conn.close()

        pending.pop(
            user_id,
            None,
        )

        await status_msg.edit_text(
            "✅ <b>Account Added Successfully!</b>\n\n"
            f"👤 Account: {mask_phone(phone)}\n"
            f"🆔 Telegram ID: <code>{me.id}</code>\n\n"
            "এই account এখন Join Live ব্যবহার করতে পারবে।",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(
                user_id
            ),
        )

    except SessionPasswordNeededError:
        pending[user_id] = {
            **state,
            "state": "password",
        }

        await status_msg.edit_text(
            "🔐 <b>Two-Step Verification (2FA)</b>\n\n"
            "এই account-এ 2FA চালু আছে। আপনার 2FA Password লিখে পাঠান।",
            parse_mode=ParseMode.HTML,
        )

    except PhoneCodeInvalidError:
        await status_msg.edit_text(
            "❌ OTP code ভুল। আবার সঠিক code পাঠান।"
        )

    except PhoneCodeExpiredError:
        pending.pop(
            user_id,
            None,
        )
        await delete_account(account_id)
        await status_msg.edit_text(
            "❌ OTP code expired। আবার Add Account করুন。",
            reply_markup=main_keyboard(
                user_id
            ),
        )

    except FloodWaitError as e:
        await status_msg.edit_text(
            f"⏳ Telegram FloodWait দিয়েছে ({e.seconds}s)। কিছুক্ষণ পর চেষ্টা করুন।"
        )

    except asyncio.TimeoutError:
        await status_msg.edit_text(
            "⏱️ কোড যাচাই করার সময় টাইমআউট হয়েছে। আবার চেষ্টা করুন।"
        )

    except Exception as e:
        logger.exception(
            "OTP error"
        )

        await status_msg.edit_text(
            f"❌ Login failed:\n{str(e)[:300]}"
        )

    return True


# ============================================================
# PROCESS PASSWORD
# ============================================================

async def process_password(
    update,
    context,
):
    user_id = update.effective_user.id

    state = pending.get(
        user_id
    )

    if not state:
        return False

    if state.get("state") != "password":
        return False

    password = update.message.text.strip()

    account_id = state["account_id"]
    phone = state.get("phone", "")

    client = clients.get(
        account_id
    )

    if not client:
        pending.pop(
            user_id,
            None,
        )

        await update.message.reply_text(
            "❌ Login session পাওয়া যায়নি।"
        )

        return True

    status_msg = await update.message.reply_text(
        "⏳ 2FA পাসওয়ার্ড যাচাই করা হচ্ছে..."
    )

    try:
        await asyncio.wait_for(
            client.sign_in(
                password=password
            ),
            timeout=25,
        )

        me = await asyncio.wait_for(
            client.get_me(),
            timeout=15,
        )

        conn = db_connect()

        try:
            conn.execute(
                """
                UPDATE accounts
                SET telegram_user_id=?
                WHERE id=? AND owner_id=?
                """,
                (
                    me.id,
                    account_id,
                    user_id,
                ),
            )

            conn.commit()

        finally:
            conn.close()

        pending.pop(
            user_id,
            None,
        )

        await status_msg.edit_text(
            "✅ <b>Account Added Successfully!</b>\n\n"
            f"👤 Account: {mask_phone(phone)}\n"
            f"🆔 Telegram ID: <code>{me.id}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(
                user_id
            ),
        )

    except FloodWaitError as e:
        await status_msg.edit_text(
            f"⏳ Telegram FloodWait দিয়েছে ({e.seconds}s)। কিছুক্ষণ পর চেষ্টা করুন।"
        )

    except asyncio.TimeoutError:
        await status_msg.edit_text(
            "⏱️ 2FA ভেরিফিকেশন টাইমআউট হয়েছে। আবার চেষ্টা করুন।"
        )

    except Exception as e:
        logger.exception(
            "2FA error"
        )

        await status_msg.edit_text(
            f"❌ 2FA Login failed:\n{str(e)[:300]}"
        )

    return True


# ============================================================
# DELETE ACCOUNT INTERNAL
# ============================================================

async def delete_account(
    account_id
):
    chat_id = active_call_chats.pop(
        account_id,
        None,
    )

    call = calls.pop(
        account_id,
        None,
    )

    if call and chat_id:
        try:
            await asyncio.wait_for(call.leave_call(int(chat_id)), timeout=3)
        except Exception:
            pass

    desired_live.pop(
        account_id,
        None,
    )

    stop_requested.discard(
        account_id
    )

    missing_counts.pop(
        account_id,
        None,
    )

    rejoin_locks.pop(
        account_id,
        None,
    )

    client = clients.pop(
        account_id,
        None,
    )

    if client:
        try:
            await client.disconnect()
        except Exception:
            pass

    conn = db_connect()

    row = None

    try:
        row = conn.execute(
            """
            SELECT session_name
            FROM accounts
            WHERE id=?
            """,
            (account_id,),
        ).fetchone()

        conn.execute(
            """
            DELETE FROM accounts
            WHERE id=?
            """,
            (account_id,),
        )

        conn.commit()

    finally:
        conn.close()

    if row:
        try:
            session_file = row["session_name"]

            if os.path.exists(
                session_file + ".session"
            ):
                os.remove(
                    session_file + ".session"
                )

        except Exception:
            pass


# ============================================================
# GET CLIENT
# ============================================================

async def get_client_for_account(
    account
):
    account_id = account["id"]

    client = clients.get(
        account_id
    )

    if client:
        try:
            if client.is_connected():
                return client

            await asyncio.wait_for(
                client.connect(),
                timeout=15,
            )

            authorized = await asyncio.wait_for(
                client.is_user_authorized(),
                timeout=15,
            )

            if authorized:
                return client

        except Exception as e:
            logger.warning(
                "Client reconnect failed account=%s error=%s",
                account_id,
                str(e)[:300],
            )

        # Do NOT disconnect or destroy client if actively participating in a live call
        if account_id in active_call_chats and calls.get(account_id) is not None:
            logger.warning(
                "Account %s client reconnect had error, but active call exists. Preserving client.",
                account_id,
            )
            return client

        try:
            await client.disconnect()
        except Exception:
            pass

        clients.pop(
            account_id,
            None,
        )

    client = TelegramClient(
        account["session_name"],
        API_ID,
        API_HASH,
        flood_sleep_threshold=0,
        request_retries=5,
        connection_retries=5,
        timeout=15,
        auto_reconnect=True,
    )

    try:
        await asyncio.wait_for(
            client.connect(),
            timeout=20,
        )

        authorized = await asyncio.wait_for(
            client.is_user_authorized(),
            timeout=20,
        )

        if not authorized:
            raise RuntimeError(
                "Saved Telegram session is not authorized."
            )

    except Exception as auth_err:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass

        try:
            if hasattr(client, "session") and hasattr(client.session, "close"):
                client.session.close()
        except Exception:
            pass

        raise auth_err

    clients[account_id] = client

    return client


# ============================================================
# PARSE TELEGRAM LINK
# ============================================================

def parse_live_link(link):
    link = link.strip()
    if not link:
        raise ValueError("Link খালি হতে পারে না।")

    if link.startswith("@"):
        return {
            "type": "username",
            "username": link[1:],
            "livestream": None,
            "raw_link": link,
        }

    raw_link = link
    if not link.startswith(("http://", "https://")):
        link = "https://" + link

    parsed = urlparse(link)

    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    if host not in ("t.me", "telegram.me", "telegram.dog"):
        raise ValueError("শুধু Telegram link ব্যবহার করুন।")

    path = parsed.path.strip("/")
    query = parse_qs(parsed.query)

    livestream = (
        query.get("livestream", [None])[0]
        or query.get("videochat", [None])[0]
        or query.get("voicechat", [None])[0]
    )

    if path.startswith("+"):
        return {
            "type": "invite",
            "invite": path[1:],
            "livestream": livestream,
            "raw_link": raw_link,
        }

    if path.startswith("joinchat/"):
        return {
            "type": "invite",
            "invite": path.split("joinchat/", 1)[1],
            "livestream": livestream,
            "raw_link": raw_link,
        }

    if path.startswith("c/"):
        parts = path.split("/")
        if len(parts) >= 2 and parts[1].isdigit():
            chat_id = int("-100" + parts[1])
            return {
                "type": "channel_id",
                "chat_id": chat_id,
                "livestream": livestream,
                "raw_link": raw_link,
            }

    username = path.split("/", 1)[0]
    if not username:
        raise ValueError("Telegram username পাওয়া যায়নি।")

    return {
        "type": "username",
        "username": username,
        "livestream": livestream,
        "raw_link": raw_link,
    }


# ============================================================
# RESOLVE CHAT
# ============================================================

async def resolve_chat(
    client,
    info,
):
    if info["type"] == "invite":
        try:
            result = await client(
                ImportChatInviteRequest(
                    info["invite"]
                )
            )

            if result.chats:
                return result.chats[0]

        except UserAlreadyParticipantError:
            try:
                check = await client(
                    CheckChatInviteRequest(
                        info["invite"]
                    )
                )
                if hasattr(check, "chat") and check.chat:
                    return check.chat
            except Exception:
                pass

            try:
                entity = await client.get_entity(
                    "https://t.me/+"
                    + info["invite"]
                )
                return entity
            except Exception:
                pass

            target_id = info.get("chat_id")
            if target_id:
                try:
                    return await client.get_entity(int(target_id))
                except Exception:
                    pass

            raise RuntimeError(
                "Invite chat resolve করা যায়নি (Already a participant)।"
            )

        except InviteHashInvalidError:
            raise RuntimeError(
                "Invite link invalid."
            )

        except InviteHashExpiredError:
            raise RuntimeError(
                "Invite link expired."
            )

    if info["type"] == "channel_id":
        target_id = info["chat_id"]
        try:
            return await client.get_entity(target_id)
        except Exception:
            pass

        try:
            clean_id = abs(int(str(target_id).replace("-100", "")))
            return await client.get_entity(PeerChannel(clean_id))
        except Exception:
            pass

        try:
            async for dialog in client.iter_dialogs(limit=50):
                if dialog.id == target_id:
                    return dialog.entity
        except Exception:
            pass

        return await client.get_entity(target_id)

    entity = await client.get_entity(
        info["username"]
    )

    try:
        from telethon.tl.functions.channels import JoinChannelRequest
        await client(JoinChannelRequest(entity))
    except Exception:
        pass

    return entity


# ============================================================
# RESOLVE LIVE TARGET
# ============================================================

async def resolve_live_target(
    client,
    info,
):
    entity = await resolve_chat(
        client,
        info,
    )

    chat_id = telethon_utils.get_peer_id(
        entity
    )

    if not info.get("chat_id"):
        info["chat_id"] = chat_id

    return entity, chat_id


# ============================================================
# ============================================================
# CLEANUP RESOURCES
# ============================================================

async def cleanup_account_resources(
    account_id,
    disconnect_client=False,
):
    old_call = calls.pop(
        account_id,
        None,
    )

    if old_call is not None:
        chat_id = active_call_chats.pop(
            account_id,
            None,
        )
        if chat_id:
            try:
                await asyncio.wait_for(
                    old_call.leave_call(int(chat_id)),
                    timeout=3,
                )
            except Exception:
                pass

        try:
            binding = getattr(old_call, "_binding", None)
            if binding and hasattr(binding, "stop"):
                if chat_id:
                    try:
                        await binding.stop(int(chat_id))
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            del old_call
        except Exception:
            pass

    if disconnect_client:
        client = clients.pop(
            account_id,
            None,
        )

        if client is not None:
            try:
                if client.is_connected():
                    await asyncio.wait_for(
                        client.disconnect(),
                        timeout=3,
                    )
            except Exception:
                pass

            try:
                if hasattr(client, "session") and hasattr(client.session, "close"):
                    client.session.close()
            except Exception:
                pass

            try:
                del client
            except Exception:
                pass


# ============================================================
# DROP CALL
# ============================================================

def drop_call_object(
    account_id
):
    active_call_chats.pop(
        account_id,
        None,
    )

    old_call = calls.pop(
        account_id,
        None,
    )

    if old_call is None:
        return

    try:
        del old_call
    except Exception:
        pass


# ============================================================
# CREATE CALL
# ============================================================

async def create_fresh_call(
    account_id,
    client,
):
    drop_call_object(
        account_id
    )

    call = PyTgCalls(
        client,
        workers=1,
    )

    try:
        call.executor.shutdown(wait=False)
    except Exception:
        pass
    call.executor = shared_tgcalls_executor

    try:
        await asyncio.wait_for(
            call.start(),
            timeout=15,
        )

    except asyncio.TimeoutError:
        try:
            del call
        except Exception:
            pass

        raise RuntimeError(
            "PyTgCalls start timeout"
        )

    call.account_id = account_id

    @call.on_update()
    async def _on_call_update(*args, **kwargs):
        try:
            update = args[1] if len(args) > 1 else (args[0] if args else kwargs.get("update"))
            from pytgcalls.types import ChatUpdate
            if isinstance(update, ChatUpdate):
                if update.status & (
                    ChatUpdate.Status.CLOSED_VOICE_CHAT
                    | ChatUpdate.Status.DISCARDED_CALL
                ):
                    logger.warning(
                        "Voice chat ended/closed on Telegram for account %s (status=%s)",
                        account_id,
                        update.status,
                    )
                    active_call_chats.pop(account_id, None)
                    calls.pop(account_id, None)
                    desired_live.pop(account_id, None)
                    missing_counts.pop(account_id, None)
                    return

                if update.status & (
                    ChatUpdate.Status.KICKED
                    | ChatUpdate.Status.LEFT_GROUP
                ):
                    logger.warning(
                        "Telegram ChatUpdate kick/leave for account %s (status=%s)",
                        account_id,
                        update.status,
                    )
                    active_call_chats.pop(account_id, None)
                    calls.pop(account_id, None)
                    if account_id in desired_live and account_id not in stop_requested:
                        schedule_account_rejoin(account_id)
        except Exception as e:
            logger.debug("_on_call_update error account %s: %s", account_id, e)

    calls[account_id] = call

    return call


# ============================================================
# JOIN ONE
# ============================================================

async def join_one_account(
    owner_id,
    account,
    info,
):
    if not isinstance(account, dict):
        account = dict(account)
    account_id = account["id"]

    try:
        if account_id in stop_requested:
            return (
                False,
                (
                    f"⛔ {mask_phone(account['phone'])}"
                    " → Stopped"
                ),
            )

        client = await get_client_for_account(
            account
        )

        entity, chat_id = await asyncio.wait_for(
            resolve_live_target(
                client,
                info,
            ),
            timeout=20,
        )

        # Check if account was in a different call before
        old_chat = active_call_chats.get(account_id)
        if old_chat and int(old_chat) != int(chat_id):
            await cleanup_account_resources(account_id, disconnect_client=False)

        call = calls.get(
            account_id
        )

        if call is None or not getattr(call, "_is_running", False):
            call = await create_fresh_call(
                account_id,
                client,
            )

        config = GroupCallConfig(
            invite_hash=info.get(
                "livestream"
            ),
            auto_start=False,
        )

        # If already joined and active in this call, do not replay
        if (
            call is not None
            and active_call_chats.get(account_id) == chat_id
        ):
            binding = getattr(call, "_binding", None)
            if binding and hasattr(binding, "calls"):
                try:
                    active = await asyncio.wait_for(binding.calls(), timeout=1.5)
                    if int(chat_id) in active:
                        missing_counts[account_id] = 0
                        return (
                            True,
                            (
                                f"✅ {mask_phone(account['phone'])}"
                                " → Already in Live"
                            ),
                        )
                except Exception:
                    pass

        await asyncio.wait_for(
            call.play(
                chat_id,
                stream=None,
                config=config,
            ),
            timeout=25,
        )

        active_call_chats[
            account_id
        ] = chat_id

        desired_live[
            account_id
        ] = {
            "owner_id": owner_id,
            "chat_id": chat_id,
            "info": info,
        }

        existing_live = user_active_live.get(owner_id)
        orig_start = existing_live.get("start_time") if existing_live else None

        user_active_live[owner_id] = {
            "link": info.get("raw_link") or (f"https://t.me/{info.get('username')}" if info.get("username") else "Live Voice Chat"),
            "info": info,
            "chat_id": str(chat_id),
            "start_time": orig_start or time.time(),
        }

        missing_counts[
            account_id
        ] = 0

        conn = db_connect()

        try:
            conn.execute(
                """
                UPDATE accounts
                SET active_chat_id=?
                WHERE id=? AND owner_id=?
                """,
                (
                    str(chat_id),
                    account_id,
                    owner_id,
                ),
            )

            conn.commit()

        finally:
            conn.close()

        log_operation(
            owner_id,
            account_id,
            "join_live",
            chat_id,
            "success",
        )

        return (
            True,
            (
                f"✅ {mask_phone(account['phone'])}"
                " → Joined Live"
            ),
        )

    except NoActiveGroupCall:
        await cleanup_account_resources(
            account_id,
            disconnect_client=False,
        )

        desired_live.pop(
            account_id,
            None,
        )

        missing_counts.pop(
            account_id,
            None,
        )

        log_operation(
            owner_id,
            account_id,
            "join_live",
            "",
            "failed",
            "No active group call",
        )

        return (
            False,
            (
                f"❌ {mask_phone(account['phone'])}"
                " → Live/Voice Chat চালু নেই"
            ),
        )

    except FloodWaitError as e:
        await cleanup_account_resources(
            account_id,
            disconnect_client=False,
        )

        log_operation(
            owner_id,
            account_id,
            "join_live",
            "",
            "failed",
            f"FloodWait {e.seconds}s",
        )

        return (
            False,
            (
                f"⏳ {mask_phone(account['phone'])}"
                f" → FloodWait {e.seconds}s"
            ),
        )

    except Exception as e:
        err_msg = str(e)
        if "not authorized" in err_msg.lower():
            logger.warning(
                "Account %s (%s) session expired / logged out",
                account_id,
                mask_phone(account.get("phone")),
            )
            display_err = "Session Expired / Logged Out"
            desired_live.pop(account_id, None)
            missing_counts.pop(account_id, None)
        else:
            logger.exception(
                "Join error account=%s",
                account_id,
            )
            display_err = err_msg[:200]

        drop_call_object(account_id)

        log_operation(
            owner_id,
            account_id,
            "join_live",
            "",
            "failed",
            err_msg,
        )

        return (
            False,
            (
                f"❌ {mask_phone(account['phone'])}"
                f" → {display_err}"
            ),
        )


# ============================================================
# SAFE JOIN
# ============================================================

async def safe_join_one_account(
    owner_id,
    account,
    info,
):
    if not isinstance(account, dict):
        account = dict(account)
    account_id = account["id"]

    try:
        return await asyncio.wait_for(
            join_one_account(
                owner_id,
                account,
                info,
            ),
            timeout=JOIN_ACCOUNT_TIMEOUT,
        )

    except asyncio.TimeoutError:
        logger.error(
            "JOIN TIMEOUT account=%s",
            account_id,
        )

        drop_call_object(account_id)

        return (
            False,
            (
                f"⏱️ {mask_phone(account['phone'])}"
                " → Timeout"
            ),
        )

    except asyncio.CancelledError:
        raise

    except Exception as e:
        logger.warning(
            "Safe join error account=%s error=%s",
            account_id,
            str(e)[:200],
        )

        drop_call_object(account_id)

        return (
            False,
            (
                f"❌ {mask_phone(account['phone'])}"
                f" → {str(e)[:200]}"
            ),
        )


# ============================================================
# JOIN ALL
# ============================================================

async def join_all(
    owner_id,
    link,
    message,
    count=None,
):
    global live_batch_running

    lock = lock_for(owner_id)
    if lock.locked():
        await message.reply_text(
            "⏳ একটি operation ইতিমধ্যে চলছে।",
            reply_markup=main_keyboard(owner_id),
        )
        return

    async with lock:
        try:
            info = parse_live_link(
                link
            )

        except Exception as e:
            await message.reply_text(
                f"❌ Link Error:\n{str(e)}",
                reply_markup=main_keyboard(
                    owner_id
                ),
            )

            return

        accounts = get_accounts(
            owner_id
        )

        if not accounts:
            await message.reply_text(
                "❌ আপনার কোনো Telegram account যোগ করা নেই।",
                reply_markup=main_keyboard(
                    owner_id
                ),
            )

            return

        if count is not None and isinstance(count, int) and count > 0:
            accounts = accounts[:count]

        live_batch_running = True

        try:
            for account in accounts:
                stop_requested.discard(
                    account["id"]
                )

                missing_counts[
                    account["id"]
                ] = 0

                desired_live[account["id"]] = {
                    "owner_id": owner_id,
                    "chat_id": None,
                    "info": info,
                }

            user_active_live[owner_id] = {
                "link": info.get("raw_link") or (f"https://t.me/{info.get('username')}" if info.get("username") else "Live Voice Chat"),
                "info": info,
                "chat_id": None,
                "start_time": time.time(),
            }

            status_message = await message.reply_text(
                "🔴 <b>Joining Live...</b>\n\n"
                f"📱 Total: {len(accounts)}\n"
                f"⚡ Parallel: {JOIN_CONCURRENCY}\n"
                "🛡️ Anti-Freeze: ON\n"
                "⏳ Please wait...",
                parse_mode=ParseMode.HTML,
            )

            results = [None] * len(
                accounts
            )

            success = 0
            failed = 0
            completed = 0

            semaphore = asyncio.Semaphore(
                JOIN_CONCURRENCY
            )

            async def worker(
                index,
                account,
            ):
                if index > 0:
                    await asyncio.sleep(
                        index * JOIN_STAGGER
                    )

                async with semaphore:
                    if account["id"] in stop_requested:
                        return (
                            False,
                            (
                                f"⛔ {mask_phone(account['phone'])}"
                                " → Stopped"
                            ),
                        )

                    return await safe_join_one_account(
                        owner_id,
                        account,
                        info,
                    )

            tasks = [
                asyncio.create_task(
                    worker(
                        index,
                        account,
                    )
                )
                for index, account in enumerate(
                    accounts
                )
            ]

            try:
                for future in asyncio.as_completed(
                    tasks
                ):
                    try:
                        ok, result = await future

                    except asyncio.CancelledError:
                        raise

                    except Exception as e:
                        ok = False
                        result = (
                            f"❌ Worker failed: "
                            f"{str(e)[:200]}"
                        )

                    completed += 1

                    if ok:
                        success += 1
                    else:
                        failed += 1

                    results[
                        completed - 1
                    ] = result

                    if (
                        completed == 1
                        or completed % 4 == 0
                        or completed == len(accounts)
                    ):
                        visible = [
                            x
                            for x in results
                            if x
                        ]

                        try:
                            await status_message.edit_text(
                                "🔴 <b>Joining Live...</b>\n\n"
                                f"📱 Total: {len(accounts)}\n"
                                f"⚡ Completed: "
                                f"{completed}/{len(accounts)}\n"
                                f"✅ Success: {success}\n"
                                f"❌ Failed: {failed}\n\n"
                                + "\n".join(
                                    visible[-8:]
                                ),
                                parse_mode=ParseMode.HTML,
                            )

                        except Exception:
                            pass

            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()

                await asyncio.gather(
                    *tasks,
                    return_exceptions=True,
                )

            # Build clean final summary
            success_list = [x for x in results if x and x.startswith("✅")]
            failed_list = [x for x in results if x and not x.startswith("✅")]

            lines = [
                "🔴 <b>JOIN LIVE RESULT</b>",
                "",
                f"📱 Total Accounts: <b>{len(accounts)}</b>",
                f"⚡ Completed: <b>{completed}/{len(accounts)}</b>",
                f"✅ Joined: <b>{success}</b>",
                f"❌ Failed: <b>{failed}</b>",
                "",
            ]

            if failed_list:
                lines.append("<b>Failed Details:</b>")
                lines.extend(failed_list[:15])
                if len(failed_list) > 15:
                    lines.append(f"<i>... and {len(failed_list) - 15} more</i>")
            elif success_list:
                lines.append("<b>Joined Accounts:</b>")
                lines.extend(success_list[:10])
                if len(success_list) > 10:
                    lines.append(f"<i>... and {len(success_list) - 10} more</i>")

            final_text = "\n".join(lines)

            try:
                await status_message.edit_text(
                    final_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_keyboard(
                        owner_id
                    ),
                )

            except Exception:
                try:
                    clean_text = final_text.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", "")
                    await status_message.edit_text(
                        clean_text[:4000],
                        reply_markup=main_keyboard(
                            owner_id
                        ),
                    )
                except Exception:
                    pass

            if success > 0:
                cur_live = user_active_live.get(owner_id) or {}
                real_chat_id = cur_live.get("chat_id")
                if not real_chat_id:
                    for acc in accounts:
                        if acc["id"] in active_call_chats:
                            real_chat_id = str(active_call_chats[acc["id"]])
                            break
                user_active_live[owner_id] = {
                    "link": info.get("raw_link") or link,
                    "info": info,
                    "chat_id": str(real_chat_id or ""),
                    "start_time": cur_live.get("start_time") or time.time(),
                }

        finally:
            live_batch_running = False


# ============================================================
# PARTICIPANTS
# ============================================================

async def get_voice_chat_participant_ids(
    client,
    chat_id,
):
    try:
        input_peer = await client.get_input_entity(
            chat_id
        )

        input_call = None

        if isinstance(
            input_peer,
            InputPeerChannel,
        ):
            channel = InputChannel(
                input_peer.channel_id,
                input_peer.access_hash,
            )

            full = await client(
                GetFullChannelRequest(
                    channel=channel
                )
            )

            input_call = getattr(
                full.full_chat,
                "call",
                None,
            )

        elif isinstance(
            input_peer,
            InputPeerChat,
        ):
            full = await client(
                GetFullChatRequest(
                    chat_id=input_peer.chat_id
                )
            )

            input_call = getattr(
                full.full_chat,
                "call",
                None,
            )

        else:
            return None

        if input_call is None:
            return set()

        if not isinstance(
            input_call,
            InputGroupCall,
        ):
            return set()

        participant_ids = set()

        offset = ""

        while True:
            result = await client(
                GetGroupParticipantsRequest(
                    call=input_call,
                    ids=[],
                    sources=[],
                    offset=offset,
                    limit=100,
                )
            )

            participants = getattr(
                result,
                "participants",
                [],
            )

            for participant in participants:
                peer = getattr(
                    participant,
                    "peer",
                    None,
                )

                user_id = getattr(
                    peer,
                    "user_id",
                    None,
                )

                if user_id is None:
                    user_id = getattr(
                        participant,
                        "user_id",
                        None,
                    )

                if user_id is not None:
                    try:
                        participant_ids.add(
                            int(user_id)
                        )
                    except Exception:
                        pass

            next_offset = getattr(
                result,
                "next_offset",
                None,
            )

            if not next_offset:
                break

            if next_offset == offset:
                break

            offset = next_offset

        return participant_ids

    except FloodWaitError:
        raise

    except RPCError as e:
        logger.warning(
            "Participant RPC error: %s",
            str(e)[:300],
        )

        return None

    except Exception as e:
        logger.warning(
            "Participant scan failed: %s",
            str(e)[:300],
        )

        return None


# ============================================================
# CHECK PARTICIPANT
# ============================================================

voice_chat_participants_cache = {}

async def is_account_in_voice_chat(
    client,
    chat_id,
    telegram_user_id,
):
    now = time.time()
    cached = voice_chat_participants_cache.get(chat_id)
    if cached and (now - cached[0]) < 8.0:
        participant_ids = cached[1]
    else:
        participant_ids = await get_voice_chat_participant_ids(
            client,
            chat_id,
        )
        if participant_ids is not None:
            voice_chat_participants_cache[chat_id] = (now, participant_ids)
        elif cached:
            participant_ids = cached[1]

    if participant_ids is None:
        return None

    return (
        int(telegram_user_id)
        in participant_ids
    )


# ============================================================
# PYTGCALLS CONNECTION HOOK
# ============================================================

_orig_handle_connection_changed = PyTgCalls._handle_connection_changed

async def _hooked_handle_connection_changed(self, chat_id: int, net_state):
    try:
        await _orig_handle_connection_changed(self, chat_id, net_state)
    except Exception as e:
        logger.warning("Original _handle_connection_changed error: %s", e)

    try:
        state = getattr(net_state, "state", None)
        account_id = getattr(self, "account_id", None)
        if account_id is not None and state is not None:
            if state in (ConnectionState.CLOSED, ConnectionState.FAILED):
                logger.warning(
                    "NTgCalls connection lost for account %s (state=%s). Marking disconnected.",
                    account_id,
                    state,
                )
                active_call_chats.pop(account_id, None)
                calls.pop(account_id, None)
                if account_id in desired_live and account_id not in stop_requested:
                    schedule_account_rejoin(account_id)
    except Exception as hook_err:
        logger.warning("Error in hooked connection handler: %s", hook_err)

PyTgCalls._handle_connection_changed = _hooked_handle_connection_changed


# ============================================================
# SEQUENTIAL REJOIN QUEUE
# ============================================================

rejoin_queue = None
queued_rejoin_accounts = set()

def get_rejoin_queue():
    global rejoin_queue
    if rejoin_queue is None:
        rejoin_queue = asyncio.Queue()
    return rejoin_queue

def schedule_account_rejoin(account_id):
    if account_id in stop_requested or account_id not in desired_live:
        return
    if account_id not in queued_rejoin_accounts:
        queued_rejoin_accounts.add(account_id)
        get_rejoin_queue().put_nowait(account_id)
        logger.info(
            "Queued account %s for smooth rejoin (queue size=%d)",
            account_id,
            get_rejoin_queue().qsize(),
        )

async def rejoin_worker_loop():
    logger.info("REJOIN WORKER LOOP STARTED")
    q = get_rejoin_queue()
    while True:
        try:
            account_id = await q.get()
            queued_rejoin_accounts.discard(account_id)

            if account_id in stop_requested or account_id not in desired_live:
                continue

            live_data = desired_live.get(account_id)
            if not live_data:
                continue

            owner_id = live_data.get("owner_id")
            info = live_data.get("info")
            if not owner_id or not info:
                continue

            account = get_account(account_id, owner_id)
            if not account:
                desired_live.pop(account_id, None)
                continue

            # Verify it's not already happily active in call
            call = calls.get(account_id)
            if call is not None and account_id in active_call_chats:
                continue

            logger.info(
                "Smoothly rejoining account %s (%s)...",
                account_id,
                mask_phone(account.get("phone")),
            )
            drop_call_object(account_id)

            ok, result = await safe_join_one_account(
                owner_id,
                account,
                info,
            )

            if ok:
                missing_counts[account_id] = 0
                logger.info(
                    "Account %s auto-rejoined successfully: %s",
                    account_id,
                    result,
                )
            else:
                res_str = str(result).lower()
                if "not authorized" in res_str or "session expired" in res_str:
                    logger.warning(
                        "Account %s session not authorized/expired, removing from desired_live",
                        account_id,
                    )
                    desired_live.pop(account_id, None)
                    missing_counts.pop(account_id, None)
                elif "চালু নেই" in res_str or "no active group call" in res_str:
                    logger.warning(
                        "Live voice chat is closed on Telegram. Removing account %s from desired_live.",
                        account_id,
                    )
                    desired_live.pop(account_id, None)
                    missing_counts.pop(account_id, None)
                else:
                    logger.warning(
                        "Account %s auto-rejoin failed: %s",
                        account_id,
                        result,
                    )
                    await asyncio.sleep(2.5)
                    schedule_account_rejoin(account_id)

            # Smooth delay between individual account joins (no massive avalanches!)
            await asyncio.sleep(1.5)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("Rejoin worker loop exception: %s", e)
            await asyncio.sleep(1.0)

    logger.info("REJOIN WORKER LOOP STOPPED")


# ============================================================
# MONITOR ONE
# ============================================================

async def monitor_one_account(
    account_id,
    live_data,
):
    if account_id in stop_requested or account_id not in desired_live:
        return

    owner_id = live_data.get(
        "owner_id"
    )

    chat_id = live_data.get(
        "chat_id"
    )

    info = live_data.get(
        "info"
    ) or {}

    if not owner_id:
        return

    account = get_account(
        account_id,
        owner_id,
    )

    if not account:
        desired_live.pop(
            account_id,
            None,
        )
        return

    call = calls.get(account_id)

    # If call is None or account_id not in active_call_chats:
    # Genuinely unjoined or dropped -> schedule smooth rejoin!
    if call is None or account_id not in active_call_chats:
        schedule_account_rejoin(account_id)
        return

    # Call exists and account is registered in active_call_chats:
    # The account is healthy and participating in voice chat.
    # Real disconnects are caught immediately by event hooks (_hooked_handle_connection_changed & _on_call_update).
    # Never forcefully drop active calls here to avoid false-positive drop cascades!
    missing_counts[account_id] = 0


# ============================================================
# SAFE MONITOR
# ============================================================

async def safe_monitor_one_account(
    account_id,
    live_data,
):
    try:
        await asyncio.wait_for(
            monitor_one_account(
                account_id,
                live_data,
            ),
            timeout=REJOIN_ACCOUNT_TIMEOUT,
        )
    except asyncio.TimeoutError:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.debug("Safe monitor error account=%s: %s", account_id, str(e)[:200])


# ============================================================
# LIVE MONITOR
# ============================================================

async def live_monitor():
    logger.info(
        "LIVE MONITOR STARTED"
    )

    while True:
        try:
            if live_batch_running:
                await asyncio.sleep(
                    1.0
                )
                continue

            current = list(
                desired_live.items()
            )

            if not current:
                await asyncio.sleep(
                    LIVE_MONITOR_INTERVAL
                )
                continue

            for account_id, live_data in current:
                if account_id in stop_requested or account_id not in desired_live:
                    continue
                try:
                    await safe_monitor_one_account(account_id, live_data)
                except Exception:
                    pass

        except asyncio.CancelledError:
            break

        except Exception as e:
            logger.warning(
                "Live monitor error: %s",
                str(e)[:300],
            )

        await asyncio.sleep(
            LIVE_MONITOR_INTERVAL
        )

    logger.info(
        "LIVE MONITOR STOPPED"
    )


# ============================================================
# LEAVE ONE
# ============================================================

async def leave_one_account(
    owner_id,
    account,
):
    if not isinstance(account, dict):
        account = dict(account)
    account_id = account["id"]

    stop_requested.add(
        account_id
    )

    desired_live.pop(
        account_id,
        None,
    )

    missing_counts.pop(
        account_id,
        None,
    )

    try:
        chat_id = active_call_chats.get(
            account_id
        )

        if not chat_id:
            chat_id = account[
                "active_chat_id"
            ]

        if not chat_id:
            drop_call_object(
                account_id
            )

            return (
                True,
                (
                    f"⚪ {mask_phone(account['phone'])}"
                    " → Already stopped"
                ),
            )

        call = calls.get(
            account_id
        )

        if call is not None and getattr(call, "_is_running", False) and chat_id:
            try:
                await asyncio.wait_for(
                    call.leave_call(
                        int(chat_id)
                    ),
                    timeout=10,
                )
            except Exception:
                pass

        active_call_chats.pop(
            account_id,
            None,
        )

        drop_call_object(
            account_id
        )

        conn = db_connect()

        try:
            conn.execute(
                """
                UPDATE accounts
                SET active_chat_id=NULL
                WHERE id=? AND owner_id=?
                """,
                (
                    account_id,
                    owner_id,
                ),
            )

            conn.commit()

        finally:
            conn.close()

        log_operation(
            owner_id,
            account_id,
            "leave_live",
            chat_id,
            "success",
        )

        return (
            True,
            (
                f"✅ {mask_phone(account['phone'])}"
                " → Left Live"
            ),
        )

    except Exception as e:
        await cleanup_account_resources(
            account_id,
            disconnect_client=False,
        )

        log_operation(
            owner_id,
            account_id,
            "leave_live",
            "",
            "failed",
            str(e),
        )

        return (
            False,
            (
                f"❌ {mask_phone(account['phone'])}"
                f" → {str(e)[:250]}"
            ),
        )


# ============================================================
# SAFE LEAVE
# ============================================================

async def safe_leave_one_account(
    owner_id,
    account,
):
    if not isinstance(account, dict):
        account = dict(account)
    account_id = account["id"]

    try:
        return await asyncio.wait_for(
            leave_one_account(
                owner_id,
                account,
            ),
            timeout=LEAVE_ACCOUNT_TIMEOUT,
        )

    except asyncio.TimeoutError:
        await cleanup_account_resources(
            account_id,
            disconnect_client=False,
        )

        return (
            False,
            (
                f"⏱️ {mask_phone(account['phone'])}"
                " → Timeout"
            ),
        )

    except asyncio.CancelledError:
        raise

    except Exception as e:
        return (
            False,
            (
                f"❌ {mask_phone(account['phone'])}"
                f" → {str(e)[:250]}"
            ),
        )


# ============================================================
# LEAVE ALL
# ============================================================

async def leave_all(
    owner_id,
    message,
):
    global live_batch_running

    user_active_live.pop(
        owner_id,
        None,
    )

    accounts = get_accounts(
        owner_id
    )

    if not accounts:
        await message.reply_text(
            "❌ কোনো account পাওয়া যায়নি।",
            reply_markup=main_keyboard(
                owner_id
            ),
        )

        return

    queued_rejoin_accounts.clear()

    for account in accounts:
        stop_requested.add(
            account["id"]
        )

        desired_live.pop(
            account["id"],
            None,
        )

        missing_counts.pop(
            account["id"],
            None,
        )

    live_batch_running = True

    try:
        status_message = await message.reply_text(
            "🔵 <b>Stopping Live...</b>\n\n"
            f"📱 Accounts: {len(accounts)}\n"
            f"⚡ Parallel: {LEAVE_CONCURRENCY}\n"
            "⏳ Please wait...",
            parse_mode=ParseMode.HTML,
        )

        semaphore = asyncio.Semaphore(
            LEAVE_CONCURRENCY
        )

        async def worker(
            account
        ):
            async with semaphore:
                return await safe_leave_one_account(
                    owner_id,
                    account,
                )

        tasks = [
            asyncio.create_task(
                worker(account)
            )
            for account in accounts
        ]

        success = 0
        failed = 0
        completed = 0

        try:
            for future in asyncio.as_completed(
                tasks
            ):
                try:
                    ok, result = await future

                except Exception as e:
                    ok = False
                    result = (
                        f"❌ Error: "
                        f"{str(e)[:200]}"
                    )

                completed += 1

                if ok:
                    success += 1
                else:
                    failed += 1

                if (
                    completed == 1
                    or completed % 3 == 0
                    or completed == len(accounts)
                ):
                    try:
                        await status_message.edit_text(
                            "🔵 <b>Stopping Live...</b>\n\n"
                            f"📱 Total: {len(accounts)}\n"
                            f"⚡ Completed: "
                            f"{completed}/{len(accounts)}\n"
                            f"✅ Left: {success}\n"
                            f"❌ Failed: {failed}",
                            parse_mode=ParseMode.HTML,
                        )

                    except Exception:
                        pass

        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        try:
            await status_message.edit_text(
                "🔵 <b>STOP LIVE RESULT</b>\n\n"
                f"📱 Total: {len(accounts)}\n"
                f"⚡ Completed: {completed}/{len(accounts)}\n"
                f"✅ Left: {success}\n"
                f"❌ Failed: {failed}",
                parse_mode=ParseMode.HTML,
                reply_markup=main_keyboard(
                    owner_id
                ),
            )

        except Exception:
            pass

    finally:
        live_batch_running = False


# ============================================================
# JOIN LIVE BUTTON
# ============================================================

async def join_live_button(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    user_id = query.from_user.id

    if account_count(user_id) == 0:
        await query.edit_message_text(
            "❌ আগে অন্তত ১টি Telegram account যোগ করুন।",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "👤 My Accounts",
                            callback_data="accounts",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ Back",
                            callback_data="back_main",
                        )
                    ],
                ]
            ),
        )

        return

    lock = lock_for(
        user_id
    )

    if lock.locked():
        await safe_answer_query(
            query,
            "⏳ একটি operation ইতিমধ্যে চলছে।",
            show_alert=True,
        )

        return

    pending[user_id] = {
        "state": "live_link"
    }

    await query.edit_message_text(
        "🔴 <b>Join Live</b>\n\n"
        "Telegram Voice Chat / Live link পাঠান।\n\n"
        "উদাহরণ:\n"
        "<code>https://t.me/channel?livestream=xxxx</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data="back_main",
                    )
                ]
            ]
        ),
    )


# ============================================================
# STOP LIVE BUTTON
# ============================================================

async def stop_live_button(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    user_id = query.from_user.id

    lock = lock_for(
        user_id
    )

    if lock.locked():
        await safe_answer_query(
            query,
            "⏳ একটি operation ইতিমধ্যে চলছে।",
            show_alert=True,
        )

        return

    await query.edit_message_text(
        "🔵 <b>Stop Live Starting...</b>\n\n"
        "সব account থেকে Voice Chat leave করার চেষ্টা চলছে।",
        parse_mode=ParseMode.HTML,
    )

    async def _run_stop():
        async with lock:
            await leave_all(
                user_id,
                query.message,
            )

    asyncio.create_task(_run_stop())


# ============================================================
# LIVE CONFIG & COUNT SELECTION
# ============================================================

async def prompt_join_config(user_id, link, message_or_query):
    total = account_count(user_id)
    if total == 0:
        text = "❌ আগে অন্তত ১টি Telegram account যোগ করুন।"
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("👤 My Accounts", callback_data="accounts")],
                [InlineKeyboardButton("⬅️ Back", callback_data="back_main")],
            ]
        )
        if hasattr(message_or_query, "edit_message_text"):
            await message_or_query.edit_message_text(text, reply_markup=kb)
        else:
            await message_or_query.reply_text(text, reply_markup=kb)
        return

    pending_live_links[user_id] = link
    pending.pop(user_id, None)

    buttons = []
    # All button
    buttons.append([InlineKeyboardButton(f"🚀 All Accounts ({total})", callback_data="jcnt:all")])

    # Presets
    preset_row = []
    for count_val in [5, 10, 20, 30, 50]:
        if count_val < total:
            preset_row.append(InlineKeyboardButton(str(count_val), callback_data=f"jcnt:{count_val}"))
    if preset_row:
        for i in range(0, len(preset_row), 4):
            buttons.append(preset_row[i:i + 4])

    buttons.append([InlineKeyboardButton("✏️ Custom Number", callback_data="jcnt:custom")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="back_main")])

    text = (
        "🔴 <b>Join Live Settings</b>\n\n"
        f"🎯 <b>Target:</b> <code>{link}</code>\n"
        f"📱 <b>Available Accounts:</b> {total}\n\n"
        "লাইভে কতটি account join করাতে চান সিলেক্ট করুন:"
    )

    if hasattr(message_or_query, "edit_message_text"):
        await message_or_query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    else:
        await message_or_query.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def join_count_callback(update, context):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id

    choice = context.match.group(1) if context.match else None
    link = pending_live_links.get(user_id)

    if not link:
        await query.edit_message_text(
            "❌ কোনো লাইভ লিংক পাওয়া যায়নি। পুনরায় লিংক পাঠান।",
            reply_markup=main_keyboard(user_id),
        )
        return

    if choice == "custom":
        pending[user_id] = {
            "state": "custom_join_count",
            "link": link,
        }
        total = account_count(user_id)
        await query.edit_message_text(
            "✏️ <b>Custom Account Count</b>\n\n"
            f"১ থেকে {total}-এর মধ্যে কতটি account join করাতে চান লিখে পাঠান:\n\n"
            f"🎯 <b>Target:</b> <code>{link}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="back_main")]]
            ),
        )
        return

    pending_live_links.pop(user_id, None)

    count_to_join = None
    if choice != "all" and choice and choice.isdigit():
        count_to_join = int(choice)

    lock = lock_for(user_id)
    if lock.locked():
        await safe_answer_query(query, "⏳ একটি operation ইতিমধ্যে চলছে।", show_alert=True)
        return

    asyncio.create_task(
        join_all(
            user_id,
            link,
            query.message,
            count=count_to_join,
        )
    )


# ============================================================
# LIVE STATUS DASHBOARD
# ============================================================

async def show_live_status(message_or_query, user_id):
    accounts = get_accounts(user_id)
    total_acc = len(accounts)

    desired_for_user = [acc for acc in accounts if acc["id"] in desired_live]
    active_connected = 0
    dropped_accounts = []
    idle_accounts = [acc for acc in accounts if acc["id"] not in desired_live]

    for acc in desired_for_user:
        acc_id = acc["id"]
        call = calls.get(acc_id)
        if (
            call
            and getattr(call, "_is_running", False)
            and acc_id in active_call_chats
            and missing_counts.get(acc_id, 0) < MISSING_REQUIRED_SCANS
        ):
            active_connected += 1
        else:
            dropped_accounts.append(acc)

    live_info = user_active_live.get(user_id)
    if not live_info and desired_for_user:
        for d in desired_live.values():
            if d.get("owner_id") == user_id and d.get("info"):
                live_info = {
                    "link": d["info"].get("raw_link") or "Live Voice Chat",
                    "info": d["info"],
                    "chat_id": str(d.get("chat_id") or ""),
                    "start_time": time.time(),
                }
                user_active_live[user_id] = live_info
                break

    if not desired_for_user and not live_info:
        text = (
            "📊 <b>Live Voice Chat Status</b>\n\n"
            "⚪ <b>বর্তমানে কোনো active live সেশন নেই।</b>\n\n"
            f"📱 আপনার মোট Accounts: <b>{total_acc}</b>\n\n"
            "নতুন লাইভে জয়েন করতে নিচের '🔴 Join Live' বাটনে চাপুন।"
        )
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🔴 Join Live", callback_data="join_live")],
                [InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main")],
            ]
        )
        if hasattr(message_or_query, "edit_message_text"):
            try:
                await message_or_query.edit_message_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
            except Exception:
                pass
        else:
            await message_or_query.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        return

    target = live_info.get("link", "Voice Chat") if live_info else "Voice Chat"
    start_time = live_info.get("start_time") if live_info else None
    if start_time:
        elapsed = int(time.time() - start_time)
        hours = elapsed // 3600
        mins = (elapsed % 3600) // 60
        secs = elapsed % 60
        duration_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
    else:
        duration_str = "Active"

    text = (
        "📊 <b>Live Voice Chat Status Dashboard</b>\n\n"
        f"🎯 <b>Target:</b> <code>{target}</code>\n"
        f"⏱️ <b>Duration:</b> <code>{duration_str}</code>\n\n"
        f"👥 <b>Total Accounts:</b> <b>{total_acc}</b>\n"
        f"🟢 <b>Joined & Active:</b> <b>{active_connected}</b>\n"
        f"🔴 <b>Missing / Dropped:</b> <b>{len(dropped_accounts)}</b>\n"
        f"⚪ <b>Idle (Not in Live):</b> <b>{len(idle_accounts)}</b>\n\n"
        "♻️ <b>Auto-Rejoin:</b> 🟢 Active (Always Monitoring)\n"
        "🛡️ <b>Anti-Freeze:</b> Enabled"
    )

    buttons = [
        [
            InlineKeyboardButton("➕ Add Accounts", callback_data="live_add_menu"),
            InlineKeyboardButton("➖ Remove Accounts", callback_data="live_rem_menu"),
        ],
        [
            InlineKeyboardButton("⚡ Force Rejoin", callback_data="force_rejoin"),
            InlineKeyboardButton("🔄 Refresh", callback_data="live_status_refresh"),
        ],
        [
            InlineKeyboardButton("🔵 Stop Live", callback_data="stop_live"),
            InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main"),
        ],
    ]

    kb = InlineKeyboardMarkup(buttons)
    if hasattr(message_or_query, "edit_message_text"):
        try:
            await message_or_query.edit_message_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        except Exception:
            pass
    else:
        await message_or_query.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )


async def live_status_menu(update, context):
    query = update.callback_query
    if query.data == "live_status_refresh":
        await safe_answer_query(query, "🔄 Status Updated!")
    else:
        await safe_answer_query(query)
    user_id = query.from_user.id
    await show_live_status(query, user_id)


# ============================================================
# DYNAMIC ADD / REMOVE / FORCE REJOIN
# ============================================================

async def live_add_menu_callback(update, context):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id

    accounts = get_accounts(user_id)
    idle_accounts = [acc for acc in accounts if acc["id"] not in desired_live]

    if not idle_accounts:
        await safe_answer_query(
            query,
            "⚠️ আপনার সব account ইতিমধ্যে লাইভে রয়েছে! নতুন কোনো account ফাঁকা নেই।",
            show_alert=True,
        )
        return

    buttons = []
    preset_row = []
    for n in [5, 10, 20]:
        if n <= len(idle_accounts):
            preset_row.append(InlineKeyboardButton(f"+{n}", callback_data=f"live_add:{n}"))
    if preset_row:
        buttons.append(preset_row)

    buttons.append([
        InlineKeyboardButton(
            f"➕ All Remaining ({len(idle_accounts)})",
            callback_data="live_add:all",
        )
    ])
    buttons.append([InlineKeyboardButton("✏️ Custom Count", callback_data="live_add:custom")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Status", callback_data="live_status")])

    text = (
        "➕ <b>Add Accounts to Live</b>\n\n"
        f"⚪ ফাঁকা Accounts আছে: <b>{len(idle_accounts)}</b>\n\n"
        "কতটি নতুন account লাইভে যুক্ত করতে চান?"
    )

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def live_add_choice_callback(update, context):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id

    choice = context.match.group(1) if context.match else None
    accounts = get_accounts(user_id)
    idle_accounts = [acc for acc in accounts if acc["id"] not in desired_live]

    if choice == "custom":
        pending[user_id] = {
            "state": "custom_add_count",
        }
        await query.edit_message_text(
            "✏️ <b>Custom Add Accounts</b>\n\n"
            f"১ থেকে {len(idle_accounts)}-এর মধ্যে কতটি account যোগ করতে চান লিখে পাঠান:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel", callback_data="live_status")]]
            ),
        )
        return

    add_count = len(idle_accounts) if choice == "all" else int(choice)
    asyncio.create_task(
        add_more_accounts_logic(
            user_id,
            add_count,
            query.message,
        )
    )


async def add_more_accounts_logic(user_id, count, message):
    global live_batch_running
    live_info = user_active_live.get(user_id)
    if not live_info or not live_info.get("info"):
        for d in desired_live.values():
            if d.get("owner_id") == user_id and d.get("info"):
                live_info = d
                break

    if not live_info or not live_info.get("info"):
        await message.reply_text(
            "❌ কোনো active live সেশন পাওয়া যায়নি।",
            reply_markup=main_keyboard(user_id),
        )
        return

    info = live_info["info"]
    accounts = get_accounts(user_id)
    idle_accounts = [acc for acc in accounts if acc["id"] not in desired_live][:count]

    if not idle_accounts:
        await message.reply_text(
            "⚠️ যুক্ত করার মতো কোনো ফাঁকা account নেই।",
            reply_markup=main_keyboard(user_id),
        )
        return

    lock = lock_for(user_id)
    if lock.locked():
        await message.reply_text(
            "⏳ একটি operation ইতিমধ্যে চলছে।",
            reply_markup=main_keyboard(user_id),
        )
        return

    async with lock:
        live_batch_running = True
        try:
            for acc in idle_accounts:
                stop_requested.discard(acc["id"])
                missing_counts[acc["id"]] = 0

            status_msg = await message.reply_text(
                "➕ <b>Adding Accounts to Live...</b>\n\n"
                f"📱 Adding: {len(idle_accounts)}\n"
                "⏳ Please wait...",
                parse_mode=ParseMode.HTML,
            )

            semaphore = asyncio.Semaphore(JOIN_CONCURRENCY)

            async def _add_worker(index, acc):
                if index > 0:
                    await asyncio.sleep(index * JOIN_STAGGER)
                async with semaphore:
                    if acc["id"] in stop_requested:
                        return False, f"⛔ {mask_phone(acc['phone'])} → Stopped"
                    return await safe_join_one_account(user_id, acc, info)

            tasks = [asyncio.create_task(_add_worker(idx, acc)) for idx, acc in enumerate(idle_accounts)]
            success = 0
            failed = 0
            completed = 0

            for future in asyncio.as_completed(tasks):
                try:
                    ok, _ = await future
                except Exception:
                    ok = False
                completed += 1
                if ok:
                    success += 1
                else:
                    failed += 1

                if completed == 1 or completed % 3 == 0 or completed == len(idle_accounts):
                    try:
                        await status_msg.edit_text(
                            "➕ <b>Adding Accounts to Live...</b>\n\n"
                            f"⚡ Progress: {completed}/{len(idle_accounts)}\n"
                            f"✅ Success: {success}\n"
                            f"❌ Failed: {failed}",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📊 View Live Status", callback_data="live_status")],
                    [InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main")],
                ]
            )
            await status_msg.edit_text(
                "➕ <b>Account Addition Complete!</b>\n\n"
                f"📱 Target Accounts: {len(idle_accounts)}\n"
                f"✅ Successfully Added: <b>{success}</b>\n"
                f"❌ Failed: <b>{failed}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        finally:
            live_batch_running = False


async def live_rem_menu_callback(update, context):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id

    accounts = get_accounts(user_id)
    live_accounts = [acc for acc in accounts if acc["id"] in desired_live]

    if not live_accounts:
        await safe_answer_query(
            query,
            "⚠️ লাইভে কোনো account যুক্ত নেই।",
            show_alert=True,
        )
        return

    buttons = []
    preset_row = []
    for n in [5, 10, 20]:
        if n <= len(live_accounts):
            preset_row.append(InlineKeyboardButton(f"-{n}", callback_data=f"live_rem:{n}"))
    if preset_row:
        buttons.append(preset_row)

    buttons.append([
        InlineKeyboardButton(
            f"➖ All Active ({len(live_accounts)})",
            callback_data="live_rem:all",
        )
    ])
    buttons.append([InlineKeyboardButton("✏️ Custom Count", callback_data="live_rem:custom")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Status", callback_data="live_status")])

    text = (
        "➖ <b>Remove Accounts from Live</b>\n\n"
        f"🟢 বর্তমানে লাইভে আছে: <b>{len(live_accounts)}</b>\n\n"
        "কতটি account লাইভ থেকে বের করতে চান?"
    )

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def live_rem_choice_callback(update, context):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id

    choice = context.match.group(1) if context.match else None
    accounts = get_accounts(user_id)
    live_accounts = [acc for acc in accounts if acc["id"] in desired_live]

    if choice == "custom":
        pending[user_id] = {
            "state": "custom_rem_count",
        }
        await query.edit_message_text(
            "✏️ <b>Custom Remove Accounts</b>\n\n"
            f"১ থেকে {len(live_accounts)}-এর মধ্যে কতটি account বের করতে চান লিখে পাঠান:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel", callback_data="live_status")]]
            ),
        )
        return

    rem_count = len(live_accounts) if choice == "all" else (int(choice) if choice and choice.isdigit() else 0)
    if rem_count <= 0:
        return

    asyncio.create_task(
        remove_accounts_logic(
            user_id,
            rem_count,
            query.message,
        )
    )


async def remove_accounts_logic(user_id, count, message):
    global live_batch_running
    accounts = get_accounts(user_id)
    live_accounts = [acc for acc in accounts if acc["id"] in desired_live]

    if not live_accounts:
        await message.reply_text(
            "⚠️ লাইভে কোনো account যুক্ত নেই।",
            reply_markup=main_keyboard(user_id),
        )
        return

    to_remove = live_accounts[-count:]

    lock = lock_for(user_id)
    if lock.locked():
        await message.reply_text(
            "⏳ একটি operation ইতিমধ্যে চলছে।",
            reply_markup=main_keyboard(user_id),
        )
        return

    async with lock:
        live_batch_running = True
        try:
            for acc in to_remove:
                stop_requested.add(acc["id"])
                desired_live.pop(acc["id"], None)
                missing_counts.pop(acc["id"], None)

            status_msg = await message.reply_text(
                "➖ <b>Removing Accounts from Live...</b>\n\n"
                f"📱 Removing: {len(to_remove)}\n"
                "⏳ Please wait...",
                parse_mode=ParseMode.HTML,
            )

            semaphore = asyncio.Semaphore(LEAVE_CONCURRENCY)

            async def _rem_worker(index, acc):
                if index > 0:
                    await asyncio.sleep(index * 0.10)
                async with semaphore:
                    return await safe_leave_one_account(user_id, acc)

            tasks = [asyncio.create_task(_rem_worker(idx, acc)) for idx, acc in enumerate(to_remove)]
            success = 0
            failed = 0
            completed = 0

            for future in asyncio.as_completed(tasks):
                try:
                    ok, _ = await future
                except Exception:
                    ok = False
                completed += 1
                if ok:
                    success += 1
                else:
                    failed += 1

                if completed == 1 or completed % 3 == 0 or completed == len(to_remove):
                    try:
                        await status_msg.edit_text(
                            "➖ <b>Removing Accounts from Live...</b>\n\n"
                            f"⚡ Progress: {completed}/{len(to_remove)}\n"
                            f"✅ Left: {success}\n"
                            f"❌ Failed: {failed}",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📊 View Live Status", callback_data="live_status")],
                    [InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main")],
                ]
            )
            await status_msg.edit_text(
                "➖ <b>Account Removal Complete!</b>\n\n"
                f"📱 Target Accounts: {len(to_remove)}\n"
                f"✅ Successfully Left: <b>{success}</b>\n"
                f"❌ Failed: <b>{failed}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        finally:
            live_batch_running = False


async def force_rejoin_callback(update, context):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    force_all = (query.data == "force_rejoin_all")
    asyncio.create_task(force_rejoin_logic(user_id, query.message, force_all=force_all))


async def force_rejoin_logic(user_id, message, force_all=False):
    global live_batch_running
    lock = lock_for(user_id)
    if lock.locked():
        await message.reply_text(
            "⏳ একটি operation ইতিমধ্যে চলছে।",
            reply_markup=main_keyboard(user_id),
        )
        return

    accounts = get_accounts(user_id)
    desired_accounts = [acc for acc in accounts if acc["id"] in desired_live]

    if not desired_accounts:
        await message.reply_text(
            "⚠️ বর্তমানে কোনো live active নেই। আগে 🔴 Join Live করুন।",
            reply_markup=main_keyboard(user_id),
        )
        return

    live_info = user_active_live.get(user_id)
    info = None
    if live_info and live_info.get("info"):
        info = live_info["info"]
    else:
        for acc in desired_accounts:
            d = desired_live.get(acc["id"])
            if d and d.get("info"):
                info = d["info"]
                break

    if not info:
        await message.reply_text(
            "❌ লাইভ টার্গেট তথ্য পাওয়া যায়নি।",
            reply_markup=main_keyboard(user_id),
        )
        return

    dropped = [
        acc for acc in desired_accounts
        if (
            acc["id"] not in active_call_chats
            or calls.get(acc["id"]) is None
            or not getattr(calls.get(acc["id"]), "_is_running", False)
        )
    ]

    if not dropped and not force_all:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Reconnect All Anyway", callback_data="force_rejoin_all")],
            [InlineKeyboardButton("📊 View Live Status", callback_data="live_status")],
            [InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main")],
        ])
        await message.reply_text(
            "✅ <b>সবগুলো একাউন্ট ইতিমধ্যে লাইভে সক্রিয় আছে!</b>\n\n"
            f"📱 মোট একাউন্ট: <b>{len(desired_accounts)}</b>\n"
            "🟢 কোনো একাউন্ট ড্রপ হয়নি।\n\n"
            "আপনি কি সব একাউন্ট আবার ফ্রেশভাবে রিকানেক্ট করাতে চান?",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return

    targets = desired_accounts if force_all else dropped

    async with lock:
        live_batch_running = True
        try:
            status_msg = await message.reply_text(
                "⚡ <b>Force Rejoining Accounts...</b>\n\n"
                f"📱 Target: {len(targets)}\n"
                f"🎯 Live: <code>{info.get('raw_link') or 'Voice Chat'}</code>\n"
                "⏳ Reconnecting please wait...",
                parse_mode=ParseMode.HTML,
            )

            for acc in targets:
                stop_requested.discard(acc["id"])
                missing_counts[acc["id"]] = 0

            semaphore = asyncio.Semaphore(JOIN_CONCURRENCY)

            async def _rejoin_worker(index, acc):
                if index > 0:
                    await asyncio.sleep(index * JOIN_STAGGER)
                async with semaphore:
                    if acc["id"] in stop_requested:
                        return False, f"⛔ {mask_phone(acc['phone'])} → Stopped"
                    drop_call_object(acc["id"])
                    return await safe_join_one_account(user_id, acc, info)

            tasks = [asyncio.create_task(_rejoin_worker(idx, acc)) for idx, acc in enumerate(targets)]
            success = 0
            failed = 0
            completed = 0

            for future in asyncio.as_completed(tasks):
                try:
                    ok, _ = await future
                except Exception:
                    ok = False
                completed += 1
                if ok:
                    success += 1
                else:
                    failed += 1

                if completed == 1 or completed % 3 == 0 or completed == len(targets):
                    try:
                        await status_msg.edit_text(
                            "⚡ <b>Force Rejoining Accounts...</b>\n\n"
                            f"⚡ Progress: {completed}/{len(targets)}\n"
                            f"✅ Rejoined: {success}\n"
                            f"❌ Failed: {failed}",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📊 View Live Status", callback_data="live_status")],
                    [InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main")],
                ]
            )
            await status_msg.edit_text(
                "⚡ <b>Force Rejoin Completed!</b>\n\n"
                f"📱 Target Accounts: {len(targets)}\n"
                f"✅ Rejoined: <b>{success}</b>\n"
                f"❌ Failed: <b>{failed}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        finally:
            live_batch_running = False


# ============================================================
# SETTINGS
# ============================================================

async def settings_button(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    await query.edit_message_text(
        "⚙️ <b>Settings</b>\n\n"
        "🚀 TS SAJOL VC JOIN BOT\n"
        "📱 Maximum Accounts: 100\n"
        "🔴 Join Live: All Accounts\n"
        "🔵 Stop Live: All Accounts\n"
        "⚡ Fast Join: Enabled\n"
        "🛡️ Anti-Freeze: Enabled\n"
        "♻️ Auto Rejoin: Enabled\n"
        "💾 Database: SQLite\n"
        "🟢 Bot: Online",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="back_main",
                    )
                ]
            ]
        ),
    )


# ============================================================
# ADMIN PANEL
# ============================================================

async def admin_panel(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    if not is_admin(
        query.from_user.id
    ):
        return

    await query.edit_message_text(
        "🛠 <b>ADMIN PANEL</b>\n\n"
        "নিচের অপশন থেকে কাজ করুন।",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "👥 Users",
                        callback_data="admin_users",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "📱 All Accounts",
                        callback_data="admin_accounts",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "👑 Manage Admins",
                        callback_data="manage_admins",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🔄 Transfer Accounts",
                        callback_data="transfer_accounts",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="back_main",
                    )
                ],
            ]
        ),
    )


# ============================================================
# ADMIN USERS
# ============================================================

async def admin_users(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    if not is_admin(
        query.from_user.id
    ):
        return

    conn = db_connect()

    try:
        count = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM users
            """
        ).fetchone()["c"]

    finally:
        conn.close()

    await query.edit_message_text(
        "👥 <b>Users</b>\n\n"
        f"Total Users: {count}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_panel",
                    )
                ]
            ]
        ),
    )


# ============================================================
# ADMIN ACCOUNTS
# ============================================================

async def admin_accounts(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    if not is_admin(
        query.from_user.id
    ):
        return

    conn = db_connect()

    try:
        rows = conn.execute(
            """
            SELECT
                owner_id,
                COUNT(*) AS total
            FROM accounts
            GROUP BY owner_id
            ORDER BY owner_id
            """
        ).fetchall()

    finally:
        conn.close()

    if not rows:
        text = (
            "📱 <b>All Accounts</b>\n\n"
            "No accounts."
        )

    else:
        parts = [
            "📱 <b>All Accounts</b>\n"
        ]

        for row in rows:
            parts.append(
                f"👤 Owner: "
                f"<code>{row['owner_id']}</code>\n"
                f"📱 Accounts: "
                f"{row['total']}\n"
            )

        text = "\n".join(
            parts
        )

    await query.edit_message_text(
        text[:4000],
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_panel",
                    )
                ]
            ]
        ),
    )


# ============================================================
# MANAGE ADMINS
# ============================================================

async def manage_admins(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    if not is_owner(
        query.from_user.id
    ):
        return

    conn = db_connect()

    try:
        rows = conn.execute(
            """
            SELECT user_id
            FROM admins
            ORDER BY created_at DESC
            """
        ).fetchall()

    finally:
        conn.close()

    text = (
        "👑 <b>Manage Admins</b>\n\n"
    )

    if rows:
        text += "\n".join(
            f"• <code>{row['user_id']}</code>"
            for row in rows
        )

    else:
        text += (
            "কোনো extra admin নেই।"
        )

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "➕ Add Admin",
                        callback_data="add_admin",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "➖ Remove Admin",
                        callback_data="remove_admin",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_panel",
                    )
                ],
            ]
        ),
    )


# ============================================================
# ADD ADMIN
# ============================================================

async def add_admin_button(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    if not is_owner(
        query.from_user.id
    ):
        return

    pending[
        query.from_user.id
    ] = {
        "state": "add_admin"
    }

    await query.edit_message_text(
        "➕ <b>Add Admin</b>\n\n"
        "যার Telegram Numeric ID admin করতে চান "
        "সেই ID পাঠান।",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data="back_main",
                    )
                ]
            ]
        ),
    )


# ============================================================
# REMOVE ADMIN
# ============================================================

async def remove_admin_button(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    if not is_owner(
        query.from_user.id
    ):
        return

    pending[
        query.from_user.id
    ] = {
        "state": "remove_admin"
    }

    await query.edit_message_text(
        "➖ <b>Remove Admin</b>\n\n"
        "Admin-এর Numeric Telegram ID পাঠান।",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data="back_main",
                    )
                ]
            ]
        ),
    )


# ============================================================
# TRANSFER
# ============================================================

async def transfer_accounts_button(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    if not is_admin(
        query.from_user.id
    ):
        return

    pending[
        query.from_user.id
    ] = {
        "state": "transfer_accounts"
    }

    await query.edit_message_text(
        "🔄 <b>Copy Accounts</b>\n\n"
        "Target Telegram User ID পাঠান।\n\n"
        "📌 Original accounts আপনার কাছেই থাকবে।\n"
        "📌 Target user-এর জন্য আলাদা session copy তৈরি হবে।",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data="back_main",
                    )
                ]
            ]
        ),
    )


# ============================================================
# TEXT HANDLER
# ============================================================

async def text_handler(
    update,
    context,
):
    user = update.effective_user

    if not user or not update.message:
        return

    register_user(
        user
    )

    user_id = user.id

    state = pending.get(
        user_id
    )

    if not state:
        text = update.message.text.strip() if update.message.text else ""
        if (
            text.startswith("http://t.me/")
            or text.startswith("https://t.me/")
            or text.startswith("t.me/")
            or text.startswith("tg://")
            or text.startswith("@")
            or "t.me/" in text
        ):
            lock = lock_for(user_id)
            if lock.locked():
                await update.message.reply_text(
                    "⏳ একটি operation ইতিমধ্যে চলছে।",
                    reply_markup=main_keyboard(user_id),
                )
                return

            await prompt_join_config(user_id, text, update.message)
            return

        return

    current_state = state.get(
        "state"
    )

    # --------------------------------------------------------
    # PHONE
    # --------------------------------------------------------

    if current_state == "phone":
        await process_phone(
            update,
            context,
        )
        return

    # --------------------------------------------------------
    # CODE
    # --------------------------------------------------------

    if current_state == "code":
        await process_code(
            update,
            context,
        )
        return

    # --------------------------------------------------------
    # PASSWORD
    # --------------------------------------------------------

    if current_state == "password":
        await process_password(
            update,
            context,
        )
        return

    # --------------------------------------------------------
    # LIVE LINK
    # --------------------------------------------------------

    if current_state == "live_link":
        pending.pop(
            user_id,
            None,
        )

        link = update.message.text.strip()

        lock = lock_for(
            user_id
        )

        if lock.locked():
            await update.message.reply_text(
                "⏳ একটি operation ইতিমধ্যে চলছে।",
                reply_markup=main_keyboard(
                    user_id
                ),
            )
            return

        await prompt_join_config(user_id, link, update.message)
        return

    # --------------------------------------------------------
    # CUSTOM JOIN COUNT
    # --------------------------------------------------------

    if current_state == "custom_join_count":
        link = state.get("link") or pending_live_links.get(user_id)
        val_str = update.message.text.strip()
        total = account_count(user_id)
        if not val_str.isdigit() or int(val_str) <= 0 or int(val_str) > total:
            await update.message.reply_text(
                f"❌ অনুগ্রহ করে ১ থেকে {total}-এর মধ্যে একটি সঠিক সংখ্যা পাঠান।",
            )
            return

        pending.pop(user_id, None)
        pending_live_links.pop(user_id, None)
        cnt = int(val_str)
        asyncio.create_task(
            join_all(
                user_id,
                link,
                update.message,
                count=cnt,
            )
        )
        return

    # --------------------------------------------------------
    # CUSTOM ADD COUNT
    # --------------------------------------------------------

    if current_state == "custom_add_count":
        val_str = update.message.text.strip()
        accounts = get_accounts(user_id)
        idle_accounts = [acc for acc in accounts if acc["id"] not in desired_live]
        max_can_add = len(idle_accounts)
        if not val_str.isdigit() or int(val_str) <= 0 or int(val_str) > max_can_add:
            await update.message.reply_text(
                f"❌ অনুগ্রহ করে ১ থেকে {max_can_add}-এর মধ্যে একটি সঠিক সংখ্যা পাঠান।",
            )
            return

        pending.pop(user_id, None)
        cnt = int(val_str)
        asyncio.create_task(
            add_more_accounts_logic(
                user_id,
                cnt,
                update.message,
            )
        )
        return

    # --------------------------------------------------------
    # CUSTOM REMOVE COUNT
    # --------------------------------------------------------

    if current_state == "custom_rem_count":
        val_str = update.message.text.strip()
        accounts = get_accounts(user_id)
        live_accounts = [acc for acc in accounts if acc["id"] in desired_live]
        max_can_rem = len(live_accounts)
        if not val_str.isdigit() or int(val_str) <= 0 or int(val_str) > max_can_rem:
            await update.message.reply_text(
                f"❌ অনুগ্রহ করে ১ থেকে {max_can_rem}-এর মধ্যে একটি সঠিক সংখ্যা পাঠান।",
            )
            return

        pending.pop(user_id, None)
        cnt = int(val_str)
        asyncio.create_task(
            remove_accounts_logic(
                user_id,
                cnt,
                update.message,
            )
        )
        return

    # --------------------------------------------------------
    # ADD ADMIN
    # --------------------------------------------------------

    if current_state == "add_admin":
        pending.pop(
            user_id,
            None,
        )

        text = update.message.text.strip()

        if not text.isdigit():
            await update.message.reply_text(
                "❌ শুধুমাত্র numeric Telegram User ID পাঠান।",
                reply_markup=main_keyboard(
                    user_id
                ),
            )
            return

        target_id = int(text)

        conn = db_connect()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO admins
                (
                    user_id,
                    added_by,
                    created_at
                )
                VALUES (?, ?, ?)
                """,
                (
                    target_id,
                    user_id,
                    int(time.time()),
                ),
            )
            conn.commit()
        finally:
            conn.close()

        await update.message.reply_text(
            f"✅ User ID <code>{target_id}</code> সফলভাবে Admin করা হয়েছে।",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(
                user_id
            ),
        )
        return

    # --------------------------------------------------------
    # REMOVE ADMIN
    # --------------------------------------------------------

    if current_state == "remove_admin":
        pending.pop(
            user_id,
            None,
        )

        text = update.message.text.strip()

        if not text.isdigit():
            await update.message.reply_text(
                "❌ শুধুমাত্র numeric Telegram User ID পাঠান।",
                reply_markup=main_keyboard(
                    user_id
                ),
            )
            return

        target_id = int(text)

        if is_owner(target_id):
            await update.message.reply_text(
                "❌ Owner-কে admin তালিকা থেকে সরানো যাবে না।",
                reply_markup=main_keyboard(
                    user_id
                ),
            )
            return

        conn = db_connect()
        try:
            conn.execute(
                """
                DELETE FROM admins
                WHERE user_id=?
                """,
                (target_id,),
            )
            conn.commit()
        finally:
            conn.close()

        await update.message.reply_text(
            f"✅ User ID <code>{target_id}</code>-কে Admin তালিকা থেকে সরানো হয়েছে।",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(
                user_id
            ),
        )
        return

    # --------------------------------------------------------
    # TRANSFER ACCOUNTS
    # --------------------------------------------------------

    if current_state == "transfer_accounts":
        pending.pop(
            user_id,
            None,
        )

        text = update.message.text.strip()

        if not text.isdigit():
            await update.message.reply_text(
                "❌ শুধুমাত্র numeric Telegram User ID পাঠান।",
                reply_markup=main_keyboard(
                    user_id
                ),
            )
            return

        target_id = int(text)

        accounts = get_accounts(
            user_id
        )

        if not accounts:
            await update.message.reply_text(
                "❌ আপনার কোনো account নেই copy করার জন্য।",
                reply_markup=main_keyboard(
                    user_id
                ),
            )
            return

        copied = 0
        conn = db_connect()

        try:
            for acc in accounts:
                try:
                    cur = conn.execute(
                        """
                        INSERT INTO accounts
                        (
                            owner_id,
                            phone,
                            telegram_user_id,
                            session_name,
                            created_at,
                            active_chat_id
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            target_id,
                            acc["phone"],
                            acc.get("telegram_user_id"),
                            f"pending_{target_id}_{int(time.time())}_{copied}",
                            int(time.time()),
                            None,
                        ),
                    )

                    new_id = cur.lastrowid
                    new_session = safe_session_name(new_id)

                    conn.execute(
                        """
                        UPDATE accounts
                        SET session_name=?
                        WHERE id=?
                        """,
                        (
                            new_session,
                            new_id,
                        ),
                    )

                    old_sess = acc["session_name"] + ".session"
                    new_sess = new_session + ".session"

                    if os.path.exists(old_sess):
                        shutil.copy2(
                            old_sess,
                            new_sess,
                        )

                    copied += 1

                except Exception as e:
                    logger.error(
                        "Copy account error: %s",
                        e,
                    )

            conn.commit()

        finally:
            conn.close()

        await update.message.reply_text(
            f"✅ মোট {copied}টি account User <code>{target_id}</code>-এ সফলভাবে copy করা হয়েছে।",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(
                user_id
            ),
        )
        return


# ============================================================
# CALLBACKS & NAVIGATION
# ============================================================

async def back_main_callback(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    user_id = query.from_user.id

    state_data = pending.pop(
        user_id,
        None,
    )
    if state_data and state_data.get("state") in ("phone", "code", "password"):
        acc_id = state_data.get("account_id")
        if acc_id:
            conn = db_connect()
            try:
                acc_row = conn.execute("SELECT telegram_user_id FROM accounts WHERE id=?", (acc_id,)).fetchone()
                if acc_row and acc_row["telegram_user_id"] is None:
                    asyncio.create_task(delete_account(acc_id))
            finally:
                conn.close()

    count = account_count(
        user_id
    )

    text = (
        "✨ <b>𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢</b>\n\n"
        "<b>𝙏𝙎 𝙎𝘼𝙅𝙊𝙇 𝙑𝘾 𝙅𝙊𝙄𝙉 𝘽𝙊𝙏</b>\n\n"
        "🚀 Telegram Voice Chat Manager\n"
        f"📱 Accounts: {count}/{MAX_ACCOUNTS}\n"
        "🟢 Bot Status: Online"
    )

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(
            user_id
        ),
    )


async def account_info_callback(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    user_id = query.from_user.id
    data = query.data

    parts = data.split(":")
    try:
        account_id = int(parts[1])
        page = int(parts[2]) if len(parts) > 2 else 1
    except (IndexError, ValueError):
        return

    account = get_account(account_id)

    if not account or (not is_admin(user_id) and account["owner_id"] != user_id):
        await query.edit_message_text(
            "❌ Account পাওয়া যায়নি।",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Back",
                            callback_data=f"accounts:{page}",
                        )
                    ]
                ]
            ),
        )
        return

    text = (
        "👤 <b>Account Details</b>\n\n"
        f"📱 Phone: <code>{account['phone']}</code>\n"
        f"🆔 ID: <code>{account['id']}</code>\n"
        f"👑 Owner ID: <code>{account['owner_id']}</code>\n"
        "🟢 Status: Ready"
    )

    buttons = [
        [
            InlineKeyboardButton(
                "🗑 Delete Account",
                callback_data=f"delete_acc:{account_id}:{page}",
            )
        ],
        [
            InlineKeyboardButton(
                "⬅️ Back to Accounts",
                callback_data=f"accounts:{page}",
            )
        ],
    ]

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def delete_account_callback(
    update,
    context,
):
    query = update.callback_query

    await safe_answer_query(query)

    user_id = query.from_user.id
    data = query.data

    parts = data.split(":")
    try:
        account_id = int(parts[1])
        page = int(parts[2]) if len(parts) > 2 else 1
    except (IndexError, ValueError):
        return

    account = get_account(account_id)

    if not account or (not is_admin(user_id) and account["owner_id"] != user_id):
        await query.edit_message_text(
            "❌ Account পাওয়া যায়নি।",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Back",
                            callback_data=f"accounts:{page}",
                        )
                    ]
                ]
            ),
        )
        return

    await delete_account(account_id)

    await query.edit_message_text(
        "✅ Account সফলভাবে মুছে ফেলা হয়েছে।",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⬅️ Back to Accounts",
                        callback_data=f"accounts:{page}",
                    )
                ]
            ]
        ),
    )


# ============================================================
# POST INIT & MAIN
# ============================================================

async def post_init(application: Application):
    logger.info("Initializing database and background tasks...")
    db_init()
    asyncio.create_task(rejoin_worker_loop())
    asyncio.create_task(live_monitor())
    logger.info("Bot is ready and running!")


async def status_command(update, context):
    user = update.effective_user
    if not user:
        return
    await show_live_status(update.message, user.id)


async def join_command(update, context):
    user = update.effective_user
    if not user:
        return
    if context.args:
        link = context.args[0].strip()
        await prompt_join_config(user.id, link, update.message)
    else:
        pending[user.id] = {"state": "live_link"}
        await update.message.reply_text(
            "🔴 <b>Join Live</b>\n\n"
            "Telegram Voice Chat / Live link অথবা username পাঠান।\n\n"
            "উদাহরণ:\n<code>https://t.me/channel?livestream=xxxx</code> অথবা <code>@channel</code>",
            parse_mode=ParseMode.HTML,
        )


async def stop_command(update, context):
    user = update.effective_user
    if not user:
        return
    lock = lock_for(user.id)
    if lock.locked():
        await update.message.reply_text(
            "⏳ একটি operation ইতিমধ্যে চলছে।",
            reply_markup=main_keyboard(user.id),
        )
        return

    async def _run_stop():
        async with lock:
            await leave_all(user.id, update.message)

    asyncio.create_task(_run_stop())


def main():
    logger.info("Starting %s...", BOT_NAME)
    db_init()

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(post_init)
        .build()
    )

    # Command Handlers
    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )
    application.add_handler(
        CommandHandler(
            "status",
            status_command,
        )
    )
    application.add_handler(
        CommandHandler(
            "join",
            join_command,
        )
    )
    application.add_handler(
        CommandHandler(
            "stop",
            stop_command,
        )
    )

    # Callback Query Handlers
    application.add_handler(
        CallbackQueryHandler(
            accounts_menu,
            pattern="^accounts(:\\d+)?$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            add_account_button,
            pattern="^add_account$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            account_info_callback,
            pattern="^account_info:\\d+(:\\d+)?$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            delete_account_callback,
            pattern="^delete_acc:\\d+(:\\d+)?$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            join_live_button,
            pattern="^join_live$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            stop_live_button,
            pattern="^stop_live$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            live_status_menu,
            pattern="^(live_status|live_status_refresh)$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            force_rejoin_callback,
            pattern="^(force_rejoin|force_rejoin_all)$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            join_count_callback,
            pattern="^jcnt:(.+)$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            live_add_menu_callback,
            pattern="^live_add_menu$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            live_add_choice_callback,
            pattern="^live_add:(.+)$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            live_rem_menu_callback,
            pattern="^live_rem_menu$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            live_rem_choice_callback,
            pattern="^live_rem:(.+)$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            settings_button,
            pattern="^settings$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            admin_panel,
            pattern="^admin_panel$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            admin_users,
            pattern="^admin_users$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            admin_accounts,
            pattern="^admin_accounts$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            manage_admins,
            pattern="^manage_admins$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            add_admin_button,
            pattern="^add_admin$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            remove_admin_button,
            pattern="^remove_admin$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            transfer_accounts_button,
            pattern="^transfer_accounts$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            back_main_callback,
            pattern="^back_main$",
        )
    )

    # Message Handlers
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_handler,
        )
    )

    # Error Handler
    async def global_error_handler(update, context):
        err_msg = str(context.error) if context.error else ""
        if "Query is too old" in err_msg or "Message is not modified" in err_msg:
            return
        logger.warning("Bot update error: %s", err_msg)

    application.add_error_handler(global_error_handler)

    # Start Polling
    application.run_polling(
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()