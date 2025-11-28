import asyncio
import logging
import mimetypes
import os
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta
from typing import Optional, Tuple

import aiosqlite
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import (
    AIORateLimiter,
    Application,
    ApplicationBuilder,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

# Conversation states
SELECT, NAME, ID_CARD_PHOTO, ID_NUMBER, SELFIE_WITH_ID, EMAIL, EX_HASH, EX_SCREEN = range(8)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Default credentials (can be overridden by environment variables)
DEFAULT_BOT_TOKEN = "8493960238:AAHfWadR-gKrOwcoEcXtjHCcfgmcd_LixGo"
DEFAULT_ADMIN_CHAT_ID = 6378017908
DEFAULT_COLLECTION_WALLET = ""
DEFAULT_TOKEN_LIMIT = "100 MML"
DEFAULT_CONTACT_EMAIL = "mmlexchange@gmail.com"


@dataclass
class Config:
    bot_token: str
    admin_chat_id: int
    collection_wallet: str
    token_limit: str
    contact_email: str
    mode: str
    webhook_url: Optional[str]
    port: int


async def init_db(db_path: str = "bot.db") -> aiosqlite.Connection:
    conn = await aiosqlite.connect(db_path)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            name TEXT,
            id_number TEXT,
            id_card_file_id TEXT,
            selfie_with_id_file_id TEXT,
            email TEXT,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 1,
            entry_count INTEGER DEFAULT 0,
            username TEXT,
            bio TEXT,
            profile_photo_file_id TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tx_hash TEXT,
            screenshot_file_id TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(telegram_id)
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS exchange_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            tx_hash TEXT,
            screenshot_file_id TEXT,
            status TEXT DEFAULT 'pending_admin',
            approved_at TEXT,
            expires_at TEXT,
            payout_tx_hash TEXT,
            payout_screenshot_file_id TEXT,
            completed_at TEXT,
            wallet_address TEXT,
            user_wallet_address TEXT,
            created_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(telegram_id)
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            msg_type TEXT,
            content TEXT,
            file_id TEXT,
            file_data BLOB,
            file_mime TEXT,
            chat_type TEXT,
            created_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(telegram_id)
        )
        """
    )
    await conn.commit()
    # Ensure new columns exist if table was created previously
    await ensure_user_columns(conn)
    return conn


async def ensure_user_columns(conn: aiosqlite.Connection) -> None:
    """Add missing columns for backwards compatibility."""
    required_cols = {
        "id_number": "ALTER TABLE users ADD COLUMN id_number TEXT;",
        "id_card_file_id": "ALTER TABLE users ADD COLUMN id_card_file_id TEXT;",
        "selfie_with_id_file_id": "ALTER TABLE users ADD COLUMN selfie_with_id_file_id TEXT;",
        "email": "ALTER TABLE users ADD COLUMN email TEXT;",
        "attempts": "ALTER TABLE users ADD COLUMN attempts INTEGER DEFAULT 1;",
        "entry_count": "ALTER TABLE users ADD COLUMN entry_count INTEGER DEFAULT 0;",
        "username": "ALTER TABLE users ADD COLUMN username TEXT;",
        "bio": "ALTER TABLE users ADD COLUMN bio TEXT;",
        "profile_photo_file_id": "ALTER TABLE users ADD COLUMN profile_photo_file_id TEXT;",
        "pending_field": "ALTER TABLE users ADD COLUMN pending_field TEXT;",
    }
    # extra tables columns for exchange_requests
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute("PRAGMA table_info(users);")
    existing = {row["name"] for row in await cursor.fetchall()}
    for col, alter_sql in required_cols.items():
        if col not in existing:
            await conn.execute(alter_sql)
    # ensure chat_type column in user_messages
    cursor = await conn.execute("PRAGMA table_info(user_messages);")
    msg_cols = {row["name"] for row in await cursor.fetchall()}
    if "chat_type" not in msg_cols:
        await conn.execute("ALTER TABLE user_messages ADD COLUMN chat_type TEXT;")
    if "file_data" not in msg_cols:
        await conn.execute("ALTER TABLE user_messages ADD COLUMN file_data BLOB;")
    if "file_mime" not in msg_cols:
        await conn.execute("ALTER TABLE user_messages ADD COLUMN file_mime TEXT;")
    # ensure payout columns in exchange_requests
    cursor = await conn.execute("PRAGMA table_info(exchange_requests);")
    ex_cols = {row["name"] for row in await cursor.fetchall()}
    if "payout_tx_hash" not in ex_cols:
        await conn.execute("ALTER TABLE exchange_requests ADD COLUMN payout_tx_hash TEXT;")
    if "payout_screenshot_file_id" not in ex_cols:
        await conn.execute(
            "ALTER TABLE exchange_requests ADD COLUMN payout_screenshot_file_id TEXT;"
        )
    if "completed_at" not in ex_cols:
        await conn.execute("ALTER TABLE exchange_requests ADD COLUMN completed_at TEXT;")
    if "wallet_address" not in ex_cols:
        await conn.execute("ALTER TABLE exchange_requests ADD COLUMN wallet_address TEXT;")
    if "user_wallet_address" not in ex_cols:
        await conn.execute("ALTER TABLE exchange_requests ADD COLUMN user_wallet_address TEXT;")
    await conn.commit()


async def set_user_status(conn: aiosqlite.Connection, telegram_id: int, status: str) -> None:
    await conn.execute(
        "UPDATE users SET status = ?, updated_at = ? WHERE telegram_id = ?",
        (status, datetime.now(UTC).isoformat(), telegram_id),
    )
    await conn.commit()


async def set_pending_field(conn: aiosqlite.Connection, telegram_id: int, field: Optional[str]) -> None:
    await conn.execute(
        "UPDATE users SET pending_field = ?, updated_at = ? WHERE telegram_id = ?",
        (field, datetime.now(UTC).isoformat(), telegram_id),
    )
    await conn.commit()


async def update_user_field(
    conn: aiosqlite.Connection, telegram_id: int, field: str, value: str
) -> None:
    await conn.execute(
        f"UPDATE users SET {field} = ?, updated_at = ? WHERE telegram_id = ?",
        (value, datetime.now(UTC).isoformat(), telegram_id),
    )
    await conn.commit()


async def increment_entry_count(conn: aiosqlite.Connection, telegram_id: int) -> None:
    await conn.execute(
        """
        INSERT INTO users (telegram_id, entry_count, created_at, updated_at)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            entry_count = COALESCE(entry_count, 0) + 1,
            updated_at = excluded.updated_at
        """,
        (telegram_id, datetime.now(UTC).isoformat(), datetime.now(UTC).isoformat()),
    )
    await conn.commit()


async def get_user(conn: aiosqlite.Connection, telegram_id: int) -> Optional[aiosqlite.Row]:
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
    return await cursor.fetchone()


async def insert_user(
    conn: aiosqlite.Connection,
    telegram_id: int,
    name: str,
    id_number: str,
    id_card_file_id: str,
    selfie_with_id_file_id: str,
    email: str,
    username: str,
    bio: str,
    profile_photo_file_id: str,
) -> None:
    # keep attempts/created_at if user existed
    existing = await get_user(conn, telegram_id)
    if existing and existing["status"] == "needs_update":
        attempts = existing["attempts"] or 1
    else:
        attempts = ((existing["attempts"] or 1) + 1) if existing else 1
    entry_count = existing["entry_count"] if existing else 0
    created_at = existing["created_at"] if existing else datetime.now(UTC).isoformat()
    await conn.execute(
        """
        INSERT INTO users
            (telegram_id, name, id_number, id_card_file_id, selfie_with_id_file_id, email, username, bio, profile_photo_file_id, status, attempts, entry_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            name = excluded.name,
            id_number = excluded.id_number,
            id_card_file_id = excluded.id_card_file_id,
            selfie_with_id_file_id = excluded.selfie_with_id_file_id,
            email = excluded.email,
            username = excluded.username,
            bio = excluded.bio,
            profile_photo_file_id = excluded.profile_photo_file_id,
            status = 'pending',
            attempts = excluded.attempts,
            entry_count = excluded.entry_count,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        """,
        (
            telegram_id,
            name,
            id_number,
            id_card_file_id,
            selfie_with_id_file_id,
            email,
            username,
            bio,
            profile_photo_file_id,
            attempts,
            entry_count,
            created_at,
            datetime.now(UTC).isoformat(),
        ),
    )
    await conn.commit()


async def insert_payment(
    conn: aiosqlite.Connection, user_id: int, tx_hash: str, screenshot_file_id: str
) -> None:
    await conn.execute(
        """
        INSERT INTO payments (user_id, tx_hash, screenshot_file_id, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (user_id, tx_hash, screenshot_file_id, datetime.now(UTC).isoformat()),
    )
    await conn.commit()


async def insert_exchange_request(
    conn: aiosqlite.Connection, user_id: int, tx_hash: str, screenshot_file_id: str
) -> int:
    cursor = await conn.execute(
        """
        INSERT INTO exchange_requests (user_id, tx_hash, screenshot_file_id, status, created_at)
        VALUES (?, ?, ?, 'pending_admin', ?)
        """,
        (user_id, tx_hash, screenshot_file_id, datetime.now(UTC).isoformat()),
    )
    await conn.commit()
    return cursor.lastrowid


async def set_exchange_status(
    conn: aiosqlite.Connection,
    exchange_id: int,
    status: str,
    approved_at: Optional[str] = None,
    expires_at: Optional[str] = None,
    payout_tx_hash: Optional[str] = None,
    payout_screenshot_file_id: Optional[str] = None,
    completed_at: Optional[str] = None,
    wallet_address: Optional[str] = None,
    user_wallet_address: Optional[str] = None,
) -> None:
    await conn.execute(
        """
        UPDATE exchange_requests
        SET status = ?,
            approved_at = COALESCE(approved_at, ?),
            expires_at = COALESCE(expires_at, ?),
            payout_tx_hash = COALESCE(payout_tx_hash, ?),
            payout_screenshot_file_id = COALESCE(payout_screenshot_file_id, ?),
            completed_at = COALESCE(completed_at, ?),
            wallet_address = COALESCE(wallet_address, ?),
            user_wallet_address = COALESCE(user_wallet_address, ?)
        WHERE id = ?
        """,
        (
            status,
            approved_at,
            expires_at,
            payout_tx_hash,
            payout_screenshot_file_id,
            completed_at,
            wallet_address,
            user_wallet_address,
            exchange_id,
        ),
    )
    await conn.commit()


async def get_exchange(conn: aiosqlite.Connection, exchange_id: int) -> Optional[aiosqlite.Row]:
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute("SELECT * FROM exchange_requests WHERE id = ?", (exchange_id,))
    return await cursor.fetchone()


async def get_last_completed_exchange_date(conn: aiosqlite.Connection, user_id: int) -> Optional[datetime]:
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute(
        """
        SELECT completed_at FROM exchange_requests
        WHERE user_id = ? AND status = 'completed' AND completed_at IS NOT NULL
        ORDER BY completed_at DESC LIMIT 1
        """,
        (user_id,),
    )
    row = await cursor.fetchone()
    if row and row["completed_at"]:
        try:
            return datetime.fromisoformat(row["completed_at"])
        except Exception:
            return None
    return None


async def log_user_message(
    conn: aiosqlite.Connection,
    user_id: int,
    msg_type: str,
    content: str = "",
    file_id: str = "",
    chat_type: str = "",
    file_bytes: Optional[bytes] = None,
    file_mime: str = "",
) -> None:
    await conn.execute(
        """
        INSERT INTO user_messages (user_id, msg_type, content, file_id, file_data, file_mime, chat_type, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            msg_type,
            content,
            file_id,
            file_bytes,
            file_mime,
            chat_type,
            datetime.now(UTC).isoformat(),
        ),
    )
    await conn.commit()


async def log_bot_message(app: Application, chat_id: int, msg_type: str, content: str = "", file_id: str = "") -> None:
    """Log outgoing bot messages so they appear in the viewer."""
    conn: aiosqlite.Connection = app.bot_data.get("db") if app and getattr(app, "bot_data", None) else None
    if not conn:
        return
    file_bytes = None
    file_mime = ""
    if file_id:
        file_bytes, file_mime = await fetch_file_bytes(app.bot, file_id)
    await log_user_message(
        conn,
        chat_id,
        f"bot_{msg_type}",
        content=content,
        file_id=file_id,
        file_bytes=file_bytes,
        file_mime=file_mime,
        chat_type="bot",
    )


async def send_message_logged(context: CallbackContext, *args, **kwargs):
    """Send a message and log it as bot_text."""
    chat_id = kwargs.get("chat_id") or (args[0] if args else None)
    text = kwargs.get("text", "")
    result = await context.bot.send_message(*args, **kwargs)
    try:
        if chat_id is not None:
            await log_bot_message(context.application, chat_id, "text", content=text)
    except Exception as exc:  # pragma: no cover
        logger.debug("Could not log bot message: %s", exc)
    return result


async def send_photo_logged(context: CallbackContext, *args, **kwargs):
    """Send a photo and log it as bot_photo."""
    chat_id = kwargs.get("chat_id") or (args[0] if args else None)
    photo = kwargs.get("photo") or (args[1] if len(args) > 1 else None)
    result = await context.bot.send_photo(*args, **kwargs)
    try:
        file_id = ""
        if hasattr(result, "photo") and result.photo:
            file_id = result.photo[-1].file_id
        elif isinstance(photo, str):
            file_id = photo
        if chat_id is not None:
            await log_bot_message(context.application, chat_id, "photo", file_id=file_id)
    except Exception as exc:  # pragma: no cover
        logger.debug("Could not log bot photo: %s", exc)
    return result


def _logged_set(context: CallbackContext):
    return context.chat_data.setdefault("_logged_message_ids", set())


def mark_logged(context: CallbackContext, message_id: int) -> None:
    try:
        _logged_set(context).add(message_id)
    except Exception:
        return


def already_logged(context: CallbackContext, message_id: int) -> bool:
    try:
        return message_id in _logged_set(context)
    except Exception:
        return False


async def fetch_file_bytes(bot, file_id: str) -> Tuple[Optional[bytes], str]:
    """Download file bytes+mime from Telegram."""
    if not file_id:
        return None, ""
    try:
        file = await bot.get_file(file_id)
        file_path = getattr(file, "file_path", "") or ""
        mime = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
        data: Optional[bytes] = None
        if hasattr(file, "download_as_bytearray"):
            try:
                ba = await file.download_as_bytearray()
                data = bytes(ba)
            except Exception:
                data = None
        if data is None and hasattr(file, "download_to_memory"):
            buf = bytearray()
            await file.download_to_memory(out=buf)
            data = bytes(buf)
        return data, mime
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to fetch file bytes: %s", exc)
        return None, ""


async def reply_text_logged(message, context: CallbackContext, text: str, **kwargs):
    """Send reply_text and log as bot_text."""
    result = await message.reply_text(text, **kwargs)
    try:
        await log_bot_message(context.application, message.chat_id, "text", content=text)
    except Exception as exc:  # pragma: no cover
        logger.debug("Could not log reply_text: %s", exc)
    return result


async def log_any_message(update: Update, context: CallbackContext) -> None:
    if not update.message:
        return
    if already_logged(context, update.message.message_id):
        return
    # Handle admin payout flow messages
    payout_flow = context.bot_data.get("payout_flow", {})
    wallet_flow = context.bot_data.get("wallet_flow", {})
    exchange_collect = context.bot_data.setdefault("exchange_collect", {})
    payout_wallet_collect = context.bot_data.setdefault("payout_wallet_collect", {})
    if update.effective_user and context.bot_data.get("config") and update.effective_user.id == context.bot_data["config"].admin_chat_id:
        wflow = wallet_flow.get(update.effective_user.id)
        if wflow and update.message.text:
            wallet_addr = update.message.text.strip()
            exchange_id = wflow["exchange_id"]
            user_id = wflow["user_id"]
            conn: aiosqlite.Connection = context.bot_data["db"]
            now = datetime.now(UTC)
            expires_at = now + timedelta(minutes=30)
            await set_exchange_status(
                conn,
                exchange_id,
                "awaiting_transfer",
                expires_at=expires_at.isoformat(),
                wallet_address=wallet_addr,
            )
            app = getattr(context, "application", None)
            jq = getattr(app, "job_queue", None) if app else None
            if jq:
                jq.run_once(
                    expire_exchange_request,
                    when=timedelta(minutes=30),
                    data={"exchange_id": exchange_id, "user_id": user_id},
                )
            else:
                logger.warning("JobQueue not available; exchange expiry timer not scheduled.")
            await send_message_logged(
                context,
                chat_id=user_id,
                text=(
                    "✅ Admin approved your exchange.\n"
                    f"💼 Deposit wallet: {wallet_addr}\n"
                    "⏳ You have 30 minutes from now to send the token.\n"
                    "⚠️ After this time, your request will be cancelled. Do not send after expiry.\n"
                    "📤 After transfer, first send the tx hash, then send the transaction image."
                ),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("📜 Rules", callback_data="show_rules")],
                        [InlineKeyboardButton("🔄 Exchange", callback_data="show_exchange")],
                    ]
                ),
            )
            # Track that we are waiting for the user to send hash then screenshot
            exchange_collect[user_id] = {"exchange_id": exchange_id, "stage": "wait_hash"}
            await send_message_logged(context, chat_id=user_id, text="🔗 Please send the transaction hash now.")
            user_row = await get_user(conn, user_id)
            username = user_row["username"] if user_row and user_row["username"] else "No username"
            info_text = build_user_info_text(user_row, username, prefix=f"Exchange #{exchange_id}")
            await reply_text_logged(update.message, context, 
                f"{info_text}\nWallet sent to user for exchange #{exchange_id}."
            )
            wallet_flow.pop(update.effective_user.id, None)
            return
        flow = payout_flow.get(update.effective_user.id)
        if flow:
            stage = flow.get("stage")
            if stage in {"wait_hash_or_photo", "wait_hash"} and update.message.text:
                flow["payout_tx_hash"] = update.message.text.strip()
                if flow.get("payout_screenshot_file_id"):
                    await finalize_payout(update, context, flow)
                    payout_flow.pop(update.effective_user.id, None)
                    return
                flow["stage"] = "wait_photo"
                await reply_text_logged(update.message, context, 
                    f"Got hash. Now send payout screenshot for exchange #{flow['exchange_id']}."
                )
                return
            if stage in {"wait_hash_or_photo", "wait_photo"} and update.message.photo:
                flow["payout_screenshot_file_id"] = update.message.photo[-1].file_id
                if flow.get("payout_tx_hash"):
                    await finalize_payout(update, context, flow)
                    payout_flow.pop(update.effective_user.id, None)
                    return
                flow["stage"] = "wait_hash"
                await reply_text_logged(update.message, context, 
                    f"Screenshot saved. Now send payout hash for exchange #{flow['exchange_id']}."
                )
                return

    # Handle user-side exchange submission after wallet approval
    if update.effective_user:
        wallet_wait = payout_wallet_collect.get(update.effective_user.id)
        if wallet_wait:
            if not update.message.text:
                await reply_text_logged(update.message, context, "Please send your BEP20 wallet address as text.")
                return
            exchange_id = wallet_wait["exchange_id"]
            wallet_addr = update.message.text.strip()
            conn: aiosqlite.Connection = context.bot_data["db"]
            await set_exchange_status(
                conn,
                exchange_id,
                "awaiting_payout",
                user_wallet_address=wallet_addr,
            )
            await log_user_message(
                conn,
                update.effective_user.id,
                "payout_wallet",
                content=wallet_addr,
                chat_type=update.effective_chat.type if update.effective_chat else "",
            )
            if update.message:
                mark_logged(context, update.message.message_id)
            payout_wallet_collect.pop(update.effective_user.id, None)
            await reply_text_logged(update.message, context, "Wallet received. Your payout is being prepared.")

            user_row = await get_user(conn, update.effective_user.id)
            username = f"@{update.effective_user.username}" if update.effective_user.username else "No username"
            info_text = build_user_info_text(
                user_row, username, prefix=f"Exchange #{exchange_id} (payout wallet received)"
            )
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Send payment", callback_data=f"send_ex:{exchange_id}")]]
            )
            await send_message_logged(
                context,
                chat_id=context.bot_data["config"].admin_chat_id,
                text=f"{info_text}\nUser payout wallet: {wallet_addr}",
                reply_markup=keyboard,
            )
            return

        uflow = exchange_collect.get(update.effective_user.id)
        if uflow:
            exchange_id = uflow["exchange_id"]
            conn: aiosqlite.Connection = context.bot_data["db"]
            if uflow["stage"] == "wait_hash":
                if not update.message.text:
                    await reply_text_logged(update.message, context, "Please send the transaction hash.")
                    return
                uflow["tx_hash"] = update.message.text.strip()
                uflow["stage"] = "wait_photo"
                await reply_text_logged(update.message, context, "Hash received. Now send the payment screenshot.")
                return
            if uflow["stage"] == "wait_photo":
                photo = update.message.photo[-1] if update.message.photo else None
                if not photo:
                    await reply_text_logged(update.message, context, "Please send the payment screenshot.")
                    return
                tx_hash = uflow.get("tx_hash", "")
                screenshot_file_id = photo.file_id
                # Persist on exchange record
                await conn.execute(
                    """
                    UPDATE exchange_requests
                    SET tx_hash = ?, screenshot_file_id = ?, status = 'pending_admin'
                    WHERE id = ?
                    """,
                    (tx_hash, screenshot_file_id, exchange_id),
                )
                await conn.commit()
                exchange_collect.pop(update.effective_user.id, None)
                await reply_text_logged(update.message, context, "Payment submitted. Await admin approval.")

                # Notify admin with full user info and screenshot
                user_row = await get_user(conn, update.effective_user.id)
                username = f"@{update.effective_user.username}" if update.effective_user.username else "No username"
                info_text = build_user_info_text(
                    user_row, username, prefix=f"Exchange request #{exchange_id} (payment submitted)"
                )
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "Confirm payment ✅",
                                callback_data=f"confirm_ex_pay:{exchange_id}",
                            ),
                            InlineKeyboardButton(
                                "Reject payment ❌",
                                callback_data=f"reject_ex_pay:{exchange_id}",
                            ),
                        ]
                    ]
                )
                try:
                    await send_message_logged(
                        context,
                        chat_id=context.bot_data["config"].admin_chat_id,
                        text=f"{info_text}\nUser tx hash: {tx_hash or '---'}",
                        reply_markup=keyboard,
                    )
                    await send_photo_logged(
                        context,
                        chat_id=context.bot_data["config"].admin_chat_id,
                        photo=screenshot_file_id,
                        caption=f"{info_text}\nUser tx hash: {tx_hash or '---'}",
                    )
                except BadRequest as exc:
                    logger.error("Failed to notify admin of exchange payment: %s", exc)
                return

    msg = update.message
    msg_type = "text"
    content = msg.text or msg.caption or ""
    file_id = ""
    file_bytes = None
    file_mime = ""
    if msg.photo:
        msg_type = "photo"
        file_id = msg.photo[-1].file_id
        file_bytes, file_mime = await fetch_file_bytes(context.bot, file_id)
    elif msg.document:
        msg_type = "document"
        file_id = msg.document.file_id
        file_bytes, file_mime = await fetch_file_bytes(context.bot, file_id)
    await log_user_message(
        context.bot_data["db"],
        msg.from_user.id,
        msg_type,
        content=content,
        file_id=file_id,
        chat_type=msg.chat.type if msg.chat else "",
        file_bytes=file_bytes,
        file_mime=file_mime,
    )
    mark_logged(context, msg.message_id)


async def finalize_payout(update: Update, context: CallbackContext, flow: dict) -> None:
    exchange_id = flow.get("exchange_id")
    user_id = flow.get("user_id")
    payout_tx_hash = flow.get("payout_tx_hash", "")
    payout_screenshot_file_id = flow.get("payout_screenshot_file_id", "")
    conn: aiosqlite.Connection = context.bot_data["db"]
    await set_exchange_status(
        conn,
        exchange_id,
        "completed",
        payout_tx_hash=payout_tx_hash,
        payout_screenshot_file_id=payout_screenshot_file_id,
        completed_at=datetime.now(UTC).isoformat(),
    )
    # notify user
    text = (
        "Your exchange has been completed.\n"
        f"Payout tx hash: {payout_tx_hash or '---'}"
    )
    await send_message_logged(
        context,
        chat_id=user_id,
        text=text + "\nPlease allow up to 48 hours for your token to arrive.",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📜 Rules", callback_data="show_rules")],
                [InlineKeyboardButton("🔄 Exchange", callback_data="show_exchange")],
            ]
        ),
    )
    if payout_screenshot_file_id:
        await send_photo_logged(
            context,
            chat_id=user_id,
            photo=payout_screenshot_file_id,
            caption="Payout screenshot",
        )
    await reply_text_logged(update.message, context, 
        f"Payout sent to user {user_id} for exchange #{exchange_id}."
    )


def require_config() -> Config:
    bot_token = os.getenv("BOT_TOKEN", DEFAULT_BOT_TOKEN)
    admin_chat = os.getenv("ADMIN_CHAT_ID", str(DEFAULT_ADMIN_CHAT_ID))
    wallet = os.getenv("COLLECTION_WALLET", DEFAULT_COLLECTION_WALLET)
    token_limit = os.getenv("TOKEN_LIMIT", DEFAULT_TOKEN_LIMIT)
    contact_email = os.getenv("CONTACT_EMAIL", DEFAULT_CONTACT_EMAIL)
    mode = os.getenv("MODE", "polling").lower()
    webhook_url = os.getenv("WEBHOOK_URL")
    port = int(os.getenv("PORT", "8000"))

    if mode == "webhook" and not webhook_url:
        raise RuntimeError("WEBHOOK_URL is required in webhook mode")

    try:
        admin_chat_id = int(admin_chat)
    except ValueError as exc:
        raise RuntimeError("ADMIN_CHAT_ID must be an integer") from exc

    return Config(
        bot_token=bot_token,
        admin_chat_id=admin_chat_id,
        collection_wallet=wallet,
        token_limit=token_limit,
        contact_email=contact_email,
        mode=mode,
        webhook_url=webhook_url,
        port=port,
    )


async def get_profile_info(update: Update, context: CallbackContext) -> dict:
    user = update.effective_user
    username = f"@{user.username}" if user and user.username else ""
    bio = ""
    profile_photo_file_id = ""

    try:
        chat = await context.bot.get_chat(user.id)
        if chat and getattr(chat, "bio", None):
            bio = chat.bio or ""
    except Exception as exc:  # pragma: no cover - non critical
        logger.debug("Could not fetch bio: %s", exc)

    try:
        photos = await context.bot.get_user_profile_photos(user.id, limit=1)
        if photos and photos.total_count > 0:
            profile_photo_file_id = photos.photos[0][-1].file_id
    except Exception as exc:  # pragma: no cover - non critical
        logger.debug("Could not fetch profile photo: %s", exc)

    return {
        "username": username or "",
        "bio": bio,
        "profile_photo_file_id": profile_photo_file_id,
    }


async def upsert_profile_meta(
    conn: aiosqlite.Connection,
    telegram_id: int,
    username: str,
    bio: str,
    profile_photo_file_id: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO users (telegram_id, username, bio, profile_photo_file_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            username = excluded.username,
            bio = excluded.bio,
            profile_photo_file_id = excluded.profile_photo_file_id,
            updated_at = excluded.updated_at
        """,
        (
            telegram_id,
            username,
            bio,
            profile_photo_file_id,
            datetime.now(UTC).isoformat(),
            datetime.now(UTC).isoformat(),
        ),
    )
    await conn.commit()


async def start(update: Update, context: CallbackContext) -> int:
    conn: aiosqlite.Connection = context.bot_data["db"]
    await increment_entry_count(conn, update.effective_user.id)
    await log_user_message(
        conn,
        update.effective_user.id,
        "start",
        content="User tapped /start",
        chat_type=update.effective_chat.type if update.effective_chat else "",
    )
    if update.message:
        mark_logged(context, update.message.message_id)
    # refresh profile metadata on each start
    profile_info = await get_profile_info(update, context)
    await upsert_profile_meta(
        conn,
        update.effective_user.id,
        profile_info["username"],
        profile_info["bio"],
        profile_info["profile_photo_file_id"],
    )
    user = await get_user(conn, update.effective_user.id)
    status = user["status"] if user else "new"
    keyboard = build_main_menu(status)
    prompt = "👋 Hi! Please choose an option:"
    if update.message:
        await reply_text_logged(update.message, context, prompt, reply_markup=keyboard)
    elif update.callback_query:
        await update.callback_query.edit_message_text(prompt, reply_markup=keyboard)
        await update.callback_query.answer()
    return SELECT


async def begin_auth(update: Update, context: CallbackContext) -> int:
    conn: aiosqlite.Connection = context.bot_data["db"]
    user = await get_user(conn, update.effective_user.id)
    if user and user["status"] == "approved":
        keyboard = build_main_menu("approved")
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(
                "✅ You are already verified. Choose an option:", reply_markup=keyboard
            )
        except BadRequest as exc:
            logger.debug("Skip edit_message_text (already set): %s", exc)
        return SELECT
    if user and user["status"] == "needs_update":
        await update.callback_query.answer()
        pending_field = user["pending_field"] or ""
        field_mapping = {
            "name": ("⚠️ Your name seems incorrect. Please enter your full name:", NAME),
            "idnumber": ("⚠️ Your ID number seems incorrect. Please enter it again:", ID_NUMBER),
            "idcard": ("⚠️ Your ID card photo seems unclear. Please re-upload it:", ID_CARD_PHOTO),
            "selfie": ("⚠️ Your selfie with ID seems unclear. Please re-upload it:", SELFIE_WITH_ID),
            "email": ("⚠️ Your email seems incorrect. Please enter it again:", EMAIL),
        }
        prompt, state = field_mapping.get(pending_field, ("✍️ Please enter your full name:", NAME))
        try:
            await update.callback_query.edit_message_text(prompt)
        except BadRequest as exc:
            logger.debug("Skip edit_message_text (already set): %s", exc)
        return state
    await update.callback_query.answer()
    try:
        await update.callback_query.edit_message_text("✍️ Please enter your full name:")
    except BadRequest as exc:
        logger.debug("Skip edit_message_text (already set): %s", exc)
    return NAME


async def show_rules(update: Update, context: CallbackContext) -> int:
    await update.callback_query.answer()
    conn: aiosqlite.Connection = context.bot_data["db"]
    user = await get_user(conn, update.effective_user.id)
    status = user["status"] if user else "new"
    if update.callback_query.data == "back_to_menu":
        keyboard = build_main_menu(status)
        try:
            await update.callback_query.edit_message_text("Hi! Please choose an option:", reply_markup=keyboard)
        except BadRequest as exc:
            logger.debug("Skip edit_message_text (already set): %s", exc)
        return SELECT
    text = (
        "📜 Rules:\n"
        "1) ✅ After full verification you may proceed to change.\n"
        "2) ⏳ Verification takes up to 48 hours.\n"
        "3) 🕑 After sending MML token, wait at least 48 hours.\n"
        "4) 📈 Max change amount for start is 100 MML.\n"
        "5) 🛡️ Bot is for active team members; service stops if misuse is found.\n"
        "6) 🔁 You may request exchange only once every 30 days.\n"
        "7) 💸 Exchange is processed with a 12% fee."
    )
    keyboard = build_rules_menu(status)
    try:
        await update.callback_query.edit_message_text(text, reply_markup=keyboard)
    except BadRequest as exc:
        logger.debug("Skip edit_message_text (already set): %s", exc)
    return SELECT


async def show_exchange(update: Update, context: CallbackContext) -> int:
    await update.callback_query.answer()
    conn: aiosqlite.Connection = context.bot_data["db"]
    user = await get_user(conn, update.effective_user.id)
    status = user["status"] if user else "new"
    if status != "approved":
        text = "⛔ You must be approved first. Please verify, then try Exchange."
        keyboard = build_rules_menu(status)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=keyboard)
        except BadRequest as exc:
            logger.debug("Skip edit_message_text (already set): %s", exc)
        return SELECT
    last_completed = await get_last_completed_exchange_date(conn, update.effective_user.id)
    if last_completed:
        now = datetime.now(UTC)
        delta = now - last_completed
        if delta < timedelta(days=30):
            remaining = timedelta(days=30) - delta
            remaining_days = remaining.days or 1
            try:
                await update.callback_query.edit_message_text(
                    f"⏳ You can exchange only once every 30 days. Please try again in {remaining_days} day(s).",
                    reply_markup=build_rules_menu(status),
                )
            except BadRequest as exc:
                logger.debug("Skip edit_message_text (already set): %s", exc)
            return SELECT

    # Prevent duplicate open exchange
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute(
        """
        SELECT id FROM exchange_requests
        WHERE user_id = ? AND status IN ('pending_admin','awaiting_wallet','awaiting_transfer','awaiting_user_wallet','awaiting_payout')
        ORDER BY id DESC LIMIT 1
        """,
        (update.effective_user.id,),
    )
    existing = await cursor.fetchone()
    if existing:
        try:
            await update.callback_query.edit_message_text(
                "Your exchange request is already waiting for admin. Please wait.",
                reply_markup=build_rules_menu(status),
            )
        except BadRequest as exc:
            logger.debug("Skip edit_message_text (already set): %s", exc)
        return SELECT

    exchange_id = await insert_exchange_request(conn, update.effective_user.id, "", "")
    info_text = build_user_info_text(user, f"@{update.effective_user.username}" if update.effective_user.username else "No username", prefix=f"Exchange request #{exchange_id}")
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Allow exchange ✅",
                    callback_data=f"start_ex_approve:{exchange_id}",
                ),
                InlineKeyboardButton(
                    "Reject exchange ❌",
                    callback_data=f"start_ex_reject:{exchange_id}",
                ),
            ]
        ]
    )
    try:
        await send_message_logged(
            context,
            chat_id=context.bot_data["config"].admin_chat_id,
            text=info_text,
            reply_markup=keyboard,
        )
    except BadRequest as exc:
        logger.error("Failed to notify admin for exchange request: %s", exc)

    try:
        await update.callback_query.edit_message_text(
            "Your exchange request was sent to admin. Please wait for approval."
        )
    except BadRequest as exc:
        logger.debug("Skip edit_message_text (already set): %s", exc)
    return SELECT


async def handle_pending_update(update: Update, context: CallbackContext) -> Optional[int]:
    if not update.message:
        return None
    conn: aiosqlite.Connection = context.bot_data["db"]
    user = await get_user(conn, update.effective_user.id)
    if not user or user["status"] != "needs_update":
        return None
    pending_field = user["pending_field"] or ""

    # Validate and update based on pending_field
    if pending_field == "name":
        if not update.message.text:
            await reply_text_logged(update.message, context, "Please enter your full name in English letters only.")
            return None
        name = update.message.text.strip()
        if not is_english_name(name):
            await reply_text_logged(update.message, context, "Invalid name. Use English letters only.")
            return None
        await update_user_field(conn, user["telegram_id"], "name", name)
        await finalize_pending_update(context, user, "Name", value=name)
        await reply_text_logged(update.message, context, "Name updated. Await admin review.")
        return ConversationHandler.END

    if pending_field == "idnumber":
        if not update.message.text:
            await reply_text_logged(update.message, context, "Please enter your ID number (digits).")
            return None
        idnum = update.message.text.strip()
        if not idnum.isdigit():
            await reply_text_logged(update.message, context, "Invalid ID number. Use digits only.")
            return None
        await update_user_field(conn, user["telegram_id"], "id_number", idnum)
        await finalize_pending_update(context, user, "ID number", value=idnum)
        await reply_text_logged(update.message, context, "ID number updated. Await admin review.")
        return ConversationHandler.END

    if pending_field == "idcard":
        if not update.message.photo:
            await reply_text_logged(update.message, context, "Please resend a clear photo of your ID card.")
            return None
        file_id = update.message.photo[-1].file_id
        await update_user_field(conn, user["telegram_id"], "id_card_file_id", file_id)
        await finalize_pending_update(context, user, "ID card photo", file_id=file_id)
        await reply_text_logged(update.message, context, "ID card photo updated. Await admin review.")
        return ConversationHandler.END

    if pending_field == "selfie":
        if not update.message.photo:
            await reply_text_logged(update.message, context, "Please resend a clear selfie holding the ID card.")
            return None
        file_id = update.message.photo[-1].file_id
        await update_user_field(conn, user["telegram_id"], "selfie_with_id_file_id", file_id)
        await finalize_pending_update(context, user, "Selfie with ID", file_id=file_id)
        await reply_text_logged(update.message, context, "Selfie updated. Await admin review.")
        return ConversationHandler.END

    if pending_field == "email":
        if not update.message.text:
            await reply_text_logged(update.message, context, "Please enter your email.")
            return None
        email = update.message.text.strip()
        await update_user_field(conn, user["telegram_id"], "email", email)
        await finalize_pending_update(context, user, "Email", value=email)
        await reply_text_logged(update.message, context, "Email updated. Await admin review.")
        return ConversationHandler.END

    return None


async def collect_name(update: Update, context: CallbackContext) -> int:
    if not update.message or not update.message.text:
        await reply_text_logged(update.message, context, "Invalid input. Please enter your full name (text only).")
        return NAME
    name = update.message.text.strip()
    if not is_english_name(name):
        await reply_text_logged(update.message, context, 
            "Invalid input. Please enter your full name in English letters only."
        )
        return NAME
    context.user_data["name"] = name
    conn: aiosqlite.Connection = context.bot_data["db"]
    existing = await get_user(conn, update.effective_user.id)
    if existing and existing["status"] == "needs_update":
        await update_user_field(conn, update.effective_user.id, "name", name)
        await set_pending_field(conn, update.effective_user.id, None)
        await set_user_status(conn, update.effective_user.id, "pending")
        await send_full_info_to_admin(
            context,
            user_id=update.effective_user.id,
            id_card_file_id=existing.get("id_card_file_id"),
            selfie_file_id=existing.get("selfie_with_id_file_id"),
            keyboard=build_verification_keyboard(update.effective_user.id),
            prefix="Verification update:",
        )
        await reply_text_logged(update.message, context, "✅ Name updated. Await admin review.")
        return ConversationHandler.END
    await log_user_message(
        context.bot_data["db"],
        update.effective_user.id,
        "name",
        context.user_data["name"],
        chat_type=update.effective_chat.type if update.effective_chat else "",
    )
    if update.message:
        mark_logged(context, update.message.message_id)
    await reply_text_logged(update.message, context, "Please send a photo of your ID card:")
    return ID_CARD_PHOTO


async def collect_id_card(update: Update, context: CallbackContext) -> int:
    photo = update.message.photo[-1] if update.message and update.message.photo else None
    if not photo:
        await reply_text_logged(update.message, context, "Invalid input. Please send a photo of your ID card.")
        return ID_CARD_PHOTO
    context.user_data["id_card_file_id"] = photo.file_id
    file_bytes, file_mime = await fetch_file_bytes(context.bot, photo.file_id)
    conn: aiosqlite.Connection = context.bot_data["db"]
    existing = await get_user(conn, update.effective_user.id)
    if existing and existing["status"] == "needs_update":
        await update_user_field(conn, update.effective_user.id, "id_card_file_id", photo.file_id)
        await set_pending_field(conn, update.effective_user.id, None)
        await set_user_status(conn, update.effective_user.id, "pending")
        await send_full_info_to_admin(
            context,
            user_id=update.effective_user.id,
            id_card_file_id=photo.file_id,
            selfie_file_id=existing.get("selfie_with_id_file_id"),
            keyboard=build_verification_keyboard(update.effective_user.id),
            prefix="Verification update:",
        )
        await reply_text_logged(update.message, context, "✅ ID card photo updated. Await admin review.")
        return ConversationHandler.END
    await log_user_message(
        context.bot_data["db"],
        update.effective_user.id,
        "id_card_photo",
        file_id=photo.file_id,
        chat_type=update.effective_chat.type if update.effective_chat else "",
        file_bytes=file_bytes,
        file_mime=file_mime,
    )
    if update.message:
        mark_logged(context, update.message.message_id)
    await reply_text_logged(update.message, context, "Enter your ID number:")
    return ID_NUMBER


async def collect_id_number(update: Update, context: CallbackContext) -> int:
    if not update.message or not update.message.text:
        await reply_text_logged(update.message, context, "Invalid input. Please enter your ID number (text).")
        return ID_NUMBER
    id_number = update.message.text.strip()
    if not id_number.isdigit():
        await reply_text_logged(update.message, context, "Invalid input. Please enter your ID number using digits only.")
        return ID_NUMBER
    context.user_data["id_number"] = id_number
    conn: aiosqlite.Connection = context.bot_data["db"]
    existing = await get_user(conn, update.effective_user.id)
    if existing and existing["status"] == "needs_update":
        await update_user_field(conn, update.effective_user.id, "id_number", id_number)
        await set_pending_field(conn, update.effective_user.id, None)
        await set_user_status(conn, update.effective_user.id, "pending")
        await send_full_info_to_admin(
            context,
            user_id=update.effective_user.id,
            id_card_file_id=existing.get("id_card_file_id"),
            selfie_file_id=existing.get("selfie_with_id_file_id"),
            keyboard=build_verification_keyboard(update.effective_user.id),
            prefix="Verification update:",
        )
        await reply_text_logged(update.message, context, "✅ ID number updated. Await admin review.")
        return ConversationHandler.END
    await log_user_message(
        context.bot_data["db"],
        update.effective_user.id,
        "id_number",
        context.user_data["id_number"],
        chat_type=update.effective_chat.type if update.effective_chat else "",
    )
    if update.message:
        mark_logged(context, update.message.message_id)
    await reply_text_logged(update.message, context, "Send a photo of yourself holding the ID card:")
    return SELFIE_WITH_ID


async def collect_selfie_with_id(update: Update, context: CallbackContext) -> int:
    photo = update.message.photo[-1] if update.message and update.message.photo else None
    if not photo:
        await reply_text_logged(update.message, context, 
            "Invalid input. Please send a photo of yourself holding the ID card."
        )
        return SELFIE_WITH_ID
    context.user_data["selfie_with_id_file_id"] = photo.file_id
    file_bytes, file_mime = await fetch_file_bytes(context.bot, photo.file_id)
    conn: aiosqlite.Connection = context.bot_data["db"]
    existing = await get_user(conn, update.effective_user.id)
    if existing and existing["status"] == "needs_update":
        await update_user_field(
            conn, update.effective_user.id, "selfie_with_id_file_id", photo.file_id
        )
        await set_pending_field(conn, update.effective_user.id, None)
        await set_user_status(conn, update.effective_user.id, "pending")
        await send_full_info_to_admin(
            context,
            user_id=update.effective_user.id,
            id_card_file_id=existing.get("id_card_file_id"),
            selfie_file_id=photo.file_id,
            keyboard=build_verification_keyboard(update.effective_user.id),
            prefix="Verification update:",
        )
        await reply_text_logged(update.message, context, "✅ Selfie with ID updated. Await admin review.")
        return ConversationHandler.END
    await log_user_message(
        context.bot_data["db"],
        update.effective_user.id,
        "selfie_with_id",
        file_id=photo.file_id,
        chat_type=update.effective_chat.type if update.effective_chat else "",
        file_bytes=file_bytes,
        file_mime=file_mime,
    )
    if update.message:
        mark_logged(context, update.message.message_id)
    await reply_text_logged(update.message, context, "Enter your email address:")
    return EMAIL


async def collect_email(update: Update, context: CallbackContext) -> int:
    if not update.message or not update.message.text:
        await reply_text_logged(update.message, context, "Invalid input. Please enter your email address (text).")
        return EMAIL
    context.user_data["email"] = update.message.text.strip()
    conn: aiosqlite.Connection = context.bot_data["db"]
    existing = await get_user(conn, update.effective_user.id)
    if existing and existing["status"] == "needs_update":
        await update_user_field(conn, update.effective_user.id, "email", context.user_data["email"])
        await set_pending_field(conn, update.effective_user.id, None)
        await set_user_status(conn, update.effective_user.id, "pending")
        await send_full_info_to_admin(
            context,
            user_id=update.effective_user.id,
            id_card_file_id=existing.get("id_card_file_id"),
            selfie_file_id=existing.get("selfie_with_id_file_id"),
            keyboard=build_verification_keyboard(update.effective_user.id),
            prefix="Verification update:",
        )
        await reply_text_logged(update.message, context, "✅ Email updated. Await admin review.")
        return ConversationHandler.END
    await log_user_message(
        context.bot_data["db"],
        update.effective_user.id,
        "email",
        context.user_data["email"],
        chat_type=update.effective_chat.type if update.effective_chat else "",
    )
    if update.message:
        mark_logged(context, update.message.message_id)
    profile_info = await get_profile_info(update, context)

    await insert_user(
        conn=conn,
        telegram_id=update.effective_user.id,
        name=context.user_data["name"],
        id_number=context.user_data["id_number"],
        id_card_file_id=context.user_data["id_card_file_id"],
        selfie_with_id_file_id=context.user_data["selfie_with_id_file_id"],
        email=context.user_data["email"],
        username=profile_info["username"],
        bio=profile_info["bio"],
        profile_photo_file_id=profile_info["profile_photo_file_id"],
    )
    await send_full_info_to_admin(
        context,
        user_id=update.effective_user.id,
        id_card_file_id=context.user_data["id_card_file_id"],
        selfie_file_id=context.user_data["selfie_with_id_file_id"],
        keyboard=build_verification_keyboard(update.effective_user.id),
    )

    await reply_text_logged(update.message, context, "Your info has been received. Please wait for admin approval.")
    return ConversationHandler.END


async def collect_exchange_hash(update: Update, context: CallbackContext) -> int:
    tx_hash = (update.message.text or "").strip()
    if not tx_hash:
        await reply_text_logged(update.message, context, "Please send the transaction hash.")
        return EX_HASH
    context.user_data["exchange_tx_hash"] = tx_hash
    await log_user_message(
        context.bot_data["db"],
        update.effective_user.id,
        "exchange_hash",
        content=tx_hash,
        chat_type=update.effective_chat.type if update.effective_chat else "",
    )
    if update.message:
        mark_logged(context, update.message.message_id)
    await reply_text_logged(update.message, context, "Now send the payment screenshot.")
    return EX_SCREEN


async def collect_exchange_screenshot(update: Update, context: CallbackContext) -> int:
    photo = update.message.photo[-1] if update.message.photo else None
    if not photo:
        await reply_text_logged(update.message, context, "Please send the payment screenshot.")
        return EX_SCREEN
    file_bytes, file_mime = await fetch_file_bytes(context.bot, photo.file_id)

    conn: aiosqlite.Connection = context.bot_data["db"]
    exchange_id = await insert_exchange_request(
        conn,
        update.effective_user.id,
        context.user_data.get("exchange_tx_hash", ""),
        photo.file_id,
    )
    await log_user_message(
        conn,
        update.effective_user.id,
        "exchange_screenshot",
        file_id=photo.file_id,
        chat_type=update.effective_chat.type if update.effective_chat else "",
        file_bytes=file_bytes,
        file_mime=file_mime,
    )
    if update.message:
        mark_logged(context, update.message.message_id)

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Allow exchange ✅",
                    callback_data=f"start_ex_approve:{exchange_id}",
                ),
                InlineKeyboardButton(
                    "Reject exchange ❌",
                    callback_data=f"start_ex_reject:{exchange_id}",
                ),
            ]
        ]
    )
    username = f"@{update.effective_user.username}" if update.effective_user.username else "No username"
    user_row = await get_user(conn, update.effective_user.id)
    info_text = build_user_info_text(user_row, username, prefix=f"Exchange request #{exchange_id}")
    await send_message_logged(
        context,
        chat_id=context.bot_data["config"].admin_chat_id,
        text=info_text,
        reply_markup=keyboard,
    )
    await send_photo_logged(
        context,
        chat_id=context.bot_data["config"].admin_chat_id,
        photo=photo.file_id,
        caption=info_text,
    )

    await reply_text_logged(update.message, context, "Your request has been submitted. Please wait.")
    context.user_data.pop("exchange_tx_hash", None)
    return ConversationHandler.END


async def handle_user_approval(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != context.bot_data["config"].admin_chat_id:
        await query.edit_message_text("Access denied.")
        return
    data = query.data
    action, telegram_id_str = data.split(":", 1)
    telegram_id = int(telegram_id_str)
    conn: aiosqlite.Connection = context.bot_data["db"]

    if action == "approve_user":
        await set_user_status(conn, telegram_id, "approved")
        await send_message_logged(
            context,
            chat_id=telegram_id,
            text=(
                "Your information has been approved. Choose your next step.\n"
                f"Deposit wallet: {context.bot_data['config'].collection_wallet}\n"
                f"Limit: {context.bot_data['config'].token_limit}\n"
                "After deposit, send tx hash and screenshot.\n"
                f"For more info, contact: {context.bot_data['config'].contact_email}"
            ),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Rules", callback_data="show_rules")],
                    [InlineKeyboardButton("Exchange", callback_data="show_exchange")],
                ]
            ),
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        await reply_text_logged(query.message, context, f"User {telegram_id} approved.")
    elif action == "reject_user":
        await set_user_status(conn, telegram_id, "rejected")
        await send_message_logged(
            context,
            chat_id=telegram_id,
            text=(
                "Your information was not approved. You can /start again to edit and resubmit your details."
            ),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Rules", callback_data="show_rules")],
                    [InlineKeyboardButton("Verify & Start", callback_data="begin_auth")],
                ]
            ),
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        await reply_text_logged(query.message, context, f"User {telegram_id} rejected.")


async def handle_field_issue(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != context.bot_data["config"].admin_chat_id:
        await query.edit_message_text("Access denied.")
        return
    _, field_key, telegram_id_str = query.data.split(":", 2)
    telegram_id = int(telegram_id_str)
    conn: aiosqlite.Connection = context.bot_data["db"]
    await set_user_status(conn, telegram_id, "needs_update")
    await set_pending_field(conn, telegram_id, field_key)

    field_messages = {
        "name": "Your name seems incorrect. Please enter your correct full name in English.",
        "idnumber": "Your ID number seems incorrect. Please enter the correct ID number (digits).",
        "idcard": "Your ID card photo is not clear. Please resend a clear ID card photo.",
        "selfie": "Your selfie with ID is not clear. Please resend a clear selfie with the ID card.",
        "email": "Your email seems incorrect. Please enter the correct email.",
    }
    msg = field_messages.get(
        field_key,
        "There is an issue with your submission. Please resend the correct information.",
    )
    await send_message_logged(
        context,
        chat_id=telegram_id,
        text=msg,
    )
    # Remove buttons but keep the info message intact
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except BadRequest:
        pass
    await reply_text_logged(query.message, context, f"Requested resubmission of {field_key} from user {telegram_id}.")


async def payment_handler(update: Update, context: CallbackContext) -> None:
    conn: aiosqlite.Connection = context.bot_data["db"]
    user = await get_user(conn, update.effective_user.id)
    if not user or user["status"] != "approved":
        # Ignore payments from non-approved users to avoid confusing messages during verification updates
        return

    photo = update.message.photo[-1] if update.message.photo else None
    incoming_hash = (update.message.caption or update.message.text or "").strip()

    # If only hash is sent, store it and ask for screenshot
    if not photo and incoming_hash:
        context.user_data["pending_payment_hash"] = incoming_hash
        await reply_text_logged(update.message, context, "Hash received. Now send the payment screenshot.")
        return

    # If only photo is sent but no hash known, ask for hash first
    if photo and not (incoming_hash or context.user_data.get("pending_payment_hash")):
        await reply_text_logged(update.message, context, "Please send the transaction hash first, then the screenshot.")
        return

    # Both photo and hash available
    tx_hash = incoming_hash or context.user_data.get("pending_payment_hash", "")
    if not photo:
        await reply_text_logged(update.message, context, "Please send the payment screenshot.")
        return

    screenshot_file_id = photo.file_id
    file_bytes, file_mime = await fetch_file_bytes(context.bot, screenshot_file_id)
    context.user_data.pop("pending_payment_hash", None)

    await insert_payment(conn, update.effective_user.id, tx_hash, screenshot_file_id)
    await log_user_message(
        conn,
        update.effective_user.id,
        "payment",
        content=tx_hash,
        file_id=screenshot_file_id,
        chat_type=update.effective_chat.type if update.effective_chat else "",
        file_bytes=file_bytes,
        file_mime=file_mime,
    )
    if update.message:
        mark_logged(context, update.message.message_id)

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Approve payment ✅",
                    callback_data=f"approve_pay:{update.effective_user.id}",
                ),
                InlineKeyboardButton(
                    "Reject payment ❌",
                    callback_data=f"reject_pay:{update.effective_user.id}",
                ),
            ]
        ]
    )
    try:
        await send_message_logged(
            context,
            chat_id=context.bot_data["config"].admin_chat_id,
            text=(
                f"New payment from user {update.effective_user.id}\n"
                f"Hash/Note: {tx_hash or '---'}"
            ),
            reply_markup=keyboard,
        )
        if photo:
            await send_photo_logged(
                context,
                chat_id=context.bot_data["config"].admin_chat_id,
                photo=screenshot_file_id,
                caption=f"Payment screenshot user {update.effective_user.id}",
            )
    except BadRequest as exc:
        logger.error("Failed to send payment to admin: %s", exc)
        await reply_text_logged(update.message, context, 
            "Payment stored but failed to notify admin. Please check admin chat id."
        )

    await reply_text_logged(update.message, context, "Payment recorded. Await admin approval.")


async def handle_payment_approval(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != context.bot_data["config"].admin_chat_id:
        await query.edit_message_text("Access denied.")
        return
    action, telegram_id_str = query.data.split(":", 1)
    telegram_id = int(telegram_id_str)
    conn: aiosqlite.Connection = context.bot_data["db"]

    if action == "approve_pay":
        await conn.execute(
            "UPDATE payments SET status = 'approved' WHERE user_id = ? AND status = 'pending'",
            (telegram_id,),
        )
        await conn.commit()
        await send_message_logged(
            context,
            chat_id=telegram_id,
            text=(
                "Payment approved.\n"
                "Please send your USDT BEP20 address. If the address is incorrect, the responsibility is on you."
            ),
        )
        await query.edit_message_text(f"Payment of user {telegram_id} approved.")
    elif action == "reject_pay":
        await conn.execute(
            "UPDATE payments SET status = 'rejected' WHERE user_id = ? AND status = 'pending'",
            (telegram_id,),
        )
        await conn.commit()
        await send_message_logged(
            context,
            chat_id=telegram_id,
            text="Payment rejected. Please contact support.",
        )
        await query.edit_message_text(f"Payment of user {telegram_id} rejected.")


async def handle_exchange_approval(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != context.bot_data["config"].admin_chat_id:
        await query.edit_message_text("Access denied.")
        return

    action, ex_id_str = query.data.split(":", 1)
    exchange_id = int(ex_id_str)
    conn: aiosqlite.Connection = context.bot_data["db"]
    exchange = await get_exchange(conn, exchange_id)
    if not exchange:
        await query.edit_message_text("Exchange request not found.")
        return

    user_id = exchange["user_id"]

    if action in {"start_ex_approve", "approve_ex"}:
        approved_at = datetime.now(UTC)
        await set_exchange_status(
            conn,
            exchange_id,
            "awaiting_wallet",
            approved_at=approved_at.isoformat(),
        )
        context.bot_data.setdefault("wallet_flow", {})[update.effective_user.id] = {
            "exchange_id": exchange_id,
            "user_id": user_id,
            "stage": "wait_wallet",
        }
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        await reply_text_logged(query.message, context, 
            f"Send deposit wallet address for exchange #{exchange_id} (user {user_id})."
        )
    elif action == "confirm_ex_pay":
        await set_exchange_status(conn, exchange_id, "awaiting_user_wallet")
        context.bot_data.setdefault("payout_wallet_collect", {})[user_id] = {
            "exchange_id": exchange_id
        }
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        await reply_text_logged(query.message, context, 
            f"Payment confirmed for exchange #{exchange_id}. Waiting for user's BEP20 wallet."
        )
        await send_message_logged(
            context,
            chat_id=user_id,
            text=(
                "✅ Your payment has been approved.\n"
                "⏳ Your tokens will be deposited within 48 hours.\n"
                "🏦 Please send your BEP20 wallet address."
            ),
        )
    elif action == "reject_ex_pay":
        await set_exchange_status(conn, exchange_id, "rejected")
        context.bot_data.setdefault("payout_wallet_collect", {}).pop(user_id, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        await send_message_logged(
            context,
            chat_id=user_id,
            text="Your payment was not approved. Please contact support.",
        )
        await reply_text_logged(query.message, context, f"Exchange request #{exchange_id} rejected after review.")
    elif action in {"start_ex_reject", "reject_ex"}:
        await set_exchange_status(conn, exchange_id, "rejected")
        await send_message_logged(
            context,
            chat_id=user_id,
            text="Your exchange request was rejected. You can try again via Exchange.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Rules", callback_data="show_rules")],
                    [InlineKeyboardButton("Exchange", callback_data="show_exchange")],
                ]
            ),
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        await reply_text_logged(query.message, context, f"Exchange request #{exchange_id} rejected.")
    elif action == "send_ex":
        # begin payout flow for admin: request tx hash then screenshot
        admin_id = update.effective_user.id
        user_wallet_addr = (
            exchange["user_wallet_address"] if "user_wallet_address" in exchange.keys() else None
        )
        if exchange["status"] not in {"awaiting_payout"}:
            await reply_text_logged(query.message, context, 
                f"Exchange #{exchange_id} is in status '{exchange['status']}'. Cannot start payout."
            )
            return
        if not user_wallet_addr:
            await reply_text_logged(query.message, context, 
                f"Payout wallet for exchange #{exchange_id} not received yet."
            )
            return
        context.bot_data.setdefault("payout_flow", {})[admin_id] = {
            "exchange_id": exchange_id,
            "user_id": user_id,
            "user_wallet": user_wallet_addr,
            "stage": "wait_hash_or_photo",
        }
        user_row = await get_user(conn, user_id)
        username = user_row["username"] if user_row and user_row["username"] else "No username"
        info_text = build_user_info_text(user_row, username, prefix=f"Exchange #{exchange_id}")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        await reply_text_logged(query.message, context, 
            (
                f"{info_text}\n"
                f"User payout wallet: {user_wallet_addr}\n"
                f"Send payout tx hash and screenshot for exchange #{exchange_id} (any order)."
            )
        )


async def expire_exchange_request(context: CallbackContext) -> None:
    data = context.job.data or {}
    exchange_id = data.get("exchange_id")
    user_id = data.get("user_id")
    if exchange_id is None or user_id is None:
        return
    conn: aiosqlite.Connection = context.bot_data["db"]
    exchange = await get_exchange(conn, exchange_id)
    if not exchange or exchange["status"] != "awaiting_transfer":
        return
    await set_exchange_status(conn, exchange_id, "expired")
    try:
        await send_message_logged(
            context,
            chat_id=user_id,
            text="Your exchange request expired. Please try again via Exchange.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📜 Rules", callback_data="show_rules")],
                    [InlineKeyboardButton("🔄 Exchange", callback_data="show_exchange")],
                ]
            ),
        )
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to notify user on expiration: %s", exc)


async def error_handler(update: Optional[Update], context: CallbackContext) -> None:
    logger.exception("Unhandled exception: %s", context.error)


async def status_cmd(update: Update, context: CallbackContext) -> None:
    conn: aiosqlite.Connection = context.bot_data["db"]
    user = await get_user(conn, update.effective_user.id)
    if not user:
        await reply_text_logged(update.message, context, "Please /start first.")
        return
    await reply_text_logged(update.message, context, f"Your status: {user['status']}")


def build_main_menu(status: str) -> InlineKeyboardMarkup:
    if status == "approved":
        buttons = [
            [InlineKeyboardButton("Rules", callback_data="show_rules")],
            [InlineKeyboardButton("Exchange", callback_data="show_exchange")],
        ]
    else:
        buttons = [
            [InlineKeyboardButton("Verify & Start", callback_data="begin_auth")],
            [InlineKeyboardButton("Rules", callback_data="show_rules")],
        ]
    return InlineKeyboardMarkup(buttons)


def build_rules_menu(status: str) -> InlineKeyboardMarkup:
    if status == "approved":
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Exchange", callback_data="show_exchange")],
                [InlineKeyboardButton("Back", callback_data="back_to_menu")],
            ]
        )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Verify & Start", callback_data="begin_auth")],
            [InlineKeyboardButton("Back", callback_data="back_to_menu")],
        ]
    )


async def update_field_choice(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()
    _, field_key = query.data.split("_", 1)
    mapping = {
        "name": ("Please enter your full name:", NAME),
        "idnum": ("Enter your ID number:", ID_NUMBER),
        "idcard": ("Please send a photo of your ID card:", ID_CARD_PHOTO),
        "selfie": ("Send a photo of yourself holding the ID card:", SELFIE_WITH_ID),
        "email": ("Enter your email address:", EMAIL),
    }
    prompt, state = mapping.get(field_key, ("Please enter your full name:", NAME))
    try:
        await query.edit_message_text(prompt)
    except BadRequest as exc:
        logger.debug("Skip edit_message_text (already set): %s", exc)
    return state


async def send_update_menu(query, user_row) -> None:
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Update name", callback_data="update_name"),
                InlineKeyboardButton("Update ID number", callback_data="update_idnum"),
            ],
            [
                InlineKeyboardButton("Update ID card photo", callback_data="update_idcard"),
                InlineKeyboardButton("Update selfie with ID", callback_data="update_selfie"),
            ],
            [
                InlineKeyboardButton("Update email", callback_data="update_email"),
                InlineKeyboardButton("Rules", callback_data="show_rules"),
            ],
        ]
    )
    text = "You have a pending update request. Choose the field to update."
    try:
        await query.edit_message_text(text, reply_markup=keyboard)
    except BadRequest as exc:
        logger.debug("Skip edit_message_text (already set): %s", exc)


def build_verification_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Approve & Verify ✅", callback_data=f"approve_user:{user_id}"),
                InlineKeyboardButton("Reject ❌", callback_data=f"reject_user:{user_id}"),
            ],
            [
                InlineKeyboardButton("Issue: Name", callback_data=f"field_issue:name:{user_id}"),
                InlineKeyboardButton("Issue: ID number", callback_data=f"field_issue:idnumber:{user_id}"),
            ],
            [
                InlineKeyboardButton("Issue: ID card photo", callback_data=f"field_issue:idcard:{user_id}"),
                InlineKeyboardButton("Issue: Selfie with ID", callback_data=f"field_issue:selfie:{user_id}"),
                InlineKeyboardButton("Issue: Email", callback_data=f"field_issue:email:{user_id}"),
            ],
        ]
    )


def build_user_info_text(user_row: Optional[aiosqlite.Row], username: str, prefix: str = "New verification request:") -> str:
    if not user_row:
        return f"{prefix}\n(No user data found)"
    get = lambda key: user_row[key] if key in user_row.keys() else None
    return (
        f"{prefix}\n"
        f"Name: {get('name') or 'N/A'}\n"
        f"ID number: {get('id_number') or 'N/A'}\n"
        f"Email: {get('email') or 'N/A'}\n"
        f"Username: {username or 'No username'}\n"
        f"user_id: {get('telegram_id')}"
    )


async def send_full_info_to_admin(
    context: CallbackContext,
    user_id: int,
    id_card_file_id: Optional[str] = None,
    selfie_file_id: Optional[str] = None,
    keyboard: Optional[InlineKeyboardMarkup] = None,
    prefix: str = "New verification request:",
) -> None:
    conn: aiosqlite.Connection = context.bot_data["db"]
    user_row = await get_user(conn, user_id)
    username = user_row["username"] if user_row and user_row["username"] else "No username"
    info_text = build_user_info_text(user_row, username, prefix=prefix)
    try:
        await send_message_logged(
            context,
            chat_id=context.bot_data["config"].admin_chat_id,
            text=info_text,
            reply_markup=keyboard,
        )
        if id_card_file_id:
            await send_photo_logged(
                context,
                chat_id=context.bot_data["config"].admin_chat_id,
                photo=id_card_file_id,
                caption=info_text,
            )
        if selfie_file_id:
            await send_photo_logged(
                context,
                chat_id=context.bot_data["config"].admin_chat_id,
                photo=selfie_file_id,
                caption=info_text,
            )
    except BadRequest as exc:
        logger.error("Failed to send admin notification: %s", exc)


def is_english_name(text: str) -> bool:
    if not text:
        return False
    allowed_extra = set(" -'")
    has_letter = False
    for ch in text:
        if ch.isascii() and ch.isalpha():
            has_letter = True
            continue
        if ch in allowed_extra:
            continue
        return False
    return has_letter


async def notify_admin_field_update(
    context: CallbackContext,
    existing_user: aiosqlite.Row,
    field_label: str,
    value: str = "",
    file_id: str = "",
) -> None:
    admin_chat = context.bot_data["config"].admin_chat_id
    user_id = existing_user["telegram_id"]
    username = existing_user["username"] or "No username"
    text = build_user_info_text(existing_user, username, prefix="Verification update:")
    if field_label:
        text = f"{text}\nUpdated field: {field_label}\n{('Value: ' + value) if value else ''}"
    try:
        await send_message_logged(
            context,
            chat_id=admin_chat,
            text=text,
            reply_markup=build_verification_keyboard(user_id),
        )
        if file_id:
            await send_photo_logged(
                context,
                chat_id=admin_chat,
                photo=file_id,
                caption=f"Updated {field_label} for user {user_id}",
            )
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to notify admin of field update: %s", exc)


async def finalize_pending_update(
    context: CallbackContext, existing_user: aiosqlite.Row, field_label: str, value: str = "", file_id: str = ""
) -> None:
    conn: aiosqlite.Connection = context.bot_data["db"]
    await set_pending_field(conn, existing_user["telegram_id"], None)
    await set_user_status(conn, existing_user["telegram_id"], "pending")
    await send_full_info_to_admin(
        context,
        user_id=existing_user["telegram_id"],
        id_card_file_id=file_id if field_label == "ID card photo" else None,
        selfie_file_id=file_id if field_label == "Selfie with ID" else None,
        keyboard=build_verification_keyboard(existing_user["telegram_id"]),
        prefix="Verification update:",
    )


def main() -> None:
    config = require_config()

    async def post_init(app: Application) -> None:
        app.bot_data["db"] = await init_db()
        app.bot_data["config"] = config

    application: Application = (
        ApplicationBuilder()
        .token(config.bot_token)
        .rate_limiter(AIORateLimiter())
        .post_init(post_init)
        .build()
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            SELECT: [
                CallbackQueryHandler(begin_auth, pattern="^begin_auth$"),
                CallbackQueryHandler(show_rules, pattern="^(show_rules|back_to_menu)$"),
                CallbackQueryHandler(show_exchange, pattern="^show_exchange$"),
                CallbackQueryHandler(update_field_choice, pattern="^update_(name|idnum|idcard|selfie|email)$"),
            ],
            NAME: [MessageHandler(filters.ALL & ~filters.COMMAND, collect_name)],
            ID_CARD_PHOTO: [MessageHandler(filters.ALL & ~filters.COMMAND, collect_id_card)],
            ID_NUMBER: [MessageHandler(filters.ALL & ~filters.COMMAND, collect_id_number)],
            SELFIE_WITH_ID: [MessageHandler(filters.ALL & ~filters.COMMAND, collect_selfie_with_id)],
            EMAIL: [MessageHandler(filters.ALL & ~filters.COMMAND, collect_email)],
            EX_HASH: [MessageHandler(filters.TEXT & ~filters.COMMAND, collect_exchange_hash)],
            EX_SCREEN: [MessageHandler(filters.PHOTO, collect_exchange_screenshot)],
        },
        fallbacks=[CommandHandler("start", start)],
    )

    application.add_handler(conv)
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CallbackQueryHandler(handle_user_approval, pattern="^(approve_user|reject_user):"))
    application.add_handler(CallbackQueryHandler(handle_field_issue, pattern="^field_issue:"))
    application.add_handler(
        CallbackQueryHandler(handle_payment_approval, pattern="^(approve_pay|reject_pay):")
    )
    application.add_handler(
        CallbackQueryHandler(
            handle_exchange_approval,
            pattern="^(start_ex_approve|start_ex_reject|approve_ex|reject_ex|send_ex|confirm_ex_pay|reject_ex_pay):",
        )
    )
    # Global handlers for menu callbacks (work even outside ConversationHandler)
    application.add_handler(CallbackQueryHandler(begin_auth, pattern="^begin_auth$"), group=2)
    application.add_handler(CallbackQueryHandler(show_rules, pattern="^(show_rules|back_to_menu)$"), group=2)
    application.add_handler(CallbackQueryHandler(show_exchange, pattern="^show_exchange$"), group=2)
    # Handle user resubmissions for fields flagged by admin before other message handlers
    application.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, handle_pending_update),
    )
    application.add_handler(
        MessageHandler(
            (filters.PHOTO | (filters.TEXT & ~filters.COMMAND)),
            payment_handler,
        )
    )
    # Log everything else (after main handlers)
    application.add_handler(MessageHandler(filters.ALL, log_any_message), group=1)
    application.add_error_handler(error_handler)

    logger.info("Bot starting in %s mode", config.mode)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        if config.mode == "webhook":
            application.run_webhook(
                listen="0.0.0.0",
                port=config.port,
                webhook_url=config.webhook_url,
                allowed_updates=Update.ALL_TYPES,
                stop_signals=None,
            )
        else:
            application.run_polling(allowed_updates=Update.ALL_TYPES, stop_signals=None)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")


if __name__ == "__main__":
    main()
