"""Telegram Bot System - Single-file edition.

Komponen:
- Userbot (Telethon)   : akun tumbal yang memantau channel.
- Main Bot (aiogram)   : tempat user mendaftarkan bot anak (pakai premium emoji).
- Child Bots (aiogram) : 1 dispatcher dipakai bersama, satu polling task per bot.
- Database (aiosqlite) : SQLite, foreign keys ON, WAL mode.

Cara pakai:
    pip install -r requirements.txt
    python main.py auth     # login akun tumbal pertama kali (interaktif: phone+OTP)
    python main.py          # jalankan semua service
"""

from __future__ import annotations

import asyncio
import html
import logging
import re
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon import TelegramClient, events, utils as tl_utils
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    InviteHashInvalidError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    ImportChatInviteRequest,
)


# =============================================================================
# CONFIG
# =============================================================================
BOT_TOKEN = "8842160943:AAGyDhc2XGNw3oBQtPm7zda3o3pSsx2x_xI"
API_ID = 32066244
API_HASH = "0e20794ad29409b7f18cc37f7e3c4001"
SESSION_NAME = "userbot_session"
DB_PATH = "data.db"

# Kosong = semua user boleh /addbot. Isi list user_id untuk mengunci main bot.
ADMIN_IDS: list[int] = []


# =============================================================================
# PREMIUM EMOJI HELPERS (HTML <tg-emoji> tag dengan fallback)
# =============================================================================
def _e(emoji_id: str, fallback: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


E_TICK   = _e("5472239203590888751", "✅")
E_WARN   = _e("6298650293559101195", "⚠️")
E_INFO   = _e("5382357040008021292", "ℹ️")
E_LIST   = _e("6301010077440542621", "📋")
E_LOCK   = _e("5217572051237228647", "🔒")
E_CROWN  = _e("6300763696641607387", "👑")
E_ROBOT  = _e("5307843983102204243", "🤖")
E_CHART  = _e("5203993413346680064", "📊")
E_BROAD  = _e("5334854907872712254", "📢")
E_USER   = _e("5818715087237549366", "👤")
E_LINK   = _e("6300565608454948938", "🔗")
E_ID     = _e("5818687127000452892", "🆔")
E_GEAR   = _e("5307843983102204243", "⚙️")
E_ROCKET = _e("5328221650884260273", "🚀")
E_TRASH  = _e("5472239203590888751", "🗑")


# =============================================================================
# DATABASE (aiosqlite)
# =============================================================================
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS child_bots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    token        TEXT    UNIQUE NOT NULL,
    bot_id       INTEGER UNIQUE NOT NULL,
    bot_username TEXT,
    owner_id     INTEGER NOT NULL,
    active       INTEGER DEFAULT 1,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS channels (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id   INTEGER UNIQUE NOT NULL,
    title     TEXT,
    username  TEXT
);

CREATE TABLE IF NOT EXISTS bot_channels (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_id     INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    UNIQUE(bot_id, channel_id),
    FOREIGN KEY(bot_id)     REFERENCES child_bots(id) ON DELETE CASCADE,
    FOREIGN KEY(channel_id) REFERENCES channels(id)   ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS triggers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_id     INTEGER NOT NULL,
    keyword    TEXT    NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bot_id, keyword),
    FOREIGN KEY(bot_id) REFERENCES child_bots(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bot_channels_channel ON bot_channels(channel_id);
CREATE INDEX IF NOT EXISTS idx_triggers_bot         ON triggers(bot_id);
"""


class Database:
    def __init__(self, path: str):
        self.path = path

    @asynccontextmanager
    async def _conn(self):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA foreign_keys=ON")
            yield db

    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            await db.commit()

    # ---------- child_bots ----------
    async def add_child_bot(self, token, bot_id, bot_username, owner_id) -> int:
        async with self._conn() as db:
            cur = await db.execute(
                "INSERT INTO child_bots (token, bot_id, bot_username, owner_id) VALUES (?, ?, ?, ?)",
                (token, bot_id, bot_username, owner_id),
            )
            await db.commit()
            return cur.lastrowid

    async def get_child_bot(self, bot_db_id):
        async with self._conn() as db:
            cur = await db.execute("SELECT * FROM child_bots WHERE id=?", (bot_db_id,))
            return await cur.fetchone()

    async def get_child_bot_by_token(self, token):
        async with self._conn() as db:
            cur = await db.execute("SELECT * FROM child_bots WHERE token=?", (token,))
            return await cur.fetchone()

    async def get_child_bots_by_owner(self, owner_id):
        async with self._conn() as db:
            cur = await db.execute(
                "SELECT * FROM child_bots WHERE owner_id=? ORDER BY id DESC", (owner_id,)
            )
            return list(await cur.fetchall())

    async def get_all_child_bots(self):
        async with self._conn() as db:
            cur = await db.execute("SELECT * FROM child_bots WHERE active=1")
            return list(await cur.fetchall())

    async def delete_child_bot(self, bot_db_id, owner_id) -> bool:
        async with self._conn() as db:
            cur = await db.execute(
                "DELETE FROM child_bots WHERE id=? AND owner_id=?", (bot_db_id, owner_id)
            )
            await db.commit()
            return cur.rowcount > 0

    async def deactivate_child_bot(self, bot_db_id) -> None:
        async with self._conn() as db:
            await db.execute(
                "UPDATE child_bots SET active=0 WHERE id=?", (bot_db_id,)
            )
            await db.commit()

    # ---------- channels ----------
    async def upsert_channel(self, chat_id, title, username) -> int:
        async with self._conn() as db:
            cur = await db.execute("SELECT id FROM channels WHERE chat_id=?", (chat_id,))
            row = await cur.fetchone()
            if row:
                await db.execute(
                    "UPDATE channels SET title=?, username=? WHERE id=?",
                    (title, username, row["id"]),
                )
                await db.commit()
                return row["id"]
            cur = await db.execute(
                "INSERT INTO channels (chat_id, title, username) VALUES (?, ?, ?)",
                (chat_id, title, username),
            )
            await db.commit()
            return cur.lastrowid

    async def get_all_channel_chat_ids(self):
        async with self._conn() as db:
            cur = await db.execute(
                "SELECT DISTINCT c.chat_id FROM channels c "
                "JOIN bot_channels bc ON bc.channel_id = c.id"
            )
            return [r["chat_id"] for r in await cur.fetchall()]

    async def get_all_channels_full(self):
        """Semua row channel yang punya minimal 1 bot anak terkait."""
        async with self._conn() as db:
            cur = await db.execute(
                "SELECT DISTINCT c.* FROM channels c "
                "JOIN bot_channels bc ON bc.channel_id = c.id"
            )
            return list(await cur.fetchall())

    async def update_channel_chat_id(self, channel_db_id: int, new_chat_id: int) -> bool:
        async with self._conn() as db:
            try:
                await db.execute(
                    "UPDATE channels SET chat_id=? WHERE id=?",
                    (new_chat_id, channel_db_id),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                # ada row lain dengan chat_id sama -> hapus duplikat
                await db.execute(
                    "DELETE FROM channels WHERE id=?", (channel_db_id,)
                )
                await db.commit()
                return False

    # ---------- bot_channels ----------
    async def link_bot_channel(self, bot_db_id, channel_db_id) -> bool:
        async with self._conn() as db:
            try:
                await db.execute(
                    "INSERT INTO bot_channels (bot_id, channel_id) VALUES (?, ?)",
                    (bot_db_id, channel_db_id),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def unlink_bot_channel(self, bot_db_id, channel_db_id) -> bool:
        async with self._conn() as db:
            cur = await db.execute(
                "DELETE FROM bot_channels WHERE bot_id=? AND channel_id=?",
                (bot_db_id, channel_db_id),
            )
            await db.commit()
            return cur.rowcount > 0

    async def get_channels_for_bot(self, bot_db_id):
        async with self._conn() as db:
            cur = await db.execute(
                "SELECT c.* FROM channels c "
                "JOIN bot_channels bc ON bc.channel_id = c.id "
                "WHERE bc.bot_id=? ORDER BY c.id DESC",
                (bot_db_id,),
            )
            return list(await cur.fetchall())

    async def get_bots_for_channel(self, chat_id):
        async with self._conn() as db:
            cur = await db.execute(
                "SELECT cb.* FROM child_bots cb "
                "JOIN bot_channels bc ON bc.bot_id = cb.id "
                "JOIN channels c ON c.id = bc.channel_id "
                "WHERE c.chat_id=? AND cb.active=1",
                (chat_id,),
            )
            return list(await cur.fetchall())

    # ---------- triggers ----------
    async def add_trigger(self, bot_db_id, keyword) -> bool:
        async with self._conn() as db:
            try:
                await db.execute(
                    "INSERT INTO triggers (bot_id, keyword) VALUES (?, ?)",
                    (bot_db_id, keyword.strip()),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def remove_trigger(self, trigger_id, bot_db_id) -> bool:
        async with self._conn() as db:
            cur = await db.execute(
                "DELETE FROM triggers WHERE id=? AND bot_id=?", (trigger_id, bot_db_id)
            )
            await db.commit()
            return cur.rowcount > 0

    async def get_triggers_for_bot(self, bot_db_id):
        async with self._conn() as db:
            cur = await db.execute(
                "SELECT * FROM triggers WHERE bot_id=? ORDER BY id DESC", (bot_db_id,)
            )
            return list(await cur.fetchall())


# =============================================================================
# USERBOT (Telethon "tumbal")
# =============================================================================
log = logging.getLogger("system")


class Userbot:
    def __init__(self, api_id, api_hash, session, db: "Database",
                 child_manager: "ChildBotManager"):
        self.client = TelegramClient(session, api_id, api_hash)
        self.db = db
        self.child_manager = child_manager
        self._me = None

    async def start(self) -> None:
        # Connect dulu untuk cek session
        if not self.client.is_connected():
            await self.client.connect()
        if not await self.client.is_user_authorized():
            log.warning("=" * 60)
            log.warning("Session userbot BELUM ADA. Memulai login interaktif.")
            log.warning("Anda akan diminta:")
            log.warning("  1) Nomor HP (format internasional, contoh: +628xxx)")
            log.warning("  2) Kode OTP yang dikirim Telegram")
            log.warning("  3) Password 2FA (kalau akun mengaktifkan)")
            log.warning("=" * 60)
            # client.start() handle phone + code + 2FA password lewat input()
            await self.client.start()
        self._me = await self.client.get_me()
        log.info("Userbot login as @%s (id=%s)", self._me.username, self._me.id)

        # Warm up cache + migrasi chat_id ke format yang konsisten dengan
        # event.chat_id. Channel yang ditambahkan di versi lama mungkin
        # punya chat_id format berbeda. Tanpa ini, get_bots_for_channel()
        # tidak akan match dengan event.chat_id dan notif tidak nyala.
        for ch in await self.db.get_all_channels_full():
            try:
                entity = await self.client.get_entity(ch["chat_id"])
                canonical = tl_utils.get_peer_id(entity)
                if canonical != ch["chat_id"]:
                    log.warning(
                        "[migrate] channel db_id=%s chat_id %s -> %s",
                        ch["id"], ch["chat_id"], canonical,
                    )
                    await self.db.update_channel_chat_id(ch["id"], canonical)
            except Exception as e:
                log.warning(
                    "Resolve channel %s gagal saat warm-up: %s. "
                    "Pastikan akun tumbal sudah join.",
                    ch["chat_id"], e,
                )

        self.client.add_event_handler(self._on_message, events.NewMessage())
        log.info("Userbot listening untuk pesan baru di semua channel...")

    async def run(self) -> None:
        await self.client.run_until_disconnected()

    async def _on_message(self, event: events.NewMessage.Event) -> None:
        try:
            chat_id = event.chat_id
            if chat_id is None:
                return
            text = event.message.message or ""
            log.debug(
                "[userbot] new message chat_id=%s text=%r",
                chat_id, (text[:80] + "...") if len(text) > 80 else text,
            )
            if not text:
                return
            bots = await self.db.get_bots_for_channel(chat_id)
            if not bots:
                log.debug("[userbot] no bot watching chat_id=%s", chat_id)
                return
            text_lower = text.lower()
            for bot_row in bots:
                triggers = await self.db.get_triggers_for_bot(bot_row["id"])
                matched = next(
                    (t for t in triggers if t["keyword"].lower() in text_lower),
                    None,
                )
                if not matched:
                    log.debug(
                        "[userbot] no trigger matched bot_id=%s (triggers=%s)",
                        bot_row["id"], [t["keyword"] for t in triggers],
                    )
                    continue
                log.info(
                    "[userbot] MATCH bot_id=%s keyword=%r chat_id=%s",
                    bot_row["id"], matched["keyword"], chat_id,
                )
                link = await self._build_link(event)
                await self.child_manager.notify_owner(
                    bot_row=bot_row,
                    keyword=matched["keyword"],
                    link=link,
                    text=text,
                    chat_title=getattr(event.chat, "title", None) or "",
                )
        except Exception:
            log.exception("Error handling new message")

    async def _build_link(self, event) -> str:
        chat = await event.get_chat()
        msg_id = event.message.id
        username = getattr(chat, "username", None)
        if username:
            return f"https://t.me/{username}/{msg_id}"
        # Private channel/supergroup: format https://t.me/c/<raw_id>/<msg_id>
        raw_id = getattr(chat, "id", None)
        if raw_id is None:
            return ""
        return f"https://t.me/c/{raw_id}/{msg_id}"

    async def join_channel(self, identifier: str):
        """Join channel berdasarkan @username, t.me link, atau invite link.

        Mendukung:
          - @username
          - https://t.me/username
          - https://t.me/joinchat/HASH       (private invite)
          - https://t.me/+HASH               (private invite, format baru)
        """
        ident = identifier.strip()

        # deteksi invite hash dari berbagai format link
        invite_hash = None
        for prefix in ("https://t.me/joinchat/", "http://t.me/joinchat/",
                       "t.me/joinchat/", "joinchat/"):
            if ident.startswith(prefix):
                invite_hash = ident[len(prefix):]
                break
        if invite_hash is None:
            for prefix in ("https://t.me/+", "http://t.me/+", "t.me/+"):
                if ident.startswith(prefix):
                    invite_hash = ident[len(prefix):]
                    break

        if invite_hash:
            invite_hash = invite_hash.split("/")[0].split("?")[0]
            try:
                info = await self.client(CheckChatInviteRequest(invite_hash))
                if hasattr(info, "chat") and info.chat is not None:
                    return info.chat
                upd = await self.client(ImportChatInviteRequest(invite_hash))
                if getattr(upd, "chats", None):
                    return upd.chats[0]
                raise RuntimeError("Tidak bisa resolve channel dari invite link.")
            except UserAlreadyParticipantError:
                info = await self.client(CheckChatInviteRequest(invite_hash))
                if hasattr(info, "chat") and info.chat is not None:
                    return info.chat
                raise RuntimeError("Sudah join tapi tidak bisa resolve chat.")
            except (InviteHashExpiredError, InviteHashInvalidError):
                raise RuntimeError("Invite link expired/invalid.")
            except FloodWaitError as e:
                raise RuntimeError(f"FloodWait {e.seconds}s, coba lagi nanti.")

        # public username / t.me link
        for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
            if ident.startswith(prefix):
                ident = ident[len(prefix):]
                break
        ident = ident.lstrip("@").split("/")[0].split("?")[0]
        if not ident:
            raise RuntimeError("Identifier kosong.")
        entity = await self.client.get_entity(ident)
        try:
            await self.client(JoinChannelRequest(entity))
        except UserAlreadyParticipantError:
            pass
        except FloodWaitError as e:
            raise RuntimeError(f"FloodWait {e.seconds}s, coba lagi nanti.")
        except ChannelPrivateError:
            raise RuntimeError("Channel privat / tidak bisa diakses.")
        return entity


# =============================================================================
# MAIN BOT (aiogram + premium emoji)
# =============================================================================
TOKEN_RE = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,}$")

# Telegram menolak premium emoji yang invalid dengan error berikut:
_PREMIUM_REJECT_TOKENS = ("DOCUMENT_INVALID", "MEDIA_INVALID",
                          "EMOJI_INVALID", "CUSTOM_EMOJI")
_TG_EMOJI_RE = re.compile(r"<tg-emoji[^>]*>(.*?)</tg-emoji>", re.DOTALL)


def _strip_premium_emoji_html(s: str) -> str:
    """Replace <tg-emoji ...>FALLBACK</tg-emoji> with FALLBACK text."""
    return _TG_EMOJI_RE.sub(r"\1", s)


class SafeBot(Bot):
    """Bot yang otomatis fallback kalau premium emoji ditolak Telegram.

    Beberapa premium emoji ID mungkin tidak bisa diakses oleh bot
    (placeholder ID, sticker premium yang tidak available, dll).
    Telegram membalas DOCUMENT_INVALID / MEDIA_INVALID / EMOJI_INVALID
    untuk seluruh pesan. Tanpa fallback, bot akan crash dan user
    tidak terima apa pun.

    SafeBot men-catch error itu, strip semua <tg-emoji> tag jadi
    fallback emoji biasa, lalu retry.
    """

    async def __call__(self, method, request_timeout=None):
        try:
            return await super().__call__(method, request_timeout=request_timeout)
        except TelegramBadRequest as e:
            err = str(e).upper()
            if not any(tok in err for tok in _PREMIUM_REJECT_TOKENS):
                raise
            updates = {}
            for field in ("text", "caption"):
                val = getattr(method, field, None)
                if isinstance(val, str) and "<tg-emoji" in val:
                    updates[field] = _strip_premium_emoji_html(val)
            if not updates:
                raise
            log.warning(
                "Premium emoji ditolak Telegram (%s), retry tanpa premium emoji.",
                e.message if hasattr(e, "message") else e,
            )
            method2 = method.model_copy(update=updates)
            return await super().__call__(method2, request_timeout=request_timeout)


def _kb(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=list(rows))


def _is_admin(uid: int) -> bool:
    return not ADMIN_IDS or uid in ADMIN_IDS


class AddBotState(StatesGroup):
    waiting_token = State()


class MainBot:
    def __init__(self, token: str, db: Database, child_manager: "ChildBotManager"):
        self.bot = SafeBot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
        self.dp = Dispatcher()
        self.db = db
        self.child_manager = child_manager
        self._register()

    def _register(self) -> None:
        dp = self.dp
        dp.message.register(self.cmd_start, CommandStart())
        dp.message.register(self.cmd_help, Command("help"))
        dp.message.register(self.cmd_addbot, Command("addbot"))
        dp.message.register(self.cmd_mybots, Command("mybots"))
        dp.message.register(self.cmd_id, Command("id"))
        dp.message.register(self.process_token, AddBotState.waiting_token)

        dp.callback_query.register(self.cb_addbot, F.data == "addbot")
        dp.callback_query.register(self.cb_mybots, F.data == "mybots")
        dp.callback_query.register(self.cb_back, F.data == "back")
        dp.callback_query.register(self.cb_view_bot, F.data.startswith("bot:"))
        dp.callback_query.register(self.cb_del_bot, F.data.startswith("delbot:"))
        dp.callback_query.register(self.cb_confirm_del, F.data.startswith("confirmdel:"))

    def _menu_main(self) -> InlineKeyboardMarkup:
        return _kb(
            [InlineKeyboardButton(text="➕ Tambah Bot Anak", callback_data="addbot")],
            [InlineKeyboardButton(text="📋 Bot Saya", callback_data="mybots")],
        )

    def _welcome(self, name: str) -> str:
        return (
            f"{E_CROWN} <b>Halo {html.escape(name)}!</b>\n\n"
            f"{E_ROBOT} Bot ini adalah <b>Main Bot</b> untuk membuat & mengelola "
            f"<b>bot anak</b> yang memantau channel Telegram.\n\n"
            f"{E_INFO} <b>Cara kerja:</b>\n"
            f"  • Buat bot di @BotFather, dapatkan token\n"
            f"  • Tambahkan token-nya di sini\n"
            f"  • Pakai bot anak untuk add <b>channel</b> + <b>trigger</b>\n"
            f"  • Saat trigger cocok di channel, kamu dapat notif + link\n\n"
            f"{E_LOCK} Setiap bot anak hanya bisa dipakai owner-nya.\n\n"
            f"{E_ROCKET} Pilih menu di bawah:"
        )

    # commands
    async def cmd_start(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        if not _is_admin(message.from_user.id):
            await message.answer(f"{E_LOCK} <b>Akses ditolak.</b>")
            return
        await message.answer(
            self._welcome(message.from_user.first_name or "user"),
            reply_markup=self._menu_main(),
        )

    async def cmd_help(self, message: Message) -> None:
        await message.answer(
            f"{E_INFO} <b>Bantuan</b>\n\n"
            f"  /start  - menu utama\n"
            f"  /addbot - tambah bot anak\n"
            f"  /mybots - daftar bot anak kamu\n"
            f"  /id     - lihat user id kamu"
        )

    async def cmd_id(self, message: Message) -> None:
        await message.answer(
            f"{E_ID} <b>User ID:</b> <code>{message.from_user.id}</code>"
        )

    async def cmd_addbot(self, message: Message, state: FSMContext) -> None:
        if not _is_admin(message.from_user.id):
            await message.answer(f"{E_LOCK} <b>Akses ditolak.</b>")
            return
        await self._prompt_token(message, state)

    async def cmd_mybots(self, message: Message) -> None:
        await self._send_my_bots(message.chat.id, message.from_user.id)

    # callbacks
    async def cb_addbot(self, query: CallbackQuery, state: FSMContext) -> None:
        if not _is_admin(query.from_user.id):
            await query.answer("Ditolak.", show_alert=True)
            return
        await query.answer()
        await self._prompt_token(query.message, state)

    async def cb_mybots(self, query: CallbackQuery) -> None:
        await query.answer()
        await self._send_my_bots(query.message.chat.id, query.from_user.id, edit=query.message)

    async def cb_back(self, query: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await query.answer()
        text = self._welcome(query.from_user.first_name or "user")
        try:
            await query.message.edit_text(text, reply_markup=self._menu_main())
        except TelegramBadRequest:
            await query.message.answer(text, reply_markup=self._menu_main())

    async def cb_view_bot(self, query: CallbackQuery) -> None:
        await query.answer()
        bot_db_id = int(query.data.split(":", 1)[1])
        row = await self.db.get_child_bot(bot_db_id)
        if not row or row["owner_id"] != query.from_user.id:
            await query.message.edit_text(f"{E_WARN} Bot tidak ditemukan.")
            return
        chs = await self.db.get_channels_for_bot(row["id"])
        tgs = await self.db.get_triggers_for_bot(row["id"])
        text = (
            f"{E_ROBOT} <b>@{html.escape(row['bot_username'])}</b>\n"
            f"{E_ID} ID: <code>{row['bot_id']}</code>\n\n"
            f"{E_BROAD} Channel : <b>{len(chs)}</b>\n"
            f"{E_GEAR} Trigger : <b>{len(tgs)}</b>\n\n"
            f"{E_INFO} Buka <b>@{html.escape(row['bot_username'])}</b> untuk "
            f"mengelola channel & trigger."
        )
        kb = _kb(
            [InlineKeyboardButton(
                text="🤖 Buka Bot Anak",
                url=f"https://t.me/{row['bot_username']}?start=open",
            )],
            [InlineKeyboardButton(text="🗑 Hapus", callback_data=f"delbot:{row['id']}"),
             InlineKeyboardButton(text="« Kembali", callback_data="mybots")],
        )
        await query.message.edit_text(text, reply_markup=kb)

    async def cb_del_bot(self, query: CallbackQuery) -> None:
        await query.answer()
        bot_db_id = int(query.data.split(":", 1)[1])
        row = await self.db.get_child_bot(bot_db_id)
        if not row or row["owner_id"] != query.from_user.id:
            return
        kb = _kb(
            [InlineKeyboardButton(text="✅ Ya, Hapus",
                                  callback_data=f"confirmdel:{row['id']}"),
             InlineKeyboardButton(text="❌ Batal",
                                  callback_data=f"bot:{row['id']}")],
        )
        await query.message.edit_text(
            f"{E_WARN} <b>Hapus bot @{html.escape(row['bot_username'])}?</b>\n\n"
            f"Semua channel & trigger bot ini akan ikut terhapus.",
            reply_markup=kb,
        )

    async def cb_confirm_del(self, query: CallbackQuery) -> None:
        bot_db_id = int(query.data.split(":", 1)[1])
        row = await self.db.get_child_bot(bot_db_id)
        if not row or row["owner_id"] != query.from_user.id:
            await query.answer("Tidak diizinkan.", show_alert=True)
            return
        await self.child_manager.remove_bot(row["id"])
        await self.db.delete_child_bot(row["id"], query.from_user.id)
        await query.answer("Terhapus.")
        await self._send_my_bots(query.message.chat.id, query.from_user.id, edit=query.message)

    # flow
    async def _prompt_token(self, target: Message, state: FSMContext) -> None:
        await state.set_state(AddBotState.waiting_token)
        kb = _kb([InlineKeyboardButton(text="« Batal", callback_data="back")])
        await target.answer(
            f"{E_INFO} <b>Kirim token bot anak.</b>\n\n"
            f"Buat bot baru di <a href='https://t.me/BotFather'>@BotFather</a>, "
            f"copy token-nya, lalu paste di sini.\n\n"
            f"Format: <code>123456:ABC-DEF...</code>",
            reply_markup=kb,
            disable_web_page_preview=True,
        )

    async def process_token(self, message: Message, state: FSMContext) -> None:
        token = (message.text or "").strip()
        if not TOKEN_RE.match(token):
            await message.answer(f"{E_WARN} Token tidak valid. Coba lagi.")
            return
        try:
            tmp = Bot(token=token)
            me = await tmp.get_me()
            await tmp.session.close()
        except Exception as e:
            await message.answer(
                f"{E_WARN} Token tidak bekerja: <code>{html.escape(str(e))}</code>"
            )
            return
        if await self.db.get_child_bot_by_token(token):
            await message.answer(f"{E_WARN} Bot ini sudah terdaftar.")
            await state.clear()
            return

        bot_db_id = await self.db.add_child_bot(
            token=token, bot_id=me.id,
            bot_username=me.username, owner_id=message.from_user.id,
        )
        await self.child_manager.add_bot(token=token, bot_db_id=bot_db_id)
        await state.clear()

        kb = _kb(
            [InlineKeyboardButton(
                text="🤖 Buka Bot Anak",
                url=f"https://t.me/{me.username}?start=open",
            )],
            [InlineKeyboardButton(text="📋 Bot Saya", callback_data="mybots")],
        )
        await message.answer(
            f"{E_TICK} <b>Bot anak berhasil ditambahkan!</b>\n\n"
            f"{E_ROBOT} Bot   : @{html.escape(me.username)}\n"
            f"{E_ID} ID    : <code>{me.id}</code>\n"
            f"{E_USER} Owner : <code>{message.from_user.id}</code>\n\n"
            f"{E_ROCKET} Buka bot anak untuk mulai tambah channel & trigger.",
            reply_markup=kb,
        )

    async def _send_my_bots(self, chat_id: int, user_id: int,
                            edit: Optional[Message] = None) -> None:
        bots = await self.db.get_child_bots_by_owner(user_id)
        if not bots:
            text = (
                f"{E_LIST} <b>Bot Saya</b>\n\n"
                f"{E_INFO} Belum ada bot anak. Tekan tombol di bawah."
            )
            kb = _kb(
                [InlineKeyboardButton(text="➕ Tambah Bot Anak", callback_data="addbot")],
                [InlineKeyboardButton(text="« Kembali", callback_data="back")],
            )
        else:
            text = f"{E_LIST} <b>Bot Saya ({len(bots)})</b>\n\nPilih bot untuk detail:"
            rows = [
                [InlineKeyboardButton(
                    text=f"🤖 @{b['bot_username']}",
                    callback_data=f"bot:{b['id']}",
                )]
                for b in bots
            ]
            rows.append([InlineKeyboardButton(text="➕ Tambah", callback_data="addbot"),
                         InlineKeyboardButton(text="« Kembali", callback_data="back")])
            kb = InlineKeyboardMarkup(inline_keyboard=rows)
        if edit is not None:
            try:
                await edit.edit_text(text, reply_markup=kb)
                return
            except TelegramBadRequest:
                pass
        await self.bot.send_message(chat_id, text, reply_markup=kb)

    async def start(self) -> None:
        log.info("Main bot polling...")
        await self.dp.start_polling(self.bot, handle_signals=False)


# =============================================================================
# CHILD BOT (dispatcher bersama untuk semua bot anak)
# =============================================================================
class ChildState(StatesGroup):
    add_channel = State()
    add_trigger = State()


def build_child_dispatcher(db: Database, userbot: Userbot) -> Dispatcher:
    dp = Dispatcher()

    async def _bot_row(bot: Bot):
        return await db.get_child_bot_by_token(bot.token)

    async def _owner_only(event, bot: Bot):
        row = await _bot_row(bot)
        if not row:
            return None
        if event.from_user.id != row["owner_id"]:
            if isinstance(event, CallbackQuery):
                await event.answer("🔒 Bot ini privat.", show_alert=True)
            else:
                await event.answer("🔒 Bot ini privat. Hanya owner yang bisa pakai.")
            return None
        return row

    async def _show_status(target: Message, row, edit: bool = False) -> None:
        chs = await db.get_channels_for_bot(row["id"])
        tgs = await db.get_triggers_for_bot(row["id"])
        text = (
            f"📋 <b>Status Bot</b>\n\n"
            f"🤖 @{html.escape(row['bot_username'])}\n"
            f"📢 Channel : <b>{len(chs)}</b>\n"
            f"🔔 Trigger : <b>{len(tgs)}</b>"
        )
        kb = _kb(
            [InlineKeyboardButton(text="📢 Channel", callback_data="ch:menu"),
             InlineKeyboardButton(text="🔔 Trigger", callback_data="tg:menu")],
            [InlineKeyboardButton(text="🏠 Home", callback_data="home")],
        )
        if edit:
            try:
                await target.edit_text(text, reply_markup=kb)
                return
            except TelegramBadRequest:
                pass
        await target.answer(text, reply_markup=kb)

    async def _show_channels(target: Message, row, edit: bool = False) -> None:
        chs = await db.get_channels_for_bot(row["id"])
        if not chs:
            text = "📢 <b>Channel</b>\n\nBelum ada channel."
            rows = [[InlineKeyboardButton(text="➕ Tambah", callback_data="ch:add")],
                    [InlineKeyboardButton(text="🏠 Home", callback_data="home")]]
        else:
            text = f"📢 <b>Channel ({len(chs)})</b>\n\nKlik untuk hapus:"
            rows = []
            for c in chs:
                label = c["title"] or c["username"] or str(c["chat_id"])
                rows.append([InlineKeyboardButton(
                    text=f"🗑 {label[:40]}",
                    callback_data=f"ch:del:{c['id']}",
                )])
            rows.append([InlineKeyboardButton(text="➕ Tambah", callback_data="ch:add"),
                         InlineKeyboardButton(text="🏠 Home", callback_data="home")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        if edit:
            try:
                await target.edit_text(text, reply_markup=kb)
                return
            except TelegramBadRequest:
                pass
        await target.answer(text, reply_markup=kb)

    async def _show_triggers(target: Message, row, edit: bool = False) -> None:
        tgs = await db.get_triggers_for_bot(row["id"])
        if not tgs:
            text = "🔔 <b>Trigger</b>\n\nBelum ada trigger."
            rows = [[InlineKeyboardButton(text="➕ Tambah", callback_data="tg:add")],
                    [InlineKeyboardButton(text="🏠 Home", callback_data="home")]]
        else:
            text = f"🔔 <b>Trigger ({len(tgs)})</b>\n\nKlik untuk hapus:"
            rows = []
            for t in tgs:
                rows.append([InlineKeyboardButton(
                    text=f"🗑 {t['keyword'][:40]}",
                    callback_data=f"tg:del:{t['id']}",
                )])
            rows.append([InlineKeyboardButton(text="➕ Tambah", callback_data="tg:add"),
                         InlineKeyboardButton(text="🏠 Home", callback_data="home")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        if edit:
            try:
                await target.edit_text(text, reply_markup=kb)
                return
            except TelegramBadRequest:
                pass
        await target.answer(text, reply_markup=kb)

    async def _prompt_add_channel(target: Message, state: FSMContext) -> None:
        await state.set_state(ChildState.add_channel)
        kb = _kb([InlineKeyboardButton(text="« Batal", callback_data="ch:menu")])
        await target.answer(
            "📢 <b>Tambah Channel</b>\n\n"
            "Kirim <b>@username</b> channel atau <b>invite link</b>.\n"
            "Akun tumbal akan otomatis bergabung untuk memantau.",
            reply_markup=kb,
        )

    async def _prompt_add_trigger(target: Message, state: FSMContext) -> None:
        await state.set_state(ChildState.add_trigger)
        kb = _kb([InlineKeyboardButton(text="« Batal", callback_data="tg:menu")])
        await target.answer(
            "🔔 <b>Tambah Trigger</b>\n\n"
            "Kirim keyword (min. 2 karakter).\n"
            "Match dilakukan <b>case-insensitive</b> (substring).",
            reply_markup=kb,
        )

    # ---- /start ----
    @dp.message(CommandStart())
    async def start_cmd(message: Message, bot: Bot, state: FSMContext) -> None:
        await state.clear()
        row = await _owner_only(message, bot)
        if not row:
            return
        text = (
            f"👑 <b>Halo Owner!</b>\n\n"
            f"🤖 Ini bot anak <b>@{html.escape(row['bot_username'])}</b>.\n\n"
            f"📢 Tambah channel yang mau dipantau.\n"
            f"🔔 Tambah trigger keyword.\n\n"
            f"Saat ada pesan baru di channel yang cocok dengan trigger, "
            f"kamu akan dapat notifikasi + link pesan."
        )
        kb = _kb(
            [InlineKeyboardButton(text="📢 Channel", callback_data="ch:menu"),
             InlineKeyboardButton(text="🔔 Trigger", callback_data="tg:menu")],
            [InlineKeyboardButton(text="📋 Status", callback_data="status")],
        )
        await message.answer(text, reply_markup=kb)

    @dp.message(Command("help"))
    async def help_cmd(message: Message, bot: Bot) -> None:
        if not await _owner_only(message, bot):
            return
        await message.answer(
            "<b>Perintah:</b>\n"
            "  /start - menu utama\n"
            "  /addchannel - tambah channel\n"
            "  /addtrigger - tambah trigger keyword\n"
            "  /list - lihat channel & trigger"
        )

    @dp.message(Command("addchannel"))
    async def addchannel_cmd(message: Message, bot: Bot, state: FSMContext) -> None:
        if not await _owner_only(message, bot):
            return
        await _prompt_add_channel(message, state)

    @dp.message(Command("addtrigger"))
    async def addtrigger_cmd(message: Message, bot: Bot, state: FSMContext) -> None:
        if not await _owner_only(message, bot):
            return
        await _prompt_add_trigger(message, state)

    @dp.message(Command("list"))
    async def list_cmd(message: Message, bot: Bot) -> None:
        row = await _owner_only(message, bot)
        if row:
            await _show_status(message, row)

    @dp.message(Command("debug"))
    async def debug_cmd(message: Message, bot: Bot) -> None:
        """Diagnostik: tampilkan state internal bot anak ini."""
        row = await _owner_only(message, bot)
        if not row:
            return
        chs = await db.get_channels_for_bot(row["id"])
        tgs = await db.get_triggers_for_bot(row["id"])

        ub_status = "OFFLINE"
        ub_id = ub_user = "-"
        try:
            if userbot.client.is_connected() and userbot._me is not None:
                ub_status = "ONLINE"
                ub_id = userbot._me.id
                ub_user = f"@{userbot._me.username}" if userbot._me.username else "-"
        except Exception:
            pass

        ch_lines = []
        for c in chs:
            line = f"  • <code>{c['chat_id']}</code> — {html.escape(c['title'] or '')}"
            if c["username"]:
                line += f" (@{c['username']})"
            ch_lines.append(line)
        tg_lines = [f"  • <code>{html.escape(t['keyword'])}</code>" for t in tgs]

        text = (
            f"🛠 <b>Debug Info</b>\n\n"
            f"<b>Bot anak</b>\n"
            f"  db_id   : <code>{row['id']}</code>\n"
            f"  bot_id  : <code>{row['bot_id']}</code>\n"
            f"  owner   : <code>{row['owner_id']}</code>\n"
            f"  active  : <code>{row['active']}</code>\n\n"
            f"<b>Userbot tumbal</b>\n"
            f"  status  : <code>{ub_status}</code>\n"
            f"  user    : <code>{ub_user}</code>\n"
            f"  user_id : <code>{ub_id}</code>\n\n"
            f"<b>Channel ({len(chs)})</b>\n"
            + ("\n".join(ch_lines) if ch_lines else "  <i>(kosong)</i>")
            + f"\n\n<b>Trigger ({len(tgs)})</b>\n"
            + ("\n".join(tg_lines) if tg_lines else "  <i>(kosong)</i>")
            + "\n\n<b>Cara cek</b>\n"
            f"Pastikan akun tumbal <b>sudah join</b> ke channel di atas. "
            f"Lalu kirim pesan di channel itu yang mengandung salah satu trigger."
        )
        await message.answer(text)

    # ---- nav ----
    @dp.callback_query(F.data == "home")
    async def cb_home(query: CallbackQuery, bot: Bot, state: FSMContext) -> None:
        await state.clear()
        row = await _owner_only(query, bot)
        if not row:
            return
        await query.answer()
        text = (
            f"👑 <b>Halo Owner!</b>\n\n"
            f"🤖 Bot anak <b>@{html.escape(row['bot_username'])}</b>.\n\nPilih menu:"
        )
        kb = _kb(
            [InlineKeyboardButton(text="📢 Channel", callback_data="ch:menu"),
             InlineKeyboardButton(text="🔔 Trigger", callback_data="tg:menu")],
            [InlineKeyboardButton(text="📋 Status", callback_data="status")],
        )
        try:
            await query.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            await query.message.answer(text, reply_markup=kb)

    @dp.callback_query(F.data == "status")
    async def cb_status(query: CallbackQuery, bot: Bot) -> None:
        row = await _owner_only(query, bot)
        if not row:
            return
        await query.answer()
        await _show_status(query.message, row, edit=True)

    # ---- channel callbacks ----
    @dp.callback_query(F.data == "ch:menu")
    async def cb_ch_menu(query: CallbackQuery, bot: Bot) -> None:
        row = await _owner_only(query, bot)
        if not row:
            return
        await query.answer()
        await _show_channels(query.message, row, edit=True)

    @dp.callback_query(F.data == "ch:add")
    async def cb_ch_add(query: CallbackQuery, bot: Bot, state: FSMContext) -> None:
        if not await _owner_only(query, bot):
            return
        await query.answer()
        await _prompt_add_channel(query.message, state)

    @dp.callback_query(F.data.startswith("ch:del:"))
    async def cb_ch_del(query: CallbackQuery, bot: Bot) -> None:
        row = await _owner_only(query, bot)
        if not row:
            return
        ch_id = int(query.data.split(":")[2])
        await db.unlink_bot_channel(row["id"], ch_id)
        await query.answer("Channel dihapus.")
        await _show_channels(query.message, row, edit=True)

    @dp.message(ChildState.add_channel)
    async def process_channel(message: Message, bot: Bot, state: FSMContext) -> None:
        row = await _owner_only(message, bot)
        if not row:
            return
        ident = (message.text or "").strip()
        if not ident:
            await message.answer("⚠️ Format salah. Kirim @username atau invite link.")
            return
        try:
            entity = await userbot.join_channel(ident)
        except Exception as e:
            await message.answer(
                f"⚠️ Gagal join channel: <code>{html.escape(str(e))}</code>"
            )
            return

        chat_id_raw = getattr(entity, "id", None)
        if chat_id_raw is None:
            await message.answer("⚠️ Gagal resolve channel.")
            return
        # Pakai utils.get_peer_id() supaya format chat_id sama dengan
        # event.chat_id yang akan dilihat oleh handler NewMessage.
        try:
            chat_id_full = tl_utils.get_peer_id(entity)
        except Exception:
            is_channel = (
                getattr(entity, "broadcast", False)
                or getattr(entity, "megagroup", False)
            )
            chat_id_full = int(f"-100{chat_id_raw}") if is_channel else chat_id_raw
        title = getattr(entity, "title", "") or ""
        username = getattr(entity, "username", None)

        ch_db_id = await db.upsert_channel(chat_id_full, title, username)
        linked = await db.link_bot_channel(row["id"], ch_db_id)
        await state.clear()

        if linked:
            txt = (
                f"✅ <b>Channel ditambahkan!</b>\n\n"
                f"📢 <b>{html.escape(title)}</b>\n"
                f"🆔 <code>{chat_id_full}</code>"
                + (f"\n🔗 @{username}" if username else "")
            )
        else:
            txt = f"⚠️ Channel <b>{html.escape(title)}</b> sudah terdaftar di bot ini."
        kb = _kb([InlineKeyboardButton(text="« Channel", callback_data="ch:menu"),
                  InlineKeyboardButton(text="🏠 Home", callback_data="home")])
        await message.answer(txt, reply_markup=kb)

    # ---- trigger callbacks ----
    @dp.callback_query(F.data == "tg:menu")
    async def cb_tg_menu(query: CallbackQuery, bot: Bot) -> None:
        row = await _owner_only(query, bot)
        if not row:
            return
        await query.answer()
        await _show_triggers(query.message, row, edit=True)

    @dp.callback_query(F.data == "tg:add")
    async def cb_tg_add(query: CallbackQuery, bot: Bot, state: FSMContext) -> None:
        if not await _owner_only(query, bot):
            return
        await query.answer()
        await _prompt_add_trigger(query.message, state)

    @dp.callback_query(F.data.startswith("tg:del:"))
    async def cb_tg_del(query: CallbackQuery, bot: Bot) -> None:
        row = await _owner_only(query, bot)
        if not row:
            return
        tg_id = int(query.data.split(":")[2])
        await db.remove_trigger(tg_id, row["id"])
        await query.answer("Trigger dihapus.")
        await _show_triggers(query.message, row, edit=True)

    @dp.message(ChildState.add_trigger)
    async def process_trigger(message: Message, bot: Bot, state: FSMContext) -> None:
        row = await _owner_only(message, bot)
        if not row:
            return
        kw = (message.text or "").strip()
        if not kw or len(kw) < 2:
            await message.answer("⚠️ Keyword minimal 2 karakter.")
            return
        ok = await db.add_trigger(row["id"], kw)
        await state.clear()
        kb = _kb([InlineKeyboardButton(text="« Trigger", callback_data="tg:menu"),
                  InlineKeyboardButton(text="🏠 Home", callback_data="home")])
        if ok:
            await message.answer(
                f"✅ Trigger <code>{html.escape(kw)}</code> ditambahkan.",
                reply_markup=kb,
            )
        else:
            await message.answer("⚠️ Trigger sudah ada.", reply_markup=kb)

    # ---- fallback ----
    @dp.message()
    async def fallback(message: Message, bot: Bot) -> None:
        row = await _bot_row(bot)
        if not row or message.from_user.id != row["owner_id"]:
            await message.answer("🔒 Bot ini privat.")
            return
        await message.answer("Ketik /start untuk membuka menu.")

    return dp


# =============================================================================
# CHILD BOT MANAGER
# =============================================================================
class ChildBotManager:
    def __init__(self, db: Database):
        self.db = db
        self.userbot: Optional[Userbot] = None
        self.dp: Optional[Dispatcher] = None
        self.bots: dict[int, Bot] = {}
        self.tasks: dict[int, asyncio.Task] = {}

    def set_userbot(self, userbot: Userbot) -> None:
        self.userbot = userbot
        self.dp = build_child_dispatcher(self.db, userbot)

    async def start(self) -> None:
        if self.dp is None:
            raise RuntimeError("set_userbot() harus dipanggil sebelum start()")
        rows = await self.db.get_all_child_bots()
        ok, fail = 0, 0
        for r in rows:
            spawned = await self.add_bot(token=r["token"], bot_db_id=r["id"])
            if spawned:
                ok += 1
            else:
                fail += 1
        log.info(
            "ChildBotManager started: %d active, %d skipped (invalid).",
            ok, fail,
        )

    async def add_bot(self, token: str, bot_db_id: int) -> bool:
        """Spawn polling task untuk bot anak. Return True kalau sukses,
        False kalau token invalid (bot dinonaktifkan di DB, tidak crash).
        """
        if bot_db_id in self.bots:
            return True
        bot = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
        # Validasi token sebelum mulai polling, supaya 1 token rusak
        # tidak menjatuhkan service.
        try:
            await bot.get_me()
        except Exception as e:
            log.warning(
                "Skip bot anak db_id=%s, token invalid: %s",
                bot_db_id, e,
            )
            try:
                await bot.session.close()
            except Exception:
                pass
            await self.db.deactivate_child_bot(bot_db_id)
            return False
        self.bots[bot_db_id] = bot
        self.tasks[bot_db_id] = asyncio.create_task(self._poll(bot, bot_db_id))
        log.info("Spawned child bot db_id=%s", bot_db_id)
        return True

    async def _poll(self, bot: Bot, bot_db_id: int) -> None:
        try:
            await self.dp.start_polling(bot, handle_signals=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "Child bot polling crashed db_id=%s (bot tetap nonaktif "
                "sampai restart, service lain jalan terus)",
                bot_db_id,
            )

    async def remove_bot(self, bot_db_id: int) -> None:
        task = self.tasks.pop(bot_db_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        bot = self.bots.pop(bot_db_id, None)
        if bot:
            try:
                await bot.session.close()
            except Exception:
                pass
        log.info("Removed child bot db_id=%s", bot_db_id)

    async def notify_owner(self, bot_row, keyword: str, link: str,
                           text: str, chat_title: str = "") -> None:
        bot = self.bots.get(bot_row["id"])
        if bot is None:
            log.warning("notify_owner: bot %s not running", bot_row["id"])
            return

        snippet = text.strip()
        if len(snippet) > 600:
            snippet = snippet[:600] + "..."
        msg = (
            f"🔔 <b>Trigger ditemukan!</b>\n\n"
            f"📢 <b>{html.escape(chat_title or '')}</b>\n"
            f"🔑 Keyword: <code>{html.escape(keyword)}</code>\n\n"
            f"📝 <blockquote expandable>{html.escape(snippet)}</blockquote>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Buka Pesan", url=link)],
        ])
        try:
            await bot.send_message(
                chat_id=bot_row["owner_id"],
                text=msg,
                reply_markup=kb,
                disable_web_page_preview=False,
            )
            log.info(
                "[notify] sent to owner=%s via bot db_id=%s keyword=%r",
                bot_row["owner_id"], bot_row["id"], keyword,
            )
        except Exception as e:
            log.warning("Gagal kirim notifikasi ke %s: %s", bot_row["owner_id"], e)


# =============================================================================
# ENTRY POINT
# =============================================================================
async def auth_userbot() -> None:
    """Login interaktif akun tumbal (sekali saja)."""
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()  # akan minta phone + OTP
    me = await client.get_me()
    print(f"Login sukses sebagai @{me.username} (id={me.id})")
    await client.disconnect()


async def run_all() -> None:
    db = Database(DB_PATH)
    await db.init()

    child_manager = ChildBotManager(db)
    userbot = Userbot(API_ID, API_HASH, SESSION_NAME, db, child_manager)
    child_manager.set_userbot(userbot)
    main_bot = MainBot(BOT_TOKEN, db, child_manager)

    await userbot.start()
    await child_manager.start()

    stop = asyncio.Event()

    def _signal_handler(*_):
        log.info("Shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    tasks = [
        asyncio.create_task(main_bot.start(), name="main_bot"),
        asyncio.create_task(userbot.run(), name="userbot"),
        asyncio.create_task(stop.wait(), name="stop_wait"),
    ]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in pending:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    finally:
        log.info("Shutting down child bots...")
        for bid in list(child_manager.bots.keys()):
            await child_manager.remove_bot(bid)
        try:
            await main_bot.bot.session.close()
        except Exception:
            pass
        try:
            await userbot.client.disconnect()
        except Exception:
            pass


def cli() -> None:
    import os
    level = logging.DEBUG if os.environ.get("LOG_DEBUG") else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    # kurangi noise dari library
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    if len(sys.argv) > 1 and sys.argv[1] == "auth":
        asyncio.run(auth_userbot())
    else:
        try:
            asyncio.run(run_all())
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    cli()
