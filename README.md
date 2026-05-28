# Telegram Bot Trigger System

Sistem bot Telegram yang terdiri dari:

- **Userbot (Telethon)** — 1 akun "tumbal" yang memantau pesan baru di channel.
- **Main Bot (aiogram)** — tempat kamu mendaftarkan bot anak (pakai premium emoji).
- **Bot Anak (aiogram)** — bot per user; tiap bot anak bisa menambahkan
  channel + trigger keyword. Saat ada pesan baru di channel yang cocok dengan
  trigger, bot anak akan kirim notifikasi + link pesan ke owner-nya.
- **Database** — SQLite (aiosqlite) dengan foreign keys + WAL.

Semua kode digabung jadi satu file: `main.py`.

## Cara pakai

```bash
# 1. install dependencies
pip install -r requirements.txt

# 2. login akun tumbal (interaktif: phone + OTP) — sekali saja
python main.py auth

# 3. jalankan semua service
python main.py
```

## Konfigurasi

Edit konstanta di bagian atas `main.py`:

```python
BOT_TOKEN = "..."          # token main bot dari @BotFather
API_ID    = ...            # dari https://my.telegram.org
API_HASH  = "..."
ADMIN_IDS: list[int] = []  # kosong = semua user boleh /addbot
```

> Sebaiknya pindahkan ke environment variable sebelum deploy public.

## Flow

1. User start main bot → `/addbot` → paste token bot anak dari @BotFather.
2. Main bot validasi token, simpan ke DB, spawn polling task untuk bot anak.
3. User buka bot anak → tambah channel (`@username` / invite link) →
   userbot tumbal otomatis join → simpan channel.
4. User tambah trigger keyword di bot anak.
5. Userbot dengar `events.NewMessage()` → cocokkan keyword (substring,
   case-insensitive) → kirim notifikasi via bot anak ke owner dengan
   tombol inline `🔗 Buka Pesan`.

## Skema database

- `child_bots` (id, token, bot_id, bot_username, owner_id, active)
- `channels` (id, chat_id, title, username)
- `bot_channels` (bot_id ↔ channel_id) — banyak ke banyak
- `triggers` (id, bot_id, keyword)

`ON DELETE CASCADE` aktif: hapus bot anak otomatis hapus channel link & trigger-nya.
