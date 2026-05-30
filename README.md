# Telegram Bot Trigger System

Sistem bot Telegram dengan arsitektur:

- **Userbot (Telethon)** — 1 akun "tumbal" yang memantau pesan baru di channel.
- **Main Bot (aiogram + premium emoji)** — register & manage bot anak.
- **Bot Anak (aiogram)** — per user; tambah channel + trigger keyword.
- **Database (aiosqlite)** — SQLite, WAL, FK cascade, single shared connection.

Saat ada pesan baru di channel target yang mengandung trigger, bot anak kirim
notifikasi + tombol "Buka Pesan" ke owner-nya.

## Cara pakai

```bash
pip install -r requirements.txt
python main.py
```

Pertama kali run, kalau session userbot belum ada, kamu akan diminta:

1. Nomor HP (format internasional, `+628xxxxxxx`)
2. Kode OTP yang dikirim Telegram
3. Password 2FA (kalau diaktifkan)

Setelah itu file `userbot_session.session` dibuat dan dipakai otomatis.

## Konfigurasi

Pakai environment variable (rekomendasi) atau edit konstanta di atas `main.py`:

| Env var | Default |
|---|---|
| `BOT_TOKEN` | (token main bot) |
| `API_ID` | `32066244` |
| `API_HASH` | (Telethon API hash) |
| `SESSION_NAME` | `userbot_session` |
| `DB_PATH` | `data.db` |
| `ADMIN_IDS` | (kosong = semua user boleh `/addbot`) |
| `LOG_DEBUG` | (set ke `1` untuk verbose log) |

## Optimasi performa

- **Single shared SQLite connection** — skip overhead open/close per query (~2-5ms).
- **In-memory cache** — `chat_id -> [bot_ids]` & `bot_id -> [triggers]`. Hot path
  event handler 100% bypass DB. Latency match path < 1ms.
- **uvloop** otomatis dipakai kalau ter-install.
- **`asyncio.create_task` untuk notif** — 1 send tidak blocking yang lain.
- **FloodWait + Forbidden handling** — owner block bot otomatis di-deactivate.
- **WAL checkpoint saat shutdown** — prevent WAL bloat.

Latency notif typical di VPS ping 20ms: **80–200ms** end-to-end (instan).

## Auto-features

- **Auto-join channel** saat add — kalau akun tumbal belum join, langsung join.
- **Auto-fallback premium emoji** — kalau emoji ID invalid, retry tanpa premium.
- **Auto-skip token bot anak invalid** — token revoked tidak crash service lain.
- **Auto-migrate chat_id** — channel lama dengan format chat_id berbeda
  otomatis di-update saat boot.
- **Auto-deactivate** bot anak yang owner-nya block bot.

## Diagnostik

Di **bot anak**, ketik `/debug` untuk lihat state lengkap (channel, trigger,
status userbot, cache stats).

Di console, set `LOG_DEBUG=1` untuk verbose logging.

## Skema database

- `child_bots` (id, token, bot_id, bot_username, owner_id, active)
- `channels` (id, chat_id, title, username)
- `bot_channels` (bot_id ↔ channel_id) — many-to-many
- `triggers` (id, bot_id, keyword)

`ON DELETE CASCADE` aktif — hapus bot anak otomatis hapus relasi & trigger-nya.
