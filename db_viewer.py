"""
Flask-based visual viewer for the local bot SQLite database (bot.db).

- Shows all users in a Telegram-like list with avatar, name, username, status.
- Clicking a user opens a chat-style view with profile info and message history.
- If BOT_TOKEN is set in env, profile photos (file_id) are fetched from Telegram.

Usage:
    pip install -r requirements.txt   # needs flask + requests
    python db_viewer.py
Then open http://127.0.0.1:5000
"""

from __future__ import annotations

import json
import os
import sqlite3
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests
from flask import Flask, Response, abort, jsonify, render_template_string, request, send_file, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("BOT_DB_PATH", BASE_DIR / "bot.db"))
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # provide to fetch avatars from Telegram

app = Flask(__name__)


# ------------ data helpers ------------ #
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row: sqlite3.Row | None) -> Dict[str, Any] | None:
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def message_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    data = {k: row[k] for k in row.keys() if k != "file_data"}
    data["has_file"] = row["file_data"] is not None
    return data


# ------------ API routes ------------ #
@app.route("/api/users")
def api_users() -> Response:
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT telegram_id, name, username, bio, status, profile_photo_file_id,
                   created_at, updated_at, entry_count
            FROM users
            ORDER BY COALESCE(updated_at, created_at) DESC
            """
        )
        rows = [row_to_dict(r) for r in cur.fetchall()]
    return jsonify({"users": rows, "db_path": str(DB_PATH)})


@app.route("/api/user/<int:telegram_id>")
def api_user(telegram_id: int) -> Response:
    limit = min(int(request.args.get("limit", 400)), 1000)
    with get_conn() as conn:
        user_row = conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
        if not user_row:
            abort(404)
        messages = conn.execute(
            """
            SELECT id, msg_type, content, file_id, file_mime, file_data, chat_type, created_at
            FROM user_messages
            WHERE user_id = ?
            ORDER BY datetime(created_at) ASC
            LIMIT ?
            """,
            (telegram_id, limit),
        ).fetchall()
        payments = conn.execute(
            "SELECT * FROM payments WHERE user_id = ? ORDER BY created_at DESC", (telegram_id,)
        ).fetchall()
        exchanges = conn.execute(
            "SELECT * FROM exchange_requests WHERE user_id = ? ORDER BY created_at DESC",
            (telegram_id,),
        ).fetchall()
    return jsonify(
        {
            "user": row_to_dict(user_row),
            "messages": [message_to_dict(m) for m in messages],
            "payments": [row_to_dict(p) for p in payments],
            "exchanges": [row_to_dict(e) for e in exchanges],
        }
    )


def placeholder_avatar(text: str = "NA") -> Response:
    # Simple SVG circle avatar with initials
    initials = (text or "NA").strip() or "NA"
    initials = initials[:2].upper()
    svg = f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="96" height="96">
      <defs>
        <linearGradient id="g" x1="0" x2="1" y1="0" y2="1">
          <stop offset="0%" stop-color="#4da3ff"/>
          <stop offset="100%" stop-color="#6dd5ed"/>
        </linearGradient>
      </defs>
      <rect width="96" height="96" rx="24" fill="url(#g)"/>
      <text x="50%" y="55%" font-family="Segoe UI, Arial" font-size="32" fill="#fff" text-anchor="middle">{initials}</text>
    </svg>
    """
    return Response(svg, mimetype="image/svg+xml")


def fetch_telegram_file(file_id: str) -> tuple[bytes, str] | None:
    """Return (content, mimetype) for a Telegram file_id, or None on failure."""
    if not BOT_TOKEN or not file_id:
        return None
    try:
        meta = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
            params={"file_id": file_id},
            timeout=10,
        )
        meta.raise_for_status()
        file_path = meta.json().get("result", {}).get("file_path")
        if not file_path:
            return None
        file_res = requests.get(
            f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}",
            timeout=15,
        )
        file_res.raise_for_status()
        mimetype = file_res.headers.get("Content-Type", "application/octet-stream")
        return file_res.content, mimetype
    except Exception:
        return None


@app.route("/avatar/<path:file_id>")
def avatar(file_id: str) -> Response:
    fetched = fetch_telegram_file(file_id)
    if not fetched:
        return placeholder_avatar()
    content, mimetype = fetched
    return send_file(BytesIO(content), mimetype=mimetype)


@app.route("/file/<path:file_id>")
def file_proxy(file_id: str) -> Response:
    fetched = fetch_telegram_file(file_id)
    if not fetched:
        abort(404)
    content, mimetype = fetched
    return send_file(BytesIO(content), mimetype=mimetype)


@app.route("/file_blob/<int:msg_id>")
def file_blob(msg_id: int) -> Response:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT file_data, file_mime FROM user_messages WHERE id = ?",
            (msg_id,),
        ).fetchone()
    if not row or row["file_data"] is None:
        abort(404)
    mime = row["file_mime"] or "application/octet-stream"
    return send_file(BytesIO(row["file_data"]), mimetype=mime)


# ------------ UI route ------------ #
@app.route("/")
def home() -> str:
    if not DB_PATH.exists():
        return f"<h2>Database not found at {DB_PATH}</h2>"
    return render_template_string(
        TEMPLATE,
        db_path=str(DB_PATH),
        has_token=bool(BOT_TOKEN),
    )


# ------------ HTML/JS template ------------ #
TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DB Viewer</title>
  <style>
    :root {
      --bg: #e9eef5;
      --sidebar: #ffffff;
      --primary: #2a9df4;
      --accent: #65c7f7;
      --text: #0f1b2d;
      --muted: #6a768a;
      --bubble-user: #ffffff;
      --bubble-bot: #d8ecff;
      --border: #dbe2ed;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; padding: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      background: radial-gradient(circle at 20% 20%, #f3f8ff, #e7eef8 35%, #e9eef5);
      color: var(--text);
      height: 100vh;
      display: flex;
      overflow: hidden;
    }
    .sidebar {
      width: 320px;
      background: var(--sidebar);
      border-right: 1px solid var(--border);
      display: flex; flex-direction: column;
      box-shadow: 0 8px 30px rgba(0,0,0,0.06);
      z-index: 2;
    }
    .sidebar header {
      padding: 16px 18px;
      border-bottom: 1px solid var(--border);
    }
    .sidebar h2 { margin: 0; font-size: 18px; }
    .sidebar .path { color: var(--muted); font-size: 12px; margin-top: 4px; }
    .user-list { flex: 1; overflow: auto; padding: 8px 6px 12px; }
    .user-card {
      display: grid; grid-template-columns: 52px 1fr; gap: 10px;
      padding: 10px; border-radius: 12px;
      cursor: pointer;
      transition: background 0.15s ease, transform 0.1s ease;
      margin: 4px 4px;
      align-items: center;
    }
    .user-card:hover { background: #f1f6ff; transform: translateY(-1px); }
    .user-card.active { background: #e3f2ff; border: 1px solid #cfe6ff; }
    .avatar {
      width: 52px; height: 52px; border-radius: 16px;
      object-fit: cover; background: #cfd7e6;
    }
    .meta .name { font-weight: 700; font-size: 15px; }
    .meta .username { color: var(--muted); font-size: 13px; }
    .pill {
      display: inline-block; background: #e6f3ff; color: #0b72d1;
      padding: 2px 8px; border-radius: 999px; font-size: 11px; margin-top: 4px;
    }
    .main {
      flex: 1; display: flex; flex-direction: column;
      background: linear-gradient(135deg, rgba(255,255,255,0.8), rgba(247,251,255,0.9));
    }
    .topbar {
      padding: 14px 18px; border-bottom: 1px solid var(--border);
      display: flex; align-items: center; gap: 12px;
    }
    .topbar .info h3 { margin: 0; font-size: 18px; }
    .topbar .info .sub { color: var(--muted); font-size: 13px; }
    .chips { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 6px; }
    .chip {
      background: #f1f6ff; border: 1px solid #d9e7ff;
      padding: 4px 10px; border-radius: 999px; font-size: 12px; color: #0f4f9c;
    }
    .content { flex: 1; display: grid; grid-template-rows: auto 1fr; overflow: hidden; }
    .profile-card {
      margin: 16px 18px 0; padding: 14px 16px;
      background: #ffffff; border: 1px solid var(--border); border-radius: 14px;
      box-shadow: 0 10px 24px rgba(0,0,0,0.04);
      display: grid; grid-template-columns: auto 1fr; gap: 12px; align-items: center;
    }
    .profile-card .avatar-lg { width: 68px; height: 68px; border-radius: 20px; object-fit: cover; }
    .profile-card .row { margin: 2px 0; color: var(--muted); font-size: 13px; }
    .profile-card b { color: var(--text); }
    .chat {
      margin: 12px 18px 18px; padding: 14px 16px; background: #ffffff;
      border: 1px solid var(--border); border-radius: 14px;
      box-shadow: 0 10px 24px rgba(0,0,0,0.04);
      display: flex; flex-direction: column; min-height: 0; overflow: hidden;
    }
    .chat h4 { margin: 0 0 10px; }
    .messages { flex: 1; overflow: auto; padding-right: 4px; display: flex; flex-direction: column; gap: 10px; }
    .bubble {
      max-width: 80%;
      padding: 10px 12px;
      border-radius: 12px;
      position: relative;
      line-height: 1.4;
      white-space: pre-wrap;
      word-break: break-word;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
    }
    .bubble.user { align-self: flex-start; background: var(--bubble-user); border: 1px solid #e6edf5; }
    .bubble.bot { align-self: flex-end; background: var(--bubble-bot); border: 1px solid #cfe4ff; }
    .bubble .ts { display: block; margin-top: 6px; color: var(--muted); font-size: 11px; }
    .msg-photo { max-width: 260px; border-radius: 10px; margin-top: 6px; box-shadow: 0 6px 18px rgba(0,0,0,0.08); }
    .section-title { font-weight: 600; margin: 14px 0 6px; color: #1b3b68; }
    .empty { color: var(--muted); }
    @media (max-width: 960px) {
      body { flex-direction: column; }
      .sidebar { width: 100%; height: 40vh; }
      .main { height: 60vh; }
    }
  </style>
</head>
<body>
  <div class="sidebar">
    <header>
      <h2>Users</h2>
      <div class="path">{{ db_path }}</div>
      {% if not has_token %}<div class="path">Set BOT_TOKEN to load Telegram avatars.</div>{% endif %}
    </header>
    <div class="user-list" id="userList"></div>
  </div>
  <div class="main">
    <div class="topbar">
      <div class="info">
        <h3 id="title">Select a user</h3>
        <div class="sub" id="subtitle">Profile and chat view will appear here.</div>
        <div class="chips" id="chips"></div>
      </div>
    </div>
    <div class="content">
      <div class="profile-card" id="profileCard" style="display:none;">
        <img id="profileAvatar" class="avatar-lg" src="" alt="avatar" />
        <div>
          <div class="row"><b>Name:</b> <span id="pName"></span></div>
          <div class="row"><b>Username:</b> <span id="pUsername"></span></div>
          <div class="row"><b>Status:</b> <span id="pStatus"></span></div>
          <div class="row"><b>Email:</b> <span id="pEmail"></span></div>
          <div class="row"><b>ID number:</b> <span id="pId"></span></div>
          <div class="row"><b>Bio:</b> <span id="pBio"></span></div>
          <div class="row"><b>Joined:</b> <span id="pCreated"></span></div>
        </div>
      </div>
      <div class="chat" id="chatPanel" style="display:none;">
        <h4>Messages</h4>
        <div class="messages" id="messages"></div>
      </div>
    </div>
  </div>
  <script>
    const userList = document.getElementById('userList');
    const title = document.getElementById('title');
    const subtitle = document.getElementById('subtitle');
    const chips = document.getElementById('chips');
    const profileCard = document.getElementById('profileCard');
    const chatPanel = document.getElementById('chatPanel');
    const messagesEl = document.getElementById('messages');
    const pName = document.getElementById('pName');
    const pUsername = document.getElementById('pUsername');
    const pStatus = document.getElementById('pStatus');
    const pEmail = document.getElementById('pEmail');
    const pId = document.getElementById('pId');
    const pBio = document.getElementById('pBio');
    const pCreated = document.getElementById('pCreated');
    const pAvatar = document.getElementById('profileAvatar');

    let currentUserId = null;

    function fmt(ts) {
      if (!ts) return '';
      try { return new Date(ts).toLocaleString(); } catch { return ts; }
    }
    function createUserCard(u) {
      const card = document.createElement('div');
      card.className = 'user-card';
      card.dataset.id = u.telegram_id;
      const avatarSrc = `/avatar/${encodeURIComponent(u.profile_photo_file_id || 'none')}`;
      card.innerHTML = `
        <img class="avatar" src="${avatarSrc}" alt="avatar" onerror="this.src='data:image/svg+xml,';"/>
        <div class="meta">
          <div class="name">${u.name || 'No name'} <span class="pill">${u.status || ''}</span></div>
          <div class="username">${u.username || ''}</div>
          <div class="username">Updated: ${fmt(u.updated_at) || fmt(u.created_at) || ''}</div>
        </div>
      `;
      card.onclick = () => loadUser(u.telegram_id, card);
      return card;
    }

    async function loadUsers() {
      const res = await fetch('/api/users');
      const data = await res.json();
      userList.innerHTML = '';
      data.users.forEach(u => userList.appendChild(createUserCard(u)));
    }

    async function loadUser(id, cardEl) {
      currentUserId = id;
      document.querySelectorAll('.user-card').forEach(c => c.classList.remove('active'));
      if (cardEl) cardEl.classList.add('active');
      title.textContent = 'Loading...';
      subtitle.textContent = '';
      chips.innerHTML = '';
      messagesEl.innerHTML = '';
      profileCard.style.display = 'none';
      chatPanel.style.display = 'none';

      const res = await fetch(`/api/user/${id}`);
      if (!res.ok) { title.textContent = 'Failed to load user'; return; }
      const data = await res.json();
      const u = data.user;
      title.textContent = u.name || 'No name';
      subtitle.textContent = u.username ? '@' + u.username : 'No username';
      chips.innerHTML = '';
      ['status','entry_count','telegram_id'].forEach(k => {
        const v = u[k];
        if (v !== undefined && v !== null) {
          const chip = document.createElement('div');
          chip.className = 'chip';
          chip.textContent = `${k}: ${v}`;
          chips.appendChild(chip);
        }
      });

      pName.textContent = u.name || '—';
      pUsername.textContent = u.username ? '@' + u.username : '—';
      pStatus.textContent = u.status || '—';
      pEmail.textContent = u.email || '—';
      pId.textContent = u.id_number || '—';
      pBio.textContent = u.bio || '—';
      pCreated.textContent = fmt(u.created_at);
      pAvatar.src = '/avatar/' + encodeURIComponent(u.profile_photo_file_id || 'none');
      pAvatar.onerror = () => { pAvatar.src = 'data:image/svg+xml,'; };
      profileCard.style.display = 'grid';

      chatPanel.style.display = 'flex';
      messagesEl.innerHTML = '';
      const msgs = data.messages || [];
      if (!msgs.length) {
        messagesEl.innerHTML = '<div class="empty">No messages logged.</div>';
      } else {
        msgs.forEach(m => {
          const b = document.createElement('div');
          const sender =
            (m.msg_type || '').toLowerCase().includes('bot') ||
            (m.chat_type || '').toLowerCase().includes('bot')
              ? 'bot'
              : 'user';
          b.className = 'bubble ' + sender;
          const isPhoto = (m.file_mime || '').startsWith('image') || (m.msg_type || '').toLowerCase().includes('photo');
          let body = escapeHtml(m.content || '');
          if (m.has_file) {
            const url = `/file_blob/${m.id}`;
            if (isPhoto) {
              body += `<div><img class="msg-photo" src="${url}" onerror="this.style.display='none'"></div>`;
            } else {
              body += `<div><a href="${url}" target="_blank">Download file</a></div>`;
            }
          } else if (m.file_id) {
            const url = `/file/${encodeURIComponent(m.file_id)}`;
            if (isPhoto) {
              body += `<div><img class="msg-photo" src="${url}" onerror="this.style.display='none'"></div>`;
            } else {
              body += `<div><a href="${url}" target="_blank">Download file</a></div>`;
            }
          }
          b.innerHTML = `${body}<span class="ts">${fmt(m.created_at)}</span>`;
          messagesEl.appendChild(b);
        });
        messagesEl.scrollTop = messagesEl.scrollHeight;
      }
    }

    function escapeHtml(str) {
      return (str || '').replace(/[&<>"']/g, s => ({
        '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
      }[s]));
    }

    loadUsers();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    if not DB_PATH.exists():
        raise SystemExit(f"Database file not found at {DB_PATH}")
    app.run(host="0.0.0.0", port=5000, debug=True)
