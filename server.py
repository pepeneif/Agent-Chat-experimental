#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent-Chat — minichat privado (PWA) entre una persona y un agente/bot.

Un solo fichero Python (stdlib). Autenticacion por token de un solo uso enviado por
email. Dispara turnos en el hilo dedicado del agente en OpenMausBot y publica sus
respuestas, con acuse rapido + respuesta final, y notificaciones Web Push (VAPID).

Toda la configuracion se toma de variables de entorno (ver .env.example).
"""
import os, re, json, time, uuid, sqlite3, secrets, smtplib, threading, mimetypes, traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote, unquote
from urllib.error import HTTPError
from urllib.request import urlopen, Request
from email.message import EmailMessage
from email.utils import make_msgid, formatdate
from email.headerregistry import Address
try:
    from pywebpush import webpush, WebPushException
    _HAS_PUSH = True
except Exception:
    _HAS_PUSH = False

# ---------- config ----------
DATA_DIR   = os.environ.get("CHAT_DATA", os.path.expanduser("~/chat/data"))
STATIC_DIR = os.environ.get("CHAT_STATIC", os.path.expanduser("~/chat/static"))
PORT       = int(os.environ.get("CHAT_PORT", "8792"))
OMB        = os.environ.get("CHAT_OMB", "http://127.0.0.1:8799")
BOT_ID     = os.environ.get("CHAT_BOT_ID", "")
THREAD_ID  = os.environ.get("CHAT_THREAD_ID", "")
OWNER      = (os.environ.get("CHAT_OWNER", "")).strip().lower()
BRAND      = os.environ.get("CHAT_BRAND", "Manager").strip() or "Manager"
MAIL_FROM_H = os.environ.get("CHAT_MAIL_NAME", BRAND)
MAIL_FROM_A = os.environ.get("CHAT_MAIL_FROM", "no-reply@example.com")
MAIL_FROM_D = os.environ.get("CHAT_MAIL_DOMAIN", MAIL_FROM_A.split("@", 1)[1] if "@" in MAIL_FROM_A else "example.com")
SMTP_HOST  = os.environ.get("SMTP_HOST", "127.0.0.1")
SMTP_PORT  = int(os.environ.get("SMTP_PORT", "25"))
SESSION_DAYS = 30
TOKEN_MIN    = 15
MAX_UPLOAD   = 25 * 1024 * 1024

os.makedirs(DATA_DIR, exist_ok=True)
FILES_DIR = os.path.join(DATA_DIR, "files")
os.makedirs(FILES_DIR, exist_ok=True)
DB = os.path.join(DATA_DIR, "chat.db")
VAPID_FILE = os.path.join(DATA_DIR, "vapid.json")
VAPID = {}
try:
    with open(VAPID_FILE) as _f:
        VAPID = json.load(_f)
except Exception:
    VAPID = {}
_dblock = threading.Lock()
_dispatch_lock = threading.Lock()   # serializa los turnos: un dispatch a la vez
_busy = {"on": False, "since": 0, "q": 0}   # hay un turno en curso (para avisar al cliente)
_debuging = {"on": False}   # hay una depuracion en curso
_viewer = {"last": 0}       # ultimo poll de /api/history: alguien mira la UI en primer plano
VIEWER_TTL_MS = 25000       # margen: si hubo poll hace <25s, no mandamos push (esta leyendo)

# ---------- db ----------
def db():
    c = sqlite3.connect(DB, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY, role TEXT, content TEXT, at INTEGER, uid TEXT);
        CREATE TABLE IF NOT EXISTS logins (
            token TEXT PRIMARY KEY, created INTEGER, expires INTEGER, used INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS sessions (
            sid TEXT PRIMARY KEY, created INTEGER, expires INTEGER);
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY, name TEXT, mime TEXT, size INTEGER, created INTEGER);
        CREATE TABLE IF NOT EXISTS rl (k TEXT PRIMARY KEY, n INTEGER, ts INTEGER);
        CREATE TABLE IF NOT EXISTS push_subs (endpoint TEXT PRIMARY KEY, sub TEXT, created INTEGER);
        CREATE TABLE IF NOT EXISTS prefs (k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS omb_deliveries (source_id TEXT PRIMARY KEY, message_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS omb_pending_turns (
            turn_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, message_at INTEGER NOT NULL,
            push_eligible INTEGER NOT NULL, next_check_at INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS omb_push_events (
            turn_id TEXT PRIMARY KEY, source_id TEXT NOT NULL UNIQUE,
            message_at INTEGER NOT NULL, attempted_at INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL DEFAULT 0);
        """)
        columns = {row[1] for row in c.execute("PRAGMA table_info(omb_push_events)")}
        if "attempts" not in columns:
            c.execute("ALTER TABLE omb_push_events ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "next_attempt_at" not in columns:
            c.execute("ALTER TABLE omb_push_events ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0")

def now_ms(): return int(time.time() * 1000)

def get_pref(k, default=None):
    try:
        with db() as c:
            r = c.execute("SELECT v FROM prefs WHERE k=?", (k,)).fetchone()
        return r["v"] if r else default
    except Exception:
        return default

def set_pref(k, v):
    with _dblock, db() as c:
        c.execute("INSERT OR REPLACE INTO prefs(k,v) VALUES(?,?)", (k, str(v)))

def add_msg(role, content, uid=None):
    mid = uuid.uuid4().hex
    with _dblock, db() as c:
        c.execute("INSERT INTO messages(id,role,content,at,uid) VALUES(?,?,?,?,?)",
                  (mid, role, content, now_ms(), uid))
    return {"id": mid, "role": role, "content": content, "at": now_ms()}

def history(limit=60, before=None):
    with db() as c:
        if before:
            rows = c.execute("SELECT * FROM messages WHERE at<? ORDER BY at DESC LIMIT ?",
                             (int(before), limit)).fetchall()
        else:
            rows = c.execute("SELECT * FROM messages ORDER BY at DESC LIMIT ?", (limit,)).fetchall()
    return list(reversed([dict(r) for r in rows]))

# ---------- email ----------
def send_token_email(to_addr, token):
    m = EmailMessage()
    m["From"] = Address(MAIL_FROM_H, MAIL_FROM_A.split("@")[0], MAIL_FROM_D)
    m["To"] = to_addr
    m["Subject"] = "Tu codigo de acceso al chat"
    m["Date"] = formatdate(localtime=True)
    m["Message-ID"] = make_msgid(domain=MAIL_FROM_D)
    body = (
        "Este es tu codigo de acceso de un solo uso para el chat.\n\n"
        f"    {token}\n\n"
        f"Caduca en {TOKEN_MIN} minutos. Pegalo en la pantalla del chat.\n"
        "Si no lo has pedido tu, ignora este correo.\n"
    )
    m.set_content(body, charset="utf-8")
    raw = m.as_string()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=25) as s:
        s.ehlo(MAIL_FROM_D)
        c1, r1 = s.mail(MAIL_FROM_A)
        c2, r2 = s.rcpt(to_addr)
        if c2 >= 400:
            raise RuntimeError(f"rcpt rejected: {c2} {r2!r}")
        c3, r3 = s.data(raw)
        log_line(f"MAIL to={to_addr} from={MAIL_FROM_A} rcpt={c2} data={c3} resp={r3!r}")
        if c3 >= 400:
            raise RuntimeError(f"data rejected: {c3} {r3!r}")
        try:
            s.quit()
        except Exception:
            pass
    return True

def log_line(msg):
    try:
        with open(os.path.join(DATA_DIR, "mail.log"), "a") as f:
            f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {msg}\n")
    except Exception:
        pass

# ---------- OMB ----------
def omb_get(path):
    with urlopen(OMB + path, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def omb_post(path, payload):
    data = json.dumps(payload).encode("utf-8")
    req = Request(OMB + path, data=data,
                  headers={"content-type": "application/json"}, method="POST")
    with urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8") or "{}")

DEBUG_KEEP = int(os.environ.get("CHAT_DEBUG_KEEP", "40"))

def _debug_busy(on):
    """Marca/desmarca el flag de trabajo (para que el chat muestre 'Manager esta trabajando')."""
    if on:
        _busy["q"] += 1
        _busy["on"] = True
        if not _busy["since"]:
            _busy["since"] = now_ms()
    else:
        _busy["q"] = max(0, _busy["q"] - 1)
        if _busy["q"] == 0:
            _busy["on"] = False
            _busy["since"] = 0

def _omb_busy():
    """True/False si el bot esta ocupado; None si no se pudo saber."""
    try:
        d = omb_get("/api/bots")
        for x in d.get("bots", []):
            if x.get("id") == BOT_ID:
                return bool(x.get("busy"))
    except Exception:
        pass
    return None

def purge_history(keep):
    """Reduce el historial del chat a los ultimos `keep` mensajes (mas recientes) y
    deja una nota de sistema. Devuelve cuantos borro."""
    keep = max(1, int(keep))
    with _dblock, db() as c:
        rows = c.execute("SELECT id FROM messages ORDER BY at").fetchall()
        total = len(rows)
        victims = [r["id"] for r in rows[:max(0, total - keep)]]
        if victims:
            c.executemany("DELETE FROM messages WHERE id=?", [(v,) for v in victims])
        note = ("Depurado: historial reducido a los ultimos %d mensajes y contexto compactado."
                % min(keep, total))
        c.execute("INSERT INTO messages(id,role,content,at,uid) VALUES(?,?,?,?,?)",
                  (uuid.uuid4().hex, "system", note, now_ms(), None))
    return len(victims)

def debug_worker(keep=None):
    """Dispara la compactacion NATIVA del hilo de OpenMausBot y poda el historial del chat.
    Nunca toca el hilo con un turno en curso (regla del chat: no reiniciar/depurar en vivo)."""
    keep = DEBUG_KEEP if keep is None else keep
    _debug_busy(True)
    try:
        deadline = time.time() + 900
        while time.time() < deadline:          # esperar a que el bot este ocioso
            b = _omb_busy()
            if b is False:
                break
            time.sleep(4)
        try:
            omb_post(f"/api/bots/{BOT_ID}/compact", {"threadId": THREAD_ID})
            log_line("debug: compact disparado")
        except Exception as e:
            log_line(f"debug: compact fallo: {e}")
        time.sleep(6)                          # deja que arranque el turno de resumen
        t1 = time.time()
        while time.time() - t1 < 420:
            b = _omb_busy()
            if b is False:
                break
            time.sleep(5)
        n = purge_history(keep)
        log_line(f"debug: purged={n} keep={keep}")
    except Exception as e:
        log_line(f"debug: error {e}")
    finally:
        _debug_busy(False)
        _debuging["on"] = False

MIRROR_POLL = 1
MIRROR_PAGE = 200
MIRROR_PENDING_BATCH = 32
MIRROR_PENDING_TTL_MS = 60 * 60 * 1000
MIRROR_BOOTSTRAP_MS = 24 * 60 * 60 * 1000
MIRROR_PREF = "omb_last_message_id"
MIRROR_BOOTSTRAP_PREF = "omb_bootstrap_at"

def _omb_unseen(cursor, cutoff):
    """Read backwards to the persisted cursor, then return messages oldest first."""
    pages = []
    before = None
    newest = None
    while True:
        path = f"/api/threads/{THREAD_ID}/messages?limit={MIRROR_PAGE}"
        if before:
            path += "&before=" + quote(before, safe="")
        response = omb_get(path)
        messages = response["messages"]
        if not isinstance(messages, list) or not isinstance(response.get("hasMore"), bool):
            raise ValueError("OMB pagination response is malformed")
        if messages and newest is None:
            newest = messages[-1]["id"]
        if cursor:
            match = next((i for i, msg in enumerate(messages) if msg["id"] == cursor), None)
            if match is not None:
                pages.append(messages[match + 1:])
                break
        elif messages and int(messages[0].get("at", 0) or 0) < cutoff:
            pages.append([msg for msg in messages if int(msg.get("at", 0) or 0) >= cutoff])
            break
        pages.append(messages)
        if not response["hasMore"]:
            if cursor and not any(msg["id"] == cursor for page in pages for msg in page):
                print("mirror: cursor no longer exists; reconciling available history", flush=True)
            break
        if not messages or messages[0]["id"] == before:
            raise ValueError("OMB pagination failed to advance")
        before = messages[0]["id"]
    return [msg for page in reversed(pages) for msg in page], newest

def _record_omb(message, legacy_since, legacy_until):
    source_id = message["id"]
    body = (message.get("text") or "").strip()
    is_text = message.get("role") == "bot" and message.get("kind") == "text" and bool(body)
    turn_id = message.get("turnId")
    turn_id = turn_id if isinstance(turn_id, str) and turn_id.strip() else None
    message_at = int(message.get("at", 0) or 0)
    with _dblock, db() as conn:
        if is_text:
            seen = conn.execute("SELECT 1 FROM omb_deliveries WHERE source_id=?", (source_id,)).fetchone()
            inserted = False
            if not seen:
                old = None
                if legacy_since <= message_at <= legacy_until:
                    old = conn.execute("""SELECT m.id FROM messages m
                        LEFT JOIN omb_deliveries d ON d.message_id=m.id
                        WHERE m.role='bot' AND m.content=? AND m.at BETWEEN ? AND ?
                          AND d.source_id IS NULL
                        ORDER BY m.at, m.id LIMIT 1""",
                        (body, legacy_since, legacy_until)).fetchone()
                message_id = old["id"] if old else uuid.uuid4().hex
                if not old:
                    conn.execute("INSERT INTO messages(id,role,content,at,uid) VALUES(?,?,?,?,?)",
                                 (message_id, "bot", body, now_ms(), None))
                    inserted = True
                conn.execute("INSERT INTO omb_deliveries(source_id,message_id) VALUES(?,?)",
                             (source_id, message_id))
            if turn_id and not conn.execute(
                    "SELECT 1 FROM omb_push_events WHERE turn_id=?", (turn_id,)).fetchone():
                if message.get("turnTerminal"):
                    pending = conn.execute("""SELECT push_eligible FROM omb_pending_turns
                        WHERE turn_id=? AND source_id=?""", (turn_id, source_id)).fetchone()
                    conn.execute("""INSERT OR IGNORE INTO omb_push_events
                        (turn_id,source_id,message_at,attempted_at) VALUES(?,?,?,?)""",
                        (turn_id, source_id, message_at,
                         None if inserted or pending and pending["push_eligible"] else now_ms()))
                    conn.execute("DELETE FROM omb_pending_turns WHERE turn_id=?", (turn_id,))
                else:
                    conn.execute("""INSERT INTO omb_pending_turns
                        (turn_id,source_id,message_at,push_eligible,next_check_at)
                        VALUES(?,?,?,?,0) ON CONFLICT(turn_id) DO UPDATE SET
                        source_id=excluded.source_id, message_at=excluded.message_at,
                        push_eligible=CASE WHEN omb_pending_turns.source_id=excluded.source_id
                            THEN omb_pending_turns.push_eligible ELSE excluded.push_eligible END,
                        next_check_at=0""",
                        (turn_id, source_id, message_at, int(inserted)))
        conn.execute("INSERT OR REPLACE INTO prefs(k,v) VALUES(?,?)", (MIRROR_PREF, source_id))

def _check_pending_turns():
    with db() as conn:
        pending = conn.execute("""SELECT turn_id,source_id,message_at,push_eligible FROM omb_pending_turns
            WHERE next_check_at<=? ORDER BY next_check_at,turn_id LIMIT ?""",
            (now_ms(), MIRROR_PENDING_BATCH)).fetchall()
    for row in pending:
        if now_ms() - row["message_at"] > MIRROR_PENDING_TTL_MS:
            with _dblock, db() as conn:
                removed = conn.execute("DELETE FROM omb_pending_turns WHERE turn_id=? AND source_id=?",
                                       (row["turn_id"], row["source_id"])).rowcount
            if removed:
                print("mirror: expired pending turn after one hour without terminal", flush=True)
            continue
        try:
            response = omb_get(f"/api/threads/{THREAD_ID}/messages?limit=3&around=" + quote(row["source_id"], safe=""))
        except HTTPError as exc:
            if exc.code != 404:
                raise
            with _dblock, db() as conn:
                conn.execute("UPDATE omb_pending_turns SET next_check_at=? WHERE turn_id=? AND source_id=?",
                             (now_ms() + 60_000, row["turn_id"], row["source_id"]))
            continue
        message = next((item for item in response["messages"] if item["id"] == row["source_id"]), None)
        if message is None:
            raise ValueError("OMB around response omitted pending message")
        if message.get("turnId") != row["turn_id"] or message.get("role") != "bot" or message.get("kind") != "text":
            raise ValueError("OMB pending message changed identity")
        with _dblock, db() as conn:
            current = conn.execute("SELECT source_id FROM omb_pending_turns WHERE turn_id=?",
                                   (row["turn_id"],)).fetchone()
            if not current or current["source_id"] != row["source_id"]:
                continue
            if message.get("turnTerminal"):
                conn.execute("""INSERT OR IGNORE INTO omb_push_events
                    (turn_id,source_id,message_at,attempted_at) VALUES(?,?,?,?)""",
                    (row["turn_id"], row["source_id"], row["message_at"], None))
                conn.execute("DELETE FROM omb_pending_turns WHERE turn_id=?", (row["turn_id"],))
            else:
                delay = 60_000 if now_ms() - row["message_at"] >= 300_000 else MIRROR_POLL * 1000
                conn.execute("UPDATE omb_pending_turns SET next_check_at=? WHERE turn_id=?",
                             (now_ms() + delay, row["turn_id"]))

PUSH_MAX_ATTEMPTS = 3
PUSH_RETRY_MS = 1000
PUSH_LEASE_MS = 60_000
_push_inflight = set()

def _finish_push_event(event, outcome):
    exhausted = False
    with _dblock:
        try:
            with db() as conn:
                if outcome is not False or event["attempts"] >= PUSH_MAX_ATTEMPTS:
                    changed = conn.execute("""UPDATE omb_push_events SET attempted_at=?,next_attempt_at=0
                        WHERE turn_id=? AND attempted_at IS NULL AND attempts=?""",
                        (now_ms(), event["turn_id"], event["attempts"])).rowcount
                    exhausted = bool(changed and outcome is False)
                else:
                    conn.execute("""UPDATE omb_push_events SET next_attempt_at=?
                        WHERE turn_id=? AND attempted_at IS NULL AND attempts=?""",
                        (now_ms() + PUSH_RETRY_MS * 2 ** (event["attempts"] - 1),
                         event["turn_id"], event["attempts"]))
        finally:
            _push_inflight.discard(event["turn_id"])
    if exhausted:
        print(f"mirror: push delivery exhausted after {PUSH_MAX_ATTEMPTS} attempts", flush=True)

def _deliver_push_event(event):
    try:
        outcome = send_push(BRAND, event["content"])
    except Exception:
        outcome = False
    _finish_push_event(event, outcome)

def _dispatch_push(event):
    threading.Thread(target=_deliver_push_event, args=(event,), daemon=True).start()

def _drain_push_events():
    push_enabled = get_pref("push_enabled", "true") != "false"
    with _dblock, db() as conn:
        current = now_ms()
        events = conn.execute("""SELECT e.turn_id,e.message_at,e.attempts,m.content FROM omb_push_events e
            JOIN omb_deliveries d ON d.source_id=e.source_id
            JOIN messages m ON m.id=d.message_id
            WHERE e.attempted_at IS NULL AND e.next_attempt_at<=?
            ORDER BY e.rowid LIMIT 50""", (current,)).fetchall()
        ready = []
        for event in events:
            if event["turn_id"] in _push_inflight:
                continue
            if event["attempts"] >= PUSH_MAX_ATTEMPTS:
                conn.execute("UPDATE omb_push_events SET attempted_at=? WHERE turn_id=? AND attempted_at IS NULL",
                             (current, event["turn_id"]))
                print(f"mirror: push delivery exhausted after {PUSH_MAX_ATTEMPTS} attempts", flush=True)
            elif not push_enabled or (event["attempts"] == 0 and not -5_000 <= current - event["message_at"] < 300_000):
                conn.execute("UPDATE omb_push_events SET attempted_at=? WHERE turn_id=? AND attempted_at IS NULL",
                             (current, event["turn_id"]))
            else:
                attempt = event["attempts"] + 1
                conn.execute("""UPDATE omb_push_events SET attempts=?,next_attempt_at=?
                    WHERE turn_id=? AND attempted_at IS NULL""",
                    (attempt, current + PUSH_LEASE_MS, event["turn_id"]))
                ready.append(dict(event) | {"attempts": attempt})
                _push_inflight.add(event["turn_id"])
    for event in ready:
        try:
            _dispatch_push(event)
        except Exception:
            _finish_push_event(event, False)

def _mirror_cursor():
    with db() as conn:
        row = conn.execute("SELECT v FROM prefs WHERE k=?", (MIRROR_PREF,)).fetchone()
    return row["v"] if row else None

def _bootstrap_at():
    with _dblock, db() as conn:
        row = conn.execute("SELECT v FROM prefs WHERE k=?", (MIRROR_BOOTSTRAP_PREF,)).fetchone()
        if row:
            return int(row["v"])
        started = now_ms()
        conn.execute("INSERT INTO prefs(k,v) VALUES(?,?)", (MIRROR_BOOTSTRAP_PREF, str(started)))
        return started

def _mirror_follow():
    """Continuously reconcile OMB, independent of dispatch and agent turn state."""
    bootstrap_at = _bootstrap_at()
    cutoff = bootstrap_at - MIRROR_BOOTSTRAP_MS
    while True:
        try:
            cursor = _mirror_cursor()
            messages, newest = _omb_unseen(cursor, cutoff)
            for message in messages:
                _record_omb(message, cutoff, bootstrap_at)
            if cursor is None and not messages and newest:
                set_pref(MIRROR_PREF, newest)
        except Exception as exc:
            print(f"mirror: reconciliation failed: {type(exc).__name__}: {exc}", flush=True)
        try:
            _check_pending_turns()
        except Exception as exc:
            print(f"mirror: pending turn check failed: {type(exc).__name__}", flush=True)
        try:
            _drain_push_events()
        except Exception as exc:
            print(f"mirror: push event check failed: {type(exc).__name__}", flush=True)
        time.sleep(MIRROR_POLL)

def send_push(title, body, force=False):
    if not (_HAS_PUSH and VAPID.get("private_key")):
        return None
    # Si la UI del chat estuvo en primer plano hace poco (poll reciente de /api/history),
    # Jose esta leyendo: no duplicamos con una notificacion. Al minimizar, iOS congela la
    # pagina, cesan los polls y a los ~25s vuelven los avisos. (force=para pruebas/push manual)
    if not force and (now_ms() - _viewer["last"]) < VIEWER_TTL_MS:
        return None
    try:
        with db() as c:
            subs = [r["sub"] for r in c.execute("SELECT sub FROM push_subs").fetchall()]
    except Exception:
        return False
    if not subs:
        return None
    payload = json.dumps({"title": title, "body": body or ""})
    failed = False
    for sub in subs:
        try:
            info = json.loads(sub)
            webpush(subscription_info=info, data=payload,
                    vapid_private_key=VAPID["private_key"],
                    vapid_claims={"sub": os.environ.get("VAPID_SUB", "mailto:no-reply@example.com")})
        except WebPushException as error:
            code = getattr(getattr(error, "response", None), "status_code", None)
            if code in (404, 410):
                try:
                    with _dblock, db() as c:
                        c.execute("DELETE FROM push_subs WHERE sub=?", (sub,))
                except Exception:
                    failed = True
            else:
                failed = True
        except Exception:
            failed = True
    return not failed

def dispatch(text, uid=None):
    """Guarda el mensaje del usuario, dispara el turno del agente y espera su respuesta.

    Serializado con `_dispatch_lock`: si dos turnos se solapan, el segundo espera a que
    el primero cierre. Sin esto, dos hilos leen los mismos mensajes del bot y publican la
    respuesta por duplicado (bug observado 2026-10-08).
    """
    with _dispatch_lock:
        _busy["on"] = True
        _busy["since"] = now_ms()
        _busy["q"] += 1
        try:
            _dispatch_locked(text, uid)
        finally:
            _busy["q"] -= 1
            if _busy["q"] <= 0:
                _busy["q"] = 0
                _busy["on"] = False

def _dispatch_locked(text, uid=None):
    add_msg("user", text, uid)
    try:
        omb_post(f"/api/bots/{BOT_ID}/messages", {"text": text, "threadId": THREAD_ID})
    except Exception as e:
        add_msg("system", f"No se pudo disparar el turno: {e}")
        return
    deadline = time.time() + 600
    saw_busy = False
    unknown_since = None
    while time.time() < deadline:
        time.sleep(2)
        busy = _omb_busy()
        if busy is None:
            if unknown_since is None:
                unknown_since = time.monotonic()
            elif time.monotonic() - unknown_since >= 30:
                add_msg("system", "No se pudo verificar el estado de Manager. El mensaje se envió; la respuesta seguirá sincronizándose.")
                return
            continue
        unknown_since = None
        saw_busy = saw_busy or busy is True
        if busy is False and (saw_busy or time.time() >= deadline - 596):
            return
    add_msg("system", "Manager no respondió a tiempo. El mensaje está guardado; la respuesta seguirá sincronizándose.")

# ---------- http ----------
def parse_cookies(h):
    out = {}
    for part in (h or "").split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            out[k.strip()] = v.strip()
    return out

class Handler(BaseHTTPRequestHandler):
    server_version = "chat/1.0"

    def log_message(self, fmt, *a):
        syslog = f"{self.address_string()} {fmt % a}"
        try:
            with open(os.path.join(DATA_DIR, "access.log"), "a") as f:
                f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {syslog}\n")
        except Exception:
            pass

    # ---- helpers ----
    def _json(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(b)))
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(b)

    def _bytes(self, code, b, ctype, extra=None, disp=None):
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(b)))
        self.send_header("cache-control", "no-store")
        if disp:
            self.send_header("content-disposition", disp)
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(b)

    def _body(self):
        n = int(self.headers.get("content-length") or 0)
        if n <= 0:
            return b""
        if n > MAX_UPLOAD + 1024:
            raise ValueError("body too large")
        return self.rfile.read(n)

    def _session_ok(self):
        sid = parse_cookies(self.headers.get("cookie")).get("chat_session")
        if not sid:
            return False
        with db() as c:
            row = c.execute("SELECT expires FROM sessions WHERE sid=?", (sid,)).fetchone()
        return bool(row) and row["expires"] > time.time()

    def _require(self):
        if self._session_ok():
            return True
        self._json(401, {"error": "no-session"})
        return False

    # ---- routing ----
    def do_GET(self):
        u = urlparse(self.path)
        p = u.path
        try:
            if p in ("/", "/index.html"):
                return self._static("index.html")
            if p == "/manifest.webmanifest":
                return self._manifest()
            if p in ("/sw.js", "/theme.css", "/app.js", "/icon.svg", "/icon-512.png", "/apple-touch-icon.png"):
                return self._static(p.lstrip("/"))
            if p == "/api/me":
                return self._json(200, {"ok": self._session_ok(), "brand": BRAND, "email": OWNER})
            if p == "/api/push/key":
                if not self._require():
                    return
                return self._json(200, {"key": VAPID.get("public_key")})
            if p == "/api/push/prefs":
                if not self._require():
                    return
                return self._json(200, {"enabled": get_pref("push_enabled", "true") != "false"})
            if p == "/api/history":
                if not self._require():
                    return
                q = parse_qs(u.query)
                before = q.get("before", [None])[0]
                lim = min(int(q.get("limit", ["60"])[0] or 60), 200)
                _viewer["last"] = now_ms()
                return self._json(200, {"messages": history(lim, before),
                                         "busy": bool(_busy["on"]),
                                         "busySince": int(_busy["since"] or 0)})
            if p.startswith("/api/file/"):
                if not self._require():
                    return
                fid = p.rsplit("/", 1)[-1]
                return self._file(fid)
            return self._json(404, {"error": "not found"})
        except Exception as e:
            traceback.print_exc()
            return self._json(500, {"error": str(e)})

    def do_POST(self):
        u = urlparse(self.path)
        p = u.path
        try:
            if p == "/api/request-token":
                return self._request_token()
            if p == "/api/verify":
                return self._verify()
            if p == "/api/logout":
                return self._logout()
            if p == "/api/upload":
                if not self._require():
                    return
                return self._upload()
            if p == "/api/push/subscribe":
                if not self._require():
                    return
                return self._push_subscribe()
            if p == "/api/push/unsubscribe":
                if not self._require():
                    return
                return self._push_unsubscribe()
            if p == "/api/push/status":
                if not self._require():
                    return
                return self._push_status()
            if p == "/api/push/test":
                if not self._require():
                    return
                threading.Thread(target=send_push, args=(BRAND, "Prueba de notificacion"), kwargs={"force": True}, daemon=True).start()
                return self._json(200, {"ok": True})
            if p == "/api/push/prefs":
                if not self._require():
                    return
                return self._push_prefs_set()
            if p == "/api/send":
                if not self._require():
                    return
                return self._send()
            if p == "/api/debug":
                if not self._require():
                    return
                return self._debug()
            return self._json(404, {"error": "not found"})
        except Exception as e:
            traceback.print_exc()
            return self._json(500, {"error": str(e)})

    # ---- static ----
    def _manifest(self):
        path = os.path.join(STATIC_DIR, "manifest.webmanifest")
        try:
            with open(path, encoding="utf-8") as f:
                man = json.load(f)
        except Exception:
            man = {}
        name = os.environ.get("CHAT_PWA_NAME", "").strip()
        if name:
            man["name"] = name
        man["short_name"] = BRAND
        return self._json(200, man)

    def _static(self, name):
        path = os.path.join(STATIC_DIR, name)
        if not os.path.isfile(path):
            return self._json(404, {"error": "missing"})
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        with open(path, "rb") as f:
            b = f.read()
        extra = {}
        if name in ("sw.js", "index.html"):
            extra["cache-control"] = "no-store"
        return self._bytes(200, b, ctype, extra)

    # ---- auth ----
    def _client_ip(self):
        return (self.headers.get("cf-connecting-ip")
                or (self.headers.get("x-forwarded-for") or "").split(",")[0].strip()
                or self.client_address[0])

    def _rate_ok(self, key, limit, window):
        with _dblock, db() as c:
            row = c.execute("SELECT n,ts FROM rl WHERE k=?", (key,)).fetchone()
            if not row or time.time() - row["ts"] > window:
                c.execute("INSERT OR REPLACE INTO rl(k,n,ts) VALUES(?,1,?)", (key, int(time.time())))
                return True
            if row["n"] >= limit:
                return False
            c.execute("UPDATE rl SET n=n+1 WHERE k=?", (key,))
            return True

    def _request_token(self):
        ip = self._client_ip()
        if not self._rate_ok("ip:" + ip, 6, 3600):
            return self._json(429, {"error": "too many requests"})
        body = json.loads(self._body().decode("utf-8") or "{}")
        email = (body.get("email") or "").strip()
        if email.lower() != OWNER:
            time.sleep(1.0)
            return self._json(200, {"ok": True})
        tok = secrets.token_urlsafe(18)
        with _dblock, db() as c:
            c.execute("INSERT INTO logins(token,created,expires,used) VALUES(?,?,?,0)",
                      (tok, int(time.time()), int(time.time()) + TOKEN_MIN * 60))
        try:
            send_token_email(OWNER, tok)
        except Exception as e:
            traceback.print_exc()
            return self._json(502, {"error": "mail failed"})
        return self._json(200, {"ok": True})

    def _verify(self):
        ip = self._client_ip()
        if not self._rate_ok("verify:" + ip, 20, 3600):
            return self._json(429, {"error": "too many requests"})
        body = json.loads(self._body().decode("utf-8") or "{}")
        tok = (body.get("token") or "").strip()
        with _dblock, db() as c:
            row = c.execute("SELECT expires,used FROM logins WHERE token=?", (tok,)).fetchone()
            if not row or row["used"] or row["expires"] < time.time():
                time.sleep(0.8)
                return self._json(401, {"error": "invalid"})
            c.execute("UPDATE logins SET used=1 WHERE token=?", (tok,))
            sid = secrets.token_urlsafe(32)
            c.execute("INSERT INTO sessions(sid,created,expires) VALUES(?,?,?)",
                      (sid, int(time.time()), int(time.time()) + SESSION_DAYS * 86400))
        b = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(b)))
        self.send_header("set-cookie",
                         f"chat_session={sid}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={SESSION_DAYS*86400}")
        self.end_headers()
        self.wfile.write(b)

    def _logout(self):
        sid = parse_cookies(self.headers.get("cookie")).get("chat_session")
        if sid:
            with _dblock, db() as c:
                c.execute("DELETE FROM sessions WHERE sid=?", (sid,))
        b = b'{"ok":true}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(b)))
        self.send_header("set-cookie", "chat_session=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0")
        self.end_headers()
        self.wfile.write(b)

    # ---- chat ----
    def _debug(self):
        if _debuging["on"]:
            return self._json(200, {"ok": True, "started": False, "reason": "running"})
        _debuging["on"] = True
        threading.Thread(target=debug_worker, daemon=True).start()
        return self._json(200, {"ok": True, "started": True})

    def _send(self):
        body = json.loads(self._body().decode("utf-8") or "{}")
        text = (body.get("text") or "").strip()
        atts = body.get("attachments") or []
        if not text and not atts:
            return self._json(400, {"error": "empty"})
        if len(text) > 20000:
            return self._json(413, {"error": "too long"})
        lines = []
        if text:
            lines.append(text)
        for fid in atts:
            with db() as c:
                r = c.execute("SELECT name,mime,size FROM files WHERE id=?", (fid,)).fetchone()
            if r:
                lines.append(f"[adjunto] {r['name']} ({r['mime']}, {r['size']} B) -> /api/file/{fid}")
        payload = "\n".join(lines)
        threading.Thread(target=dispatch, args=(payload,), daemon=True).start()
        return self._json(200, {"ok": True, "accepted": True})

    def _push_subscribe(self):
        body = json.loads(self._body().decode("utf-8") or "{}")
        sub = body.get("subscription")
        if not isinstance(sub, dict) or not sub.get("endpoint"):
            return self._json(400, {"error": "bad subscription"})
        blob = json.dumps(sub)
        with _dblock, db() as c:
            c.execute("INSERT OR REPLACE INTO push_subs(endpoint,sub,created) VALUES(?,?,?)",
                      (sub["endpoint"], blob, now_ms()))
        return self._json(200, {"ok": True})

    def _push_status(self):
        body = json.loads(self._body().decode("utf-8") or "{}")
        endpoint = body.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint:
            return self._json(200, {"registered": False})
        with db() as c:
            r = c.execute("SELECT 1 FROM push_subs WHERE endpoint=?", (endpoint,)).fetchone()
        return self._json(200, {"registered": bool(r)})

    def _push_prefs_set(self):
        body = json.loads(self._body().decode("utf-8") or "{}")
        if "enabled" in body:
            set_pref("push_enabled", "true" if body.get("enabled") else "false")
        return self._json(200, {"enabled": get_pref("push_enabled", "true") != "false"})

    def _push_unsubscribe(self):
        body = json.loads(self._body().decode("utf-8") or "{}")
        endpoint = body.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint:
            return self._json(400, {"error": "bad endpoint"})
        with _dblock, db() as c:
            c.execute("DELETE FROM push_subs WHERE endpoint=?", (endpoint,))
        return self._json(200, {"ok": True})

    def _file(self, fid):
        if not re.fullmatch(r"[0-9a-f]{32}", fid or ""):
            return self._json(404, {"error": "not found"})
        with db() as c:
            r = c.execute("SELECT * FROM files WHERE id=?", (fid,)).fetchone()
        if not r:
            return self._json(404, {"error": "not found"})
        path = os.path.join(FILES_DIR, fid)
        if not os.path.isfile(path):
            return self._json(404, {"error": "gone"})
        with open(path, "rb") as f:
            b = f.read()
        inline = r["mime"].startswith("image/") or r["mime"].startswith("text/") or r["mime"] == "application/pdf"
        cd = ("inline" if inline else "attachment")
        disp = f'{cd}; filename="{quote(r["name"])}"'
        return self._bytes(200, b, r["mime"] or "application/octet-stream", disp=disp)

    def _upload(self):
        ctype = self.headers.get("content-type") or ""
        if "multipart/form-data" not in ctype:
            return self._json(400, {"error": "expected multipart"})
        raw = self._body()
        if len(raw) > MAX_UPLOAD + 4096:
            return self._json(413, {"error": "too large"})
        m = re.search(r'boundary=(?:"([^"]+)"|([^;]+))', ctype)
        if not m:
            return self._json(400, {"error": "no boundary"})
        boundary = (m.group(1) or m.group(2)).strip().encode()
        parts = raw.split(b"--" + boundary)
        fname, fdata, fmime = None, None, "application/octet-stream"
        for part in parts:
            if b"\r\n\r\n" not in part:
                continue
            head, _, data = part.partition(b"\r\n\r\n")
            if b'name="file"' not in head:
                continue
            if data.endswith(b"\r\n"):
                data = data[:-2]
            fdata = data
            hm = re.search(rb'filename="([^"]*)"', head)
            if hm:
                fname = hm.group(1).decode("utf-8", "replace")
            cm = re.search(rb"Content-Type:\s*([^\r\n]+)", head, re.I)
            if cm:
                fmime = cm.group(1).decode("utf-8", "replace").strip()
            break
        if fdata is None:
            return self._json(400, {"error": "no file"})
        fid = uuid.uuid4().hex
        with open(os.path.join(FILES_DIR, fid), "wb") as f:
            f.write(fdata)
        safe = re.sub(r"[^\w.\-() ]", "_", (fname or "adjunto"))[:120] or "adjunto"
        with _dblock, db() as c:
            c.execute("INSERT INTO files(id,name,mime,size,created) VALUES(?,?,?,?,?)",
                      (fid, safe, fmime, len(fdata), now_ms()))
        return self._json(200, {"id": fid, "name": safe, "mime": fmime,
                                "size": len(fdata), "url": f"/api/file/{fid}"})

class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

if __name__ == "__main__":
    init_db()
    threading.Thread(target=_mirror_follow, daemon=True, name="omb-mirror").start()
    print(f"chat backend on 127.0.0.1:{PORT}  data={DATA_DIR}", flush=True)
    Server(("127.0.0.1", PORT), Handler).serve_forever()
