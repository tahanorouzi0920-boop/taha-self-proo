import os, json, threading, secrets, random, time
import urllib.request, urllib.parse
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, HTTPServer

TOKEN = os.environ["BOT_TOKEN"]
ADMIN = str(os.environ.get("ADMIN_TELEGRAM_ID", "0"))
TURSO_URL = os.environ["TURSO_URL"].replace("libsql://", "https://").rstrip("/")
TURSO_TOKEN = os.environ["TURSO_TOKEN"]
PORT = int(os.environ.get("PORT", "10000"))
WELCOME, REWARD = 100, 20
RATE_LIMIT, RATE_WINDOW = 6, 10
API = f"https://api.telegram.org/bot{TOKEN}/"
lock = threading.RLock()
recent = defaultdict(deque)

# ---------- اتصال به Turso (HTTP API) ----------
def _enc(v):
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": str(int(v))}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    return {"type": "text", "value": str(v)}

def _dec(v):
    t = v.get("type")
    if t == "null":
        return None
    if t == "integer":
        return int(v["value"])
    if t == "float":
        return float(v["value"])
    return v.get("value")

class Result:
    def __init__(self, rows, rowcount):
        self._rows = rows
        self.rowcount = rowcount
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows

class Turso:
    def __init__(self):
        self.baton = None
        self.io = threading.Lock()

    def _post(self, sql, params):
        body = {"requests": [{"type": "execute",
                              "stmt": {"sql": sql, "args": [_enc(p) for p in params]}}]}
        if self.baton:
            body["baton"] = self.baton
        req = urllib.request.Request(
            TURSO_URL + "/v2/pipeline", data=json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + TURSO_TOKEN,
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read())

    def execute(self, sql, params=()):
        with self.io:
            try:
                data = self._post(sql, params)
            except Exception:
                self.baton = None  # جریان منقضی شده؛ جریان جدید
                data = self._post(sql, params)
            self.baton = data.get("baton")
            res = data["results"][0]
            if res["type"] == "error":
                raise RuntimeError(res["error"]["message"])
            r = res["response"]["result"]
            cols = [c["name"] for c in r["cols"]]
            rows = [dict(zip(cols, [_dec(v) for v in row])) for row in r["rows"]]
            return Result(rows, r.get("affected_row_count", 0))

db = Turso()

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY, name TEXT, username TEXT,
  points INTEGER NOT NULL DEFAULT 0 CHECK(points>=0),
  wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, games INTEGER DEFAULT 0,
  status TEXT DEFAULT 'active', joined TEXT DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS transactions(
  id INTEGER PRIMARY KEY AUTOINCREMENT, from_id INTEGER, to_id INTEGER,
  amount INTEGER NOT NULL, type TEXT NOT NULL, reason TEXT,
  created TEXT DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS games(
  id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, level INTEGER,
  p1 INTEGER, p2 INTEGER, winner INTEGER, status TEXT DEFAULT 'waiting',
  chat_id INTEGER, created TEXT DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS auto_replies(
  id INTEGER PRIMARY KEY AUTOINCREMENT, pattern TEXT NOT NULL, reply TEXT NOT NULL,
  enabled INTEGER DEFAULT 1, hits INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS audit_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, actor INTEGER, action TEXT,
  target TEXT, details TEXT, created TEXT DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
INSERT OR IGNORE INTO settings VALUES('points','1');
INSERT OR IGNORE INTO settings VALUES('games','1');
INSERT OR IGNORE INTO settings VALUES('autoreply','1');
"""
for _stmt in SCHEMA.split(";"):
    if _stmt.strip():
        db.execute(_stmt)

# ---------- ابزارهای پایه ----------
def api(method, **p):
    data = urllib.parse.urlencode({k: json.dumps(v) if isinstance(v, (dict, list)) else v
                                   for k, v in p.items() if v is not None}).encode()
    with urllib.request.urlopen(API + method, data=data, timeout=60) as r:
        return json.loads(r.read())

def send(chat, text, markup=None):
    return api("sendMessage", chat_id=chat, text=text, parse_mode="HTML", reply_markup=markup)

def is_admin(uid):
    return str(uid) == ADMIN

def get_setting(k):
    r = db.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
    return r is None or r["value"] == "1"

def set_setting(k, on):
    with lock:
        db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                   (k, "1" if on else "0"))

def audit(actor, action, target="", details=None):
    with lock:
        db.execute("INSERT INTO audit_log(actor,action,target,details) VALUES(?,?,?,?)",
                   (actor, action, str(target),
                    json.dumps(details, ensure_ascii=False) if details else None))

def record(frm, to, amount, typ, reason=""):
    with lock:
        db.execute("INSERT INTO transactions(from_id,to_id,amount,type,reason) VALUES(?,?,?,?,?)",
                   (frm, to, amount, typ, reason))

def ensure(u):
    with lock:
        row = db.execute("SELECT 1 FROM users WHERE id=?", (u["id"],)).fetchone()
        if row is None:
            db.execute("INSERT INTO users(id,name,username,points) VALUES(?,?,?,?)",
                       (u["id"], u.get("first_name", ""), u.get("username"), WELCOME))
            record(None, u["id"], WELCOME, "welcome", "امتیاز خوشامدگویی")
        else:
            db.execute("UPDATE users SET name=?, username=? WHERE id=?",
                       (u.get("first_name", ""), u.get("username"), u["id"]))
    return db.execute("SELECT * FROM users WHERE id=?", (u["id"],)).fetchone()

def rate_limited(uid):
    now = time.time()
    q = recent[uid]
    while q and now - q[0] > RATE_WINDOW:
        q.popleft()
    if len(q) >= RATE_LIMIT:
        return True
    q.append(now)
    return False

# ---------- امتیاز ----------
def transfer(src, dst, amount):
    if amount <= 0:
        return False, "مقدار نامعتبر است."
    with lock:
        db.execute("BEGIN")
        try:
            if db.execute("UPDATE users SET points=points-? WHERE id=? AND points>=? AND status='active'",
                          (amount, src, amount)).rowcount == 0:
                db.execute("ROLLBACK")
                return False, "موجودی کافی نیست یا حساب فعال نیست."
            if db.execute("UPDATE users SET points=points+? WHERE id=?", (amount, dst)).rowcount == 0:
                db.execute("ROLLBACK")
                return False, "گیرنده پیدا نشد."
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise
    record(src, dst, amount, "transfer", "انتقال کاربر")
    return True, "✅ انتقال انجام شد."

# ---------- بازی دونفره ----------
def join_game(cq):
    code = cq["data"].split(":", 1)[1]
    uid = cq["from"]["id"]
    with lock:
        db.execute("BEGIN")
        try:
            g = db.execute("SELECT * FROM games WHERE code=? AND status='waiting'", (code,)).fetchone()
            if not g:
                db.execute("ROLLBACK")
                return "این اتاق بسته است."
            if g["p1"] == uid:
                db.execute("ROLLBACK")
                return "به اتاق خودت نمی‌توانی بپیوندی."
            winner = g["p1"] if random.random() < 0.5 else uid
            loser = uid if winner == g["p1"] else g["p1"]
            db.execute("UPDATE games SET p2=?, winner=?, status='finished' WHERE id=?", (uid, winner, g["id"]))
            db.execute("UPDATE users SET wins=wins+1, games=games+1, points=points+? WHERE id=?", (REWARD, winner))
            db.execute("UPDATE users SET losses=losses+1, games=games+1 WHERE id=?", (loser,))
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise
    record(None, winner, REWARD, "game_reward", f"اتاق {code}")
    names = {r["id"]: r["name"] for r in
             db.execute("SELECT id,name FROM users WHERE id IN (?,?)", (winner, loser)).fetchall()}
    send(g["chat_id"], f"🏆 <b>نتیجه بازی</b>\nاتاق: <code>{code}</code>\n"
         f"برنده: {names[winner]}\nبازنده: {names[loser]}\n"
         f"{REWARD} امتیاز مجازی رایگان برای برنده ثبت شد. از بازنده چیزی کم نشد.")
    return "نتیجه اعلام شد."

# ---------- پاسخ خودکار ----------
def auto_reply(msg):
    if msg["chat"]["type"] != "private" or not get_setting("autoreply"):
        return False
    for r in db.execute("SELECT * FROM auto_replies WHERE enabled=1").fetchall():
        if r["pattern"] in msg["text"]:
            with lock:
                db.execute("UPDATE auto_replies SET hits=hits+1 WHERE id=?", (r["id"],))
            send(msg["chat"]["id"], r["reply"])
            return True
    return False

# ---------- دستورات ----------
ADMIN_CMDS = {"/grant", "/deduct", "/settings", "/stats", "/block", "/unblock",
              "/users", "/logs", "/پاسخ", "/حذف_پاسخ", "/پاسخ_ها"}

def handle(msg):
    chat, frm = msg["chat"]["id"], msg["from"]
    user = ensure(frm)
    if user["status"] == "blocked":
        return send(chat, "⛔ دسترسی شما مسدود است.")
    if rate_limited(frm["id"]):
        return send(chat, "⏳ پیام‌ها خیلی سریع است. چند ثانیه صبر کن.")
    parts = msg["text"].split()
    cmd, args = parts[0].split("@")[0], parts[1:]
    if cmd in ADMIN_CMDS and not is_admin(frm["id"]):
        return send(chat, "⛔ فقط مدیر اصلی.")

    if cmd in ("/start", "/help"):
        return send(chat, f"سلام {user['name']}! 🌱\nموجودی: {user['points']} امتیاز مجازی\n\n"
                    "/balance موجودی\n/profile پروفایل\n/top رتبه‌بندی\n/history سابقه\n"
                    "/transfer آیدی مقدار\n/بازی سطح  ساخت اتاق بازی")

    if cmd in ("/balance", "/موجودی"):
        return send(chat, f"💰 موجودی: {user['points']} امتیاز مجازی")

    if cmd == "/profile":
        return send(chat, f"👤 <b>{user['name']}</b>\nآیدی عددی: <code>{user['id']}</code>\n"
                    f"بردها: {user['wins']}\nباخت‌ها: {user['losses']}\nبازی‌ها: {user['games']}\n"
                    f"امتیاز: {user['points']}\nعضویت: {user['joined']}")

    if cmd == "/top":
        rows = db.execute("SELECT name,wins,games FROM users ORDER BY wins DESC, losses ASC LIMIT 10").fetchall()
        lines = [f"{i+1}. {r['name']} - برد: {r['wins']} از {r['games']}" for i, r in enumerate(rows)]
        return send(chat, "🏅 <b>رتبه‌بندی</b>\n" + ("\n".join(lines) or "هنوز بازی‌ای نیست"))

    if cmd == "/history":
        rows = db.execute("SELECT * FROM transactions WHERE from_id=? OR to_id=? ORDER BY id DESC LIMIT 10",
                          (frm["id"], frm["id"])).fetchall()
        lines = []
        for r in rows:
            sign = "-" if r["from_id"] == frm["id"] else "+"
            lines.append(f"{r['created']} | {sign}{r['amount']} | {r['reason'] or r['type']}")
        return send(chat, "🧾 <b>سابقه</b>\n" + ("\n".join(lines) or "خالی"))

    if cmd == "/بازی":
        if not get_setting("games"):
            return send(chat, "⏸ بازی‌ها خاموش است.")
        try:
            level = int(args[0])
            assert level > 0
        except Exception:
            return send(chat, "مثال: /بازی 100")
        code = secrets.token_hex(4).upper()
        with lock:
            db.execute("INSERT INTO games(code,level,p1,chat_id) VALUES(?,?,?,?)", (code, level, frm["id"], chat))
        return send(chat, f"🎮 اتاق ساخته شد\nشناسه: <code>{code}</code>\nسطح: {level}",
                    {"inline_keyboard": [[{"text": "پیوستن", "callback_data": f"join:{code}"}]]})

    if cmd == "/transfer":
        if not get_setting("points"):
            return send(chat, "⏸ سیستم امتیاز خاموش است.")
        try:
            dst, amt = int(args[0]), int(args[1])
        except Exception:
            return send(chat, "مثال: /transfer 123456 50")
        ok, text = transfer(frm["id"], dst, amt)
        return send(chat, text if ok else f"❌ {text}")

    if cmd == "/grant":
        if not get_setting("points"):
            return send(chat, "⏸ سیستم امتیاز خاموش است.")
        try:
            uid, amt = int(args[0]), int(args[1])
            assert amt > 0
        except Exception:
            return send(chat, "مثال: /grant 123456 500")
        with lock:
            n = db.execute("UPDATE users SET points=points+? WHERE id=?", (amt, uid)).rowcount
        if not n:
            return send(chat, "کاربر پیدا نشد.")
        record(None, uid, amt, "admin_grant", "افزودن توسط مدیر")
        audit(frm["id"], "grant", uid, {"amount": amt})
        return send(chat, f"✅ {amt} امتیاز افزوده شد.")

    if cmd == "/deduct":
        if not get_setting("points"):
            return send(chat, "⏸ سیستم امتیاز خاموش است.")
        try:
            uid, amt = int(args[0]), int(args[1])
            assert amt > 0
        except Exception:
            return send(chat, "مثال: /deduct 123456 50")
        with lock:
            n = db.execute("UPDATE users SET points=points-? WHERE id=? AND points>=?", (amt, uid, amt)).rowcount
        if not n:
            return send(chat, "❌ کاربر پیدا نشد یا موجودی کافی نیست.")
        record(uid, None, amt, "admin_deduct", "کسر توسط مدیر")
        audit(frm["id"], "deduct", uid, {"amount": amt})
        return send(chat, f"✅ {amt} امتیاز کسر شد.")

    if cmd in ("/block", "/unblock"):
        status = "blocked" if cmd == "/block" else "active"
        try:
            uid = int(args[0])
        except Exception:
            return send(chat, f"مثال: {cmd} 123456")
        with lock:
            n = db.execute("UPDATE users SET status=? WHERE id=?", (status, uid)).rowcount
        audit(frm["id"], cmd[1:], uid)
        return send(chat, "انجام شد." if n else "کاربر پیدا نشد.")

    if cmd == "/users":
        rows = db.execute("SELECT id,name,points,status FROM users ORDER BY joined DESC LIMIT 20").fetchall()
        return send(chat, "👥 <b>کاربران</b>\n" + "\n".join(
            f"<code>{r['id']}</code> {r['name']} | {r['points']} | {r['status']}" for r in rows))

    if cmd == "/logs":
        rows = db.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 15").fetchall()
        return send(chat, "📜 <b>گزارش مدیر</b>\n" + ("\n".join(
            f"{r['created']} | {r['action']} | {r['target']}" for r in rows) or "خالی"))

    if cmd == "/stats":
        s = db.execute("SELECT COUNT(*) AS u, COALESCE(SUM(points),0) AS p FROM users").fetchone()
        f = db.execute("SELECT COUNT(*) AS c FROM games WHERE status='finished'").fetchone()
        o = db.execute("SELECT COUNT(*) AS c FROM games WHERE status='waiting'").fetchone()
        return send(chat, f"📊 کاربران: {s['u']}\nبازی‌های تمام‌شده: {f['c']}\n"
                    f"اتاق‌های باز: {o['c']}\nکل امتیازها: {s['p']}")

    if cmd == "/settings":
        names = {"امتیاز": "points", "points": "points", "بازی": "games", "games": "games",
                 "پاسخ": "autoreply", "autoreply": "autoreply"}
        if len(args) == 2 and args[0] in names and args[1] in ("on", "off"):
            set_setting(names[args[0]], args[1] == "on")
            audit(frm["id"], "setting", names[args[0]], {"value": args[1]})
            return send(chat, f"✅ {args[0]} {'روشن' if args[1] == 'on' else 'خاموش'} شد.")
        st = lambda k: "روشن" if get_setting(k) else "خاموش"
        return send(chat, f"⚙️ <b>تنظیمات</b>\nامتیاز: {st('points')}\nبازی: {st('games')}\n"
                    f"پاسخ خودکار: {st('autoreply')}\n\n"
                    "/settings امتیاز on|off\n/settings بازی on|off\n/settings پاسخ on|off")

    if cmd == "/پاسخ":
        body = msg["text"][len("/پاسخ"):].strip()
        if "|" not in body:
            return send(chat, "مثال: /پاسخ سلام | سلام! خوش آمدی")
        pattern, reply = [x.strip() for x in body.split("|", 1)]
        with lock:
            db.execute("INSERT INTO auto_replies(pattern,reply) VALUES(?,?)", (pattern, reply))
        audit(frm["id"], "add_reply", pattern)
        return send(chat, "✅ پاسخ خودکار ثبت شد.")

    if cmd == "/پاسخ_ها":
        rows = db.execute("SELECT * FROM auto_replies").fetchall()
        return send(chat, "🤖 <b>پاسخ‌ها</b>\n" + ("\n".join(
            f"#{r['id']} «{r['pattern']}» ({r['hits']} بار) {'✅' if r['enabled'] else '⏸'}"
            for r in rows) or "خالی"))

    if cmd == "/حذف_پاسخ":
        try:
            rid = int(args[0])
        except Exception:
            return send(chat, "مثال: /حذف_پاسخ 3")
        with lock:
            n = db.execute("DELETE FROM auto_replies WHERE id=?", (rid,)).rowcount
        audit(frm["id"], "del_reply", rid)
        return send(chat, "حذف شد." if n else "پیدا نشد.")

    return send(chat, "دستور ناشناخته است. /help را بزن.")

def handle_update(up):
    if "callback_query" in up:
        cq = up["callback_query"]
        text = join_game(cq) if get_setting("games") else "بازی‌ها خاموش است."
        api("answerCallbackQuery", callback_query_id=cq["id"], text=text)
        return
    msg = up.get("message")
    if not msg or "text" not in msg:
        return
    if msg["text"].startswith("/"):
        handle(msg)
    else:
        ensure(msg["from"])
        auto_reply(msg)

def poll():
    api("deleteWebhook")
    offset = 0
    while True:
        try:
            for up in api("getUpdates", offset=offset, timeout=50).get("result", []):
                offset = up["update_id"] + 1
                try:
                    handle_update(up)
                except Exception as e:
                    print("handler error:", e)
        except Exception as e:
            print("poll error:", e)
            time.sleep(5)

def keep_alive():
    url = os.environ.get("RENDER_EXTERNAL_URL")
    while True:
        time.sleep(600)
        if url:
            try:
                urllib.request.urlopen(url, timeout=30)
            except Exception as e:
                print("ping error:", e)

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Taha Self Pro is running")
    def log_message(self, *a):
        pass

if __name__ == "__main__":
    threading.Thread(target=poll, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    try:
        send(int(ADMIN), "✅ ربات روشن شد و آماده است.")
    except Exception as e:
        print("startup notify error:", e)
    HTTPServer(("0.0.0.0", PORT), Health).serve_forever()
