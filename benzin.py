#!/usr/bin/env python3
"""Сбор отчётов о топливе из публичного канала @voronezh_benzin.

Канал читается через открытую веб-версию t.me/s/... — аккаунт и ключи не нужны.
Канал удаляет старые посты (примерно через 1–2 часа), поэтому история копится,
только пока сборщик регулярно запускается. Это делает GitHub Actions каждые 30 минут
(.github/workflows/collect.yml); данные хранятся в data/obs.csv в этом репозитории.

Команды:
  collect             забрать свежие посты и дописать наблюдения в data/obs.csv
  status              последнее известное состояние на отслеживаемых заправках
  telegram            отправить полную сводку в Telegram
  telegram --alerts   написать в Telegram, только если на какой-то заправке появился нужный бензин
  update              collect + status + страница report.html (--json — для приложения на Mac)
  report --out PATH   только записать страницу со сводкой (для публикации на GitHub Pages)
  worker              непрерывная работа на GitHub Actions: сбор по расписанию 12–24 МСК
"""
import argparse
import csv
import html
import json
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

CHANNEL = "voronezh_benzin"
MSK = timezone(timedelta(hours=3))
BASE = Path(__file__).resolve().parent
OBS_CSV = BASE / "data/obs.csv"
TG_CHAT_FILE = BASE / "data/telegram_chat.txt"
TG_ALERTS_FILE = BASE / "data/telegram_alerts.json"  # что бот уже сообщал
TG_MENU_FILE = BASE / "data/telegram_menu.txt"       # адрес, уже установленный на кнопку меню бота
PAGE_URL = "https://dooodoivan.github.io/benzin-page/"  # сводка, которую публикует GitHub Actions
RUN_URL = "https://github.com/dooodoIvan/benzin/actions/workflows/collect.yml"  # ручной запуск сбора (Run workflow)
REPORT_PATH = BASE / "report.html"  # локальный файл, в git не попадает

# Отслеживаемые заправки: название → (короткое имя, шаблон адреса как пишет канал)
STATIONS = {
    "Газпром, Бабяково, Транспортная": ("Бабяково", r"бабяково.*транспортн|транспортн.*бабяково"),
    "Роснефть, Ленинский пр-т 182": ("Ленинский 182", r"ленинский проспект,\s*182(?![\dа-я])"),
    "Роснефть, Землячки 7А": ("Землячки 7А", r"землячки,\s*7\s*а(?![\dа-я])"),
    "Роснефть, Н. Усмань, Дорожная 31": ("Дорожная 31", r"новая усмань.*дорожная улица,\s*31(?![\dа-я])"),
    "Роснефть, Н. Усмань, Дорожная 101": ("Дорожная 101", r"новая усмань.*дорожная улица,\s*101(?![\dа-я])"),
    "Татнефть, Ленинский пр-т 154А": ("Ленинский 154А", r"ленинский проспект,\s*154\s*а(?![\dа-я])"),
}
FUELS = ["95", "98"]  # интересующие марки (95+ / Pulsar не учитываем)
FRESH = timedelta(hours=24)  # старше — считаем «нет свежих данных»
CONFIRM_FRESH = timedelta(hours=2)  # «есть» старше 2 часов показываем жёлтым «?»
ALERT_WINDOW = timedelta(hours=4)    # «есть» засчитываем, если подтверждено за последние 4 часа
ALERT_COOLDOWN = timedelta(hours=3)  # не повторять оповещение по той же заправке чаще
KIND_RU = {"report": "водитель", "summary": "сводка канала", "signal": "терминалы оплаты, не подтверждено"}


def status_word(avail, kind, seen_at=None, now=None):
    """Текстовый статус (Telegram, консоль)."""
    if kind == "signal":
        return "❓ по терминалу" if avail else "нет (по терминалу)"
    if avail and seen_at and now and now - seen_at > CONFIRM_FRESH:
        return "было «есть» (больше 2 ч назад)"
    return "есть" if avail else "нет"


def status_html(avail, kind, seen_at, now):
    """Статус для страницы: «есть»; жёлтый «?» — «есть» подтверждали больше 2 ч назад;
    красный «?» — только сигнал терминала оплаты; «нет» — красным."""
    if kind == "signal":
        if avail:
            return '<span class="st-qr" title="по терминалу оплаты, водители не подтверждали">?</span>'
        return '<span class="st-no" title="по терминалу оплаты">нет</span>'
    if not avail:
        return '<span class="st-no">нет</span>'
    if now - seen_at > CONFIRM_FRESH:
        return f'<span class="st-q" title="«есть» подтверждали в {seen_at:%H:%M}, больше 2 часов назад">?</span>'
    return '<span class="st-yes">есть</span>'


OBS_FIELDS = ["seen_at", "address", "fuel", "available", "status", "kind", "queue", "post_id"]


# ---------- хранение: data/obs.csv ↔ SQLite в памяти ----------

def open_db():
    db = sqlite3.connect(":memory:")
    db.execute(
        """CREATE TABLE obs (
            seen_at   TEXT NOT NULL,        -- время наблюдения, MSK ISO
            address   TEXT NOT NULL,
            fuel      TEXT NOT NULL,
            available INTEGER NOT NULL,     -- 1 есть, 0 нет
            status    TEXT NOT NULL,        -- исходная формулировка
            kind      TEXT NOT NULL,        -- report | summary | signal
            queue     TEXT,
            post_id   INTEGER NOT NULL,
            UNIQUE (seen_at, address, fuel, available, kind)
        )""")
    if OBS_CSV.exists():
        with OBS_CSV.open(encoding="utf-8", newline="") as f:
            rows = [(r["seen_at"], r["address"], r["fuel"], int(r["available"]), r["status"], r["kind"],
                     r["queue"] or None, int(r["post_id"])) for r in csv.DictReader(f)]
        db.executemany("INSERT OR IGNORE INTO obs VALUES (?,?,?,?,?,?,?,?)", rows)
    return db


def save_db(db):
    """Пишет все наблюдения в CSV в стабильном порядке — так изменения в git остаются маленькими."""
    OBS_CSV.parent.mkdir(parents=True, exist_ok=True)
    tmp = OBS_CSV.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(OBS_FIELDS)
        w.writerows(db.execute("SELECT * FROM obs ORDER BY seen_at, address, fuel, kind, available"))
    tmp.replace(OBS_CSV)


# ---------- чтение и разбор канала ----------

def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8")
        except Exception as e:  # сеть иногда отваливается — пробуем ещё
            if attempt == 2:
                raise
            print(f"повтор после ошибки: {e}", file=sys.stderr)
            time.sleep(5 * (attempt + 1))


def to_text(fragment):
    fragment = re.sub(r"<br\s*/?>", "\n", fragment)
    fragment = re.sub(r"<[^>]+>", "", fragment)
    return html.unescape(fragment).strip()


def split_posts(page):
    """→ [(post_id, ts_utc, text)]"""
    out = []
    for chunk in page.split('class="tgme_widget_message_wrap')[1:]:
        m_id = re.search(rf'data-post="{CHANNEL}/(\d+)"', chunk)
        m_time = re.search(r'<time datetime="([^"]+)"', chunk)
        m_text = re.search(
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>\s*'
            r'(?:<div class="tgme_widget_message_(?:footer|reactions|link_preview))',
            chunk, re.S)
        if m_id and m_time and m_text:
            ts = datetime.fromisoformat(m_time.group(1)).astimezone(timezone.utc)
            out.append((int(m_id.group(1)), ts, to_text(m_text.group(1))))
    return out


def is_available(status):
    s = status.lower()
    if any(w in s for w in ("нет", "законч", "пусто")):
        return 0
    if "есть" in s or "мало" in s or "заканч" in s:
        return 1
    return None


def stamp(hhmm, post_ts):
    """Время «HH:MM» из текста → полная дата (в MSK).
    Канал редактирует сводки на месте, поэтому время в тексте может быть позже публикации;
    если оно «раньше» публикации больше чем на 3 часа — значит, правка была уже на следующий день."""
    post_msk = post_ts.astimezone(MSK)
    h, m = map(int, hhmm.split(":"))
    t = post_msk.replace(hour=h, minute=m, second=0, microsecond=0)
    if t < post_msk - timedelta(hours=3):
        t += timedelta(days=1)
    return t.isoformat()


def clean_addr(a):
    return re.sub(r"\s*↗\s*$", "", a).strip()


def parse_fuel_groups(line):
    """'🟢 92/100: есть, 95/95+: нет · 🚗 11–20 машин' или '92: есть · 95: есть' → [(fuel, status)], queue"""
    queue = None
    m_q = re.search(r"🚗\s*(.+)$", line)
    if m_q:
        queue = m_q.group(1).strip()
        line = line[:m_q.start()]
    pairs = []
    for fuels, status in re.findall(r"([\w+]+(?:/[\w+]+)*)\s*:\s*([^,·]+)", line):
        for fuel in fuels.split("/"):
            pairs.append((fuel, status.strip()))
    return pairs, queue


def parse_observations(post_id, post_ts, text):
    """Разбирает пост любого из трёх видов → список кортежей для таблицы obs."""
    obs = []
    lines = [l.strip() for l in text.splitlines()]

    def add(seen_at, address, pairs, kind, queue):
        for fuel, status in pairs:
            avail = is_available(status)
            if avail is not None:
                obs.append((seen_at, clean_addr(address), fuel, avail, status, kind, queue, post_id))

    # 1. Отчёт по одной АЗС: «📍 адрес», «🕒 Обновлено в HH:MM», строки «🟢 95: есть», «🚗 …»
    addr = next((l.lstrip("📍 ").strip() for l in lines if l.startswith("📍")), None)
    if addr:
        m_upd = re.search(r"Обновлено в (\d{1,2}:\d{2})", text)
        seen_at = stamp(m_upd.group(1), post_ts) if m_upd else post_ts.astimezone(MSK).isoformat()
        queue = next((l.lstrip("🚗 ").strip() for l in lines if l.startswith("🚗")), None)
        pairs = []
        for l in lines:
            m = re.match(r"^[🟢🔴🟡🟠⚪]\s*([\w+]+)\s*:\s*(.+)$", l)
            if m:
                pairs.append((m.group(1), m.group(2)))
        add(seen_at, addr, pairs, "report", queue)
        return obs

    # 2. Часовая сводка: «✅ 13:23 · адрес ↗» + следующая строка с топливом
    # 3. Сигналы терминалов: «• 13:07 · адрес» + следующая строка с топливом
    for i, l in enumerate(lines[:-1]):
        m = re.match(r"^(✅|•)\s*(\d{1,2}:\d{2})\s*·\s*(.+)$", l)
        if m:
            pairs, queue = parse_fuel_groups(lines[i + 1])
            kind = "summary" if m.group(1) == "✅" else "signal"
            add(stamp(m.group(2), post_ts), m.group(3), pairs, kind, queue)
    return obs


def collect(db, pages):
    """Читает до `pages` страниц канала (по 20 постов) → число новых наблюдений."""
    before, new_obs = None, 0
    for _ in range(pages):
        posts = split_posts(http_get(f"https://t.me/s/{CHANNEL}" + (f"?before={before}" if before else "")))
        if not posts:
            break
        for pid, ts, text in posts:
            for o in parse_observations(pid, ts, text):
                new_obs += db.execute("INSERT OR IGNORE INTO obs VALUES (?,?,?,?,?,?,?,?)", o).rowcount
        before = min(p[0] for p in posts)
        time.sleep(1)
    return new_obs


def cmd_collect(args):
    db = open_db()
    new_obs = collect(db, args.pages)
    if new_obs and not args.no_save:
        save_db(db)
    print(f"{datetime.now(MSK):%d.%m %H:%M} новых наблюдений: {new_obs}", file=sys.stderr)
    return db


# ---------- состояние заправок ----------

def station_of(address):
    a = address.lower()
    for name, (_, pattern) in STATIONS.items():
        if re.search(pattern, a):
            return name
    return None


def latest_state(db):
    """→ {станция: {топливо: (seen_at, available, status, queue, kind)}} — последнее по каждому топливу."""
    latest = {name: {} for name in STATIONS}
    for seen_at, address, fuel, avail, status, kind, queue in db.execute(
            "SELECT seen_at, address, fuel, available, status, kind, queue FROM obs ORDER BY seen_at"):
        name = station_of(address)
        if name and fuel in FUELS:
            latest[name][fuel] = (datetime.fromisoformat(seen_at), avail, status, queue, kind)
    return latest


def station_state(fuels, now):
    """Состояние заправки → (css-класс, подпись, ранг для сортировки, время последних данных).
    Ранги: 0 есть (≤2 ч), 1 «есть» было давно, 2 только терминал, 3 нет, 4 нет данных."""
    fresh = {f: v for f, v in fuels.items() if now - v[0] <= FRESH}
    yes = [v[0] for v in fresh.values() if v[1] and v[4] != "signal"]
    term = [v[0] for v in fresh.values() if v[1] and v[4] == "signal"]
    if yes and now - max(yes) <= CONFIRM_FRESH:
        return "have", "Есть", 0, max(yes)
    if yes:
        return "stale", f"? было {when_text(max(yes), now)}", 1, max(yes)
    if term:
        return "term", "? терминал", 2, max(term)
    if fresh:
        return "none", "Нет", 3, max(v[0] for v in fresh.values())
    return "unknown", "Нет данных", 4, None


def when_text(t, now):
    """«в 14:31», «вчера в 22:17» или «07.10 в 18:05»."""
    days = (now.date() - t.date()).days
    return f"в {t:%H:%M}" if days == 0 else f"вчера в {t:%H:%M}" if days == 1 else f"{t:%d.%m} в {t:%H:%M}"


def ordered_stations(db, now):
    """Заправки по порядку: где бензин есть (свежее выше) → было давно → терминал → нет → без данных."""
    rows = [(name, fuels, station_state(fuels, now)) for name, fuels in latest_state(db).items()]
    return sorted(rows, key=lambda r: (r[2][2], -(r[2][3].timestamp() if r[2][3] else 0)))


def cmd_status(args, db=None):
    db = db or open_db()
    for name, fuels in latest_state(db).items():
        if not fuels:
            print(f"{name}: данных пока нет")
            continue
        print(name)
        for fuel in FUELS:
            if fuel in fuels:
                seen_at, avail, status, queue, kind = fuels[fuel]
                mark = "🟡" if kind == "signal" else ("🟢" if avail else "🔴")
                print(f"  {mark} АИ-{fuel}: {status_word(avail, kind, seen_at, datetime.now(MSK))} — {seen_at:%d.%m %H:%M} ({KIND_RU[kind]})"
                      + (f", очередь {queue}" if queue else ""))


def notify_text(db):
    """→ (заголовок, текст): заправки сгруппированы по состоянию, чтобы влезть в несколько строк уведомления."""
    now = datetime.now(MSK)
    groups = {"have": [], "stale": [], "term": [], "none": [], "unknown": []}
    for name, fuels, (cls, label, _, when) in ordered_stations(db, now):
        short = STATIONS[name][0]
        groups[cls].append(f"{short} ({when:%H:%M})" if when else short)
    heads = {"have": "✅ Есть", "stale": "🟡 Было давно", "term": "❓ Терминал", "none": "❌ Нет", "unknown": "⚪ Нет данных"}
    lines = [f"{heads[c]}: " + ", ".join(v) for c, v in groups.items() if v]
    title = f"⛽ АИ-95/98 · {now:%H:%M} · есть на {len(groups['have'])} из {len(STATIONS)}"
    return title, "\n".join(lines)


# ---------- Telegram ----------

def telegram_text(db):
    """Подробная сводка для Telegram (HTML-разметка)."""
    now = datetime.now(MSK)
    title, _ = notify_text(db)
    icons = {"have": "✅", "stale": "🟡", "term": "❓", "none": "❌", "unknown": "⚪"}
    parts = [f"<b>{html.escape(title)}</b>"]
    for name, fuels, (cls, label, _, _) in ordered_stations(db, now):
        lines = [f"{icons[cls]} <b>{html.escape(name)}</b> — {label}"]
        for fuel in FUELS:
            if fuel in fuels and now - fuels[fuel][0] <= FRESH:
                seen_at, avail, _, queue, kind = fuels[fuel]
                lines.append(f"   АИ-{fuel}: {status_word(avail, kind, seen_at, now)}, {seen_at:%H:%M} ({KIND_RU[kind]})"
                             + (f", очередь {html.escape(queue)}" if queue else ""))
        forecast = short_forecast(db, name, now)
        if forecast:
            lines.append(f"   📊 {forecast}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def tg_call(token, method, **params):
    data = urllib.parse.urlencode(params).encode() if params else None
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/{method}", data=data)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def page_button(now):
    """Кнопка под сообщением: открывает сводку внутри Telegram (параметр — чтобы не показывалась старая копия)."""
    return json.dumps({"inline_keyboard": [[{"text": "⛽ Открыть сводку",
                                             "web_app": {"url": f"{PAGE_URL}?t={now:%m%d%H%M}"}}]]})


def ensure_menu_button(token, chat_id):
    """Постоянная кнопка «⛽ Сводка» рядом с полем ввода в чате с ботом (ставится один раз)."""
    if TG_MENU_FILE.exists() and TG_MENU_FILE.read_text().strip() == PAGE_URL:
        return
    tg_call(token, "setChatMenuButton", chat_id=chat_id, menu_button=json.dumps(
        {"type": "web_app", "text": "⛽ Сводка", "web_app": {"url": PAGE_URL}}))
    TG_MENU_FILE.write_text(PAGE_URL)
    print("кнопка «Сводка» установлена в боте", file=sys.stderr)


def tg_chat_id(token):
    """Чат получателя: из переменной/файла, иначе — первый, кто написал боту /start."""
    if os.environ.get("TELEGRAM_CHAT_ID"):
        return os.environ["TELEGRAM_CHAT_ID"]
    if TG_CHAT_FILE.exists():
        return TG_CHAT_FILE.read_text().strip()
    me = tg_call(token, "getMe").get("result", {})
    updates = tg_call(token, "getUpdates").get("result", [])
    print(f"бот @{me.get('username')}: входящих сообщений {len(updates)}", file=sys.stderr)
    for upd in updates:
        chat = (upd.get("message") or {}).get("chat", {})
        if chat.get("type") == "private":
            TG_CHAT_FILE.write_text(str(chat["id"]))
            return str(chat["id"])
    return None


def available_now(fuels, now):
    """Марки, которые водители или сводка канала подтвердили как «есть» за последние ALERT_WINDOW.
    Сигналы терминалов не учитываем — по ним бот не пишет."""
    return {f: v for f, v in fuels.items()
            if v[1] and v[4] != "signal" and now - v[0] <= ALERT_WINDOW}


def alert_text(db, appeared, now):
    lines = ["<b>⛽ Появился бензин</b>"]
    for name, fuels in appeared:
        parts = []
        for fuel in FUELS:
            if fuel in fuels:
                seen_at, _, _, queue, kind = fuels[fuel]
                parts.append(f"АИ-{fuel} есть ({seen_at:%H:%M}, {KIND_RU[kind]}"
                             + (f", очередь {html.escape(queue)}" if queue else "") + ")")
        lines.append(f"\n✅ <b>{html.escape(name)}</b>\n   " + "; ".join(parts))
        st = station_stats(db, name, now)
        if st and median_duration(st["durations"]):
            lines.append(f"   📊 обычно держится около {median_duration(st['durations'])}")
    return "\n".join(lines)


def cmd_telegram(args):
    token = os.environ.get("TELEGRAM_TOKEN")
    if not token:
        sys.exit("Не задан TELEGRAM_TOKEN")
    chat_id = tg_chat_id(token)
    if not chat_id:
        sys.exit("Бот не знает, кому писать: отправьте ему /start в Telegram")
    ensure_menu_button(token, chat_id)
    db = open_db()
    if not args.alerts:
        tg_call(token, "sendMessage", chat_id=chat_id, text=telegram_text(db),
                parse_mode="HTML", disable_web_page_preview="true", reply_markup=page_button(datetime.now(MSK)))
        print("сводка отправлена в Telegram", file=sys.stderr)
        return

    now = datetime.now(MSK)
    state = json.loads(TG_ALERTS_FILE.read_text()) if TG_ALERTS_FILE.exists() else {}
    appeared = []
    for name, fuels in latest_state(db).items():
        avail = available_now(fuels, now)
        prev = state.get(name, {})
        last_alert = datetime.fromisoformat(prev["last_alert"]) if prev.get("last_alert") else None
        if avail and not prev.get("have") and (not last_alert or now - last_alert >= ALERT_COOLDOWN):
            appeared.append((name, avail))
            prev["last_alert"] = now.isoformat()
        prev["have"] = bool(avail)
        state[name] = prev
    if appeared:
        tg_call(token, "sendMessage", chat_id=chat_id, text=alert_text(db, appeared, now),
                parse_mode="HTML", disable_web_page_preview="true", reply_markup=page_button(now))
        print("оповещение: " + ", ".join(n for n, _ in appeared), file=sys.stderr)
    else:
        print("новых появлений нет", file=sys.stderr)
    TG_ALERTS_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=1))


# ---------- статистика: интервалы наличия, появления и окончания ----------

STATS_DAYS = 30              # за сколько дней считать статистику
MAX_GAP = timedelta(hours=2)  # сколько считаем состояние верным после последнего отчёта
EVENT_GAP = timedelta(hours=12)  # смена «нет→есть» засчитывается, если между состояниями не дольше
SIGNAL_CONFLICT = timedelta(minutes=60)


def station_series(db, name, fuel, since):
    """Отчёты по одной заправке и марке → [(время, есть?, источник)], по времени.
    Сигналы терминалов отбрасываем, если рядом (±1 ч) есть отчёт водителя или сводка: они точнее."""
    raw = [(datetime.fromisoformat(t), a, k) for t, addr, a, k in db.execute(
        "SELECT seen_at, address, available, kind FROM obs WHERE fuel = ? AND seen_at >= ? ORDER BY seen_at",
        (fuel, since.isoformat())) if station_of(addr) == name]
    confirmed = [t for t, _, k in raw if k != "signal"]
    raw = [r for r in raw if r[2] != "signal" or all(abs(r[0] - t) > SIGNAL_CONFLICT for t in confirmed)]
    merged = {}
    for t, a, k in raw:  # в одну минуту — один ответ, подтверждённый важнее
        if t not in merged or merged[t][1] == "signal":
            merged[t] = (a, k)
    return [(t, a, k) for t, (a, k) in sorted(merged.items())]


def intervals(series, now):
    """→ [(начало, конец, есть?, источник)]: состояние держится до следующего отчёта, но не дольше MAX_GAP."""
    out = []
    for i, (t, a, k) in enumerate(series):
        nxt = series[i + 1][0] if i + 1 < len(series) else None
        end = nxt if nxt and nxt - t <= MAX_GAP else t + MAX_GAP
        end = min(end, now)
        if end > t:
            out.append((t, end, a, k))
    return out


def station_timeline(db, name, now):
    """Наличие «нужного бензина» (АИ-95 или 98) на заправке во времени →
    [(начало, конец, есть?, только_терминалы?)]. Есть, если есть хоть одна из марок; нет — если все известные «нет»."""
    since = now - timedelta(days=STATS_DAYS)
    per_fuel = [intervals(station_series(db, name, f, since), now) for f in FUELS]
    bounds = sorted({t for iv in per_fuel for s, e, _, _ in iv for t in (s, e)})
    out = []
    for b0, b1 in zip(bounds, bounds[1:]):
        mid = b0 + (b1 - b0) / 2
        cover = [(a, k) for iv in per_fuel for s, e, a, k in iv if s <= mid < e]
        if not cover:
            continue
        yes = [k for a, k in cover if a]
        avail = 1 if yes else 0
        weak = all(k == "signal" for k in (yes if yes else [k for _, k in cover]))
        if out and out[-1][1] == b0 and out[-1][2] == avail and out[-1][3] == weak:
            out[-1] = (out[-1][0], b1, avail, weak)
        else:
            out.append((b0, b1, avail, weak))
    return out


def events(timeline):
    """→ (появления, окончания, длительности) по смене «нет→есть» и «есть→нет»."""
    runs = []
    for s, e, a, _ in timeline:  # склеиваем по «есть/нет», без учёта источника
        if runs and runs[-1][2] == a and s <= runs[-1][1]:
            runs[-1] = (runs[-1][0], e, a)
        else:
            runs.append((s, e, a))
    arrivals, runouts, durations = [], [], []
    last_arrival = None
    for (s0, e0, a0), (s1, e1, a1) in zip(runs, runs[1:]):
        if s1 - e0 > EVENT_GAP:
            last_arrival = None
            continue
        if a0 == 0 and a1 == 1:
            arrivals.append(s1)
            last_arrival = s1
        elif a0 == 1 and a1 == 0:
            runouts.append(s1)
            if last_arrival:
                durations.append(s1 - last_arrival)
            last_arrival = None
    return arrivals, runouts, durations


def hours_range(h, width=3):
    """«21–24 ч», а не «21–0 ч»."""
    end = (h + width) % 24 or 24
    return f"{h}–{end} ч"


def busiest_window(times, width=3):
    """Самое частое окно в `width` часов (по кругу суток) → (час начала, число случаев)."""
    counts = [0] * 24
    for t in times:
        counts[t.hour] += 1
    best = max(range(24), key=lambda h: sum(counts[(h + i) % 24] for i in range(width)))
    return best, sum(counts[(best + i) % 24] for i in range(width))


def hourly_share(timeline):
    """→ [(минут «есть», минут «нет»)] по часам суток."""
    acc = [[0.0, 0.0] for _ in range(24)]
    for start, end, a, _ in timeline:
        t = start
        while t < end:
            nxt = min(end, t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            acc[t.hour][0 if a else 1] += (nxt - t).total_seconds() / 60
            t = nxt
    return acc


def fmt_hours(td):
    h = td.total_seconds() / 3600
    return f"{h:.0f} ч" if h >= 2 else f"{h * 60:.0f} мин"


def station_stats(db, name, now):
    timeline = station_timeline(db, name, now)
    if not timeline:
        return None
    arrivals, runouts, durations = events(timeline)
    return {"timeline": timeline, "arrivals": arrivals, "runouts": runouts, "durations": durations,
            "hourly": hourly_share(timeline), "since": timeline[0][0]}


def window_text(times):
    if len(times) >= 3:
        h, n = busiest_window(times)
        return f"{hours_range(h)} ({n} из {len(times)})"
    return f"мало данных ({len(times)})" if times else "—"


def median_duration(durations):
    if len(durations) < 2:
        return None
    return fmt_hours(sorted(durations)[len(durations) // 2])


def short_forecast(db, name, now):
    """Одна строка для Telegram: когда обычно появляется/заканчивается бензин (если данных достаточно)."""
    st = station_stats(db, name, now)
    if not st:
        return None
    parts = []
    if len(st["arrivals"]) >= 3:
        h, _ = busiest_window(st["arrivals"])
        parts.append(f"привозят обычно {hours_range(h)}")
    if len(st["runouts"]) >= 3:
        h, _ = busiest_window(st["runouts"])
        parts.append(f"кончается {hours_range(h)}")
    return ", ".join(parts) or None


# ---------- графики (SVG): все заправки на общих графиках, у каждой свой цвет ----------

W = 400  # ширина графика в единицах viewBox — рассчитано на телефон; на компьютере ширина ограничена в CSS
DAYS_RU = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]


def tip(text):
    return f'data-tip="{html.escape(text, quote=True)}" tabindex="0"'


def first_hour(stats, default=12):
    """С какого часа показывать графики: с первого часа (не раньше 6:00), когда были данные, но не позже 12:00.
    Ночные часы — это лишь «хвост» вечерних отчётов, сбора там нет."""
    hours = [h for st in stats.values() if st for h in range(6, 24) if sum(st["hourly"][h]) >= 1]
    return min(hours + [default])


def chart_heat(stats, h0):
    """Тепловая карта: строка на заправку, клетка на час. Чем ярче клетка, тем чаще в этот час бензин был."""
    names, n = list(stats), 24 - h0
    x0, x1, top, row = 112, W - 6, 4, 22
    cw = (x1 - x0) / n
    height = top + row * len(names) + 18
    out = [f'<svg viewBox="0 0 {W} {height}" class="chart" role="img" aria-label="Когда обычно есть бензин">']
    for i, name in enumerate(names):
        st, y = stats[name], top + row * i
        out.append(f'<text x="{x0 - 6}" y="{y + row / 2 + 4:.1f}" class="tick label" text-anchor="end">'
                   f'{html.escape(STATIONS[name][0])}</text>')
        for h in range(h0, 24):
            x = x0 + cw * (h - h0)
            have, none = st["hourly"][h] if st else (0, 0)
            label = f"{STATIONS[name][0]} · {h:02d}:00–{(h + 1) % 24:02d}:00 · "
            if have + none < 1:
                out.append(f'<rect x="{x + 1:.1f}" y="{y + 1}" width="{cw - 2:.1f}" height="{row - 2}" rx="3" class="track" '
                           f'{tip(label + "нет данных")}/>')
                continue
            share = have / (have + none)
            out.append(f'<rect x="{x + 1:.1f}" y="{y + 1}" width="{cw - 2:.1f}" height="{row - 2}" rx="3" '
                       f'class="cell s{i + 1}" style="fill-opacity:{0.12 + 0.88 * share:.2f}" '
                       f'{tip(label + f"бензин был {share:.0%} времени")}/>')
    for h in range(h0, 25, 2 if n <= 14 else 3):
        out.append(f'<text x="{x0 + cw * (h - h0):.1f}" y="{top + row * len(names) + 14}" class="tick" text-anchor="middle">{h}</text>')
    out.append("</svg>")
    return "".join(out)


def events_list(stats, now, limit=12):
    """Последние случаи, когда бензин появлялся и заканчивался, — простым списком."""
    events_ = []
    for i, (name, st) in enumerate(stats.items()):
        if st:
            events_ += [(t, i, name, "появился") for t in st["arrivals"]]
            events_ += [(t, i, name, "закончился") for t in st["runouts"]]
    events_ = sorted((e for e in events_ if now - e[0] <= timedelta(days=7)), reverse=True)[:limit]
    if not events_:
        return '<p class="muted">Пока не было ни одного случая, когда бензин появился или закончился: нужно больше данных.</p>'
    items = "".join(
        f'<li><b>{t:%d.%m %H:%M}</b> <i class="k s{i + 1}"></i>{html.escape(STATIONS[name][0])} — '
        f'<span class="{"ev-on" if what == "появился" else "ev-off"}">бензин {what}</span></li>'
        for t, i, name, what in events_)
    return f'<ul class="events">{items}</ul>'


def chart_week(stats, now, h0):
    """Последние 7 дней (только часы сбора): строка на заправку.
    Цвет заправки — есть, бледный — возможно (терминалы), серый — нет, пусто — нет данных."""
    names = list(stats)
    x0, x1, top, row, gap = 112, W - 6, 18, 14, 6
    start = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    dayw = (x1 - x0) / 7

    def xt(t):  # время → x: в каждом дне показываем только часы с h0 до 24
        d = (t - start).days
        frac = (t - (start + timedelta(days=d))).total_seconds() / 3600
        return x0 + dayw * (d + min(max((frac - h0) / (24 - h0), 0), 1))

    height = top + (row + gap) * len(names) + 2
    out = [f'<svg viewBox="0 0 {W} {height}" class="chart" role="img" aria-label="Наличие бензина за последние 7 дней">']
    for d in range(8):
        x = x0 + dayw * d
        out.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{top - 4}" y2="{height - 2}" class="grid"/>')
        if d < 7:
            day = start + timedelta(days=d)
            out.append(f'<text x="{x + dayw / 2:.1f}" y="{top - 7}" class="tick" text-anchor="middle">'
                       f'{DAYS_RU[day.weekday()]} {day:%d}</text>')
    for i, name in enumerate(names):
        y = top + (row + gap) * i
        out.append(f'<text x="{x0 - 6}" y="{y + 11}" class="tick label" text-anchor="end">{html.escape(STATIONS[name][0])}</text>'
                   f'<rect x="{x0}" y="{y}" width="{x1 - x0}" height="{row}" rx="3" class="track"/>')
        st = stats[name]
        if not st:
            continue
        for s, e, a, weak in st["timeline"]:
            s, e = max(s, start), min(e, now)
            if e <= s:
                continue
            xs_, xe = xt(s), xt(e)
            if xe - xs_ < 0.3:
                continue  # отрезок целиком в часах без сбора
            cls = (f"s{i + 1}" + (" weak" if weak else "")) if a else "off"
            word = ("возможно есть (терминалы)" if weak else "есть") if a else ("возможно нет (терминалы)" if weak else "нет")
            out.append(f'<rect x="{xs_ + 0.5:.1f}" y="{y}" width="{max(xe - xs_ - 1, 1.2):.1f}" height="{row}" rx="2" '
                       f'class="seg {cls}" {tip(f"{STATIONS[name][0]} · {s:%d.%m %H:%M}–{e:%H:%M} · {word}")}/>')
    out.append("</svg>")
    return "".join(out)


def stats_section(db, now):
    stats = {name: station_stats(db, name, now) for name in STATIONS}
    h0 = first_hour(stats)
    esc = html.escape
    legend = "".join(f'<span><i class="k s{i + 1}"></i>{esc(STATIONS[n][0])}</span>' for i, n in enumerate(stats))
    since = min((st["since"] for st in stats.values() if st), default=now)
    note = ""
    if now - since < timedelta(days=7):
        note = (f'<p class="note">Данные собираются с {since:%d.%m}. Выводы станут надёжными примерно через 1–2 недели.</p>')

    def cell(times):
        if len(times) >= 3:
            h, k = busiest_window(times)
            return f'{hours_range(h)}<small>{k} из {len(times)} случаев</small>'
        if not times:
            return '<span class="muted">—</span>'
        return " ".join(f"{t:%H:%M}" for t in sorted(times)[-2:]) + '<small>мало данных</small>'

    rows = []
    for i, (name, st) in enumerate(stats.items()):
        sw = f'<i class="k s{i + 1}"></i>'
        name_cell = f'<span class="nm">{sw}{esc(STATIONS[name][0])}</span>'
        if not st:
            rows.append(f'<tr><td>{name_cell}</td><td colspan="3" class="muted">данных пока нет</td></tr>')
            continue
        rows.append(f'<tr><td>{name_cell}</td><td>{cell(st["arrivals"])}</td>'
                    f'<td>{cell(st["runouts"])}</td><td>{median_duration(st["durations"]) or "—"}</td></tr>')
    hour_rows = "".join(
        f"<tr><td>{h:02d}:00</td>" + "".join(
            (f"<td>{st['hourly'][h][0] / sum(st['hourly'][h]):.0%}</td>" if st and sum(st["hourly"][h]) >= 1 else "<td>—</td>")
            for st in stats.values()) + "</tr>" for h in range(h0, 24))
    return f"""
<section class="card stats">
  <div class="legend stations">{legend}</div>{note}
  <table class="tbl est"><tr><th>Заправка</th><th>Привозят</th><th>Кончается</th><th>Держится</th></tr>{"".join(rows)}</table>
  <h4>Когда обычно есть бензин</h4>
  <div class="legend"><span>чем ярче клетка, тем чаще в этот час бензин был</span><span><i class="k track"></i>нет данных</span></div>
  {chart_heat(stats, h0)}
  <h4>Последние появления и окончания</h4>{events_list(stats, now)}
  <h4>Последние 7 дней, {h0}:00–24:00</h4>
  <div class="legend"><span><i class="k sample"></i>есть (цвет заправки)</span><span><i class="k sample weak"></i>возможно (терминалы)</span><span><i class="k off"></i>нет</span><span><i class="k track"></i>нет данных</span></div>
  {chart_week(stats, now, h0)}
  <details><summary>Таблица по часам: доля времени, когда бензин есть</summary>
  <div class="scroll"><table class="tbl"><tr><th>Час</th>{"".join(f"<th>{esc(STATIONS[n][0])}</th>" for n in stats)}</tr>{hour_rows}</table></div></details>
</section>"""


# ---------- страница с подробностями (для приложения на Mac) ----------

def write_report(db, path=None):
    now = datetime.now(MSK)
    esc = html.escape
    since = (now - FRESH).isoformat()
    cards = []
    for name, fuels, (cls, label, _, _) in ordered_stations(db, now):
        rows = []
        for fuel in FUELS:
            if fuel in fuels:
                seen_at, avail, status, queue, kind = fuels[fuel]
                old = " old" if now - seen_at > FRESH else ""
                rows.append(
                    f'<tr class="{"maybe" if kind == "signal" else "yes" if avail else "no"}{old}"><td class="fuel">АИ-{esc(fuel)}</td>'
                    f'<td>{status_html(avail, kind, seen_at, now)}</td><td>{seen_at:%d.%m %H:%M}</td>'
                    f'<td>{esc(queue or "—")}</td><td class="src">{KIND_RU[kind]}</td></tr>')
        table = ('<table><tr><th>Марка</th><th>Статус</th><th>Когда</th><th>Очередь</th><th>Источник</th></tr>'
                 + "".join(rows) + "</table>") if rows else '<p class="muted">Канал ещё не присылал отчётов по этой заправке.</p>'
        hist = []
        for seen_at, address, fuel, avail, kind in db.execute(
                "SELECT seen_at, address, fuel, available, kind FROM obs WHERE seen_at >= ? ORDER BY seen_at DESC", (since,)):
            if fuel in FUELS and station_of(address) == name:
                t = datetime.fromisoformat(seen_at)
                hist.append(f'<li><b>{t:%H:%M}</b> АИ-{esc(fuel)} — '
                            f'{status_html(avail, kind, t, now)} <span class="muted">({KIND_RU[kind]})</span></li>')
        hist_html = (f'<details><summary>Все отчёты за сутки ({len(hist)})</summary><ul>{"".join(hist[:60])}</ul></details>'
                     if hist else "")
        cards.append(f'<section class="card"><div class="head"><h2>{esc(name)}</h2>'
                     f'<span class="badge {cls}">{label}</span></div>{table}{hist_html}</section>')
    page = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex, nofollow">
<title>Бензин · АИ-95/98</title>
<style>
:root {{ color-scheme: light; --bg:#f9f9f7; --card:#fcfcfb; --text:#0b0b0b; --text2:#52514e; --muted:#898781;
  --line:#e1e0d9; --axis:#c3c2b7; --yes:#2a78d6; --no:#eb6834; --track:#efeee9;
  --have:#0ca30c; --maybe:#b7791f; --none:#d03b3b; --unknown:#898781; --off:#c3c2b7;
  --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; --s4:#eda100; --s5:#e87ba4; --s6:#008300; }}
@media (prefers-color-scheme: dark) {{ :root {{ color-scheme: dark; --bg:#0d0d0d; --card:#1a1a19; --text:#fff; --text2:#c3c2b7;
  --line:#2c2c2a; --axis:#383835; --yes:#3987e5; --no:#d95926; --track:#262624; --off:#55544f;
  --s1:#3987e5; --s2:#d95926; --s3:#199e70; --s4:#c98500; --s5:#d55181; --s6:#008300; }} }}
body {{ margin:0; background:var(--bg); color:var(--text); font:15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ max-width:780px; margin:0 auto; padding:24px 16px 48px; }}
h1 {{ font-size:22px; margin:0 0 4px; }} h2 {{ font-size:17px; margin:0; }}
h3 {{ font-size:15px; margin:18px 0 6px; }} h4 {{ font-size:13px; font-weight:600; color:var(--text2); margin:14px 0 2px; }}
.muted {{ color:var(--muted); }} .note {{ color:var(--text2); font-size:13px; margin:4px 0; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:14px; padding:16px; margin-top:14px; }}
.head {{ display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; }}
.badge {{ font-size:13px; font-weight:600; padding:3px 10px; border-radius:999px; color:#fff; white-space:nowrap; }}
.badge.have {{ background:var(--have); }} .badge.stale {{ background:var(--maybe); }}
.badge.term {{ background:transparent; color:var(--none); border:1.5px solid var(--none); }}
.badge.none {{ background:var(--none); }} .badge.unknown {{ background:var(--unknown); }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th {{ text-align:left; color:var(--muted); font-weight:500; padding:4px 6px; border-bottom:1px solid var(--line); }}
td {{ padding:6px; border-bottom:1px solid var(--line); font-variant-numeric: tabular-nums; }}
tr.yes td:nth-child(2) {{ font-weight:600; }} tr.no td:nth-child(2) {{ font-weight:600; }}
tr.old td {{ opacity:.5; }} .fuel {{ font-weight:600; }} .src {{ color:var(--muted); font-size:13px; }}
details {{ margin-top:10px; }} summary {{ cursor:pointer; color:var(--text2); }} ul {{ margin:6px 0 0; padding-left:18px; }}
h2.section {{ font-size:19px; margin:28px 0 2px; }}
.topbar {{ display:flex; gap:12px; align-items:flex-start; justify-content:space-between; flex-wrap:wrap; }}
.topbar .muted {{ flex:1 1 300px; }}
.refresh {{ flex:none; background:var(--yes); color:#fff; text-decoration:none; font-weight:600; font-size:14px;
  padding:8px 14px; border-radius:10px; }}
.refresh:active {{ opacity:.8; }}
.hint {{ margin-top:10px; padding:10px 12px; border:1px solid var(--line); border-radius:10px; background:var(--card);
  color:var(--text2); font-size:14px; }}
.stats + .stats {{ border-top:1px solid var(--line); margin-top:14px; }}
.stats h3 {{ margin-top:4px; }}
.summary {{ padding-left:18px; }} .summary li {{ margin:2px 0; }}
.legend {{ display:flex; flex-wrap:wrap; gap:14px; font-size:13px; color:var(--text2); margin:8px 0 0; }}
.legend .k {{ display:inline-block; width:12px; height:12px; border-radius:3px; margin-right:6px; vertical-align:-1px; }}
.k.c-yes {{ background:var(--yes); }} .k.c-no {{ background:var(--no); }} .k.track {{ background:var(--track); border:1px solid var(--line); }}
.chart {{ width:100%; height:auto; display:block; }}
.chart .grid {{ stroke:var(--line); stroke-width:1; }} .chart .axis {{ stroke:var(--axis); stroke-width:1; }}
.chart .tick {{ fill:var(--muted); font-size:11px; font-variant-numeric: tabular-nums; }}
.stats .chart {{ max-width:560px; }} .scroll {{ overflow-x:auto; }}
.chart .c-yes {{ fill:var(--yes); }} .chart .c-no {{ fill:var(--no); }} .chart .track {{ fill:var(--track); }}
.chart .nodata {{ fill:var(--line); }} .chart .hit {{ fill:transparent; }} .chart .hit:hover {{ fill:var(--text); fill-opacity:.05; }}
.chart .seg:hover, .chart .seg:focus {{ opacity:.8; outline:none; }}
.chart .line {{ fill:none; stroke-width:2; stroke-linejoin:round; stroke-linecap:round; }}
.chart .dot {{ stroke:var(--card); stroke-width:2; }} .chart .ring {{ fill:var(--card); stroke-width:2.5; }}
.chart .label {{ fill:var(--text2); font-size:11.5px; }}
.chart .off {{ fill:var(--off); }} .chart .weak {{ opacity:.45; }}
.chart .cell:hover, .chart .cell:focus {{ stroke:var(--text); stroke-width:1.5; outline:none; }}
.events {{ list-style:none; padding:0; margin:6px 0 0; }} .events li {{ padding:4px 0; border-bottom:1px solid var(--line); }}
.events .k {{ display:inline-block; width:10px; height:10px; border-radius:3px; margin:0 6px 0 8px; }}
.ev-on {{ font-weight:600; }} .ev-off {{ color:var(--none); }}
{"".join(f".chart .line.s{i} {{ stroke:var(--s{i}); }} .chart .dot.s{i}, .chart .seg.s{i}, .chart .cell.s{i} {{ fill:var(--s{i}); }} .chart .ring.s{i} {{ stroke:var(--s{i}); }} .k.s{i} {{ background:var(--s{i}); }}" for i in range(1, 7))}
.k.sample {{ background:linear-gradient(90deg, var(--s1) 33%, var(--s2) 33% 66%, var(--s3) 66%); }} .k.weak {{ opacity:.45; }}
.k.off {{ background:var(--off); }}
.legend.stations {{ margin:0 0 10px; }} .est {{ margin-bottom:6px; table-layout:auto; }}
.card > table.est {{ table-layout:auto; }} .card > table.est th {{ width:auto; font-size:12px; }}
.est td {{ vertical-align:top; }} .est td:first-child {{ width:34%; }} .est small {{ display:block; color:var(--muted); font-size:12px; }}
.est .k, .legend .k {{ display:inline-block; width:12px; height:12px; border-radius:3px; margin-right:6px; vertical-align:-1px; }}
.st-q {{ color:var(--maybe); font-weight:800; font-size:1.1em; cursor:help; }}
.st-qr {{ color:var(--none); font-weight:800; font-size:1.1em; cursor:help; }} .st-no {{ color:var(--none); font-weight:600; }}
.card > table {{ table-layout:fixed; }}
.card > table th:nth-child(1) {{ width:16%; }} .card > table th:nth-child(2) {{ width:14%; }}
.card > table th:nth-child(3) {{ width:20%; }} .card > table th:nth-child(4) {{ width:20%; }}
.tbl td, .tbl th {{ padding:3px 6px; font-size:13px; }}
#tip {{ position:fixed; pointer-events:none; background:var(--card); color:var(--text); border:1px solid var(--line);
  border-radius:8px; padding:6px 9px; font-size:13px; box-shadow:0 4px 14px rgba(0,0,0,.15); display:none; max-width:280px; z-index:10; white-space:pre-line; }}
@media (max-width:560px) {{ th:nth-child(5), td:nth-child(5) {{ display:none; }} .tbl th:nth-child(4), .tbl td:nth-child(4) {{ display:table-cell; }} }}
</style></head><body data-updated="{now.isoformat()}"><main>
<h1>⛽ Бензин · АИ-95 / 98</h1>
<div class="topbar">
  <div class="muted">Обновлено <b id="updated">{now:%d.%m.%Y в %H:%M}</b> <span id="ago"></span>.
  Данные из канала @voronezh_benzin. Сбор: 12–18 ч каждые 45 мин, 18–24 ч каждые 10 мин, ночью и утром не ведётся. Бледные строки — старше суток.</div>
  <a class="refresh" id="refresh" href="{RUN_URL}" target="_blank" rel="noopener">🔄 Обновить сейчас</a>
</div>
<div class="hint" id="hint" hidden>Нажмите <b>Run workflow</b> на GitHub. Примерно через 1–2 минуты эта страница обновится сама.</div>
<h2 class="section">Сводка сейчас</h2>
{"".join(cards)}
<h2 class="section">Статистика: когда привозят и когда заканчивается</h2>
<div class="muted">Все заправки на общих графиках, у каждой свой цвет. «Бензин есть» — есть АИ-95 или АИ-98.
Наведите на график или нажмите на него, чтобы увидеть подробности.</div>
{stats_section(db, now)}
<p class="muted" style="margin-top:20px">«Водитель» — отчёт подписчика с заправки. «Сводка канала» — подтверждённые данные за последний час.
«Терминалы оплаты» — топливо продаётся по данным касс, но водители ещё не подтвердили; если рядом по времени есть отчёт водителя, в статистике учитывается он.
Состояние считается неизменным до следующего отчёта, но не дольше 2 часов; дальше — «нет данных».</p>
</main><div id="tip" role="tooltip"></div>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script>
// «N мин назад» и автообновление, когда сервер опубликует более свежую сводку
const updated = new Date(document.body.dataset.updated);
function tickAgo() {{ const m = Math.round((Date.now() - updated) / 60000);
  document.getElementById('ago').textContent = m < 1 ? '(только что)' : m < 120 ? `(${{m}} мин назад)` : ''; }}
tickAgo(); setInterval(tickAgo, 30000);
async function checkFresh() {{
  if (location.protocol === 'file:') return;
  try {{ const r = await fetch(location.pathname + '?check=' + Date.now(), {{cache: 'no-store'}});
    const m = (await r.text()).match(/data-updated="([^"]+)"/);
    if (m && new Date(m[1]) > updated) location.reload(); }} catch (e) {{}}
}}
setInterval(checkFresh, 30000);
// в Telegram ссылку открываем через Telegram — тогда она откроется в приложении GitHub или браузере
document.getElementById('refresh').addEventListener('click', e => {{
  document.getElementById('hint').hidden = false;
  const tg = window.Telegram && Telegram.WebApp;
  if (tg && tg.initData) {{ e.preventDefault(); tg.openLink(e.currentTarget.href); }}
}});
if (window.Telegram && Telegram.WebApp && Telegram.WebApp.initData) Telegram.WebApp.ready();
const tipEl = document.getElementById('tip');
function show(e) {{ const t = e.target.closest('[data-tip]'); if (!t) return;
  tipEl.textContent = t.getAttribute('data-tip'); tipEl.style.display = 'block'; move(e); }}
function move(e) {{ if (tipEl.style.display !== 'block') return;
  let x, y; if (e.clientX !== undefined) {{ x = e.clientX; y = e.clientY; }}
  else {{ const r = e.target.getBoundingClientRect(); x = r.left + r.width / 2; y = r.top; }}
  const w = tipEl.offsetWidth; tipEl.style.left = Math.max(8, Math.min(x + 12, innerWidth - w - 8)) + 'px';
  tipEl.style.top = Math.max(8, y - tipEl.offsetHeight - 10) + 'px'; }}
function hide(e) {{ if (e.target.closest('[data-tip]')) tipEl.style.display = 'none'; }}
document.addEventListener('pointerover', show); document.addEventListener('pointermove', move);
document.addEventListener('pointerout', hide); document.addEventListener('focusin', show); document.addEventListener('focusout', hide);
</script></body></html>"""
    Path(path or REPORT_PATH).write_text(page, encoding="utf-8")


def cmd_update(args):
    """На Mac: данные приходят из GitHub (git pull делает приложение), здесь — дособрать свежее без сохранения."""
    db = open_db()
    try:
        collect(db, args.pages)
    except Exception as e:
        print(f"сбор не удался: {e}", file=sys.stderr)
    write_report(db)
    if args.json:
        title, body = notify_text(db)
        print(json.dumps({"title": title, "body": body, "report": str(REPORT_PATH)}, ensure_ascii=False))
    else:
        cmd_status(args, db)


# ---------- сервер на GitHub Actions: непрерывная работа с 12:00 до 24:00 ----------
#
# GitHub плохо выполняет расписание (запуски опаздывают на часы или пропадают), поэтому
# одна задача работает непрерывно и сама собирает данные по расписанию ниже. GitHub ограничивает
# задачу 6 часами — перед этим она запускает себе продолжение («смену»). Кнопка «Обновить сейчас»
# создаёт задачу, которая ждёт в очереди; работающая задача замечает её за ~20 секунд,
# делает сбор и отменяет её.

WORK_START, WORK_END = 12, 24  # часы сбора, МСК
REPO = os.environ.get("GITHUB_REPOSITORY", "dooodoIvan/benzin")
WORKFLOW = "collect.yml"
MANUAL_TITLE = "Обновить сейчас"  # run-name ручного запуска (см. .github/workflows/collect.yml)


def day_slots(ws):
    """Моменты сбора за день: 12:00–17:15 каждые 45 мин, 18:00–23:50 каждые 10 мин."""
    return ([ws + timedelta(minutes=45 * k) for k in range(8)] +
            [ws + timedelta(hours=6, minutes=10 * k) for k in range(36)])


def gh_api(method, path, body=None):
    req = urllib.request.Request(f"https://api.github.com/repos/{REPO}/{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {os.environ['GH_TOKEN']}",
                                          "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def pending_manual_runs():
    """Ручные запуски («Обновить сейчас»), которые ждут в очереди за текущей задачей."""
    own = os.environ.get("GITHUB_RUN_ID")
    try:
        runs = gh_api("GET", f"actions/workflows/{WORKFLOW}/runs?status=pending&per_page=20").get("workflow_runs", [])
    except Exception as e:
        print(f"не удалось проверить очередь: {e}", file=sys.stderr)
        return []
    return [r["id"] for r in runs if r.get("display_title") == MANUAL_TITLE and str(r["id"]) != own]


def git(*args, check=True):
    return subprocess.run(["git", *args], cwd=BASE, check=check, capture_output=True, text=True)


def publish_page(db):
    """Страница со сводкой → открытое хранилище dooodoIvan/benzin-page (GitHub Pages), одной свежей версией."""
    key = os.environ.get("PAGES_KEY_FILE")
    if not key:
        return
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        page = Path(tmp)
        write_report(db, page / "index.html")
        (page / ".nojekyll").write_text("")
        (page / "README.md").write_text("# Бензин · сводка\n\nОбновляется автоматически: 12–18 ч МСК каждые 45 мин, 18–24 ч каждые 10 мин.\n")
        env = {**os.environ, "GIT_SSH_COMMAND": f"ssh -i {key} -o StrictHostKeyChecking=accept-new"}
        for cmd in (["init", "-q", "-b", "main"], ["add", "-A"],
                    ["-c", "user.name=github-actions[bot]",
                     "-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
                     "commit", "-q", "-m", f"сводка {datetime.now(MSK):%d.%m %H:%M}"],
                    ["push", "-q", "--force", "git@github.com:dooodoIvan/benzin-page.git", "main"]):
            subprocess.run(["git", *cmd], cwd=page, env=env, check=True, capture_output=True, text=True)


def save_and_push():
    git("add", "data")
    if git("diff", "--cached", "--quiet", check=False).returncode == 0:
        return
    git("commit", "-q", "-m", f"данные {datetime.now(MSK):%d.%m %H:%M}")
    for attempt in range(3):
        if git("push", "-q", check=False).returncode == 0:
            return
        git("pull", "-q", "--rebase", check=False)
    print("не удалось отправить данные в хранилище", file=sys.stderr)


def work_once():
    """Один сбор: канал → data/obs.csv → страница → оповещение в Telegram → сохранить в хранилище."""
    t0 = time.time()
    db = open_db()
    try:
        new = collect(db, 4)
        if new:
            save_db(db)
    except Exception as e:
        new = 0
        print(f"сбор не удался: {e}", file=sys.stderr)
    for step, fn in (("страница", lambda: publish_page(db)),
                     ("telegram", lambda: cmd_telegram(argparse.Namespace(alerts=True)) if os.environ.get("TELEGRAM_TOKEN") else None),
                     ("сохранение", save_and_push)):
        try:
            fn()
        except Exception as e:
            print(f"{step}: ошибка {e}", file=sys.stderr)
    print(f"{datetime.now(MSK):%d.%m %H:%M} сбор: новых наблюдений {new}, {time.time() - t0:.0f} с", file=sys.stderr, flush=True)


def cmd_worker(args):
    start = datetime.now(MSK)
    deadline = start + timedelta(minutes=args.max_minutes)
    ws = start.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
    we = ws + timedelta(hours=WORK_END - WORK_START)
    if we <= start + timedelta(minutes=355):  # до конца дня успеваем в одну задачу (предел GitHub — 360 мин)
        deadline = max(deadline, we)
    print(f"запуск: {args.reason}, {start:%d.%m %H:%M}", file=sys.stderr, flush=True)

    if start >= we or start < ws - timedelta(hours=2):
        # ночью и рано утром сбора по расписанию нет; по кнопке — один сбор
        if args.reason == "manual":
            work_once()
        return

    slots = [s for s in day_slots(ws) if s > start]
    do_now = args.reason == "manual" or start >= ws  # опоздали к началу или нажали кнопку — собираем сразу
    while True:
        now = datetime.now(MSK)
        if now >= we:
            print("рабочий день закончился", file=sys.stderr)
            return
        if now >= deadline:
            # 6-часовой предел GitHub: запускаем продолжение и завершаемся
            gh_api("POST", f"actions/workflows/{WORKFLOW}/dispatches", {"ref": "main", "inputs": {"reason": "chain"}})
            print("запущено продолжение", file=sys.stderr)
            return
        if slots and now >= slots[0]:  # подошло время по расписанию
            do_now = True
            slots = [s for s in slots if s > now]
        manual = pending_manual_runs()
        if do_now or manual:
            work_once()
            for run_id in set(manual + pending_manual_runs()):  # ручные запросы выполнены — убираем из очереди
                try:
                    gh_api("POST", f"actions/runs/{run_id}/cancel")
                except Exception:
                    pass
            do_now = False
            continue
        time.sleep(20)


def cmd_report(args):
    write_report(open_db(), args.out)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--pages", type=int, default=4, help="сколько страниц по 20 постов смотреть")
    c.add_argument("--no-save", action="store_true", help="не записывать data/obs.csv")
    u = sub.add_parser("update")
    u.add_argument("--pages", type=int, default=2)
    u.add_argument("--json", action="store_true", help="вывести текст уведомления в JSON (для приложения)")
    t = sub.add_parser("telegram")
    t.add_argument("--alerts", action="store_true", help="писать, только если где-то появился нужный бензин")
    wk = sub.add_parser("worker", help="непрерывная работа на GitHub Actions (12–24 МСК)")
    wk.add_argument("--reason", default="manual", help="manual | chain | schedule")
    wk.add_argument("--max-minutes", type=int, default=345, help="сколько работать до запуска продолжения")
    r = sub.add_parser("report")
    r.add_argument("--out", help="куда записать страницу (по умолчанию report.html)")
    sub.add_parser("status")
    args = parser.parse_args()
    {"collect": cmd_collect, "status": cmd_status, "telegram": cmd_telegram, "update": cmd_update,
     "report": cmd_report, "worker": cmd_worker}[args.cmd](args)


if __name__ == "__main__":
    main()
