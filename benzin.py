#!/usr/bin/env python3
"""Сбор отчётов о топливе из публичного канала @voronezh_benzin.

Канал читается через открытую веб-версию t.me/s/... — аккаунт и ключи не нужны.
Канал удаляет старые посты (примерно через 1–2 часа), поэтому история копится,
только пока сборщик регулярно запускается: на GitHub Actions с 7:00 до 24:00 МСК каждые 10 минут
(.github/workflows/collect.yml, команда worker). Данные хранятся в data/obs.csv в этом репозитории.

Заправки определяются по адресу из канала. Каждый пользователь бота выбирает до 8 своих заправок
(/stations): по ним приходят оповещения и строится его страница со сводкой.

Команды:
  collect             забрать свежие посты и дописать наблюдения в data/obs.csv
  status              последнее известное состояние на заправках по умолчанию
  telegram            отправить полную сводку владельцу бота
  telegram --alerts   написать в Telegram, если заправка перешла в статус «есть»
  update              collect + status + страница report.html (--json — для приложения на Mac)
  report --out PATH   только записать страницу со сводкой
  worker              непрерывная работа на GitHub Actions: сбор с 7:00 до 24:00 МСК
"""
import argparse
import csv
import hashlib
import html
import json
import os
import re
import sqlite3
import subprocess
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
TG_ALERTS_FILE = BASE / "data/telegram_alerts.json"  # последнее известное состояние заправок (для оповещений)
PAGE_URL = "https://dooodoivan.github.io/benzin-page/"  # сводка, которую публикует GitHub Actions
RUN_URL = "https://github.com/dooodoIvan/benzin/actions/workflows/collect.yml"  # ручной запуск сбора (Run workflow)
REPORT_PATH = BASE / "report.html"  # локальный файл, в git не попадает

# Заправки владельца по умолчанию: шаблон адреса (как пишет канал) → короткое имя
ALIASES = [
    (r"бабяково.*транспортн|транспортн.*бабяково", "Бабяково"),
    (r"ленинский проспект,\s*182(?![\dа-я])", "Ленинский 182"),
    (r"землячки,\s*7\s*а(?![\dа-я])", "Землячки 7А"),
    (r"новая усмань.*дорожная улица,\s*31(?![\dа-я])", "Дорожная 31"),
    (r"новая усмань.*дорожная улица,\s*101(?![\dа-я])", "Дорожная 101"),
    (r"ленинский проспект,\s*154\s*а(?![\dа-я])", "Ленинский 154А"),
]
MAX_STATIONS = 8  # столько цветов хорошо различимы на графиках
CATALOG_DAYS = 30  # в списке для выбора — заправки, о которых канал писал за последние 30 дней
FUELS = ["95", "98"]  # интересующие марки (95+ / Pulsar не учитываем)
FRESH = timedelta(hours=24)  # старше — считаем «нет свежих данных»
CONFIRM_FRESH = timedelta(hours=2)  # «есть» старше 2 часов показываем жёлтым «?»
ALERT_COOLDOWN = timedelta(hours=1)  # защита от «мигания» есть/нет: по одной заправке не чаще раза в час
KIND_RU = {"report": "водитель", "summary": "сводка канала", "signal": "терминалы оплаты, не подтверждено"}


def status_word(avail, kind, seen_at=None, now=None):
    """Текстовый статус (Telegram, консоль)."""
    if kind == "signal":
        return "❓ по терминалу" if avail else "нет (по терминалу)"
    if avail and seen_at and now and now - seen_at > CONFIRM_FRESH:
        return "было «есть» (больше 2 ч назад)"
    return "есть" if avail else "нет"


def status_html(avail, kind, seen_at, now):
    """Статус для страницы: «есть» — зелёным; жёлтый «?» — «есть» подтверждали больше 2 ч назад;
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


OBS_FIELDS = ["seen_at", "address", "fuel", "available", "status", "kind", "queue", "post_id", "brand"]


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
            brand     TEXT,                 -- сеть АЗС (Роснефть, Лукойл…), если канал её указал
            UNIQUE (seen_at, address, fuel, available, kind)
        )""")
    db.execute("CREATE INDEX obs_addr ON obs(address, fuel, seen_at)")
    if OBS_CSV.exists():
        with OBS_CSV.open(encoding="utf-8", newline="") as f:
            rows = [(r["seen_at"], r["address"], r["fuel"], int(r["available"]), r["status"], r["kind"],
                     r["queue"] or None, int(r["post_id"]), r.get("brand") or None) for r in csv.DictReader(f)]
        db.executemany("INSERT OR IGNORE INTO obs VALUES (?,?,?,?,?,?,?,?,?)", rows)
    return db


def save_db(db):
    """Пишет все наблюдения в CSV в стабильном порядке — так изменения в git остаются маленькими."""
    OBS_CSV.parent.mkdir(parents=True, exist_ok=True)
    tmp = OBS_CSV.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(OBS_FIELDS)
        w.writerows(db.execute(f"SELECT {', '.join(OBS_FIELDS)} FROM obs ORDER BY seen_at, address, fuel, kind, available"))
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


def clean_brand(b):
    b = re.sub(r"[☀-➿\U0001f000-\U0001faff️]", "", b or "").strip(" —-")
    return b or None


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

    def add(seen_at, address, pairs, kind, queue, brand):
        for fuel, status in pairs:
            avail = is_available(status)
            if avail is not None:
                obs.append((seen_at, clean_addr(address), fuel, avail, status, kind, queue, post_id, brand))

    # 1. Отчёт по одной АЗС: «⛽ Сеть», «📍 адрес», «🕒 Обновлено в HH:MM», строки «🟢 95: есть», «🚗 …»
    addr = next((l.lstrip("📍 ").strip() for l in lines if l.startswith("📍")), None)
    if addr:
        brand = clean_brand(next((l for l in lines if l.startswith("⛽") and "Воронеж:" not in l), None))
        m_upd = re.search(r"Обновлено в (\d{1,2}:\d{2})", text)
        seen_at = stamp(m_upd.group(1), post_ts) if m_upd else post_ts.astimezone(MSK).isoformat()
        queue = next((l.lstrip("🚗 ").strip() for l in lines if l.startswith("🚗")), None)
        pairs = []
        for l in lines:
            m = re.match(r"^[🟢🔴🟡🟠⚪]\s*([\w+]+)\s*:\s*(.+)$", l)
            if m:
                pairs.append((m.group(1), m.group(2)))
        add(seen_at, addr, pairs, "report", queue, brand)
        return obs

    # 2. Часовая сводка: заголовок сети «Роснефть — 4», затем «✅ 13:23 · адрес ↗» + строка с топливом
    # 3. Сигналы терминалов: «🟡 Роснефть — 1», затем «• 13:07 · адрес» + строка с топливом
    brand = None
    for i, l in enumerate(lines[:-1]):
        m_brand = re.match(r"^(?:🟡\s*)?([^\d✅•⛽🕒👇👉‼].*?)\s+[—–]\s+\d+$", l)
        if m_brand:
            brand = clean_brand(m_brand.group(1))
            continue
        m = re.match(r"^(✅|•)\s*(\d{1,2}:\d{2})\s*·\s*(.+)$", l)
        if m:
            pairs, queue = parse_fuel_groups(lines[i + 1])
            kind = "summary" if m.group(1) == "✅" else "signal"
            add(stamp(m.group(2), post_ts), m.group(3), pairs, kind, queue, brand)
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
                added = db.execute("INSERT OR IGNORE INTO obs VALUES (?,?,?,?,?,?,?,?,?)", o).rowcount
                new_obs += added
                if not added and o[8]:  # дописать сеть к уже известному наблюдению
                    db.execute("UPDATE obs SET brand = ? WHERE seen_at = ? AND address = ? AND fuel = ? "
                               "AND available = ? AND kind = ? AND brand IS NULL", (o[8], o[0], o[1], o[2], o[3], o[5]))
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


# ---------- справочник заправок ----------
#
# Заправка = адрес из канала. Её код (sid) — 6 знаков хеша адреса: он короткий, подходит для кнопок бота
# и ссылок на страницу (?s=код,код,…) и не меняется, пока канал пишет адрес одинаково.

REG = {}  # sid → {"address", "brand", "short", "name", "alias"}


def station_id(address):
    return hashlib.sha1(address.strip().lower().encode()).hexdigest()[:6]


_SID_CACHE = {}


def station_of(address):
    sid = _SID_CACHE.get(address)
    if sid is None:
        sid = _SID_CACHE[address] = station_id(address)
    return sid


def short_address(address):
    a = address
    for prefix in ("городской округ Воронеж, ", "Воронежская область, ", "Воронеж, "):
        if a.startswith(prefix):
            a = a[len(prefix):]
    for old, new in (("рабочий посёлок ", "рп "), ("посёлок ", "пос. "), ("село ", "с. "), ("хутор ", "х. "),
                     ("сельское поселение", "с/п"), ("район", "р-н"), ("улица ", "ул. "), (" улица", " ул."),
                     ("проспект", "пр-т"), ("переулок", "пер."), ("набережная", "наб."), ("-й километр", " км"),
                     ("шоссе", "ш.")):
        a = a.replace(old, new)
    parts = a.split(", ")
    if len(parts) > 2:  # за городом: район и поселение лишние, если есть улица или трасса
        parts = [x for x in parts if not re.search(r"р-н|с/п|городской округ", x)] or parts
    return ", ".join(parts[-3:])


def chart_label(sid, limit=15):
    """Подпись строки на графиках: короткое имя или «улица, дом», не длиннее limit знаков."""
    s = REG[sid]
    if s["alias"]:
        return s["alias"]
    parts = [re.sub(r"^ул\. | ул\.$", "", x) for x in s["short"].split(", ")[-2:]]
    if len(parts) == 2 and re.match(r"^\d", parts[1]):  # «улица, дом»: сокращаем улицу, номер дома оставляем
        street, num = parts
        room = limit - len(num) - 1
        return f"{street if len(street) <= room else street[:room - 1] + '…'} {num}"
    text = ", ".join(parts)
    return text if len(text) <= limit else text[:limit - 1] + "…"


def load_registry(db):
    """Справочник заправок, о которых канал писал за последние CATALOG_DAYS дней (+ заправки по умолчанию)."""
    since = (datetime.now(MSK) - timedelta(days=CATALOG_DAYS)).isoformat()
    brands, seen = {}, set()
    for address, brand, n in db.execute(
            "SELECT address, brand, COUNT(*) FROM obs WHERE seen_at >= ? GROUP BY address, brand", (since,)):
        seen.add(address)
        if brand:
            brands.setdefault(address, {})[brand] = n
    REG.clear()
    for address in seen:
        brand = max(brands[address], key=brands[address].get) if address in brands else None
        alias = next((name for pattern, name in ALIASES if re.search(pattern, address.lower())), None)
        short = short_address(address)
        REG[station_of(address)] = {"address": address, "brand": brand, "alias": alias,
                                    "short": alias or short, "name": f"{brand}, {short}" if brand else short}
    return REG


def default_sids():
    """Заправки владельца по умолчанию — в порядке ALIASES (только те, о которых канал уже писал)."""
    by_alias = {s["alias"]: sid for sid, s in REG.items() if s["alias"]}
    return [by_alias[name] for _, name in ALIASES if name in by_alias]


def clean_selection(sids):
    return [sid for sid in (sids or []) if sid in REG][:MAX_STATIONS]


# ---------- состояние заправок ----------

def latest_state(db, sids):
    """→ {sid: {топливо: (seen_at, available, status, queue, kind)}} — последнее по каждому топливу."""
    latest = {sid: {} for sid in sids}
    since = (datetime.now(MSK) - FRESH - timedelta(days=1)).isoformat()
    for sid in sids:
        address = REG.get(sid, {}).get("address")
        if not address:
            continue
        for seen_at, fuel, avail, status, kind, queue in db.execute(
                "SELECT seen_at, fuel, available, status, kind, queue FROM obs WHERE address = ? AND seen_at >= ? "
                "ORDER BY seen_at", (address, since)):
            if fuel in FUELS:
                latest[sid][fuel] = (datetime.fromisoformat(seen_at), avail, status, queue, kind)
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


def ordered_stations(db, now, sids):
    """Заправки по порядку: где бензин есть (свежее выше) → было давно → терминал → нет → без данных."""
    rows = [(sid, fuels, station_state(fuels, now)) for sid, fuels in latest_state(db, sids).items()]
    return sorted(rows, key=lambda r: (r[2][2], -(r[2][3].timestamp() if r[2][3] else 0)))


def cmd_status(args, db=None):
    db = db or open_db()
    load_registry(db)
    now = datetime.now(MSK)
    for sid, fuels in latest_state(db, default_sids()).items():
        name = REG[sid]["name"]
        if not fuels:
            print(f"{name}: данных пока нет")
            continue
        print(name)
        for fuel in FUELS:
            if fuel in fuels:
                seen_at, avail, status, queue, kind = fuels[fuel]
                mark = "🟡" if kind == "signal" else ("🟢" if avail else "🔴")
                print(f"  {mark} АИ-{fuel}: {status_word(avail, kind, seen_at, now)} — {seen_at:%d.%m %H:%M} ({KIND_RU[kind]})"
                      + (f", очередь {queue}" if queue else ""))


def notify_text(db, sids=None):
    """→ (заголовок, текст): заправки сгруппированы по состоянию, чтобы влезть в несколько строк уведомления."""
    now = datetime.now(MSK)
    if not REG:
        load_registry(db)
    sids = sids or default_sids()
    groups = {"have": [], "stale": [], "term": [], "none": [], "unknown": []}
    for sid, fuels, (cls, label, _, when) in ordered_stations(db, now, sids):
        short = REG[sid]["short"]
        groups[cls].append(f"{short} ({when:%H:%M})" if when else short)
    heads = {"have": "✅ Есть", "stale": "🟡 Было давно", "term": "❓ Терминал", "none": "❌ Нет", "unknown": "⚪ Нет данных"}
    lines = [f"{heads[c]}: " + ", ".join(v) for c, v in groups.items() if v]
    title = f"⛽ АИ-95/98 · {now:%H:%M} · есть на {len(groups['have'])} из {len(sids)}"
    return title, "\n".join(lines)


# ---------- Telegram ----------

def telegram_text(db, sids):
    """Подробная сводка для Telegram (HTML-разметка)."""
    now = datetime.now(MSK)
    title, _ = notify_text(db, sids)
    icons = {"have": "✅", "stale": "🟡", "term": "❓", "none": "❌", "unknown": "⚪"}
    parts = [f"<b>{html.escape(title)}</b>"]
    for sid, fuels, (cls, label, _, _) in ordered_stations(db, now, sids):
        lines = [f"{icons[cls]} <b>{html.escape(REG[sid]['name'])}</b> — {label}"]
        for fuel in FUELS:
            if fuel in fuels and now - fuels[fuel][0] <= FRESH:
                seen_at, avail, _, queue, kind = fuels[fuel]
                lines.append(f"   АИ-{fuel}: {status_word(avail, kind, seen_at, now)}, {seen_at:%H:%M} ({KIND_RU[kind]})"
                             + (f", очередь {html.escape(queue)}" if queue else ""))
        forecast = short_forecast(db, sid, now)
        if forecast:
            lines.append(f"   📊 {forecast}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def tg_call(token, method, timeout=30, **params):
    data = urllib.parse.urlencode(params).encode() if params else None
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/{method}", data=data)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def page_url(sids=None):
    """Адрес сводки со своими заправками: ?s=код,код,…"""
    return PAGE_URL + (f"?s={','.join(sids)}" if sids else "")


def page_button(now, sids=None):
    """Кнопка под сообщением: открывает сводку внутри Telegram (t= — чтобы не показывалась старая копия)."""
    url = page_url(sids) + ("&" if sids else "?") + f"t={now:%m%d%H%M}"
    return json.dumps({"inline_keyboard": [[{"text": "⛽ Открыть сводку", "web_app": {"url": url}}]]})


def set_menu_button(token, chat_id, sids=None):
    """Постоянная кнопка «⛽ Сводка» рядом с полем ввода — открывает сводку со своими заправками."""
    tg_call(token, "setChatMenuButton", chat_id=chat_id, menu_button=json.dumps(
        {"type": "web_app", "text": "⛽ Сводка", "web_app": {"url": page_url(sids)}}))


def tg_chat_id(token):
    """Чат владельца: секрет TELEGRAM_CHAT_ID (на Mac — локальный файл data/telegram_chat.txt)."""
    if os.environ.get("TELEGRAM_CHAT_ID"):
        return os.environ["TELEGRAM_CHAT_ID"]
    if TG_CHAT_FILE.exists():
        return TG_CHAT_FILE.read_text().strip()
    return None


def available_now(fuels, now):
    """Марки, которые водители или сводка канала подтвердили как «есть» не больше 2 часов назад."""
    return {f: v for f, v in fuels.items()
            if v[1] and v[4] != "signal" and now - v[0] <= CONFIRM_FRESH}


def alert_text(db, items, now):
    lines = ["<b>⛽ Появился бензин</b>"]
    for sid, fuels in items:
        parts = []
        for fuel in FUELS:
            if fuel in fuels:
                seen_at, _, _, queue, kind = fuels[fuel]
                parts.append(f"АИ-{fuel} есть ({seen_at:%H:%M}, {KIND_RU[kind]}"
                             + (f", очередь {html.escape(queue)}" if queue else "") + ")")
        lines.append(f"\n✅ <b>{html.escape(REG[sid]['name'])}</b>\n   " + "; ".join(parts))
        st = station_stats(db, sid, now)
        if st and median_duration(st["durations"]):
            lines.append(f"   📊 обычно держится около {median_duration(st['durations'])}")
    return "\n".join(lines)


def all_selections():
    """→ [(chat_id, [sid…])] для владельца и подписчиков (если ключ шифрования доступен)."""
    owner = owner_id()
    try:
        subs = load_subs() if os.environ.get("SUBSCRIBERS_KEY") else empty_subs()
    except Exception as e:
        print(f"подписчики недоступны: {e}", file=sys.stderr)
        subs = empty_subs()
    out = [(owner, selection(subs, owner))] if owner else []
    out += [(cid, selection(subs, cid)) for cid in subs["subscribers"] if cid != owner]
    return out


def cmd_telegram(args):
    token = os.environ.get("TELEGRAM_TOKEN")
    if not token:
        sys.exit("Не задан TELEGRAM_TOKEN")
    owner = tg_chat_id(token)
    if not owner:
        sys.exit("Не задан TELEGRAM_CHAT_ID")
    db = open_db()
    load_registry(db)
    now = datetime.now(MSK)
    if not args.alerts:
        sids = selection(load_subs() if os.environ.get("SUBSCRIBERS_KEY") else empty_subs(), owner)
        tg_call(token, "sendMessage", chat_id=owner, text=telegram_text(db, sids), parse_mode="HTML",
                disable_web_page_preview="true", reply_markup=page_button(now, sids))
        print("сводка отправлена в Telegram", file=sys.stderr)
        return

    people = all_selections()
    watched = sorted({sid for _, sids in people for sid in sids})
    state = json.loads(TG_ALERTS_FILE.read_text()) if TG_ALERTS_FILE.exists() else {}
    silent = state.get("_v") != 2  # первый запуск нового формата: только запоминаем, без оповещений
    if silent:
        state = {"_v": 2}
    latest = latest_state(db, watched)
    appeared = []
    for sid in watched:
        cls = station_state(latest[sid], now)[0]
        prev = state.get(sid, {})
        last_alert = datetime.fromisoformat(prev["last_alert"]) if prev.get("last_alert") else None
        # пишем только при переходе в «есть»; «было давно» → «есть» — бензин не пропадал, молчим
        if (not silent and cls == "have" and prev.get("state") not in ("have", "stale", None)
                and (not last_alert or now - last_alert >= ALERT_COOLDOWN)):
            appeared.append(sid)
            prev["last_alert"] = now.isoformat()
        prev["state"] = cls
        state[sid] = prev
    for cid, sids in people:
        mine = [sid for sid in sids if sid in appeared]
        if not mine:
            continue
        try:
            tg_call(token, "sendMessage", chat_id=cid, parse_mode="HTML", disable_web_page_preview="true",
                    text=alert_text(db, [(sid, available_now(latest[sid], now)) for sid in mine], now),
                    reply_markup=page_button(now, sids))
        except Exception as e:  # например, подписчик заблокировал бота
            print(f"не удалось отправить …{cid[-4:]}: {e}", file=sys.stderr)
    print(("оповещение: " + ", ".join(REG[s]["short"] for s in appeared)) if appeared else "новых появлений нет",
          file=sys.stderr)
    TG_ALERTS_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=1))


# ---------- статистика: интервалы наличия, появления и окончания ----------

STATS_DAYS = 30              # за сколько дней считать статистику
MAX_GAP = timedelta(hours=2)  # сколько считаем состояние верным после последнего отчёта
EVENT_GAP = timedelta(hours=12)  # смена «нет→есть» засчитывается, если между состояниями не дольше
SIGNAL_CONFLICT = timedelta(minutes=60)


def station_series(db, sid, fuel, since):
    """Отчёты по одной заправке и марке → [(время, есть?, источник)], по времени.
    Сигналы терминалов отбрасываем, если рядом (±1 ч) есть отчёт водителя или сводка: они точнее."""
    address = REG.get(sid, {}).get("address")
    raw = [(datetime.fromisoformat(t), a, k) for t, a, k in db.execute(
        "SELECT seen_at, available, kind FROM obs WHERE address = ? AND fuel = ? AND seen_at >= ? ORDER BY seen_at",
        (address, fuel, since.isoformat()))]
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


def station_timeline(db, sid, now):
    """Наличие «нужного бензина» (АИ-95 или 98) на заправке во времени →
    [(начало, конец, есть?, только_терминалы?)]. Есть, если есть хоть одна из марок; нет — если все известные «нет»."""
    since = now - timedelta(days=STATS_DAYS)
    per_fuel = [intervals(station_series(db, sid, f, since), now) for f in FUELS]
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


def station_stats(db, sid, now):
    timeline = station_timeline(db, sid, now)
    if not timeline:
        return None
    arrivals, runouts, durations = events(timeline)
    return {"timeline": timeline, "arrivals": arrivals, "runouts": runouts, "durations": durations,
            "hourly": hourly_share(timeline), "since": timeline[0][0]}


def median_duration(durations):
    if len(durations) < 2:
        return None
    return fmt_hours(sorted(durations)[len(durations) // 2])


def short_forecast(db, sid, now):
    """Одна строка для Telegram: когда обычно появляется/заканчивается бензин (если данных достаточно)."""
    st = station_stats(db, sid, now)
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


# ---------- графики (SVG) ----------
#
# Страница одна на всех: в ней есть все заправки, выбранные хоть кем-то. Скрипт страницы показывает
# только заправки из ссылки (?s=код,код,…) и раскрашивает их по порядку выбора (до 8 цветов).
# Поэтому каждая заправка — отдельная строка (data-sid), а её цвет задаётся переменной --c.

W = 400  # ширина графика в единицах viewBox — рассчитано на телефон; на компьютере ширина ограничена в CSS
DAYS_RU = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]


def tip(text):
    return f'data-tip="{html.escape(text, quote=True)}" tabindex="0"'


def row_attrs(sid, color, visible):
    return f'data-sid="{sid}" style="--c:var(--s{color})"' + ("" if visible else ' class="nosel"')


def first_hour(stats, default=7):
    """С какого часа показывать графики: с первого часа (не раньше 6:00), когда были данные, но не позже начала сбора."""
    hours = [h for st in stats.values() if st for h in range(6, 24) if sum(st["hourly"][h]) >= 1]
    return min(hours + [default])


def heat_rows(stats, colors, visible, h0):
    """Тепловая карта: строка на заправку, клетка на час. Чем ярче клетка, тем чаще в этот час бензин был."""
    n = 24 - h0
    x0, x1, row = 112, W - 6, 22
    cw = (x1 - x0) / n
    out = []
    for sid, st in stats.items():
        short = chart_label(sid)
        cells = []
        for h in range(h0, 24):
            x = x0 + cw * (h - h0)
            have, none = st["hourly"][h] if st else (0, 0)
            label = f"{short} · {h:02d}:00–{(h + 1) % 24:02d}:00 · "
            if have + none < 1:
                cells.append(f'<rect x="{x + 1:.1f}" y="1" width="{cw - 2:.1f}" height="{row - 2}" rx="3" class="track" '
                             f'{tip(label + "нет данных")}/>')
            else:
                share = have / (have + none)
                cells.append(f'<rect x="{x + 1:.1f}" y="1" width="{cw - 2:.1f}" height="{row - 2}" rx="3" class="cell" '
                             f'style="fill-opacity:{0.12 + 0.88 * share:.2f}" {tip(label + f"бензин был {share:.0%} времени")}/>')
        out.append(f'<svg viewBox="0 0 {W} {row}" class="chart" {row_attrs(sid, colors[sid], visible[sid])}>'
                   f'<text x="{x0 - 6}" y="{row / 2 + 4:.1f}" class="tick label" text-anchor="end">{html.escape(short)}</text>'
                   + "".join(cells) + "</svg>")
    axis = "".join(f'<text x="{x0 + cw * (h - h0):.1f}" y="13" class="tick" text-anchor="middle">{h}</text>'
                   for h in range(h0, 25, 2 if n <= 14 else 3))
    return f'<div class="rows">{"".join(out)}</div><svg viewBox="0 0 {W} 18" class="chart">{axis}</svg>'


def week_rows(stats, colors, visible, now, h0):
    """Последние 7 дней (только часы сбора): строка на заправку.
    Цвет заправки — есть, бледный — возможно (терминалы), серый — нет, пусто — нет данных."""
    x0, x1, row, gap = 112, W - 6, 14, 6
    start = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    dayw = (x1 - x0) / 7

    def xt(t):  # время → x: в каждом дне показываем только часы с h0 до 24
        d = (t - start).days
        frac = (t - (start + timedelta(days=d))).total_seconds() / 3600
        return x0 + dayw * (d + min(max((frac - h0) / (24 - h0), 0), 1))

    head = "".join(f'<text x="{x0 + dayw * d + dayw / 2:.1f}" y="12" class="tick" text-anchor="middle">'
                   f'{DAYS_RU[(start + timedelta(days=d)).weekday()]} {(start + timedelta(days=d)):%d}</text>' for d in range(7))
    grid = "".join(f'<line x1="{x0 + dayw * d:.1f}" x2="{x0 + dayw * d:.1f}" y1="0" y2="{row + gap}" class="grid"/>'
                   for d in range(8))
    out = []
    for sid, st in stats.items():
        short = chart_label(sid)
        segs = []
        for s, e, a, weak in (st["timeline"] if st else []):
            s, e = max(s, start), min(e, now)
            if e <= s:
                continue
            xs_, xe = xt(s), xt(e)
            if xe - xs_ < 0.3:
                continue  # отрезок целиком в часах без сбора
            cls = ("on" + (" weak" if weak else "")) if a else "off"
            word = ("возможно есть (терминалы)" if weak else "есть") if a else ("возможно нет (терминалы)" if weak else "нет")
            segs.append(f'<rect x="{xs_ + 0.5:.1f}" y="{gap / 2}" width="{max(xe - xs_ - 1, 1.2):.1f}" height="{row}" rx="2" '
                        f'class="seg {cls}" {tip(f"{short} · {s:%d.%m %H:%M}–{e:%H:%M} · {word}")}/>')
        out.append(f'<svg viewBox="0 0 {W} {row + gap}" class="chart" {row_attrs(sid, colors[sid], visible[sid])}>{grid}'
                   f'<text x="{x0 - 6}" y="{gap / 2 + 11}" class="tick label" text-anchor="end">{html.escape(short)}</text>'
                   f'<rect x="{x0}" y="{gap / 2}" width="{x1 - x0}" height="{row}" rx="3" class="track"/>' + "".join(segs) + "</svg>")
    return f'<svg viewBox="0 0 {W} 16" class="chart">{head}</svg><div class="rows">{"".join(out)}</div>'


def events_list(stats, colors, visible, now, limit=40):
    """Последние случаи, когда бензин появлялся и заканчивался, — простым списком (скрипт оставит нужные)."""
    items = []
    for sid, st in stats.items():
        if st:
            items += [(t, sid, "появился") for t in st["arrivals"]]
            items += [(t, sid, "закончился") for t in st["runouts"]]
    items = sorted((e for e in items if now - e[0] <= timedelta(days=7)), reverse=True)[:limit]
    lis = "".join(
        f'<li {row_attrs(sid, colors[sid], visible[sid])}><b>{t:%d.%m %H:%M}</b> <i class="k sw"></i>'
        f'{html.escape(REG[sid]["short"])} — <span class="{"ev-on" if what == "появился" else "ev-off"}">бензин {what}</span></li>'
        for t, sid, what in items)
    return (f'<ul class="events">{lis}</ul><p class="muted ev-empty" hidden>Пока не было ни одного случая, '
            f'когда бензин появился или закончился: нужно больше данных.</p>')


def stats_section(db, now, sids, shown):
    stats = {sid: station_stats(db, sid, now) for sid in sids}
    colors = {sid: (shown.index(sid) if sid in shown else i) % MAX_STATIONS + 1 for i, sid in enumerate(sids)}
    visible = {sid: sid in shown for sid in sids}
    h0 = first_hour(stats)
    esc = html.escape
    legend = "".join(f'<span {row_attrs(sid, colors[sid], visible[sid])}><i class="k sw"></i>{esc(REG[sid]["short"])}</span>'
                     for sid in sids)
    since = min((st["since"] for st in stats.values() if st), default=now)
    note = ""
    if now - since < timedelta(days=7):
        note = f'<p class="note">Данные собираются с {since:%d.%m}. Выводы станут надёжными примерно через 1–2 недели.</p>'

    def cell(times):
        if len(times) >= 3:
            h, k = busiest_window(times)
            return f'{hours_range(h)}<small>{k} из {len(times)} случаев</small>'
        if not times:
            return '<span class="muted">—</span>'
        return " ".join(f"{t:%H:%M}" for t in sorted(times)[-2:]) + '<small>мало данных</small>'

    rows, hour_rows = [], []
    for sid in sids:
        st, attrs = stats[sid], row_attrs(sid, colors[sid], visible[sid])
        name_cell = f'<td><i class="k sw"></i>{esc(REG[sid]["short"])}</td>'
        if not st:
            rows.append(f'<tr {attrs}>{name_cell}<td colspan="3" class="muted">данных пока нет</td></tr>')
        else:
            rows.append(f'<tr {attrs}>{name_cell}<td>{cell(st["arrivals"])}</td><td>{cell(st["runouts"])}</td>'
                        f'<td>{median_duration(st["durations"]) or "—"}</td></tr>')
        hour_rows.append(f'<tr {attrs}>{name_cell}' + "".join(
            (f"<td>{st['hourly'][h][0] / sum(st['hourly'][h]):.0%}</td>" if st and sum(st["hourly"][h]) >= 1 else "<td>—</td>")
            for h in range(h0, 24)) + "</tr>")
    return f"""
<section class="card stats">
  <div class="legend stations rows">{legend}</div>{note}
  <table class="tbl est"><thead><tr><th>Заправка</th><th>Привозят</th><th>Кончается</th><th>Держится</th></tr></thead>
  <tbody class="rows">{"".join(rows)}</tbody></table>
  <h4>Когда обычно есть бензин</h4>
  <div class="legend"><span>чем ярче клетка, тем чаще в этот час бензин был</span><span><i class="k track"></i>нет данных</span></div>
  {heat_rows(stats, colors, visible, h0)}
  <h4>Последние появления и окончания</h4>{events_list(stats, colors, visible, now)}
  <h4>Последние 7 дней, {h0}:00–24:00</h4>
  <div class="legend"><span><i class="k sample"></i>есть (цвет заправки)</span><span><i class="k sample weak"></i>возможно (терминалы)</span><span><i class="k off"></i>нет</span><span><i class="k track"></i>нет данных</span></div>
  {week_rows(stats, colors, visible, now, h0)}
  <details><summary>Таблица по часам: доля времени, когда бензин есть</summary>
  <div class="scroll"><table class="tbl hours"><thead><tr><th>Заправка</th>{"".join(f"<th>{h}</th>" for h in range(h0, 24))}</tr></thead>
  <tbody class="rows">{"".join(hour_rows)}</tbody></table></div></details>
</section>"""


# ---------- страница со сводкой ----------

def write_report(db, path=None, sids=None):
    """sids — все заправки, которые должны быть на странице (по умолчанию — заправки владельца).
    Без параметра ?s= в ссылке показываются заправки по умолчанию."""
    now = datetime.now(MSK)
    esc = html.escape
    load_registry(db)
    shown = default_sids()
    sids = [sid for sid in dict.fromkeys(list(sids or []) + shown) if sid in REG]
    since = (now - FRESH).isoformat()
    cards = []
    for sid, fuels, (cls, label, _, _) in ordered_stations(db, now, sids):
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
                 + "".join(rows) + "</table>") if rows else '<p class="muted">За последние сутки отчётов по этой заправке не было.</p>'
        hist = []
        for seen_at, fuel, avail, kind in db.execute(
                "SELECT seen_at, fuel, available, kind FROM obs WHERE address = ? AND seen_at >= ? ORDER BY seen_at DESC",
                (REG[sid]["address"], since)):
            if fuel in FUELS:
                t = datetime.fromisoformat(seen_at)
                hist.append(f'<li><b>{t:%H:%M}</b> АИ-{esc(fuel)} — '
                            f'{status_html(avail, kind, t, now)} <span class="muted">({KIND_RU[kind]})</span></li>')
        hist_html = (f'<details><summary>Все отчёты за сутки ({len(hist)})</summary><ul>{"".join(hist[:60])}</ul></details>'
                     if hist else "")
        cards.append(f'<section class="card{"" if sid in shown else " nosel"}" data-sid="{sid}"><div class="head">'
                     f'<h2>{esc(REG[sid]["name"])}</h2><span class="badge {cls}">{label}</span></div>{table}{hist_html}</section>')
    palette_light = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
    palette_dark = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
    s_light = " ".join(f"--s{i + 1}:{c};" for i, c in enumerate(palette_light))
    s_dark = " ".join(f"--s{i + 1}:{c};" for i, c in enumerate(palette_dark))
    page = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex, nofollow">
<title>Бензин · АИ-95/98</title>
<style>
:root {{ color-scheme: light; --bg:#f9f9f7; --card:#fcfcfb; --text:#0b0b0b; --text2:#52514e; --muted:#898781;
  --line:#e1e0d9; --axis:#c3c2b7; --yes:#2a78d6; --track:#efeee9;
  --have:#0ca30c; --maybe:#b7791f; --none:#d03b3b; --unknown:#898781; --off:#c3c2b7; {s_light} }}
@media (prefers-color-scheme: dark) {{ :root {{ color-scheme: dark; --bg:#0d0d0d; --card:#1a1a19; --text:#fff; --text2:#c3c2b7;
  --line:#2c2c2a; --axis:#383835; --yes:#3987e5; --track:#262624; --off:#55544f; {s_dark} }} }}
body {{ margin:0; background:var(--bg); color:var(--text); font:15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ max-width:780px; margin:0 auto; padding:24px 16px 48px; }}
h1 {{ font-size:22px; margin:0 0 4px; }} h2 {{ font-size:17px; margin:0; }}
h4 {{ font-size:13px; font-weight:600; color:var(--text2); margin:14px 0 2px; }}
.muted {{ color:var(--muted); }} .note {{ color:var(--text2); font-size:13px; margin:4px 0; }}
.nosel {{ display:none !important; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:14px; padding:16px; margin-top:14px; }}
.head {{ display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; }}
.badge {{ font-size:13px; font-weight:600; padding:3px 10px; border-radius:999px; color:#fff; white-space:nowrap; }}
.badge.have {{ background:var(--have); }} .badge.stale {{ background:var(--maybe); }}
.badge.term {{ background:transparent; color:var(--none); border:1.5px solid var(--none); }}
.badge.none {{ background:var(--none); }} .badge.unknown {{ background:var(--unknown); }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th {{ text-align:left; color:var(--muted); font-weight:500; padding:4px 6px; border-bottom:1px solid var(--line); }}
td {{ padding:6px; border-bottom:1px solid var(--line); font-variant-numeric: tabular-nums; }}
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
.legend {{ display:flex; flex-wrap:wrap; gap:6px 14px; font-size:13px; color:var(--text2); margin:8px 0 0; }}
.legend.stations {{ margin:0 0 10px; }}
.k {{ display:inline-block; width:12px; height:12px; border-radius:3px; margin-right:6px; vertical-align:-1px; }}
.k.sw {{ background:var(--c); }} .k.track {{ background:var(--track); border:1px solid var(--line); }}
.k.sample {{ background:linear-gradient(90deg, var(--s1) 33%, var(--s2) 33% 66%, var(--s3) 66%); }} .k.weak {{ opacity:.45; }}
.k.off {{ background:var(--off); }}
.chart {{ width:100%; height:auto; display:block; }} .stats .chart {{ max-width:560px; }} .scroll {{ overflow-x:auto; }}
.chart .grid {{ stroke:var(--line); stroke-width:1; }}
.chart .tick {{ fill:var(--muted); font-size:11px; font-variant-numeric: tabular-nums; }}
.chart .label {{ fill:var(--text2); font-size:11.5px; }}
.chart .track {{ fill:var(--track); }} .chart .cell, .chart .on {{ fill:var(--c); }} .chart .off {{ fill:var(--off); }}
.chart .weak {{ opacity:.45; }}
.chart .cell:hover, .chart .cell:focus, .chart .seg:hover, .chart .seg:focus {{ stroke:var(--text); stroke-width:1.5; outline:none; }}
.events {{ list-style:none; padding:0; margin:6px 0 0; }} .events li {{ padding:4px 0; border-bottom:1px solid var(--line); }}
.events .k {{ margin:0 6px 0 8px; width:10px; height:10px; }}
.ev-on {{ font-weight:600; }} .ev-off {{ color:var(--none); }}
.card > table.est {{ table-layout:auto; }} .card > table.est th {{ width:auto; font-size:12px; }}
.est td {{ vertical-align:top; }} .est td:first-child {{ width:34%; }} .est small {{ display:block; color:var(--muted); font-size:12px; }}
.hours td, .hours th {{ text-align:center; white-space:nowrap; }} .hours td:first-child, .hours th:first-child {{ text-align:left; }}
.st-q {{ color:var(--maybe); font-weight:800; font-size:1.1em; cursor:help; }}
.st-qr {{ color:var(--none); font-weight:800; font-size:1.1em; cursor:help; }} .st-no {{ color:var(--none); font-weight:600; }}
.st-yes {{ color:var(--have); font-weight:600; }}
.card > table:not(.tbl) {{ table-layout:fixed; }}
.card > table:not(.tbl) th:nth-child(1) {{ width:16%; }} .card > table:not(.tbl) th:nth-child(2) {{ width:14%; }}
.card > table:not(.tbl) th:nth-child(3) {{ width:20%; }} .card > table:not(.tbl) th:nth-child(4) {{ width:20%; }}
.tbl td, .tbl th {{ padding:3px 6px; font-size:13px; }}
#tip {{ position:fixed; pointer-events:none; background:var(--card); color:var(--text); border:1px solid var(--line);
  border-radius:8px; padding:6px 9px; font-size:13px; box-shadow:0 4px 14px rgba(0,0,0,.15); display:none; max-width:280px; z-index:10; white-space:pre-line; }}
@media (max-width:560px) {{ .card > table:not(.tbl) th:nth-child(5), .card > table:not(.tbl) td:nth-child(5) {{ display:none; }} }}
</style></head><body data-updated="{now.isoformat()}"><main>
<h1>⛽ Бензин · АИ-95 / 98</h1>
<div class="topbar">
  <div class="muted">Обновлено <b id="updated">{now:%d.%m.%Y в %H:%M}</b> <span id="ago"></span>.
  Данные из канала @voronezh_benzin. Сбор с 7:00 до 24:00 каждые 10 минут, ночью не ведётся. Бледные строки — старше суток.
  Свои заправки выбираются в боте командой /stations.</div>
  <a class="refresh" id="refresh" href="{RUN_URL}" target="_blank" rel="noopener">🔄 Обновить сейчас</a>
</div>
<div class="hint" id="hint" hidden>Нажмите <b>Run workflow</b> на GitHub. Примерно через 1–2 минуты эта страница обновится сама.
Если на GitHub запрос отметится как «Cancelled» — это нормально: сервер уже выполнил сбор.</div>
<h2 class="section">Сводка сейчас</h2>
{"".join(cards)}
<h2 class="section">Статистика: когда привозят и когда заканчивается</h2>
<div class="muted">Все выбранные заправки на общих графиках, у каждой свой цвет. «Бензин есть» — есть АИ-95 или АИ-98.
Наведите на график или нажмите на него, чтобы увидеть подробности.</div>
{stats_section(db, now, sids, shown)}
<p class="muted" style="margin-top:20px">«Водитель» — отчёт подписчика с заправки. «Сводка канала» — подтверждённые данные за последний час.
«Терминалы оплаты» — топливо продаётся по данным касс, но водители ещё не подтвердили; если рядом по времени есть отчёт водителя, в статистике учитывается он.
Состояние считается неизменным до следующего отчёта, но не дольше 2 часов; дальше — «нет данных».</p>
</main><div id="tip" role="tooltip"></div>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script>
// свои заправки из ссылки ?s=код,код,…: показываем только их и красим по порядку выбора
const sel = (new URLSearchParams(location.search).get('s') || '').split(',').filter(Boolean);
if (sel.length) {{
  document.querySelectorAll('[data-sid]').forEach(el => {{
    const i = sel.indexOf(el.dataset.sid);
    el.classList.toggle('nosel', i < 0);
    if (i >= 0) el.style.setProperty('--c', `var(--s${{i % {MAX_STATIONS} + 1}})`);
  }});
  document.querySelectorAll('.rows').forEach(box => {{
    [...box.children].filter(c => c.dataset.sid)
      .sort((a, b) => sel.indexOf(a.dataset.sid) - sel.indexOf(b.dataset.sid)).forEach(c => box.appendChild(c));
  }});
}}
document.querySelectorAll('.events').forEach(ul => {{
  const shown = [...ul.children].filter(li => !li.classList.contains('nosel'));
  shown.slice(12).forEach(li => li.classList.add('nosel'));
  if (!shown.length) ul.nextElementSibling.hidden = false;
}});
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


# ---------- подписчики бота и их заправки ----------
#
# Хранилище открытое, поэтому подписчики (их Telegram ID) и выбранные заправки лежат в data/subscribers.enc
# в зашифрованном виде (AES-256, ключ — секрет SUBSCRIBERS_KEY на GitHub).

SUBS_FILE = BASE / "data/subscribers.enc"
TG_OFFSET_FILE = BASE / "data/telegram_offset.txt"  # номер последнего обработанного сообщения боту
UNPUSHED = {"since": None}  # когда появились несохранённые в хранилище изменения подписчиков


def empty_subs():
    return {"subscribers": {}, "pending": {}, "owner": {}}


def _openssl(args, data):
    if not os.environ.get("SUBSCRIBERS_KEY"):
        raise RuntimeError("не задан SUBSCRIBERS_KEY")
    res = subprocess.run(["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-a", "-A", "-pass", "env:SUBSCRIBERS_KEY", *args],
                         input=data, capture_output=True, check=True)
    return res.stdout


def load_subs():
    """→ {"subscribers": {id: {"name", "since", "stations"}}, "pending": {id: {"name", "at"}}, "owner": {"stations"}}"""
    if not SUBS_FILE.exists():
        return empty_subs()
    subs = json.loads(_openssl(["-d"], SUBS_FILE.read_bytes()))
    for key, value in empty_subs().items():
        subs.setdefault(key, value)
    return subs


def save_subs(subs):
    SUBS_FILE.write_bytes(_openssl([], json.dumps(subs, ensure_ascii=False).encode()) + b"\n")
    UNPUSHED["since"] = UNPUSHED["since"] or time.time()


def owner_id():
    return os.environ.get("TELEGRAM_CHAT_ID", "")


def selection(subs, cid):
    """Заправки пользователя; если он ещё не выбирал — заправки по умолчанию."""
    entry = subs["owner"] if cid == owner_id() else subs["subscribers"].get(cid, {})
    return clean_selection(entry.get("stations")) or default_sids()


def set_selection(subs, cid, sids):
    entry = subs["owner"] if cid == owner_id() else subs["subscribers"][cid]
    entry["stations"] = sids


def user_name(user):
    name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")])) or "Без имени"
    return name + (f" (@{user['username']})" if user.get("username") else "")


def say(token, chat_id, text, markup=None):
    params = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": "true"}
    if markup:
        params["reply_markup"] = json.dumps(markup)
    return tg_call(token, "sendMessage", **params)


HELP_SUB = ("Бот присылает оповещение, когда на ваших заправках появляется АИ-95 или АИ-98.\n"
            "/stations — выбрать заправки (до 8)\n"
            "Сводка со статистикой — кнопка «⛽ Сводка» внизу.\n/stop — отписаться.")


# --- выбор заправок: /stations ---

def brand_of(sid):
    return REG[sid]["brand"] or "Другие"


def brand_key(brand):
    return hashlib.sha1(brand.encode()).hexdigest()[:4]


def brands():
    """→ [(название сети, ключ, число заправок)] — сначала крупные сети."""
    counts = {}
    for sid in REG:
        counts[brand_of(sid)] = counts.get(brand_of(sid), 0) + 1
    order = sorted(counts, key=lambda b: (b == "Другие", -counts[b], b))
    return [(b, brand_key(b), counts[b]) for b in order]


PAGE_SIZE = 15  # заправок на одной странице списка в боте


def stations_view(sids, view):
    """Текст и кнопки экрана выбора. view: "home", "my" или ключ сети."""
    def chosen_list():
        return "\n".join(f"{i + 1}. {html.escape(REG[s]['name'])}" for i, s in enumerate(sids)) or "пока не выбраны"

    def toggle_btn(sid, back):
        mark = "✅" if sid in sids else "▫️"
        return [{"text": f"{mark} {REG[sid]['short']}"[:60], "callback_data": f"st:t:{sid}:{back}"}]

    if view == "home":
        rows, row = [], []
        for b, key, n in brands():
            row.append({"text": f"{b} · {n}", "callback_data": f"st:b:{key}"})
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([{"text": f"✔️ Мои ({len(sids)})", "callback_data": "st:my"}, {"text": "Готово", "callback_data": "st:done"}])
        text = (f"<b>Ваши заправки</b> ({len(sids)} из {MAX_STATIONS}):\n{chosen_list()}\n\n"
                "Выберите сеть, чтобы добавить или убрать заправки:")
        return text, rows
    if view == "my":
        rows = [toggle_btn(s, "my") for s in sids]
        rows.append([{"text": "◀ Все сети", "callback_data": "st:home"}, {"text": "Готово", "callback_data": "st:done"}])
        return f"<b>Мои заправки</b> ({len(sids)} из {MAX_STATIONS}) — нажмите, чтобы убрать:", rows
    key, _, page = view.partition(".")  # ключ сети и номер страницы списка
    page = int(page or 0)
    brand = next((b for b, k, _ in brands() if k == key), None)
    if brand is None:
        return stations_view(sids, "home")
    in_brand = sorted((s for s in REG if brand_of(s) == brand), key=lambda s: REG[s]["short"])
    pages = max(1, -(-len(in_brand) // PAGE_SIZE))
    page = min(page, pages - 1)
    rows = [toggle_btn(s, f"{key}.{page}") for s in in_brand[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]]
    if pages > 1:
        rows.append([{"text": "◀", "callback_data": f"st:b:{key}.{(page - 1) % pages}"},
                     {"text": f"{page + 1} / {pages}", "callback_data": f"st:b:{key}.{page}"},
                     {"text": "▶", "callback_data": f"st:b:{key}.{(page + 1) % pages}"}])
    rows.append([{"text": "◀ Все сети", "callback_data": "st:home"}, {"text": "Готово", "callback_data": "st:done"}])
    return (f"<b>{html.escape(brand)}</b> — нажмите, чтобы добавить или убрать (выбрано {len(sids)} из {MAX_STATIONS}):", rows)


def handle_stations_cb(token, cb, subs):
    cid, data = str(cb["from"]["id"]), cb.get("data", "")
    msg = cb.get("message", {})
    if cid != owner_id() and cid not in subs["subscribers"]:
        tg_call(token, "answerCallbackQuery", callback_query_id=cb["id"], text="Сначала отправьте /start")
        return False
    sids, changed, notice = selection(subs, cid), False, None
    parts = data.split(":")
    view = "home"
    if parts[1] == "b":
        view = parts[2]
    elif parts[1] == "my":
        view = "my"
    elif parts[1] == "t":
        sid, view = parts[2], parts[3]
        if sid in sids:
            sids = [s for s in sids if s != sid]
            changed = True
        elif len(sids) >= MAX_STATIONS:
            notice = f"Можно выбрать не больше {MAX_STATIONS} заправок. Сначала уберите одну из выбранных."
        elif sid in REG:
            sids = sids + [sid]
            changed = True
    if changed:
        set_selection(subs, cid, sids)
        set_menu_button(token, cid, sids)
    tg_call(token, "answerCallbackQuery", callback_query_id=cb["id"], **({"text": notice, "show_alert": "true"} if notice else {}))
    if parts[1] == "done":
        text = ("✅ Сохранено. Ваши заправки:\n" + "\n".join(f"• {html.escape(REG[s]['name'])}" for s in sids)
                + "\n\nОповещения будут приходить по ним, сводка — кнопка «⛽ Сводка» внизу. Изменить — /stations.")
        tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), text=text, parse_mode="HTML",
                reply_markup=page_button(datetime.now(MSK), sids))
        return changed
    text, rows = stations_view(sids, view)
    tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), text=text, parse_mode="HTML",
            reply_markup=json.dumps({"inline_keyboard": rows}))
    return changed


# --- сообщения и кнопки бота ---

def handle_message(token, msg, subs):
    chat, user = msg.get("chat", {}), msg.get("from", {})
    if chat.get("type") != "private":
        return False
    cid, text = str(chat["id"]), (msg.get("text") or "").strip()
    owner = owner_id()
    if text.startswith("/stations") and (cid == owner or cid in subs["subscribers"]):
        view_text, rows = stations_view(selection(subs, cid), "home")
        say(token, cid, view_text, {"inline_keyboard": rows})
        return False
    if cid == owner:
        if text.startswith("/list"):
            if not subs["subscribers"]:
                say(token, cid, "Подписчиков пока нет. Чтобы подписаться, человек нажимает «Запустить» у бота, а вы подтверждаете.")
            for sid, info in subs["subscribers"].items():
                n = len(clean_selection(info.get("stations")))
                say(token, cid, f"👤 {html.escape(info['name'])}, с {info['since'][:10]}, заправок: {n or 'по умолчанию'}",
                    {"inline_keyboard": [[{"text": "❌ Удалить", "callback_data": f"sub:del:{sid}"}]]})
        else:
            say(token, cid, "Вы владелец бота: оповещения приходят вам и подтверждённым подписчикам — каждому по его заправкам.\n"
                            "/stations — выбрать свои заправки\n/list — список подписчиков.")
        return False
    if text.startswith("/stop"):
        if cid in subs["subscribers"]:
            info = subs["subscribers"].pop(cid)
            say(token, cid, "Вы отписались от оповещений. Чтобы подписаться снова, отправьте /start.")
            say(token, owner, f"👋 Отписка от оповещений: {html.escape(info['name'])}.")
            return True
        say(token, cid, "Вы и так не подписаны. /start — попросить доступ.")
        return False
    if cid in subs["subscribers"]:
        say(token, cid, ("Вы уже получаете оповещения.\n\n" if text.startswith("/start") else "") + HELP_SUB)
        return False
    if cid in subs["pending"]:
        say(token, cid, "Запрос уже отправлен владельцу бота — ждём подтверждения.")
        return False
    if text.startswith("/start"):
        name = user_name(user)
        subs["pending"][cid] = {"name": name, "at": datetime.now(MSK).isoformat()}
        say(token, owner, f"🔔 <b>{html.escape(name)}</b> хочет получать оповещения о появлении бензина.",
            {"inline_keyboard": [[{"text": "✅ Добавить", "callback_data": f"sub:ok:{cid}"},
                                  {"text": "❌ Отклонить", "callback_data": f"sub:no:{cid}"}]]})
        say(token, cid, "Запрос отправлен владельцу бота. Как только он подтвердит, вы сможете выбрать заправки "
                        "и получать оповещения, когда на них появляется АИ-95 или АИ-98.")
        return True
    say(token, cid, "Это бот оповещений о бензине в Воронеже. Отправьте /start, чтобы попросить доступ.")
    return False


def handle_callback(token, cb, subs):
    data = cb.get("data", "")
    if data.startswith("st:"):
        return handle_stations_cb(token, cb, subs)
    owner = owner_id()
    tg_call(token, "answerCallbackQuery", callback_query_id=cb["id"])
    if str(cb.get("from", {}).get("id")) != owner or not data.startswith("sub:"):
        return False
    _, action, cid = data.split(":", 2)
    msg = cb.get("message", {})

    def done(text):
        tg_call(token, "editMessageText", chat_id=owner, message_id=msg.get("message_id"), text=text, parse_mode="HTML")

    if action == "ok" and cid in subs["pending"]:
        info = subs["pending"].pop(cid)
        sids = default_sids()
        subs["subscribers"][cid] = {"name": info["name"], "since": datetime.now(MSK).isoformat(), "stations": sids}
        set_menu_button(token, cid, sids)
        say(token, cid, "✅ Владелец подтвердил доступ. Сейчас выбраны заправки по умолчанию:\n"
                        + "\n".join(f"• {html.escape(REG[s]['name'])}" for s in sids)
                        + "\n\nЧтобы выбрать свои, отправьте /stations.\n\n" + HELP_SUB,
            json.loads(page_button(datetime.now(MSK), sids)))
        done(f"✅ Добавлено в подписчики: {html.escape(info['name'])}.")
        return True
    if action == "no" and cid in subs["pending"]:
        info = subs["pending"].pop(cid)
        say(token, cid, "Владелец бота отклонил запрос на оповещения.")
        done(f"❌ Запрос от {html.escape(info['name'])} отклонён.")
        return True
    if action == "del" and cid in subs["subscribers"]:
        info = subs["subscribers"].pop(cid)
        say(token, cid, "Владелец бота отключил вам оповещения.")
        done(f"🗑 Удалено из подписчиков: {html.escape(info['name'])}.")
        return True
    done("Этот запрос уже обработан.")
    return False


def bot_ready():
    return bool(os.environ.get("TELEGRAM_TOKEN") and owner_id() and os.environ.get("SUBSCRIBERS_KEY"))


def setup_bot(db):
    """При запуске смены: команды в меню бота и кнопки «⛽ Сводка» со своими заправками у каждого."""
    if not bot_ready():
        return
    token, owner = os.environ["TELEGRAM_TOKEN"], owner_id()
    load_registry(db)
    tg_call(token, "setMyCommands", commands=json.dumps([
        {"command": "stations", "description": "Выбрать заправки"},
        {"command": "stop", "description": "Отписаться от оповещений"}]))
    tg_call(token, "setMyCommands", scope=json.dumps({"type": "chat", "chat_id": int(owner)}), commands=json.dumps([
        {"command": "stations", "description": "Выбрать свои заправки"},
        {"command": "list", "description": "Подписчики"}]))
    for cid, sids in all_selections():
        try:
            set_menu_button(token, cid, sids)
        except Exception as e:
            print(f"кнопка меню …{cid[-4:]}: {e}", file=sys.stderr)


def poll_bot(wait=0):
    """Прочитать новые сообщения боту и ответить на них. wait > 0 — ждать сообщений до wait секунд
    (длинный опрос: ответ приходит сразу, как только пользователь что-то нажал)."""
    if not bot_ready():
        if wait:
            time.sleep(wait)
        return
    token = os.environ["TELEGRAM_TOKEN"]
    offset = int(TG_OFFSET_FILE.read_text()) if TG_OFFSET_FILE.exists() else 0
    updates = tg_updates(token, offset, wait)
    if not updates:
        return
    if not REG:
        load_registry(open_db())
    subs, changed = load_subs(), False
    for upd in updates:
        offset = upd["update_id"] + 1
        try:
            if "message" in upd:
                changed |= handle_message(token, upd["message"], subs)
            elif "callback_query" in upd:
                changed |= handle_callback(token, upd["callback_query"], subs)
        except Exception as e:
            print(f"бот: ошибка {e}", file=sys.stderr)
    if changed:
        save_subs(subs)
    TG_OFFSET_FILE.write_text(str(offset))


def tg_updates(token, offset, wait):
    """getUpdates с длинным опросом: Telegram держит запрос до wait секунд, пока не придёт сообщение."""
    data = urllib.parse.urlencode({"offset": offset, "timeout": wait,
                                   "allowed_updates": json.dumps(["message", "callback_query"])}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/getUpdates", data=data)
    with urllib.request.urlopen(req, timeout=wait + 15) as resp:
        return json.load(resp).get("result", [])


# ---------- сервер на GitHub Actions: непрерывная работа с 7:00 до 24:00 ----------
#
# GitHub плохо выполняет расписание (запуски опаздывают на часы или пропадают), поэтому
# одна задача работает непрерывно и сама собирает данные по расписанию ниже. GitHub ограничивает
# задачу 6 часами — перед этим она запускает себе продолжение («смену»). Кнопка «Обновить сейчас»
# создаёт задачу, которая ждёт в очереди; работающая задача замечает её за ~20 секунд,
# делает сбор и отменяет её.

WORK_START, WORK_END = 7, 24  # часы сбора, МСК
SLOT_MINUTES = 10              # сбор каждые 10 минут
REPO = os.environ.get("GITHUB_REPOSITORY", "dooodoIvan/benzin")
WORKFLOW = "collect.yml"
MANUAL_TITLE = "Обновить сейчас"  # run-name ручного запуска (см. .github/workflows/collect.yml)


def day_slots(ws):
    """Моменты сбора за день: с 7:00 до 23:50 каждые 10 минут."""
    n = (WORK_END - WORK_START) * 60 // SLOT_MINUTES
    return [ws + timedelta(minutes=SLOT_MINUTES * k) for k in range(n)]


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
    """Страница со сводкой → открытое хранилище dooodoIvan/benzin-page (GitHub Pages), одной свежей версией.
    На странице — все заправки, выбранные хоть кем-то; каждый видит свои по ссылке с ?s=…"""
    key = os.environ.get("PAGES_KEY_FILE")
    if not key:
        return
    load_registry(db)
    watched = list(dict.fromkeys(sid for _, sids in all_selections() for sid in sids))
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        page = Path(tmp)
        write_report(db, page / "index.html", watched)
        (page / ".nojekyll").write_text("")
        (page / "README.md").write_text("# Бензин · сводка\n\nОбновляется автоматически с 7:00 до 24:00 МСК каждые 10 минут.\n")
        env = {**os.environ, "GIT_SSH_COMMAND": f"ssh -i {key} -o StrictHostKeyChecking=accept-new"}
        for cmd in (["init", "-q", "-b", "main"], ["add", "-A"],
                    ["-c", "user.name=github-actions[bot]",
                     "-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
                     "commit", "-q", "-m", f"сводка {datetime.now(MSK):%d.%m %H:%M}"],
                    ["push", "-q", "--force", "git@github.com:dooodoIvan/benzin-page.git", "main"]):
            subprocess.run(["git", *cmd], cwd=page, env=env, check=True, capture_output=True, text=True)


def save_and_push():
    git("add", "data")
    UNPUSHED["since"] = None
    if git("diff", "--cached", "--quiet", check=False).returncode == 0:
        return
    git("commit", "-q", "-m", f"данные {datetime.now(MSK):%d.%m %H:%M}")
    for attempt in range(3):
        if git("push", "-q", check=False).returncode == 0:
            return
        git("pull", "-q", "--rebase", check=False)
    print("не удалось отправить данные в хранилище", file=sys.stderr)


def work_once():
    """Один сбор: канал → data/obs.csv → страница → оповещения в Telegram → сохранить в хранилище."""
    t0 = time.time()
    db = open_db()
    try:
        new = collect(db, 4)
        if new:
            save_db(db)
    except Exception as e:
        new = 0
        print(f"сбор не удался: {e}", file=sys.stderr)
    load_registry(db)
    for step, fn in (("страница", lambda: publish_page(db)),
                     ("telegram", lambda: cmd_telegram(argparse.Namespace(alerts=True)) if os.environ.get("TELEGRAM_TOKEN") else None),
                     ("бот", poll_bot),
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

    try:
        setup_bot(open_db())
    except Exception as e:
        print(f"бот: не удалось настроить: {e}", file=sys.stderr)
    slots = [s for s in day_slots(ws) if s > start]
    do_now = args.reason == "manual" or start >= ws  # опоздали к началу или нажали кнопку — собираем сразу
    last_queue_check = 0.0
    while True:
        now = datetime.now(MSK)
        if now >= we or now >= deadline:
            if UNPUSHED["since"]:
                save_and_push()
            if now >= we:
                print("рабочий день закончился", file=sys.stderr)
                return
            # 6-часовой предел GitHub: запускаем продолжение и завершаемся
            gh_api("POST", f"actions/workflows/{WORKFLOW}/dispatches", {"ref": "main", "inputs": {"reason": "chain"}})
            print("запущено продолжение", file=sys.stderr)
            return
        if slots and now >= slots[0]:  # подошло время по расписанию
            do_now = True
            slots = [s for s in slots if s > now]
        manual = []
        if time.time() - last_queue_check >= 20:
            manual, last_queue_check = pending_manual_runs(), time.time()
        if do_now or manual:
            work_once()
            for run_id in set(manual + pending_manual_runs()):  # ручные запросы выполнены — убираем из очереди
                try:
                    gh_api("POST", f"actions/runs/{run_id}/cancel")
                except Exception:
                    pass
            do_now = False
            continue
        if UNPUSHED["since"] and time.time() - UNPUSHED["since"] > 60:
            save_and_push()  # выбор заправок и подписчиков сохраняем не реже раза в минуту
        try:
            poll_bot(wait=20)  # ждём сообщения боту до 20 с — заодно пауза цикла
        except Exception as e:
            print(f"бот: ошибка {e}", file=sys.stderr)
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
    t.add_argument("--alerts", action="store_true", help="писать, только если заправка перешла в статус «есть»")
    wk = sub.add_parser("worker", help="непрерывная работа на GitHub Actions (7–24 МСК)")
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
