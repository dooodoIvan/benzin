#!/usr/bin/env python3
"""Сбор отчётов о топливе из публичного канала @voronezh_benzin.

Канал читается через открытую веб-версию t.me/s/... — аккаунт и ключи не нужны.
Канал удаляет старые посты (примерно через 1–2 часа), поэтому история копится,
только пока сборщик регулярно запускается: на GitHub Actions с 7:00 до 24:00 МСК каждые 10 минут
(.github/workflows/collect.yml, команда worker). Данные хранятся в data/obs.csv в этом репозитории.

Новая версия программы подхватывается сама: сервер раз в 2 минуты проверяет GitHub и перезапускается.

Заправки определяются по адресу из канала. Каждый пользователь бота выбирает до 10 своих заправок
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
PRICES_CSV = BASE / "data/prices.csv"  # ориентировочные цены из отчётов канала
TG_CHAT_FILE = BASE / "data/telegram_chat.txt"
TG_ALERTS_FILE = BASE / "data/telegram_alerts.json"  # последнее известное состояние заправок (для оповещений)
PAGE_URL = "https://dooodoivan.github.io/benzin-page/"  # сводка, которую публикует GitHub Actions
RUN_URL = "https://github.com/dooodoIvan/benzin/actions/workflows/collect.yml"  # ручной запуск сбора (Run workflow)
REPORT_PATH = BASE / "report.html"  # локальный файл, в git не попадает

# Заправки владельца по умолчанию: шаблон адреса (как пишет канал) → короткое имя
ALIASES = [  # шаблон адреса, короткое имя, сеть (если канал её не указал)
    (r"бабяково.*транспортн|транспортн.*бабяково", "Бабяково", "Газпром"),
    (r"ленинский проспект,\s*182(?![\dа-я])", "Ленинский 182", "Роснефть"),
    (r"землячки,\s*7\s*а(?![\dа-я])", "Землячки 7А", "Роснефть"),
    (r"новая усмань.*дорожная улица,\s*31(?![\dа-я])", "Дорожная 31", "Роснефть"),
    (r"новая усмань.*дорожная улица,\s*101(?![\dа-я])", "Дорожная 101", "Роснефть"),
    (r"ленинский проспект,\s*154\s*а(?![\dа-я])", "Ленинский 154А", "Татнефть"),
]
MAX_STATIONS = 10  # сколько заправок может выбрать пользователь
CATALOG_DAYS = 30  # в списке для выбора — заправки, о которых канал писал за последние 30 дней
RETENTION_DAYS = 14  # записи о наличии топлива старше двух недель удаляются (статистика — за 2 недели)
STATIONS_FILE = BASE / "data/stations.json"  # справочник заправок: адрес → сеть и когда о ней писали (не удаляется)
FUEL_CHOICES = ["92", "95", "95+", "98", "100", "ДТ"]  # марки, о которых пишет канал
DEFAULT_FUELS = ["95", "98"]  # марки по умолчанию (пока пользователь не выбрал свои в /fuels)
FUEL_RU = {"92": "АИ-92", "95": "АИ-95", "95+": "АИ-95+", "98": "АИ-98", "100": "АИ-100", "ДТ": "ДТ"}


def fuel_name(fuel):
    return FUEL_RU.get(fuel, fuel)


def fuels_title(fuels):
    """["92", "95", "ДТ"] → «АИ-92/95, ДТ»."""
    ai = [f for f in FUEL_CHOICES if f in fuels and f != "ДТ"]
    parts = (["АИ-" + "/".join(ai)] if ai else []) + (["ДТ"] if "ДТ" in fuels else [])
    return ", ".join(parts)


def fuels_or(fuels):
    """«АИ-95 или АИ-98»."""
    names = [fuel_name(f) for f in FUEL_CHOICES if f in fuels]
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " или " + names[-1]


def is_petrol(fuels):
    return any(f != "ДТ" for f in fuels)
FRESH = timedelta(hours=24)  # старше — считаем «нет свежих данных»
CONFIRM_FRESH = timedelta(hours=2)  # «есть» старше 2 часов показываем жёлтым «?»
ALERT_COOLDOWN = timedelta(hours=1)  # защита от «мигания» есть/нет: по одной заправке не чаще раза в час
KIND_RU = {"report": "водитель", "summary": "общая сводка", "signal": "терминалы оплаты, не подтверждено"}


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
    db.execute("""CREATE TABLE prices (seen_at TEXT NOT NULL, address TEXT NOT NULL, fuel TEXT NOT NULL, price REAL NOT NULL,
                  UNIQUE (address, fuel, seen_at))""")
    if PRICES_CSV.exists():
        with PRICES_CSV.open(encoding="utf-8", newline="") as f:
            db.executemany("INSERT OR IGNORE INTO prices VALUES (?,?,?,?)",
                           [(r["seen_at"], r["address"], r["fuel"], float(r["price"])) for r in csv.DictReader(f)])
    if OBS_CSV.exists():
        with OBS_CSV.open(encoding="utf-8", newline="") as f:
            rows = [(r["seen_at"], r["address"], r["fuel"], int(r["available"]), r["status"], r["kind"],
                     r["queue"] or None, int(r["post_id"]), r.get("brand") or None) for r in csv.DictReader(f)]
        db.executemany("INSERT OR IGNORE INTO obs VALUES (?,?,?,?,?,?,?,?,?)", rows)
    return db


def save_db(db):
    """Пишет наблюдения в CSV в стабильном порядке — так изменения в git остаются маленькими.
    Записи старше RETENTION_DAYS удаляются (справочник заправок при этом сохраняется в stations.json)."""
    update_catalog(db)
    cutoff = (datetime.now(MSK) - timedelta(days=RETENTION_DAYS)).isoformat()
    removed = db.execute("DELETE FROM obs WHERE seen_at < ?", (cutoff,)).rowcount
    if removed:
        print(f"удалено записей старше {RETENTION_DAYS} дней: {removed}", file=sys.stderr)
    OBS_CSV.parent.mkdir(parents=True, exist_ok=True)
    tmp = OBS_CSV.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(OBS_FIELDS)
        w.writerows(db.execute(f"SELECT {', '.join(OBS_FIELDS)} FROM obs ORDER BY seen_at, address, fuel, kind, available"))
    tmp.replace(OBS_CSV)
    # цены: старше RETENTION_DAYS удаляем, но последнюю известную цену по каждой заправке и марке оставляем
    db.execute("""DELETE FROM prices WHERE seen_at < ? AND (address, fuel, seen_at) NOT IN
                  (SELECT address, fuel, MAX(seen_at) FROM prices GROUP BY address, fuel)""", (cutoff,))
    with PRICES_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seen_at", "address", "fuel", "price"])
        w.writerows(db.execute("SELECT seen_at, address, fuel, price FROM prices ORDER BY address, fuel, seen_at"))


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


def parse_prices(post_ts, text):
    """Ориентировочные цены из отчёта по одной АЗС → [(seen_at, адрес, марка, цена)]."""
    lines = [l.strip() for l in text.splitlines()]
    addr = next((l.lstrip("📍 ").strip() for l in lines if l.startswith("📍")), None)
    m = re.search(r"Ориентир:\s*(.+)", text)
    if not addr or not m:
        return []
    m_upd = re.search(r"Обновлено в (\d{1,2}:\d{2})", text)
    seen_at = stamp(m_upd.group(1), post_ts) if m_upd else post_ts.astimezone(MSK).isoformat()
    return [(seen_at, clean_addr(addr), fuel, float(price.replace(",", ".")))
            for fuel, price in re.findall(r"([\w+]+)\s*[—–-]\s*([\d]+(?:[.,]\d+)?)\s*₽", m.group(1))]


def latest_prices(db, sids):
    """→ {sid: {марка: (цена, когда, прежняя цена или None, когда цена стала текущей)}}"""
    out = {}
    for sid in sids:
        address = REG.get(sid, {}).get("address")
        cur = {}
        for seen_at, fuel, price in db.execute(
                "SELECT seen_at, fuel, price FROM prices WHERE address = ? ORDER BY seen_at", (address,)):
            if fuel not in cur:
                cur[fuel] = [price, seen_at, None, seen_at]
            elif price != cur[fuel][0]:
                cur[fuel] = [price, seen_at, cur[fuel][0], seen_at]
            else:
                cur[fuel][1] = seen_at
        if cur:
            out[sid] = {f: (v[0], datetime.fromisoformat(v[1]), v[2], datetime.fromisoformat(v[3])) for f, v in cur.items()}
    return out


def money(x):
    return f"{x:.2f}".replace(".", ",") + " ₽"


def collect(db, pages):
    """Читает до `pages` страниц канала (по 20 постов) → число новых наблюдений."""
    before, new_obs = None, 0
    for _ in range(pages):
        posts = split_posts(http_get(f"https://t.me/s/{CHANNEL}" + (f"?before={before}" if before else "")))
        if not posts:
            break
        for pid, ts, text in posts:
            for pr in parse_prices(ts, text):
                db.execute("INSERT OR IGNORE INTO prices VALUES (?,?,?,?)", pr)
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


def load_catalog():
    try:
        return json.loads(STATIONS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def update_catalog(db):
    """Дополнить справочник заправок из записей: сеть (самая частая) и когда о заправке писали последний раз."""
    cat = load_catalog()
    brands = {}
    for address, brand, n, last in db.execute(
            "SELECT address, brand, COUNT(*), MAX(seen_at) FROM obs GROUP BY address, brand"):
        entry = cat.setdefault(address, {})
        if last > entry.get("last_seen", ""):
            entry["last_seen"] = last
        if brand:
            brands.setdefault(address, {})[brand] = n
    for address, counts in brands.items():
        cat[address]["brand"] = max(counts, key=counts.get)
    if cat != load_catalog():
        STATIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATIONS_FILE.write_text(json.dumps(dict(sorted(cat.items())), ensure_ascii=False, indent=1), encoding="utf-8")
    return cat


def load_registry(db):
    """Справочник всех известных заправок (из stations.json + свежих записей). Для выбора в боте предлагаются
    те, о которых канал писал за последние CATALOG_DAYS дней (см. listable)."""
    cat = update_catalog(db)
    REG.clear()
    for address, info in cat.items():
        brand = info.get("brand")
        alias, alias_brand = next(((n, b) for pattern, n, b in ALIASES if re.search(pattern, address.lower())), (None, None))
        brand = brand or alias_brand
        short = short_address(address)
        REG[station_of(address)] = {"address": address, "brand": brand, "alias": alias, "short": alias or short,
                                    "name": f"{brand}, {short}" if brand else short,
                                    "label": f"{brand} · {alias or short}" if brand else (alias or short),
                                    "last_seen": info.get("last_seen", "")}
    return REG


def listable(sid, sids=()):
    """Показывать ли заправку в списке выбора: канал писал о ней недавно или она уже выбрана."""
    since = (datetime.now(MSK) - timedelta(days=CATALOG_DAYS)).isoformat()
    return sid in sids or REG[sid]["last_seen"] >= since


def default_sids():
    """Заправки владельца по умолчанию — в порядке ALIASES (только те, о которых канал уже писал)."""
    by_alias = {s["alias"]: sid for sid, s in REG.items() if s["alias"]}
    return [by_alias[name] for _, name, _ in ALIASES if name in by_alias]


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
            if fuel in FUEL_CHOICES:
                latest[sid][fuel] = (datetime.fromisoformat(seen_at), avail, status, queue, kind)
    return latest


def station_state(fuels, now, wanted=None):
    """Состояние заправки по маркам wanted → (css-класс, подпись, ранг для сортировки, время последних данных).
    Ранги: 0 есть (≤2 ч), 1 «есть» было давно, 2 только терминал, 3 нет, 4 нет данных."""
    wanted = wanted or DEFAULT_FUELS
    fresh = {f: v for f, v in fuels.items() if f in wanted and now - v[0] <= FRESH}
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
    return "unknown", "Нет информации", 4, None


def short_when(t, now):
    days = (now.date() - t.date()).days
    return f"{t:%H:%M}" if days == 0 else f"вчера {t:%H:%M}" if days == 1 else f"{t:%d.%m %H:%M}"


def when_text(t, now):
    """«в 14:31», «вчера в 22:17» или «07.10 в 18:05»."""
    days = (now.date() - t.date()).days
    return f"в {t:%H:%M}" if days == 0 else f"вчера в {t:%H:%M}" if days == 1 else f"{t:%d.%m} в {t:%H:%M}"


def ordered_stations(db, now, sids, wanted=None):
    """Заправки по порядку: где бензин есть (свежее выше) → было давно → терминал → нет → без данных."""
    rows = [(sid, fuels, station_state(fuels, now, wanted)) for sid, fuels in latest_state(db, sids).items()]
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
        for fuel in DEFAULT_FUELS:
            if fuel in fuels:
                seen_at, avail, status, queue, kind = fuels[fuel]
                mark = "🟡" if kind == "signal" else ("🟢" if avail else "🔴")
                print(f"  {mark} {fuel_name(fuel)}: {status_word(avail, kind, seen_at, now)} — {seen_at:%d.%m %H:%M} ({KIND_RU[kind]})"
                      + (f", очередь {queue}" if queue else ""))


def notify_text(db, sids=None, wanted=None):
    """→ (заголовок, текст): заправки сгруппированы по состоянию, чтобы влезть в несколько строк уведомления."""
    now = datetime.now(MSK)
    if not REG:
        load_registry(db)
    sids, wanted = sids or default_sids(), wanted or DEFAULT_FUELS
    groups = {"have": [], "stale": [], "term": [], "none": [], "unknown": []}
    for sid, fuels, (cls, label, _, when) in ordered_stations(db, now, sids, wanted):
        short = REG[sid]["label"]
        groups[cls].append(f"{short} ({when:%H:%M})" if when else short)
    heads = {"have": "✅ Есть", "stale": "🟡 Было давно", "term": "❓ Терминал", "none": "❌ Нет", "unknown": "⚪ Нет информации"}
    lines = [f"{heads[c]}: " + ", ".join(v) for c, v in groups.items() if v]
    title = f"⛽ {fuels_title(wanted)} · {now:%H:%M} · есть на {len(groups['have'])} из {len(sids)}"
    return title, "\n".join(lines)


# ---------- Telegram ----------

def telegram_text(db, sids, wanted):
    """Подробная сводка для Telegram (HTML-разметка)."""
    now = datetime.now(MSK)
    title, _ = notify_text(db, sids, wanted)
    icons = {"have": "✅", "stale": "🟡", "term": "❓", "none": "❌", "unknown": "⚪"}
    parts = [f"<b>{html.escape(title)}</b>"]
    prices = latest_prices(db, sids)
    for sid, fuels, (cls, label, _, _) in ordered_stations(db, now, sids, wanted):
        lines = [f"{icons[cls]} <b>{html.escape(REG[sid]['name'])}</b> — {label}"]
        for fuel in FUEL_CHOICES:
            if fuel in wanted and fuel in fuels and now - fuels[fuel][0] <= FRESH:
                seen_at, avail, _, queue, kind = fuels[fuel]
                price = prices.get(sid, {}).get(fuel)
                lines.append(f"   {fuel_name(fuel)}: {status_word(avail, kind, seen_at, now)}, {seen_at:%H:%M} ({KIND_RU[kind]})"
                             + (f", очередь {html.escape(queue)}" if queue else "") + (f", ~{money(price[0])}" if price else ""))
        forecast = short_forecast(db, sid, now, wanted)
        if forecast:
            lines.append(f"   📊 {forecast}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def tg_call(token, method, timeout=30, **params):
    data = urllib.parse.urlencode(params).encode() if params else None
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/{method}", data=data)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def page_url(sids=None, fuels=None, own=False):
    """Адрес сводки со своими заправками и марками: ?s=код,код,…&f=92,95 (own=1 — для владельца)."""
    params = []
    if sids is not None:
        params.append("s=" + (",".join(sids) or "-"))  # «-» — заправки ещё не выбраны
    if fuels and list(fuels) != DEFAULT_FUELS:
        params.append("f=" + urllib.parse.quote(",".join(fuels), safe=","))
    if own:
        params.append("own=1")
    return PAGE_URL + ("?" + "&".join(params) if params else "")


def page_button(now, sids=None, fuels=None, cid=None):
    """Кнопка под сообщением: открывает сводку внутри Telegram (t= — чтобы не показывалась старая копия)."""
    url = page_url(sids, fuels, own=bool(cid) and str(cid) == owner_id())
    url += ("&" if "?" in url else "?") + f"t={now:%m%d%H%M}"
    return json.dumps({"inline_keyboard": [[{"text": "⛽ Открыть сводку", "web_app": {"url": url}}]]})


def set_menu_button(token, chat_id, sids=None, fuels=None):
    """Постоянная кнопка «⛽ Сводка» рядом с полем ввода — открывает сводку со своими заправками и марками."""
    url = page_url(sids, fuels, own=str(chat_id) == owner_id())
    tg_call(token, "setChatMenuButton", chat_id=chat_id, menu_button=json.dumps(
        {"type": "web_app", "text": "⛽ Сводка", "web_app": {"url": url}}))


def tg_chat_id(token):
    """Чат владельца: секрет TELEGRAM_CHAT_ID (на Mac — локальный файл data/telegram_chat.txt)."""
    if os.environ.get("TELEGRAM_CHAT_ID"):
        return os.environ["TELEGRAM_CHAT_ID"]
    if TG_CHAT_FILE.exists():
        return TG_CHAT_FILE.read_text().strip()
    return None


def alert_text(db, items, now, wanted):
    """items: [(sid, [марка…])] — где только что появилось нужное топливо."""
    fuels_all = {f for _, fs in items for f in fs}
    lines = ["<b>⛽ Появился бензин</b>" if not ("ДТ" in fuels_all and len(fuels_all) == 1) else "<b>⛽ Появилось дизельное топливо</b>"]
    latest = latest_state(db, [sid for sid, _ in items])
    prices = latest_prices(db, [sid for sid, _ in items])
    for sid, fs in items:
        parts = []
        for fuel in FUEL_CHOICES:
            if fuel in fs and fuel in latest[sid]:
                seen_at, _, _, queue, kind = latest[sid][fuel]
                price = prices.get(sid, {}).get(fuel)
                parts.append(f"{fuel_name(fuel)} есть ({seen_at:%H:%M}, {KIND_RU[kind]}"
                             + (f", очередь {html.escape(queue)}" if queue else "") + ")"
                             + (f", ~{money(price[0])}" if price else ""))
        lines.append(f"\n✅ <b>{html.escape(REG[sid]['name'])}</b>\n   " + "; ".join(parts))
        st = station_stats(db, sid, now, wanted)
        if st and median_duration(st["durations"]):
            lines.append(f"   📊 обычно держится около {median_duration(st['durations'])}")
    return "\n".join(lines)


def all_selections():
    """→ [(chat_id, [sid…], [марка…])] для владельца и подписчиков (если ключ шифрования доступен)."""
    owner = owner_id()
    try:
        subs = load_subs() if os.environ.get("SUBSCRIBERS_KEY") else empty_subs()
    except Exception as e:
        print(f"подписчики недоступны: {e}", file=sys.stderr)
        subs = empty_subs()
    ids = ([owner] if owner else []) + [cid for cid in subs["subscribers"] if cid != owner]
    return [(cid, selection(subs, cid), fuel_selection(subs, cid)) for cid in ids]


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
        subs = load_subs() if os.environ.get("SUBSCRIBERS_KEY") else empty_subs()
        sids, fuels = selection(subs, owner), fuel_selection(subs, owner)
        tg_call(token, "sendMessage", chat_id=owner, text=telegram_text(db, sids, fuels), parse_mode="HTML",
                disable_web_page_preview="true", reply_markup=page_button(now, sids, fuels, owner))
        print("сводка отправлена в Telegram", file=sys.stderr)
        return

    # Состояние отслеживается отдельно для каждой пары «заправка + марка»: кому нужен АИ-92,
    # тот узнает о появлении АИ-92, даже если АИ-95 там не было.
    people = all_selections()
    pairs = sorted({(sid, f) for _, sids, fuels in people for sid in sids for f in fuels})
    state = json.loads(TG_ALERTS_FILE.read_text()) if TG_ALERTS_FILE.exists() else {}
    if state.get("_v") != 3:  # первый запуск нового формата: только запоминаем, без оповещений
        state = {"_v": 3, "_silent": True}
    silent = state.pop("_silent", False)
    latest = latest_state(db, sorted({sid for sid, _ in pairs}))
    appeared = set()
    for sid, fuel in pairs:
        key = f"{sid}|{fuel}"
        cls = station_state(latest[sid], now, [fuel])[0]
        prev = state.get(key, {})
        last_alert = datetime.fromisoformat(prev["last_alert"]) if prev.get("last_alert") else None
        # пишем только при переходе в «есть»; «было давно» → «есть» — топливо не пропадало, молчим
        if (not silent and cls == "have" and prev.get("state") not in ("have", "stale", None)
                and (not last_alert or now - last_alert >= ALERT_COOLDOWN)):
            appeared.add((sid, fuel))
            prev["last_alert"] = now.isoformat()
        prev["state"] = cls
        state[key] = prev
    paused = paused_ids() if appeared else set()
    for cid, sids, fuels in people:
        if cid in paused:
            continue
        items = [(sid, [f for f in fuels if (sid, f) in appeared]) for sid in sids]
        items = [(sid, fs) for sid, fs in items if fs]
        if not items:
            continue
        try:
            tg_call(token, "sendMessage", chat_id=cid, parse_mode="HTML", disable_web_page_preview="true",
                    text=alert_text(db, items, now, fuels), reply_markup=page_button(now, sids, fuels, cid))
        except Exception as e:  # например, подписчик заблокировал бота
            print(f"не удалось отправить …{cid[-4:]}: {e}", file=sys.stderr)
    print(("оповещение: " + ", ".join(f"{REG[s]['label']} {fuel_name(f)}" for s, f in sorted(appeared)))
          if appeared else "новых появлений нет", file=sys.stderr)
    TG_ALERTS_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=1))


# ---------- статистика: интервалы наличия, появления и окончания ----------

STATS_DAYS = RETENTION_DAYS  # за сколько дней считать статистику
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


def station_timeline(db, sid, now, fuels):
    """Наличие нужного топлива (любой из марок fuels) на заправке во времени →
    [(начало, конец, есть?, только_терминалы?)]. Есть, если есть хоть одна из марок; нет — если все известные «нет»."""
    since = now - timedelta(days=STATS_DAYS)
    per_fuel = [intervals(station_series(db, sid, f, since), now) for f in fuels]
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


STATUS_CAP = timedelta(hours=12)  # дольше без новых отчётов — «нет информации»
STATUS_RANK = {"have": 0, "stale": 1, "term": 2, "none": 3}
STATUS_RU = {"have": "есть", "stale": "давно не подтверждали", "term": "только терминал", "none": "нет", "unknown": "нет информации"}


def status_timeline(db, sid, now, fuels):
    """Состояние заправки во времени, как в сводке → [(начало, конец, статус)]:
    have — «есть» подтверждено не больше 2 ч назад; stale — «есть» подтверждали раньше, а «нет» не сообщали;
    term — только оплаты по терминалу; none — нет. Без отчётов дольше STATUS_CAP — нет информации (пропуск)."""
    since = now - timedelta(days=STATS_DAYS)
    per_fuel = []
    for f in fuels:
        series, segs = station_series(db, sid, f, since), []
        for i, (t, a, k) in enumerate(series):
            nxt = series[i + 1][0] if i + 1 < len(series) else now
            end = min(nxt, t + STATUS_CAP, now)
            if end <= t:
                continue
            if a and k != "signal":
                segs.append((t, min(end, t + CONFIRM_FRESH), "have"))
                if end > t + CONFIRM_FRESH:
                    segs.append((t + CONFIRM_FRESH, end, "stale"))
            else:
                segs.append((t, end, "term" if a else "none"))
        per_fuel.append(segs)
    bounds = sorted({t for segs in per_fuel for a, b, _ in segs for t in (a, b)})
    out = []
    for b0, b1 in zip(bounds, bounds[1:]):
        mid = b0 + (b1 - b0) / 2
        cover = [st for segs in per_fuel for a, b, st in segs if a <= mid < b]
        if not cover:
            continue
        st = min(cover, key=STATUS_RANK.get)  # лучшее из состояний по выбранным маркам
        if out and out[-1][1] == b0 and out[-1][2] == st:
            out[-1] = (out[-1][0], b1, st)
        else:
            out.append((b0, b1, st))
    return out


def hourly_status(tl):
    """→ по часам суток: {статус: минут}."""
    acc = [dict() for _ in range(24)]
    for start, end, st in tl:
        t = start
        while t < end:
            nxt = min(end, t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            acc[t.hour][st] = acc[t.hour].get(st, 0) + (nxt - t).total_seconds() / 60
            t = nxt
    return acc


def station_stats(db, sid, now, fuels=None):
    fuels = fuels or DEFAULT_FUELS
    timeline = station_timeline(db, sid, now, fuels)
    if not timeline:
        return None
    arrivals, runouts, durations = events(timeline)
    tl = status_timeline(db, sid, now, fuels)
    return {"timeline": timeline, "arrivals": arrivals, "runouts": runouts, "durations": durations,
            "hourly": hourly_share(timeline), "since": timeline[0][0], "status": tl, "hourly_status": hourly_status(tl)}


def median_duration(durations):
    if len(durations) < 2:
        return None
    return fmt_hours(sorted(durations)[len(durations) // 2])


def short_forecast(db, sid, now, fuels=None):
    """Одна строка для Telegram: когда обычно появляется/заканчивается топливо (если данных достаточно)."""
    st = station_stats(db, sid, now, fuels)
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
# только заправки из ссылки (?s=код,код,…) в порядке выбора.
# Поэтому каждая заправка — отдельная строка (data-sid), а её цвет задаётся переменной --c.

W = 400  # ширина графика в единицах viewBox — рассчитано на телефон; на компьютере ширина ограничена в CSS
DAYS_RU = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]


def tip(text):
    return f'data-tip="{html.escape(text, quote=True)}" tabindex="0"'


def row_attrs(sid, visible):
    return f'data-sid="{sid}"' + ("" if visible else ' class="nosel"')


def svg_label(sid, x, y):
    """Подпись строки графика в две строки: сеть (мелко) и адрес."""
    brand = REG[sid]["brand"]
    addr = html.escape(chart_label(sid))
    if not brand:
        return f'<text x="{x}" y="{y + 4:.1f}" class="tick label" text-anchor="end">{addr}</text>'
    return (f'<text x="{x}" y="{y - 1:.1f}" class="tick brand" text-anchor="end">{html.escape(brand)}</text>'
            f'<text x="{x}" y="{y + 9:.1f}" class="tick label" text-anchor="end">{addr}</text>')


STATUS_LEGEND = ('<div class="legend"><span><i class="k s-have"></i>есть</span>'
                 '<span><i class="k s-stale"></i>давно не подтверждали</span><span><i class="k s-term"></i>только терминал</span>'
                 '<span><i class="k s-none"></i>нет</span><span><i class="k s-unknown"></i>нет информации</span></div>')


def first_hour(stats, default=7):
    """С какого часа показывать графики: с первого часа (не раньше 6:00), когда были данные, но не позже начала сбора."""
    hours = [h for st in stats.values() if st for h in range(6, 24) if sum(st["hourly"][h]) >= 1]
    return min(hours + [default])


def heat_rows(stats, visible, h0):
    """Тепловая карта: строка на заправку, клетка на час. Цвет — что чаще всего было в этот час."""
    n = 24 - h0
    x0, x1, row = 112, W - 6, 26
    cw = (x1 - x0) / n
    out = []
    for sid, st in stats.items():
        label = REG[sid]["label"]
        cells = []
        for h in range(h0, 24):
            x = x0 + cw * (h - h0)
            mins = st["hourly_status"][h] if st else {}
            total = sum(mins.values())
            text = f"{label} · {h:02d}:00–{(h + 1) % 24:02d}:00 · "
            if total < 1:
                cells.append(f'<rect x="{x + 1:.1f}" y="2" width="{cw - 2:.1f}" height="{row - 4}" rx="3" class="s-unknown" '
                             f'{tip(text + "нет информации")}/>')
                continue
            top = max(mins, key=mins.get)
            parts = ", ".join(f"{STATUS_RU[k]} {mins[k] / total:.0%}" for k in STATUS_RANK if mins.get(k))
            cells.append(f'<rect x="{x + 1:.1f}" y="2" width="{cw - 2:.1f}" height="{row - 4}" rx="3" class="cell s-{top}" '
                         f'style="fill-opacity:{0.45 + 0.55 * mins[top] / total:.2f}" {tip(text + parts)}/>')
        out.append(f'<svg viewBox="0 0 {W} {row}" class="chart" {row_attrs(sid, visible[sid])}>'
                   + svg_label(sid, x0 - 6, row / 2) + "".join(cells) + "</svg>")
    axis = "".join(f'<text x="{x0 + cw * (h - h0):.1f}" y="13" class="tick" text-anchor="middle">{h}</text>'
                   for h in range(h0, 25, 2 if n <= 14 else 3))
    return f'<div class="rows">{"".join(out)}</div><svg viewBox="0 0 {W} 18" class="chart">{axis}</svg>'


def week_rows(stats, visible, now, h0):
    """Последние 7 дней (только часы сбора): строка на заправку, цвет — состояние."""
    x0, x1, row, gap = 112, W - 6, 16, 10
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
        label = REG[sid]["label"]
        segs = []
        for s_, e, status in (st["status"] if st else []):
            s_, e = max(s_, start), min(e, now)
            if e <= s_:
                continue
            xs_, xe = xt(s_), xt(e)
            if xe - xs_ < 0.3:
                continue  # отрезок целиком в часах без сбора
            segs.append(f'<rect x="{xs_ + 0.5:.1f}" y="{gap / 2}" width="{max(xe - xs_ - 1, 1.2):.1f}" height="{row}" rx="2" '
                        f'class="seg s-{status}" {tip(f"{label} · {s_:%d.%m %H:%M}–{e:%H:%M} · {STATUS_RU[status]}")}/>')
        out.append(f'<svg viewBox="0 0 {W} {row + gap}" class="chart" {row_attrs(sid, visible[sid])}>{grid}'
                   + svg_label(sid, x0 - 6, gap / 2 + row / 2) +
                   f'<rect x="{x0}" y="{gap / 2}" width="{x1 - x0}" height="{row}" rx="3" class="s-unknown"/>' + "".join(segs) + "</svg>")
    return f'<svg viewBox="0 0 {W} 16" class="chart">{head}</svg><div class="rows">{"".join(out)}</div>'


def events_list(stats, visible, now, limit=40):
    """Последние случаи, когда бензин появлялся и заканчивался, — простым списком (скрипт оставит нужные)."""
    items = []
    for sid, st in stats.items():
        if st:
            items += [(t, sid, "появился") for t in st["arrivals"]]
            items += [(t, sid, "закончился") for t in st["runouts"]]
    items = sorted((e for e in items if now - e[0] <= timedelta(days=7)), reverse=True)[:limit]
    lis = "".join(
        f'<li {row_attrs(sid, visible[sid])}><b>{t:%d.%m %H:%M}</b> {html.escape(REG[sid]["label"])} — '
        f'<span class="{"ev-on" if what == "появился" else "ev-off"}">бензин {what}</span></li>'
        for t, sid, what in items)
    return (f'<ul class="events">{lis}</ul><p class="muted ev-empty" hidden>Пока не было ни одного случая, '
            f'когда бензин появился или закончился: нужно больше данных.</p>')


def stats_section(db, now, sids, shown, fuels):
    stats = {sid: station_stats(db, sid, now, fuels) for sid in sids}
    visible = {sid: sid in shown for sid in sids}
    h0 = first_hour(stats)
    esc = html.escape
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
        st, attrs = stats[sid], row_attrs(sid, visible[sid])
        name_cell = f'<td>{esc(REG[sid]["label"])}</td>'
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
  <p class="note">«Есть» — есть {fuels_or(fuels)}.</p>{note}
  <table class="tbl est"><thead><tr><th>Заправка</th><th>Привозят</th><th>Кончается</th><th>Держится</th></tr></thead>
  <tbody class="rows">{"".join(rows)}</tbody></table>
  <h4>Как обычно по часам</h4>
  <p class="note">Цвет клетки — что чаще всего было в этот час (чем насыщеннее, тем чаще). Нажмите на клетку — подробности.</p>
  {STATUS_LEGEND}
  {heat_rows(stats, visible, h0)}
  <h4>Последние появления и окончания</h4>{events_list(stats, visible, now)}
  <h4>Последние 7 дней, {h0}:00–24:00</h4>
  {STATUS_LEGEND}
  {week_rows(stats, visible, now, h0)}
  <details><summary>Таблица по часам: доля времени, когда бензин есть</summary>
  <div class="dc"><div class="scroll"><table class="tbl hours"><thead><tr><th>Заправка</th>{"".join(f"<th>{h}</th>" for h in range(h0, 24))}</tr></thead>
  <tbody class="rows">{"".join(hour_rows)}</tbody></table></div></div></details>
</section>"""


# ---------- страница со сводкой ----------

PUMP_ICON = ('<span class="ico" aria-hidden="true"><svg viewBox="0 0 24 24" width="22" height="22" fill="none" '
             'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
             '<path d="M4 21V5a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v16"/><path d="M3 21h12"/><path d="M7 8h4"/>'
             '<path d="M14 10h2a2 2 0 0 1 2 2v4.5a1.5 1.5 0 0 0 3 0V8.5L18 5.5"/></svg></span>')


def write_report(db, path=None, sids=None, combos=None):
    """sids — все заправки, которые должны быть на странице (по умолчанию — заправки владельца);
    combos — наборы марок, для которых считать статистику (у разных пользователей разные).
    Без параметров ?s= и ?f= в ссылке показываются заправки и марки по умолчанию."""
    now = datetime.now(MSK)
    esc = html.escape
    load_registry(db)
    shown = default_sids()
    sids = [sid for sid in dict.fromkeys(list(sids or []) + shown) if sid in REG]
    combos = list(dict.fromkeys([tuple(DEFAULT_FUELS)] + [tuple(c) for c in (combos or [])]))
    since = (now - FRESH).isoformat()
    cards = []
    prices = latest_prices(db, sids)
    for sid, fuels, (cls, label, _, _) in ordered_stations(db, now, sids):
        rows, per_fuel = [], {}
        for fuel in FUEL_CHOICES:
            if fuel in fuels:
                seen_at, avail, status, queue, kind = fuels[fuel]
                f_cls, f_label, f_rank, f_when = station_state(fuels, now, [fuel])
                per_fuel[fuel] = [f_cls, f_label, f_rank, f_when.timestamp() if f_when else 0]
                old = " old" if now - seen_at > FRESH else ""
                hide = "" if fuel in DEFAULT_FUELS else " nosel"
                rows.append(
                    f'<tr class="{"maybe" if kind == "signal" else "yes" if avail else "no"}{old}{hide}" data-fuel="{esc(fuel)}">'
                    f'<td class="fuel">{fuel_name(fuel)}</td>'
                    f'<td>{status_html(avail, kind, seen_at, now)}</td><td>{short_when(seen_at, now)}</td>'
                    f'<td>{esc(queue or "—")}</td><td class="src">{KIND_RU[kind]}</td></tr>')
        empty = '<p class="muted nodata">Информации пока нет: за последние сутки отчётов по этой заправке и выбранным маркам не было.</p>'
        table = ('<table><tr><th>Марка</th><th>Статус</th><th>Когда</th><th>Очередь</th><th>Источник</th></tr>'
                 + "".join(rows) + "</table>" + empty) if rows else empty
        chips = []
        for fuel in FUEL_CHOICES:
            if fuel in prices.get(sid, {}):
                price, seen, prev, changed = prices[sid][fuel]
                delta = ""
                if prev is not None and price != prev:
                    up = price > prev
                    delta = (f' <span class="{"p-up" if up else "p-down"}" title="было {money(prev)}, изменилась {changed:%d.%m %H:%M}">'
                             f'{"↑" if up else "↓"}{money(abs(price - prev))[:-2]} с {changed:%d.%m}</span>')
                hide = "" if fuel in DEFAULT_FUELS else " nosel"
                chips.append(f'<span class="chip{hide}" data-fuel="{esc(fuel)}" title="ориентир на {seen:%d.%m %H:%M}">'
                             f'{fuel_name(fuel)} <b>{money(price)}</b>{delta}</span>')
        price_html = f'<p class="prices">💰 Ориентир: {"".join(chips)}</p>' if chips else ""
        hist = []
        for seen_at, fuel, avail, kind in db.execute(
                "SELECT seen_at, fuel, available, kind FROM obs WHERE address = ? AND seen_at >= ? ORDER BY seen_at DESC",
                (REG[sid]["address"], since)):
            if fuel in FUEL_CHOICES:
                t = datetime.fromisoformat(seen_at)
                hide = "" if fuel in DEFAULT_FUELS else ' class="nosel"'
                hist.append(f'<li data-fuel="{esc(fuel)}"{hide}><b>{t:%H:%M}</b> '
                            f'{fuel_name(fuel)} — {status_html(avail, kind, t, now)} <span class="muted">({KIND_RU[kind]})</span></li>')
        hist_html = (f'<details class="hist"><summary>Все отчёты за сутки (<span class="n">0</span>)</summary>'
                     f'<div class="dc"><ul>{"".join(hist[:60])}</ul></div></details>' if hist else "")
        title = REG[sid]["brand"] or "АЗС"
        cards.append(f'<section class="card st{"" if sid in shown else " nosel"}" data-sid="{sid}" '
                     f'data-st="{esc(json.dumps(per_fuel, ensure_ascii=False))}"><div class="head">{PUMP_ICON}'
                     f'<div class="ttl"><h2>{esc(title)}</h2><span class="sub">{esc(short_address(REG[sid]["address"]))}</span></div>'
                     f'<span class="badge {cls}">{label}</span></div>{table}{price_html}{hist_html}</section>')
    blocks = "".join(
        f'<div class="fblock{"" if i == 0 else " nosel"}" data-fuels="{esc(",".join(c))}">{stats_section(db, now, sids, shown, list(c))}</div>'
        for i, c in enumerate(combos))
    palette_light = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
    palette_dark = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
    s_light = " ".join(f"--s{i + 1}:{c};" for i, c in enumerate(palette_light))
    s_dark = " ".join(f"--s{i + 1}:{c};" for i, c in enumerate(palette_dark))
    page = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex, nofollow">
<title>Бензин · АИ-95/98</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script>
(function () {{  // в Telegram — тема как в самом Telegram
  const tg = window.Telegram && Telegram.WebApp;
  if (tg && tg.initData && tg.colorScheme) document.documentElement.dataset.theme = tg.colorScheme;
}})();
</script>
<style>
/* Тема: светлая по умолчанию; тёмная — по настройке устройства или теме Telegram (data-theme) */
:root {{ color-scheme: light;
  --bg:#e4e3df; --card:#efeeea; --raise:#f6f5f2; --text:#121212; --text2:#4a4944; --muted:#8a8983; --line:#d4d3cd;
  --chip:#dcdbd5; --accent:#e5483d; --accent-text:#fff; --tile:#141414; --tile-ink:#fff;
  --panel:#141414; --panel-text:#f3f2ee; --panel-text2:#c2c1bb; --panel-muted:#8c8b86; --panel-line:#2a2a28; --panel-nodata:#33332f;
  --track:#d8d7d1; --nodata:#cfcec8; --have:#1e9e48; --maybe:#b98a13; --none:#d6402f; --unknown:#8a8983; --off:#c3c2b7;
  --shadow:0 1px 0 rgba(0,0,0,.04), 0 8px 24px -16px rgba(0,0,0,.25); {s_light} }}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{ color-scheme: dark;
  --bg:#0e0e0d; --card:#1a1a19; --raise:#222220; --text:#f3f2ee; --text2:#c4c3bd; --muted:#8b8a85; --line:#2b2b29;
  --chip:#262624; --tile:#f1f0ec; --tile-ink:#141414;
  --panel:#1f1f1d; --panel-line:#30302d; --panel-nodata:#3a3a36;
  --track:#2a2a28; --nodata:#3a3a36; --have:#2fbf5c; --maybe:#e0a72a; --none:#ef5a49; --off:#55544f;
  --shadow:none; {s_dark} }} }}
:root[data-theme="dark"] {{ color-scheme: dark;
  --bg:#0e0e0d; --card:#1a1a19; --raise:#222220; --text:#f3f2ee; --text2:#c4c3bd; --muted:#8b8a85; --line:#2b2b29;
  --chip:#262624; --tile:#f1f0ec; --tile-ink:#141414;
  --panel:#1f1f1d; --panel-line:#30302d; --panel-nodata:#3a3a36;
  --track:#2a2a28; --nodata:#3a3a36; --have:#2fbf5c; --maybe:#e0a72a; --none:#ef5a49; --off:#55544f;
  --shadow:none; {s_dark} }}
* {{ -webkit-tap-highlight-color:transparent; }}
body {{ margin:0; background:var(--bg); color:var(--text);
  font:15px/1.5 "Manrope", system-ui, -apple-system, "Segoe UI", sans-serif; -webkit-font-smoothing:antialiased; }}
main {{ max-width:760px; margin:0 auto; padding:22px 16px 48px; }}
h1 {{ font-size:26px; font-weight:500; letter-spacing:-.01em; margin:0; padding-bottom:16px; border-bottom:1px solid var(--line); color:var(--muted); }}
h1 b {{ color:var(--text); font-weight:800; }} h1 .ago {{ font-size:.7em; white-space:nowrap; }}
h2 {{ font-size:17px; font-weight:700; margin:0; letter-spacing:-.01em; }}
h4 {{ font-size:14px; font-weight:600; color:inherit; margin:20px 0 6px; }}
.muted {{ color:var(--muted); }} .note {{ color:var(--text2); font-size:13px; margin:4px 0; }}
.nosel {{ display:none !important; }}
/* верхняя строка: время обновления и красная кнопка-пилюля */
.topbar {{ display:flex; gap:12px; align-items:center; justify-content:space-between; flex-wrap:wrap; margin-top:14px; }}
.topbar .muted {{ flex:1 1 260px; font-size:14px; }}
.topbar .muted b {{ color:var(--text); font-weight:700; }}
.refresh svg {{ vertical-align:-3px; margin-right:4px; }}
.refresh {{ flex:none; background:var(--accent); color:var(--accent-text); text-decoration:none; font-weight:700; font-size:14px;
  padding:11px 18px; border-radius:999px; box-shadow:0 8px 20px -10px var(--accent); }}
.refresh:active {{ transform:scale(.97); }}
.hint {{ margin-top:12px; padding:12px 14px; border-radius:16px; background:var(--card); color:var(--text2); font-size:14px; }}
h2.section {{ font-size:20px; font-weight:600; margin:30px 0 4px; }}
/* карточки заправок — как строки «Transactions history» */
.card {{ background:var(--card); border-radius:22px; padding:16px; margin-top:12px; box-shadow:var(--shadow); }}
.card .nodata {{ display:none; }} .card.empty .nodata {{ display:block; }} .card.empty table {{ display:none; }}
.head {{ display:flex; align-items:center; gap:12px; margin-bottom:12px; }}
.ico {{ flex:none; width:46px; height:46px; border-radius:14px; background:var(--tile); color:var(--tile-ink); display:grid; place-items:center; }}
.ttl {{ flex:1; min-width:0; }} .ttl h2 {{ font-size:16px; }}
.ttl .sub {{ display:block; color:var(--muted); font-size:13px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.badge {{ flex:none; font-size:12px; font-weight:700; padding:6px 12px; border-radius:999px; color:#fff; white-space:nowrap; }}
.badge.have {{ background:var(--have); }} .badge.stale {{ background:#fab219; color:#1b1500; }}
.badge.term {{ background:transparent; color:var(--none); box-shadow:inset 0 0 0 1.5px var(--none); }}
.badge.none {{ background:var(--none); }} .badge.unknown {{ background:var(--chip); color:var(--text2); }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th {{ text-align:left; color:var(--muted); font-weight:500; font-size:12px; padding:6px; border-bottom:1px solid var(--line); }}
td {{ padding:9px 6px; border-bottom:1px solid var(--line); font-variant-numeric: tabular-nums; }}
tr:last-child td {{ border-bottom:none; }}
tr.old td {{ opacity:.5; }} .fuel {{ font-weight:700; }} .src {{ color:var(--muted); font-size:12px; }}
/* раскрывающиеся списки */
details {{ margin-top:10px; }} ul {{ margin:6px 0 0; padding-left:18px; }}
summary {{ cursor:pointer; color:var(--text2); font-weight:600; font-size:14px; list-style:none; user-select:none; padding:4px 0; }}
summary::-webkit-details-marker {{ display:none; }}
summary::before {{ content:"›"; display:inline-block; width:1em; font-size:18px; line-height:1; text-align:center;
  transition:transform .4s cubic-bezier(.25,.8,.3,1); }}
details[open] > summary::before {{ transform:rotate(90deg); }}
details > .dc {{ overflow:hidden; }}
details > .dc > * {{ contain:content; will-change:opacity, transform; }}  /* содержимое не пересчитывается на каждом кадре */
.hist li {{ margin:3px 0; color:var(--text2); }}
@media (prefers-reduced-motion: reduce) {{ summary::before {{ transition:none; }} }}
/* статусы в таблицах */
.st-q {{ color:var(--maybe); font-weight:800; font-size:1.1em; cursor:help; }}
.st-qr {{ color:var(--none); font-weight:800; font-size:1.1em; cursor:help; }} .st-no {{ color:var(--none); font-weight:700; }}
.st-yes {{ color:var(--have); font-weight:700; }}
/* цены — пилюли */
.prices {{ margin:12px 0 0; font-size:13px; color:var(--muted); display:flex; flex-wrap:wrap; gap:6px; align-items:center; }}
.prices .chip {{ background:var(--chip); color:var(--text2); border-radius:999px; padding:5px 11px; }}
.prices .chip b {{ color:var(--text); font-weight:700; }} .p-up {{ color:var(--none); font-size:12px; }} .p-down {{ color:var(--have); font-size:12px; }}
.card > table:not(.tbl) {{ table-layout:fixed; }}
.card > table:not(.tbl) th:nth-child(1) {{ width:16%; }} .card > table:not(.tbl) th:nth-child(2) {{ width:14%; }}
.card > table:not(.tbl) th:nth-child(3) {{ width:20%; }} .card > table:not(.tbl) th:nth-child(4) {{ width:20%; }}
/* статистика — тёмная панель, как карточка с графиком на макете */
.card.stats {{ background:var(--panel); color:var(--panel-text); padding:18px 16px;
  --text:var(--panel-text); --text2:var(--panel-text2); --muted:var(--panel-muted); --line:var(--panel-line); --nodata:var(--panel-nodata); }}
.legend {{ display:flex; flex-wrap:wrap; gap:6px 14px; font-size:13px; color:var(--text2); margin:8px 0 6px; }}
.k {{ display:inline-block; width:12px; height:12px; border-radius:4px; margin-right:6px; vertical-align:-1px; }}
.k.track {{ background:var(--track); }} .k.off {{ background:var(--off); }}
.chart {{ width:100%; height:auto; display:block; }} .stats .chart {{ max-width:560px; }} .scroll {{ overflow-x:auto; }}
.chart .grid {{ stroke:var(--line); stroke-width:1; }}
.chart .tick {{ fill:var(--muted); font-size:11px; font-variant-numeric: tabular-nums; font-family:inherit; }}
.chart .label {{ fill:var(--text2); font-size:11.5px; font-weight:600; }}
.chart .brand {{ fill:var(--muted); font-size:10px; }}
.chart .track {{ fill:var(--track); }}
.s-have {{ fill:#22b14c; background:#22b14c; }} .s-stale {{ fill:#fab219; background:#fab219; }}
.s-term {{ fill:#f08452; background:#f08452; }} .s-none {{ fill:#e5483d; background:#e5483d; }}
.s-unknown {{ fill:var(--nodata); background:var(--nodata); }}
.chart .cell:hover, .chart .cell:focus, .chart .seg:hover, .chart .seg:focus {{ stroke:var(--text); stroke-width:1.5; outline:none; }}
.events {{ list-style:none; padding:0; margin:6px 0 0; }} .events li {{ padding:7px 0; border-bottom:1px solid var(--line); font-size:14px; }}
.events li:last-child {{ border-bottom:none; }}
.ev-on {{ font-weight:700; color:#22b14c; }} .ev-off {{ color:#f0645a; }}
.card > table.est {{ table-layout:auto; }} .card > table.est th {{ width:auto; }}
.est td {{ vertical-align:top; }} .est td:first-child {{ width:34%; font-weight:600; }} .est small {{ display:block; color:var(--muted); font-size:12px; }}
.hours td, .hours th {{ text-align:center; white-space:nowrap; }} .hours td:first-child, .hours th:first-child {{ text-align:left; }}
.tbl td, .tbl th {{ padding:5px 6px; font-size:13px; }}
.foot {{ margin-top:22px; font-size:13px; color:var(--muted); }}
#tip {{ position:fixed; pointer-events:none; background:var(--raise); color:var(--text); border-radius:12px;
  padding:8px 11px; font-size:13px; box-shadow:0 10px 30px -10px rgba(0,0,0,.35); display:none; max-width:280px; z-index:10; white-space:pre-line; }}
@media (max-width:560px) {{ .card > table:not(.tbl) th:nth-child(5), .card > table:not(.tbl) td:nth-child(5) {{ display:none; }} }}
</style></head><body data-updated="{now.isoformat()}"><main>
<h1><b>{now:%H:%M}</b>, {now:%d.%m.%Y} <span class="ago" id="ago"></span></h1>
<div class="topbar">
  <div class="muted">Сводка по <b id="ftitle">{fuels_title(DEFAULT_FUELS)}</b>. Сбор с 7:00 до 24:00 каждые 10 минут, ночью не ведётся.</div>
  <a class="refresh" id="refresh" href="{RUN_URL}" target="_blank" rel="noopener"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 11a8 8 0 1 0-2.3 5.7"/><path d="M20 4v7h-7"/></svg> Обновить сейчас</a>
  <div class="muted nosel" id="refresh-bot">Обновить данные — кнопка «🔄 Обновить» в боте.</div>
</div>
<div class="hint" id="hint" hidden>Нажмите <b>Run workflow</b> на GitHub. Примерно через 1–2 минуты эта страница обновится сама.
Если на GitHub запрос отметится как «Cancelled» — это нормально: сервер уже выполнил сбор.</div>
<h2 class="section">Сводка сейчас</h2>
<p class="hint nosel" id="empty-sel">Вы ещё не выбрали заправки — нажмите «📍 Заправки» в боте.</p>
<div id="cards">{"".join(cards)}</div>
<h2 class="section">Статистика: когда привозят и когда заканчивается</h2>
<div class="muted">Цвет — состояние: есть, давно не подтверждали, только терминал, нет, нет информации.
Нажмите на график, чтобы увидеть подробности.</div>
<p class="note nosel" id="fnote">Статистика по вашему набору марок появится при следующем обновлении страницы (до 10 минут); пока показана по АИ-95/98.</p>
{blocks}
<p class="foot">💰 «Ориентир» — ориентировочная цена из отчётов водителей (обычно одинакова для всей сети);
↑ — подорожало, ↓ — подешевело с указанной даты. «Водитель» — отчёт водителя с заправки. «Общая сводка» — подтверждённые данные за последний час.
«Терминалы оплаты» — топливо продаётся по данным касс, но водители ещё не подтвердили; если рядом по времени есть отчёт водителя, в статистике учитывается он.
Состояние считается неизменным до следующего отчёта, но не дольше 2 часов; дальше — «нет данных».</p>
</main><div id="tip" role="tooltip"></div>
<script>
// свои заправки (?s=код,код,…) и марки (?f=92,95,…): показываем только их
const params = new URLSearchParams(location.search);
const sel = (params.get('s') || '').split(',').filter(Boolean);
const ALL_FUELS = {json.dumps(FUEL_CHOICES, ensure_ascii=False)};
const fuels = ((params.get('f') || '').split(',').filter(f => ALL_FUELS.includes(f)));
const wanted = fuels.length ? ALL_FUELS.filter(f => fuels.includes(f)) : {json.dumps(DEFAULT_FUELS, ensure_ascii=False)};
if (fuels.length) {{
  const names = {json.dumps(FUEL_RU, ensure_ascii=False)};
  const ai = wanted.filter(f => f !== 'ДТ');
  document.getElementById('ftitle').textContent = [ai.length ? 'АИ-' + ai.join('/') : '', wanted.includes('ДТ') ? 'ДТ' : ''].filter(Boolean).join(', ');
  document.querySelectorAll('[data-fuel]').forEach(el => el.classList.toggle('nosel', !wanted.includes(el.dataset.fuel)));
  // статус заправки — по лучшей из выбранных марок (как на сервере): есть → было давно → терминал → нет → нет данных
  const cards = [...document.querySelectorAll('section.card.st')];
  cards.forEach(card => {{
    const st = JSON.parse(card.dataset.st || '{{}}');
    let best = null;
    wanted.forEach(f => {{ const v = st[f]; if (v && (!best || v[2] < best[2] || (v[2] === best[2] && v[3] > best[3]))) best = v; }});
    best = best || ['unknown', 'Нет информации', 4, 0];
    const badge = card.querySelector('.badge');
    badge.className = 'badge ' + best[0]; badge.textContent = best[1];
    card.dataset.rank = best[2]; card.dataset.ts = best[3];
  }});
  cards.sort((a, b) => (a.dataset.rank - b.dataset.rank) || (b.dataset.ts - a.dataset.ts))
       .forEach(c => document.getElementById('cards').appendChild(c));
  const key = wanted.join(',');
  const blocks = [...document.querySelectorAll('.fblock')];
  const match = blocks.find(b => b.dataset.fuels === key);
  blocks.forEach(b => b.classList.toggle('nosel', b !== (match || blocks[0])));
  document.getElementById('fnote').classList.toggle('nosel', !!match);
}}
document.querySelectorAll('section.card.st').forEach(card => {{
  card.classList.toggle('empty', !card.querySelector('tr[data-fuel]:not(.nosel)'));
  const n = card.querySelector('.hist .n');
  if (n) n.textContent = card.querySelectorAll('.hist li:not(.nosel)').length;
}});
if (sel.length) {{
  document.querySelectorAll('[data-sid]').forEach(el => {{
    const i = sel.indexOf(el.dataset.sid);
    el.classList.toggle('nosel', i < 0);
  }});
  document.querySelectorAll('.rows').forEach(box => {{
    [...box.children].filter(c => c.dataset.sid)
      .sort((a, b) => sel.indexOf(a.dataset.sid) - sel.indexOf(b.dataset.sid)).forEach(c => box.appendChild(c));
  }});
}}
if (sel.includes('-')) document.getElementById('empty-sel').classList.remove('nosel');
document.querySelectorAll('.fblock:not(.nosel) .events, .fblock .events').forEach(ul => {{
  const shown = [...ul.children].filter(li => !li.classList.contains('nosel'));
  shown.slice(12).forEach(li => li.classList.add('nosel'));
  if (!shown.length) ul.nextElementSibling.hidden = false;
}});
// «Обновить сейчас» (запуск сбора на GitHub) доступен только владельцу; в Telegram у остальных — подсказка про кнопку в боте
if (window.Telegram && Telegram.WebApp && Telegram.WebApp.initData && params.get('own') !== '1') {{
  document.getElementById('refresh').classList.add('nosel');
  document.getElementById('refresh-bot').classList.remove('nosel');
}}
// «N мин назад» и автообновление, когда сервер опубликует более свежую сводку
const updated = new Date(document.body.dataset.updated);
function tickAgo() {{ const m = Math.round((Date.now() - updated) / 60000);
  const h = Math.floor(m / 60), d = Math.floor(h / 24);
  document.getElementById('ago').textContent = '(' + (m < 1 ? 'только что' : m < 60 ? `${{m}} мин назад`
    : h < 24 ? `${{h}} ч${{m % 60 ? ' ' + m % 60 + ' мин' : ''}} назад` : `${{d}} дн назад`) + ')'; }}
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
if (window.Telegram && Telegram.WebApp && Telegram.WebApp.initData) {{
  const bg = getComputedStyle(document.documentElement).getPropertyValue('--bg').trim();
  try {{ Telegram.WebApp.setHeaderColor(bg); Telegram.WebApp.setBackgroundColor(bg); }} catch (e) {{}}
  Telegram.WebApp.ready();
}}
// плавное раскрытие и сворачивание списков: блок меняет высоту, а содержимое внутри отдельно
// проявляется и «опускается» на место. Высоту фиксируем до старта и держим до конца — без мелькания.
const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const EASE_OUT = 'cubic-bezier(.25,.8,.3,1)', EASE_IN_OUT = 'cubic-bezier(.45,0,.25,1)';
document.querySelectorAll('details').forEach(d => {{
  const s = d.querySelector(':scope > summary'), c = d.querySelector(':scope > .dc');
  if (!s || !c || reduceMotion) return;
  const inner = c.firstElementChild;
  let anims = [];
  const stop = () => {{ anims.forEach(a => a.cancel()); anims = []; }};
  const reset = () => {{ c.style.height = ''; inner.style.opacity = ''; inner.style.transform = ''; }};
  s.addEventListener('click', e => {{
    e.preventDefault();
    const cur = d.open ? c.getBoundingClientRect().height : 0;   // видимая высота сейчас (в том числе посреди анимации)
    const curOpacity = d.open ? parseFloat(getComputedStyle(inner).opacity) : 0;
    stop();
    if (d.open && c.dataset.state !== 'closing') {{
      c.dataset.state = 'closing';
      c.style.height = cur + 'px';
      const box = c.animate([{{height: cur + 'px'}}, {{height: '0px'}}],
                            {{duration: 340, delay: 40, easing: EASE_IN_OUT, fill: 'forwards'}});
      const fade = inner.animate([{{opacity: curOpacity, transform: 'translateY(0)'}}, {{opacity: 0, transform: 'translateY(-6px)'}}],
                                 {{duration: 180, easing: 'ease-in', fill: 'forwards'}});
      anims = [box, fade];
      box.finished.then(() => {{ d.open = false; stop(); reset(); delete c.dataset.state; }}).catch(() => {{}});
    }} else {{
      c.dataset.state = 'opening';
      c.style.height = cur + 'px';
      d.open = true;
      const full = c.scrollHeight;
      const box = c.animate([{{height: cur + 'px'}}, {{height: full + 'px'}}],
                            {{duration: 460, easing: EASE_OUT, fill: 'forwards'}});
      const show = inner.animate([{{opacity: curOpacity, transform: 'translateY(-8px)'}}, {{opacity: 1, transform: 'translateY(0)'}}],
                                 {{duration: 380, delay: cur ? 0 : 80, easing: EASE_OUT, fill: 'both'}});
      anims = [box, show];
      box.finished.then(() => {{ stop(); reset(); delete c.dataset.state; }}).catch(() => {{}});
    }}
  }});
}});
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
    """Заправки пользователя. Владелец, пока не выбирал, — заправки по умолчанию; новые пользователи — пусто."""
    entry = subs["owner"] if cid == owner_id() else subs["subscribers"].get(cid, {})
    if "stations" in entry:
        return clean_selection(entry["stations"])
    return default_sids() if cid == owner_id() else []


def fuel_selection(subs, cid):
    """Марки пользователя. Владелец, пока не выбирал, — марки по умолчанию; новые пользователи — пусто."""
    entry = subs["owner"] if cid == owner_id() else subs["subscribers"].get(cid, {})
    if "fuels" in entry:
        return [f for f in FUEL_CHOICES if f in entry["fuels"]]
    return list(DEFAULT_FUELS) if cid == owner_id() else []


def set_fuels(subs, cid, fuels):
    entry = subs["owner"] if cid == owner_id() else subs["subscribers"][cid]
    entry["fuels"] = fuels


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


BOT_NAME = "Мои заправки"
BTN_STATIONS, BTN_FUELS, BTN_REFRESH, BTN_PAUSE = "📍 Заправки", "🛢 Марки", "🔄 Обновить", "🔕 Пауза"
KEYBOARD = {"keyboard": [[{"text": BTN_STATIONS}, {"text": BTN_FUELS}], [{"text": BTN_REFRESH}, {"text": BTN_PAUSE}]],
            "resize_keyboard": True, "is_persistent": True}
KEYBOARD_VERSION = 3  # увеличить, если поменяются кнопки — тогда бот пришлёт их всем заново
BTN_SUBS, BTN_WAIT, BTN_REQUEST = "👥 Подписчики", "⏳ Ожидаю подтверждения", "📨 Отправить запрос"
REQUEST_KEYBOARD = {"keyboard": [[{"text": BTN_REQUEST}]], "resize_keyboard": True, "is_persistent": True}
INTRO_TEXT = ("👋 Здравствуйте! Это бот «Мои заправки» — он показывает, где в Воронеже есть нужное топливо, "
              "и присылает оповещения, когда оно появляется на выбранных вами заправках.\n\n"
              "Доступ к боту выдаёт его владелец. Нажмите кнопку <b>«📨 Отправить запрос»</b> внизу — "
              "как только владелец подтвердит, я напишу.")
BOT_DESCRIPTION = ("Показываю, где в Воронеже есть АИ-92, АИ-95, АИ-98 и дизель, и присылаю оповещения, когда топливо "
                   "появляется на выбранных вами заправках. Нажмите «Запустить», а затем «📨 Отправить запрос».")
BOT_SHORT_DESCRIPTION = "Где в Воронеже есть бензин: оповещения и сводка по вашим заправкам."
OWNER_KEYBOARD = {"keyboard": [[{"text": BTN_STATIONS}, {"text": BTN_FUELS}], [{"text": BTN_REFRESH}, {"text": BTN_PAUSE}],
                               [{"text": BTN_SUBS}]], "resize_keyboard": True, "is_persistent": True}
OWNER_KEYBOARD_VERSION = 4
WAIT_KEYBOARD = {"keyboard": [[{"text": BTN_WAIT}]], "resize_keyboard": True, "is_persistent": True}
WAIT_TEXT = ("⏳ <b>Ваш запрос ожидает подтверждения.</b>\n\n"
             "Я отправил его владельцу бота. Как только он подтвердит доступ, я сразу напишу — "
             "и вы сможете выбрать марки топлива и заправки, о которых хотите получать информацию.")


def pause_state(entry, now):
    """→ None (уведомления включены), "forever" или время окончания паузы."""
    v = (entry or {}).get("paused_until")
    if v == "forever":
        return "forever"
    if v:
        t = datetime.fromisoformat(v)
        return t if t > now else None
    return None


def pause_text(state, now):
    if state == "forever":
        return "пока вы их не включите"
    return "до " + (f"{state:%H:%M}" if state.date() == now.date() else f"{state:%d.%m %H:%M}")


def pause_menu(entry, now):
    state = pause_state(entry, now)
    if state:
        return (f"🔕 Уведомления на паузе {pause_text(state, now)}.",
                [[{"text": "🔔 Включить сейчас", "callback_data": "pz:on"}]])
    return ("На сколько поставить уведомления на паузу?",
            [[{"text": "1 час", "callback_data": "pz:1"}, {"text": "3 часа", "callback_data": "pz:3"}],
             [{"text": f"До утра ({WORK_START}:00)", "callback_data": "pz:morning"},
              {"text": "Пока не включу", "callback_data": "pz:forever"}]])


def handle_pause_cb(token, cb, subs):
    cid, data, msg = str(cb["from"]["id"]), cb.get("data", ""), cb.get("message", {})
    tg_call(token, "answerCallbackQuery", callback_query_id=cb["id"])
    entry = user_entry(subs, cid)
    if entry is None:
        return False
    now = datetime.now(MSK)
    action = data.split(":", 1)[1]
    if action == "on":
        entry.pop("paused_until", None)
        text = "🔔 Уведомления включены."
    else:
        if action == "forever":
            entry["paused_until"] = "forever"
        elif action == "morning":
            t = now.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
            entry["paused_until"] = (t if t > now else t + timedelta(days=1)).isoformat()
        else:
            entry["paused_until"] = (now + timedelta(hours=int(action))).isoformat()
        text = (f"🔕 Уведомления на паузе {pause_text(pause_state(entry, now), now)}.\n"
                "Сводка и кнопка «🔄 Обновить» работают как обычно. Включить раньше — кнопка «🔕 Пауза».")
    tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), text=text, parse_mode="HTML")
    return True


def check_pause_expiry():
    """Сообщить тем, у кого пауза закончилась сама."""
    if not bot_ready():
        return
    token, now = os.environ["TELEGRAM_TOKEN"], datetime.now(MSK)
    subs, changed = load_subs(), False
    people = [(owner_id(), subs["owner"])] + list(subs["subscribers"].items())
    for cid, entry in people:
        v = entry.get("paused_until")
        if v and v != "forever" and datetime.fromisoformat(v) <= now:
            entry.pop("paused_until")
            changed = True
            try:
                say(token, cid, "🔔 Пауза закончилась — уведомления снова включены.")
            except Exception as e:
                print(f"пауза …{cid[-4:]}: {e}", file=sys.stderr)
    if changed:
        save_subs(subs)


def paused_ids():
    if not os.environ.get("SUBSCRIBERS_KEY"):
        return set()
    subs, now = load_subs(), datetime.now(MSK)
    people = [(owner_id(), subs["owner"])] + list(subs["subscribers"].items())
    return {cid for cid, entry in people if pause_state(entry, now)}


def keyboard_for(cid):
    return OWNER_KEYBOARD if cid == owner_id() else KEYBOARD


def keyboard_version(cid):
    return OWNER_KEYBOARD_VERSION if cid == owner_id() else KEYBOARD_VERSION
KEYBOARD_TEXT = ("Внизу — кнопки:\n📍 Заправки — выбрать заправки\n🛢 Марки — выбрать марки топлива\n"
                 "🔄 Обновить — собрать свежие данные прямо сейчас\n"
                 "🔕 Пауза — временно не присылать оповещения\n"
                 "⛽ Сводка (слева) — сводка и статистика по вашим заправкам.")
REFRESH = {"waiting": set(), "last": 0.0}  # кто нажал «Обновить» и когда данные обновлялись в последний раз
REFRESH_MIN = 60  # не чаще раза в минуту

WELCOME = (
    "👋 Здравствуйте! Я бот «Мои заправки» — помогаю найти топливо на заправках Воронежа.\n\n"
    "<b>Что я умею</b>\n"
    "🔔 <b>Оповещения</b> — пишу, как только на ваших заправках появляется нужное топливо. "
    "Источник — отчёты водителей и данные терминалов оплаты.\n"
    "⛽ <b>Сводка</b> — кнопка слева от поля ввода: где топливо есть прямо сейчас, очереди, "
    "а также статистика — когда его обычно привозят и когда оно заканчивается.\n"
    "📍 <b>Заправки</b> и 🛢 <b>Марки</b> — кнопки под полем ввода: можно выбрать до 10 заправок и нужные марки.\n"
    "🔄 <b>Обновить</b> — собрать свежие данные прямо сейчас.\n"
    "🔕 <b>Пауза</b> — временно не присылать оповещения (на час, до утра или пока не включите).\n\n"
    "Данные обновляются сами с 7:00 до 24:00 каждые 10 минут.\n\n"
    "<b>Шаг 1 из 2.</b> Выберите марки топлива, о которых хотите получать информацию, и нажмите «Далее»:")
STEP2 = ("<b>Шаг 2 из 2.</b> Выберите заправки (до 10), о которых хотите получать информацию.\n"
         "Сначала выберите сеть, затем отметьте нужные заправки и нажмите «Готово»:")


def user_entry(subs, cid):
    return subs["owner"] if cid == owner_id() else subs["subscribers"].get(cid)


def start_onboarding(token, subs, cid):
    """Приветствие: что умеет бот + шаг 1 (марки). Дальше — шаг 2 (заправки) и итог."""
    entry = user_entry(subs, cid)
    entry["onboarding"] = "fuels"
    _, rows = fuels_view(fuel_selection(subs, cid), onboarding=True)
    say(token, cid, WELCOME, {"inline_keyboard": rows})


def finish_onboarding(token, subs, cid):
    entry = user_entry(subs, cid)
    entry.pop("onboarding", None)
    entry["kb"] = keyboard_version(cid)
    sids, fuels = selection(subs, cid), fuel_selection(subs, cid)
    say(token, cid, f"🎉 <b>Всё готово!</b>\n\nБуду писать, когда на ваших заправках появится {fuels_or(fuels)}.\n"
                    "Сводка и статистика — кнопка «⛽ Сводка» слева от поля ввода.\n"
                    "Изменить выбор — кнопки «📍 Заправки» и «🛢 Марки» внизу.", keyboard_for(cid))


HELP_SUB = ("Бот присылает оповещение, когда на ваших заправках появляется нужное вам топливо.\n"
            "📍 Заправки — выбрать заправки (до 10)\n🛢 Марки — выбрать марки топлива\n🔄 Обновить — свежие данные сейчас\n"
            "🔕 Пауза — временно не присылать оповещения\n"
            "Сводка со статистикой — кнопка «⛽ Сводка» внизу.\n/stop — отписаться.")


# --- выбор заправок: /stations ---

def brand_of(sid):
    return REG[sid]["brand"] or "Другие"


def brand_key(brand):
    return hashlib.sha1(brand.encode()).hexdigest()[:4]


def brands(sids=()):
    """→ [(название сети, ключ, число заправок)] — сначала крупные сети."""
    counts = {}
    for sid in REG:
        if listable(sid, sids):
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
        return [{"text": f"{mark} {REG[sid]['label']}"[:60], "callback_data": f"st:t:{sid}:{back}"}]

    if view == "home":
        rows, row = [], []
        for b, key, n in brands(sids):
            row.append({"text": f"{b} · {n}", "callback_data": f"st:b:{key}"})
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([{"text": f"✔️ Мои ({len(sids)})", "callback_data": "st:my"}, {"text": "⛽ Марки", "callback_data": "fu:home"},
                     {"text": "Готово", "callback_data": "st:done"}])
        text = (f"<b>Ваши заправки</b> ({len(sids)} из {MAX_STATIONS}):\n{chosen_list()}\n\n"
                "Выберите сеть, чтобы добавить или убрать заправки:")
        return text, rows
    if view == "my":
        rows = [toggle_btn(s, "my") for s in sids]
        rows.append([{"text": "◀ Все сети", "callback_data": "st:home"}, {"text": "Готово", "callback_data": "st:done"}])
        return f"<b>Мои заправки</b> ({len(sids)} из {MAX_STATIONS}) — нажмите, чтобы убрать:", rows
    key, _, page = view.partition(".")  # ключ сети и номер страницы списка
    page = int(page or 0)
    brand = next((b for b, k, _ in brands(sids) if k == key), None)
    if brand is None:
        return stations_view(sids, "home")
    in_brand = sorted((s for s in REG if brand_of(s) == brand and listable(s, sids)), key=lambda s: REG[s]["label"])
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
    if parts[1] == "done" and not sids:
        notice = "Выберите хотя бы одну заправку: откройте сеть и отметьте нужные."
    if changed:
        set_selection(subs, cid, sids)
        set_menu_button(token, cid, sids, fuel_selection(subs, cid))
    tg_call(token, "answerCallbackQuery", callback_query_id=cb["id"], **({"text": notice, "show_alert": "true"} if notice else {}))
    entry = user_entry(subs, cid)
    if parts[1] == "done" and sids and entry.get("onboarding") == "stations":  # шаг 2 пройден → итог
        tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), parse_mode="HTML",
                text="✅ <b>Шаг 2 из 2.</b> Ваши заправки:\n" + "\n".join(f"• {html.escape(REG[s]['name'])}" for s in sids),
                reply_markup=page_button(datetime.now(MSK), sids, fuel_selection(subs, cid), cid))
        finish_onboarding(token, subs, cid)
        return True
    if parts[1] == "done" and sids:
        text = ("✅ Сохранено. Ваши заправки:\n" + "\n".join(f"• {html.escape(REG[s]['name'])}" for s in sids)
                + "\n\nОповещения будут приходить по ним, сводка — кнопка «⛽ Сводка» (обновится примерно через минуту). "
                "Изменить — кнопка «📍 Заправки».")
        tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), text=text, parse_mode="HTML",
                reply_markup=page_button(datetime.now(MSK), sids, fuel_selection(subs, cid), cid))
        return changed
    text, rows = stations_view(sids, view)
    if view == "home" and entry.get("onboarding") == "stations":
        text = STEP2
    tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), text=text, parse_mode="HTML",
            reply_markup=json.dumps({"inline_keyboard": rows}))
    return changed


def fuels_view(fuels, onboarding=False):
    rows, row = [], []
    for f in FUEL_CHOICES:
        row.append({"text": f"{'✅' if f in fuels else '▫️'} {fuel_name(f)}{' (Pulsar)' if f == '95+' else ''}",
                    "callback_data": f"fu:t:{f}"})
        if len(row) == 2:
            rows.append(row)
            row = []
    rows += [row] if row else []
    rows.append([{"text": "Далее →" if onboarding else "Готово", "callback_data": "fu:done"}])
    return ("<b>Марки топлива</b> — по ним приходят оповещения и строится сводка.\n"
            f"Сейчас: {fuels_title(fuels)}. Нажмите, чтобы добавить или убрать:", rows)


def handle_fuels_cb(token, cb, subs):
    cid, data = str(cb["from"]["id"]), cb.get("data", "")
    msg = cb.get("message", {})
    if cid != owner_id() and cid not in subs["subscribers"]:
        tg_call(token, "answerCallbackQuery", callback_query_id=cb["id"], text="Сначала отправьте /start")
        return False
    fuels, changed, notice = fuel_selection(subs, cid), False, None
    if data.startswith("fu:t:"):
        f = data[5:]
        if f in FUEL_CHOICES:
            fuels = [x for x in FUEL_CHOICES if (x in fuels) != (x == f)]
            changed = True
    if data == "fu:done" and not fuels:
        notice = "Выберите хотя бы одну марку."
        data = "fu:home"
    if changed:
        set_fuels(subs, cid, fuels)
        set_menu_button(token, cid, selection(subs, cid), fuels)
    tg_call(token, "answerCallbackQuery", callback_query_id=cb["id"], **({"text": notice, "show_alert": "true"} if notice else {}))
    entry = user_entry(subs, cid)
    if data == "fu:done" and entry.get("onboarding") == "fuels":  # шаг 1 пройден → шаг 2
        entry["onboarding"] = "stations"
        tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), parse_mode="HTML",
                text=WELCOME.split("<b>Шаг 1")[0] + f"✅ <b>Шаг 1 из 2.</b> Марки: {fuels_title(fuels)}.")
        _, rows = stations_view(selection(subs, cid), "home")
        say(token, cid, STEP2, {"inline_keyboard": rows})
        return True
    if data == "fu:done":
        tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), parse_mode="HTML",
                text=f"✅ Сохранено. Марки: {fuels_title(fuels)}.\nОповещения и сводка — по ним. Изменить — кнопка «🛢 Марки».",
                reply_markup=page_button(datetime.now(MSK), selection(subs, cid), fuels, cid))
        return changed
    onboarding = entry.get("onboarding") == "fuels"
    text, rows = fuels_view(fuels, onboarding)
    tg_call(token, "editMessageText", chat_id=cid, message_id=msg.get("message_id"), parse_mode="HTML",
            text=WELCOME if onboarding else text, reply_markup=json.dumps({"inline_keyboard": rows}))
    return changed


# --- сообщения и кнопки бота ---

def handle_message(token, msg, subs):
    chat, user = msg.get("chat", {}), msg.get("from", {})
    if chat.get("type") != "private":
        return False
    cid, text = str(chat["id"]), (msg.get("text") or "").strip()
    owner = owner_id()
    text = {BTN_STATIONS: "/stations", BTN_FUELS: "/fuels", BTN_REFRESH: "/refresh", BTN_SUBS: "/list",
            BTN_PAUSE: "/pause"}.get(text, text)
    if text.startswith("/pause") and (cid == owner or cid in subs["subscribers"]):
        menu_text, rows = pause_menu(user_entry(subs, cid), datetime.now(MSK))
        say(token, cid, menu_text, {"inline_keyboard": rows})
        return False
    if text.startswith("/refresh") and (cid == owner or cid in subs["subscribers"]):
        if time.time() - REFRESH["last"] < REFRESH_MIN:
            say(token, cid, f"Данные обновлялись меньше минуты назад — сводка свежая.",
                json.loads(page_button(datetime.now(MSK), selection(subs, cid), fuel_selection(subs, cid), cid)))
        else:
            REFRESH["waiting"].add(cid)
            say(token, cid, "⏳ Обновляю данные, это займёт около минуты…")
        return False
    if text.startswith("/stations") and (cid == owner or cid in subs["subscribers"]):
        view_text, rows = stations_view(selection(subs, cid), "home")
        say(token, cid, view_text, {"inline_keyboard": rows})
        return False
    if text.startswith("/fuels") and (cid == owner or cid in subs["subscribers"]):
        view_text, rows = fuels_view(fuel_selection(subs, cid))
        say(token, cid, view_text, {"inline_keyboard": rows})
        return False
    if text.startswith("/start") and (cid == owner or cid in subs["subscribers"]):
        start_onboarding(token, subs, cid)
        return True
    if cid == owner:
        if text.startswith("/list"):
            for pid, info in subs["pending"].items():  # сначала — запросы, которые ждут подтверждения
                say(token, cid, f"🔔 Ждёт подтверждения: <b>{html.escape(info['name'])}</b> (запрос {info['at'][:16].replace('T', ' ')})",
                    {"inline_keyboard": [[{"text": "✅ Добавить", "callback_data": f"sub:ok:{pid}"},
                                          {"text": "❌ Отклонить", "callback_data": f"sub:no:{pid}"}]]})
            if not subs["pending"] and not subs["subscribers"]:
                say(token, cid, "Подписчиков и запросов пока нет. Чтобы подписаться, человек нажимает «Запустить» у бота, а вы подтверждаете.")
            elif subs["subscribers"]:
                say(token, cid, f"👥 <b>Подписчики: {len(subs['subscribers'])}</b>")
            for sid, info in subs["subscribers"].items():
                n, fs = len(clean_selection(info.get("stations"))), fuels_title(info.get("fuels") or []) or "не выбраны"
                say(token, cid, f"👤 {html.escape(info['name'])}, с {info['since'][:10]}\n   заправок: {n}, марки: {fs}",
                    {"inline_keyboard": [[{"text": "❌ Удалить", "callback_data": f"sub:del:{sid}"}]]})
        else:
            say(token, cid, "Вы владелец бота: оповещения приходят вам и подтверждённым подписчикам — каждому по его заправкам и маркам.\n"
                            "👥 Подписчики — список подписчиков и запросов.\n\n" + KEYBOARD_TEXT, keyboard_for(cid))
        return False
    if text.startswith("/stop"):
        if cid in subs["subscribers"]:
            info = subs["subscribers"].pop(cid)
            say(token, cid, "Вы отписались от оповещений. Чтобы подписаться снова, нажмите «📨 Отправить запрос».", REQUEST_KEYBOARD)
            say(token, owner, f"👋 Отписка от оповещений: {html.escape(info['name'])}.")
            return True
        say(token, cid, INTRO_TEXT, REQUEST_KEYBOARD)
        return False
    if cid in subs["subscribers"]:
        say(token, cid, ("Вы уже получаете оповещения.\n\n" if text.startswith("/start") else "") + HELP_SUB, keyboard_for(cid))
        return False
    if cid in subs["pending"]:
        say(token, cid, WAIT_TEXT, WAIT_KEYBOARD)
        return False
    if text != BTN_REQUEST:  # новый пользователь: сначала знакомство и кнопка «Отправить запрос»
        say(token, cid, INTRO_TEXT, REQUEST_KEYBOARD)
        return False
    if text == BTN_REQUEST:
        name = user_name(user)
        subs["pending"][cid] = {"name": name, "at": datetime.now(MSK).isoformat()}
        say(token, owner, f"🔔 <b>{html.escape(name)}</b> хочет получать оповещения о появлении бензина.",
            {"inline_keyboard": [[{"text": "✅ Добавить", "callback_data": f"sub:ok:{cid}"},
                                  {"text": "❌ Отклонить", "callback_data": f"sub:no:{cid}"}]]})
        say(token, cid, WAIT_TEXT, WAIT_KEYBOARD)
        return True
    return False


def handle_callback(token, cb, subs):
    data = cb.get("data", "")
    if data.startswith("st:"):
        return handle_stations_cb(token, cb, subs)
    if data.startswith("fu:"):
        return handle_fuels_cb(token, cb, subs)
    if data.startswith("pz:"):
        return handle_pause_cb(token, cb, subs)
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
        subs["subscribers"][cid] = {"name": info["name"], "since": datetime.now(MSK).isoformat(), "stations": [], "fuels": []}
        set_menu_button(token, cid, [], [])
        say(token, cid, "✅ Владелец бота подтвердил доступ.", KEYBOARD)
        subs["subscribers"][cid]["kb"] = KEYBOARD_VERSION
        start_onboarding(token, subs, cid)
        done(f"✅ Добавлено в подписчики: {html.escape(info['name'])}.")
        return True
    if action == "no" and cid in subs["pending"]:
        info = subs["pending"].pop(cid)
        say(token, cid, "Владелец бота отклонил запрос на оповещения.", {"remove_keyboard": True})
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
    try:
        if tg_call(token, "getMyDescription").get("result", {}).get("description") != BOT_DESCRIPTION:
            tg_call(token, "setMyDescription", description=BOT_DESCRIPTION)
            tg_call(token, "setMyShortDescription", short_description=BOT_SHORT_DESCRIPTION)
    except Exception as e:
        print(f"описание бота: {e}", file=sys.stderr)
    try:
        if tg_call(token, "getMyName").get("result", {}).get("name") != BOT_NAME:
            tg_call(token, "setMyName", name=BOT_NAME)
            print(f"имя бота: {BOT_NAME}", file=sys.stderr)
    except Exception as e:  # Telegram ограничивает частоту смены имени — не страшно, попробуем в следующую смену
        print(f"имя бота: {e}", file=sys.stderr)
    tg_call(token, "setMyCommands", commands=json.dumps([
        {"command": "stations", "description": "Выбрать заправки"},
        {"command": "fuels", "description": "Выбрать марки топлива"},
        {"command": "refresh", "description": "Обновить данные сейчас"},
        {"command": "pause", "description": "Пауза уведомлений"},
        {"command": "stop", "description": "Отписаться от оповещений"}]))
    tg_call(token, "setMyCommands", scope=json.dumps({"type": "chat", "chat_id": int(owner)}), commands=json.dumps([
        {"command": "stations", "description": "Выбрать свои заправки"},
        {"command": "fuels", "description": "Выбрать марки топлива"},
        {"command": "refresh", "description": "Обновить данные сейчас"},
        {"command": "pause", "description": "Пауза уведомлений"},
        {"command": "list", "description": "Подписчики и запросы"}]))
    for cid, sids, fuels in all_selections():
        try:
            set_menu_button(token, cid, sids, fuels)
        except Exception as e:
            print(f"кнопка меню …{cid[-4:]}: {e}", file=sys.stderr)
    subs, changed = load_subs(), False
    people = [(owner, subs["owner"])] + [(cid, e) for cid, e in subs["subscribers"].items() if cid != owner]
    for cid, entry in people:
        if entry.get("kb") != keyboard_version(cid):  # кнопки под полем ввода — один раз каждому
            try:
                text = KEYBOARD_TEXT + ("\n👥 Подписчики — список подписчиков и запросов." if cid == owner else "")
                say(token, cid, text, keyboard_for(cid))
                entry["kb"], changed = keyboard_version(cid), True
            except Exception as e:
                print(f"кнопки …{cid[-4:]}: {e}", file=sys.stderr)
    if changed:
        save_subs(subs)


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
    people = all_selections()
    watched = list(dict.fromkeys(sid for _, sids, _ in people for sid in sids))
    combos = [tuple(f for f in FUEL_CHOICES if f in fuels) for _, _, fuels in people if fuels]
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        page = Path(tmp)
        write_report(db, page / "index.html", watched, combos)
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


def answer_refresh():
    """После сбора: ответить всем, кто нажал «🔄 Обновить»."""
    REFRESH["last"] = time.time()
    waiting, REFRESH["waiting"] = REFRESH["waiting"], set()
    if not waiting or not bot_ready():
        return
    token, subs, now = os.environ["TELEGRAM_TOKEN"], load_subs(), datetime.now(MSK)
    for cid in waiting:
        try:
            say(token, cid, f"✅ Данные обновлены в {now:%H:%M}. Страница сводки обновится примерно через минуту.",
                json.loads(page_button(now, selection(subs, cid), fuel_selection(subs, cid), cid)))
        except Exception as e:
            print(f"обновление …{cid[-4:]}: {e}", file=sys.stderr)


HEALTH_FILE = BASE / "data/health.json"  # когда сервер последний раз работал и удачно читал канал
HEALTH_GAP = timedelta(minutes=30)      # перерыв дольше — сообщаем владельцу
CODE_FILES = ["benzin.py", ".github/workflows/collect.yml"]  # их изменение на GitHub = новая версия программы


def load_health():
    try:
        return json.loads(HEALTH_FILE.read_text())
    except Exception:
        return {}


def save_health(h):
    HEALTH_FILE.write_text(json.dumps(h, ensure_ascii=False, indent=1))


def notify_owner(text):
    """Служебное сообщение владельцу бота (сбои сбора и т. п.)."""
    token, owner = os.environ.get("TELEGRAM_TOKEN"), owner_id()
    if not token or not owner:
        return
    try:
        tg_call(token, "sendMessage", chat_id=owner, text=text, parse_mode="HTML")
    except Exception as e:
        print(f"сообщение владельцу: {e}", file=sys.stderr)


def check_gap_on_start(start, ws):
    """При запуске смены: был ли перерыв в сборе в рабочее время."""
    h = load_health()
    last = datetime.fromisoformat(h["alive"]) if h.get("alive") else None
    if last is None or start < ws:
        return
    if last >= ws - timedelta(minutes=10) and start - last > HEALTH_GAP:
        mins = int((start - last).total_seconds() // 60)
        notify_owner(f"⚠️ Сбор не работал с {last:%H:%M} до {start:%H:%M} ({mins} мин). Сервер перезапущен автоматически.")
    elif last < ws and start - ws > HEALTH_GAP:
        notify_owner(f"⚠️ Сбор сегодня начался только в {start:%H:%M} вместо {WORK_START}:00 — GitHub поздно запустил сервер. "
                     "Утренние данные за это время не собраны.")


def record_collect_result(ok):
    """Отметка «сервер жив» и учёт сбоев чтения канала; при сбое дольше 30 мин — сообщение владельцу."""
    now = datetime.now(MSK)
    h = load_health()
    h["alive"] = now.isoformat()
    if ok:
        if h.get("fail_alerted"):
            notify_owner(f"✅ Сбор снова работает (источник данных отвечает с {now:%H:%M}).")
        h["collected"] = now.isoformat()
        h.pop("fail_since", None)
        h.pop("fail_alerted", None)
    else:
        h.setdefault("fail_since", now.isoformat())
        since = datetime.fromisoformat(h["fail_since"])
        if now - since > HEALTH_GAP and not h.get("fail_alerted"):
            notify_owner(f"⚠️ Источник данных не отвечает с {since:%H:%M} — данные не обновляются. "
                         "Сервер продолжает попытки.")
            h["fail_alerted"] = True
    save_health(h)


def code_version(ref):
    """Отпечаток файлов кода в указанной версии хранилища."""
    return [git("rev-parse", f"{ref}:{f}", check=False).stdout.strip() for f in CODE_FILES]


RUNNING_CODE = {"version": None}  # версия кода, с которой запущена смена


def new_version_available():
    """Есть ли на GitHub версия программы новее той, с которой запущена эта смена."""
    if RUNNING_CODE["version"] is None:
        RUNNING_CODE["version"] = code_version("HEAD")
    if git("fetch", "-q", "origin", "main", check=False).returncode != 0:
        return False
    latest = code_version("origin/main")
    return all(latest) and latest != RUNNING_CODE["version"]


def dispatch_successor():
    """Запустить следующую смену (с повторами — сеть иногда подводит)."""
    for attempt in range(5):
        try:
            gh_api("POST", f"actions/workflows/{WORKFLOW}/dispatches", {"ref": "main", "inputs": {"reason": "chain"}})
            return True
        except Exception as e:
            print(f"запуск продолжения: {e}", file=sys.stderr)
            time.sleep(10 * (attempt + 1))
    return False


def work_once():
    """Один сбор: канал → data/obs.csv → страница → оповещения в Telegram → сохранить в хранилище."""
    t0 = time.time()
    db = open_db()
    try:
        new = collect(db, 4)
        if new:
            save_db(db)
        ok = True
    except Exception as e:
        new, ok = 0, False
        print(f"сбор не удался: {e}", file=sys.stderr)
    try:
        record_collect_result(ok)
    except Exception as e:
        print(f"здоровье: ошибка {e}", file=sys.stderr)
    load_registry(db)
    for step, fn in (("страница", lambda: publish_page(db)),
                     ("telegram", lambda: cmd_telegram(argparse.Namespace(alerts=True)) if os.environ.get("TELEGRAM_TOKEN") else None),
                     ("бот", poll_bot),
                     ("пауза", check_pause_expiry),
                     ("сохранение", save_and_push)):
        try:
            fn()
        except Exception as e:
            print(f"{step}: ошибка {e}", file=sys.stderr)
    try:
        answer_refresh()
    except Exception as e:
        print(f"обновить: ошибка {e}", file=sys.stderr)
    print(f"{datetime.now(MSK):%d.%m %H:%M} сбор: новых наблюдений {new}, {time.time() - t0:.0f} с", file=sys.stderr, flush=True)


def save_on_stop(signum, frame):
    """GitHub при отмене задачи присылает сигнал и даёт несколько секунд — сохраняем несохранённый выбор."""
    if UNPUSHED["since"]:
        try:
            save_and_push()
        except Exception:
            pass
    sys.exit(0)


def cmd_worker(args):
    import signal
    signal.signal(signal.SIGTERM, save_on_stop)
    signal.signal(signal.SIGINT, save_on_stop)
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
    try:
        check_gap_on_start(start, ws)
    except Exception as e:
        print(f"проверка перерыва: {e}", file=sys.stderr)
    slots = [s for s in day_slots(ws) if s > start]
    do_now = args.reason == "manual" or start >= ws  # опоздали к началу или нажали кнопку — собираем сразу
    last_queue_check, last_version_check = 0.0, time.time()
    RUNNING_CODE["version"] = code_version("HEAD")
    while True:
        now = datetime.now(MSK)
        if time.time() - last_version_check >= 120:
            last_version_check = time.time()
            if new_version_available():
                print("на GitHub новая версия программы — перезапуск", file=sys.stderr, flush=True)
                save_and_push()
                if dispatch_successor():
                    return  # новая смена уже в очереди и стартует с новой версией сразу после этой
        if now >= we or now >= deadline:
            if UNPUSHED["since"]:
                save_and_push()
            if now >= we:
                print("рабочий день закончился", file=sys.stderr)
                return
            # 6-часовой предел GitHub: запускаем продолжение и завершаемся
            if not dispatch_successor():
                notify_owner("⚠️ Не удалось запустить следующую смену сервера. Попробует «будильник» GitHub в течение нескольких минут.")
            print("запущено продолжение", file=sys.stderr)
            return
        if slots and now >= slots[0]:  # подошло время по расписанию
            do_now = True
            slots = [s for s in slots if s > now]
        manual = []
        if time.time() - last_queue_check >= 20:
            manual, last_queue_check = pending_manual_runs(), time.time()
        if do_now or manual or REFRESH["waiting"]:
            work_once()
            for run_id in set(manual + pending_manual_runs()):  # ручные запросы выполнены — убираем из очереди
                try:
                    gh_api("POST", f"actions/runs/{run_id}/cancel")
                except Exception:
                    pass
            do_now = False
            continue
        if UNPUSHED["since"] and time.time() - UNPUSHED["since"] > 10:
            # пользователь поменял заправки или марки: сразу пересобрать сводку и сохранить выбор
            try:
                publish_page(open_db())
            except Exception as e:
                print(f"страница: ошибка {e}", file=sys.stderr)
            save_and_push()
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
