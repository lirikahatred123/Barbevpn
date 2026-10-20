import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime
from urllib.parse import quote

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiohttp import web

# Если рядом лежит файл .env (хостинг без панели переменных), читаем настройки из него
if os.path.exists(".env"):
    for _line in open(".env", encoding="utf-8"):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("barbe-vpn")

# ───────────────────────── Настройки (из переменных окружения) ─────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
PANEL_URL = os.environ["PANEL_URL"].rstrip("/")  # http://de2.h1cloud.net:26042
PANEL_API_TOKEN = os.environ["PANEL_API_TOKEN"]  # API-токен ПАНЕЛИ (не ключ h1_ из кабинета)
SUPPORT = os.getenv("SUPPORT_USERNAME", "your_support")  # без @
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))
TRIAL_GB = float(os.getenv("TRIAL_GB", "2"))
TRIAL_DEVICES = int(os.getenv("TRIAL_DEVICES", "1"))
PAID_GB = float(os.getenv("PAID_GB", "0"))  # 0 = безлимит
# ID инбаундов панели через запятую (GET /api/inbounds). Пусто = настройки панели по умолчанию
INBOUND_IDS = [x.strip() for x in os.getenv("INBOUND_IDS", "").split(",") if x.strip()]
# Доп. локации (необязательно): {"node_id": ["inbound_id", ...]}
FED_NODES = json.loads(os.getenv("FED_NODES", "{}"))
PAY_TEXT = os.getenv(
    "PAY_INSTRUCTIONS",
    "Переведите точную сумму на карту <code>2200396103233181</code> и нажмите «Я оплатил».\nЕсли оплата не подтвердилась, напишите в поддержку и приложите чек.",
)
PORT = int(os.getenv("PORT", "10000"))

# Периоды: ключ -> (название, дней, цена за 1 устройство, доплата за каждое следующее устройство), ₽
PERIODS = {
    "w1": ("Неделя", 7, 59, 30),
    "m1": ("1 месяц", 30, 199, 100),
    "m3": ("3 месяца", 90, 549, 280),
    "m12": ("12 месяцев", 365, 1690, 900),
}
DEVICE_OPTIONS = [1, 2, 3, 5]  # какие варианты количества устройств показывать


def calc_price(period: str, devices: int) -> int:
    _, _, base, extra = PERIODS[period]
    return base + (devices - 1) * extra


# ───────────────────────── База (SQLite) ─────────────────────────
db = sqlite3.connect(os.getenv("DB_PATH", "bot.db"), check_same_thread=False)
db.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, trial_used INTEGER DEFAULT 0, balance INTEGER DEFAULT 0)")
try:
    db.execute("ALTER TABLE users ADD COLUMN balance INTEGER DEFAULT 0")
except sqlite3.OperationalError:
    pass
db.execute(
    "CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, "
    "plan TEXT, devices INTEGER DEFAULT 1, price INTEGER, status TEXT DEFAULT 'new', created TEXT)"
)
try:
    db.execute("ALTER TABLE orders ADD COLUMN devices INTEGER DEFAULT 1")
except sqlite3.OperationalError:
    pass
db.commit()


def ensure_user(uid: int):
    db.execute("INSERT OR IGNORE INTO users(id) VALUES(?)", (uid,))
    db.commit()


def get_balance(uid: int) -> int:
    row = db.execute("SELECT balance FROM users WHERE id=?", (uid,)).fetchone()
    return row[0] if row and row[0] else 0


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


# ───────────────────────── Клиент API панели H1 ─────────────────────────
class Panel:
    """API панели H1: <PANEL_URL>/api (авторизация Bearer PANEL_API_TOKEN)."""

    def __init__(self):
        self.http = httpx.AsyncClient(timeout=25)
        self.headers = {"Authorization": f"Bearer {PANEL_API_TOKEN}"}

    async def _req(self, method, path, **kw):
        return await self.http.request(method, f"{PANEL_URL}/api{path}", headers=self.headers, **kw)

    @staticmethod
    def ts(value) -> int:
        """expires_at -> unix-секунды (на случай миллисекунд или ISO-строки)."""
        if not value:
            return 0
        if isinstance(value, (int, float)):
            v = int(value)
            return v // 1000 if v > 10**11 else v
        try:
            return Panel.ts(float(value))
        except ValueError:
            return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())

    async def get_client(self, name):
        r = await self._req("GET", f"/clients/{quote(name, safe='')}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("client")

    async def _create(self, name, days, gb, devices):
        body = {"name": name, "uuid": str(uuid.uuid4()), "days": days,
                "traffic_limit_gb": gb, "device_limit": devices}
        if INBOUND_IDS:
            body.update(manual=True, channels=[], inbound_ids=INBOUND_IDS)
        r = await self._req("POST", "/create", json=body)
        r.raise_for_status()
        # дополнительные локации с тем же UUID (одна общая подписка)
        for node_id, ids in FED_NODES.items():
            rr = await self._req(
                "POST", f"/fed/lproxy/{node_id}/create",
                json={**body, "manual": True, "channels": [], "inbound_ids": ids},
            )
            rr.raise_for_status()

    async def grant(self, name, days, gb=0, devices=0):
        """Создаёт клиента или продлевает его подписку. Возвращает данные клиента."""
        client = await self.get_client(name)
        if client is None:
            await self._create(name, days, gb, devices)
        else:
            start = max(self.ts(client.get("expires_at")), int(time.time()))
            r = await self._req(
                "PATCH", f"/clients/{quote(name, safe='')}",
                json={"expires_at": start + days * 86400, "traffic_limit_gb": gb, "device_limit": devices},
            )
            r.raise_for_status()
        return await self.get_client(name)

    async def delete(self, name):
        r = await self._req("DELETE", f"/clients/{quote(name, safe='')}")
        if r.status_code == 404:
            return False
        r.raise_for_status()
        return True

    @staticmethod
    def sub_link(client):
        link = client.get("subscription_url") or client.get("sub_url") or ""
        return link if link.startswith("http") else PANEL_URL + link


panel = Panel()


def uname(user_id: int) -> str:
    return f"tg_{user_id}"


# ───────────────────────── Клавиатуры ─────────────────────────
def kb(rows):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=t, callback_data=d) if not d.startswith("http")
                          else InlineKeyboardButton(text=t, url=d) for t, d in row] for row in rows]
    )


def main_menu(uid: int = 0):
    rows = [
        [("🛒 Купить подписку", "plans")],
        [("🔑 Моя подписка", "mysub"), ("🎁 Пробный период", "trial")],
        [("💰 Баланс", "balance"), ("💬 Поддержка", f"https://t.me/{SUPPORT}")],
    ]
    if is_admin(uid):
        rows.append([("👑 Админ-панель", "adm:menu")])
    return kb(rows)


WELCOME = (
    "🔥 <b>Barbe VPN</b> — быстрый и стабильный VPN\n\n"
    "⚡️ Высокая скорость\n🔒 Без логов\n📱 iOS, Android, Windows, macOS\n"
    "♾ Много устройств на одной подписке\n\nВыберите действие 👇"
)

router = Router()


@router.message(CommandStart())
async def start(m: Message):
    ensure_user(m.from_user.id)
    await m.answer(WELCOME, reply_markup=main_menu(m.from_user.id))


@router.callback_query(F.data == "menu")
async def menu(c: CallbackQuery):
    await c.message.edit_text(WELCOME, reply_markup=main_menu(c.from_user.id))


@router.callback_query(F.data == "plans")
async def plans(c: CallbackQuery):
    rows = [[(f"{n} — от {base} ₽", f"per:{k}")] for k, (n, d, base, ex) in PERIODS.items()]
    rows.append([("⬅️ Назад", "menu")])
    await c.message.edit_text("📅 <b>На какой срок нужна подписка?</b>", reply_markup=kb(rows))


@router.callback_query(F.data.startswith("per:"))
async def choose_devices(c: CallbackQuery):
    period = c.data.split(":")[1]
    name = PERIODS[period][0]
    rows = [[(f"📱 {n} устр. — {calc_price(period, n)} ₽", f"buy:{period}:{n}")] for n in DEVICE_OPTIONS]
    rows.append([("⬅️ Назад", "plans")])
    await c.message.edit_text(
        f"Срок: <b>{name}</b>\n\n📱 <b>Сколько устройств подключить?</b>", reply_markup=kb(rows)
    )


@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery):
    _, period, devices = c.data.split(":")
    devices = int(devices)
    name = PERIODS[period][0]
    price = calc_price(period, devices)
    cur = db.execute(
        "INSERT INTO orders(user_id, plan, devices, price, created) VALUES(?,?,?,?,?)",
        (c.from_user.id, period, devices, price, datetime.now().isoformat()),
    )
    db.commit()
    oid = cur.lastrowid
    text = (
        f"🧾 <b>Заказ №{oid}</b>\nСрок: {name}\nУстройств: {devices}\nСумма: <b>{price} ₽</b>\n\n{PAY_TEXT}"
    )
    rows = []
    bal = get_balance(c.from_user.id)
    if bal >= price:
        rows.append([(f"💰 Оплатить с баланса ({bal} ₽)", f"bal:{oid}")])
    rows += [[("✅ Я оплатил", f"paid:{oid}")], [("⬅️ Назад", "plans")]]
    await c.message.edit_text(text, reply_markup=kb(rows))


@router.callback_query(F.data.startswith("paid:"))
async def paid(c: CallbackQuery, bot: Bot):
    oid = int(c.data.split(":")[1])
    row = db.execute("SELECT user_id, status FROM orders WHERE id=?", (oid,)).fetchone()
    if not row or row[0] != c.from_user.id or row[1] not in ("new", "pending"):
        return await c.answer("Заказ недоступен", show_alert=True)
    db.execute("UPDATE orders SET status='pending' WHERE id=?", (oid,))
    db.commit()
    await c.message.edit_text("⏳ Проверяем оплату, обычно это занимает несколько минут.")
    for admin in ADMIN_IDS:
        await bot.send_message(
            admin,
            f"💰 Заявка на оплату №{oid} от <code>{c.from_user.id}</code> (@{c.from_user.username})",
            reply_markup=kb([[("✅ Подтвердить", f"ok:{oid}"), ("❌ Отклонить", f"no:{oid}")]]),
        )


@router.callback_query(F.data.startswith("bal:"))
async def pay_balance(c: CallbackQuery, bot: Bot):
    oid = int(c.data.split(":")[1])
    row = db.execute("SELECT user_id, price, status FROM orders WHERE id=?", (oid,)).fetchone()
    if not row or row[0] != c.from_user.id or row[2] not in ("new", "pending"):
        return await c.answer("Заказ недоступен", show_alert=True)
    price = row[1]
    cur = db.execute("UPDATE users SET balance=balance-? WHERE id=? AND balance>=?", (price, c.from_user.id, price))
    db.commit()
    if cur.rowcount == 0:
        return await c.answer("Недостаточно средств на балансе", show_alert=True)
    try:
        await fulfill(oid, bot)
        await c.message.edit_text(f"✅ Заказ №{oid} оплачен с баланса. Остаток: {get_balance(c.from_user.id)} ₽")
    except Exception:
        log.exception("pay_balance failed")
        db.execute("UPDATE users SET balance=balance+? WHERE id=?", (price, c.from_user.id))
        db.commit()
        await c.message.edit_text(f"⚠️ Не удалось выдать подписку, деньги возвращены на баланс. Напишите в поддержку: @{SUPPORT}")


async def fulfill(order_id: int, bot: Bot):
    """Выдаёт подписку по оплаченному заказу. Её же вызовет вебхук платёжки."""
    row = db.execute("SELECT user_id, plan, status, devices FROM orders WHERE id=?", (order_id,)).fetchone()
    if not row or row[2] == "paid":
        return False
    user_id, plan, _, devices = row
    days = PERIODS[plan][1]
    user = await panel.grant(uname(user_id), days, PAID_GB, devices)
    db.execute("UPDATE orders SET status='paid' WHERE id=?", (order_id,))
    db.commit()
    await bot.send_message(
        user_id,
        f"🎉 <b>Оплата получена!</b>\n\nВаша ссылка-подписка:\n<code>{panel.sub_link(user)}</code>\n\n"
        "Скопируйте её и добавьте в приложение (Happ, V2rayNG, Streisand, Hiddify).",
        reply_markup=main_menu(user_id),
    )
    return True


@router.callback_query(F.data.startswith("ok:"))
async def admin_ok(c: CallbackQuery, bot: Bot):
    if c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа", show_alert=True)
    oid = int(c.data.split(":")[1])
    try:
        done = await fulfill(oid, bot)
        await c.message.edit_text(f"✅ Заказ №{oid} выполнен" if done else f"Заказ №{oid} уже обработан")
    except Exception as e:
        log.exception("fulfill failed")
        await c.message.edit_text(f"⚠️ Ошибка панели: {e}")


@router.callback_query(F.data.startswith("no:"))
async def admin_no(c: CallbackQuery, bot: Bot):
    if c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа", show_alert=True)
    oid = int(c.data.split(":")[1])
    row = db.execute("SELECT user_id FROM orders WHERE id=?", (oid,)).fetchone()
    db.execute("UPDATE orders SET status='rejected' WHERE id=?", (oid,))
    db.commit()
    await c.message.edit_text(f"❌ Заказ №{oid} отклонён")
    if row:
        await bot.send_message(row[0], f"Оплата по заказу №{oid} не найдена. Напишите в поддержку: @{SUPPORT}")


@router.callback_query(F.data == "mysub")
async def mysub(c: CallbackQuery):
    back = kb([[("🛒 Купить / продлить", "plans")], [("⬅️ Назад", "menu")]])
    try:
        user = await panel.get_client(uname(c.from_user.id))
    except Exception:
        log.exception("mysub failed")
        return await c.message.edit_text("⚠️ Сервис временно недоступен, попробуйте позже.", reply_markup=back)
    if not user:
        return await c.message.edit_text("У вас пока нет подписки.", reply_markup=back)
    exp = panel.ts(user.get("expires_at"))
    exp_txt = datetime.fromtimestamp(exp).strftime("%d.%m.%Y %H:%M") if exp else "бессрочно"
    used = user.get("traffic_used_gb") or 0
    limit = user.get("traffic_limit_gb") or 0
    lim_txt = f"{limit:g} ГБ" if limit else "безлимит"
    active = (not exp) or exp > time.time()
    status = "🟢 активна" if active else "🔴 истекла"
    await c.message.edit_text(
        f"🔑 <b>Ваша подписка</b>\nСтатус: {status}\nДо: <b>{exp_txt}</b>\n"
        f"Трафик: {used:.2f} ГБ / {lim_txt}\nУстройства: {user.get('devices_count', 0)} / {user.get('device_limit') or '∞'}\n\n<code>{panel.sub_link(user)}</code>",
        reply_markup=back,
    )


@router.callback_query(F.data == "trial")
async def trial(c: CallbackQuery):
    ensure_user(c.from_user.id)
    row = db.execute("SELECT trial_used FROM users WHERE id=?", (c.from_user.id,)).fetchone()
    if row and row[0]:
        return await c.answer("Пробный период уже использован", show_alert=True)
    try:
        user = await panel.grant(uname(c.from_user.id), TRIAL_DAYS, TRIAL_GB, TRIAL_DEVICES)
    except Exception:
        log.exception("trial failed")
        return await c.answer("Сервис временно недоступен, попробуйте позже", show_alert=True)
    db.execute("UPDATE users SET trial_used=1 WHERE id=?", (c.from_user.id,))
    db.commit()
    await c.message.edit_text(
        f"🎁 Пробный период на {TRIAL_DAYS} дн. активирован!\n\n<code>{panel.sub_link(user)}</code>",
        reply_markup=main_menu(c.from_user.id),
    )


@router.callback_query(F.data == "balance")
async def balance(c: CallbackQuery):
    await c.message.edit_text(
        f"💰 <b>Ваш баланс:</b> {get_balance(c.from_user.id)} ₽\n\n"
        "Баланс можно потратить на подписку при оформлении заказа.\n"
        "Для пополнения напишите в поддержку.",
        reply_markup=kb([[("💬 Пополнить", f"https://t.me/{SUPPORT}")], [("⬅️ Назад", "menu")]]),
    )


# ───────────────────────── Админ-панель ─────────────────────────
class Admin(StatesGroup):
    wait = State()


PROMPTS = {
    "give": "🎁 <b>Выдать подписку</b>\nОтправьте: <code>ID дни устройства</code>\nНапример: <code>123456789 30 3</code>",
    "bal": "💰 <b>Баланс</b>\nОтправьте: <code>ID сумма</code>\nНапример: <code>123456789 500</code> (с минусом — списать)",
    "find": "🔍 Отправьте Telegram ID пользователя",
    "del": "🗑 Отправьте Telegram ID пользователя, у которого удалить подписку",
    "cast": "📢 Отправьте текст рассылки. Её получат все, кто запускал бота.",
}


def admin_menu():
    return kb([
        [("📊 Статистика", "adm:stats"), ("📋 Заказы", "adm:orders")],
        [("🎁 Выдать подписку", "adm:give"), ("💰 Баланс", "adm:bal")],
        [("🔍 Найти пользователя", "adm:find"), ("🗑 Удалить подписку", "adm:del")],
        [("📢 Рассылка", "adm:cast")],
        [("⬅️ В меню", "menu")],
    ])


@router.message(Command("admin"))
async def admin_cmd(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    await state.clear()
    await m.answer("👑 <b>Админ-панель</b>", reply_markup=admin_menu())


@router.callback_query(F.data == "adm:menu")
async def admin_home(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await state.clear()
    await c.message.edit_text("👑 <b>Админ-панель</b>", reply_markup=admin_menu())


@router.callback_query(F.data == "adm:stats")
async def admin_stats(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    users, trials, balances = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(trial_used),0), COALESCE(SUM(balance),0) FROM users").fetchone()
    paid_n, revenue = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(price),0) FROM orders WHERE status='paid'").fetchone()
    pending = db.execute("SELECT COUNT(*) FROM orders WHERE status='pending'").fetchone()[0]
    await c.message.edit_text(
        f"📊 <b>Статистика</b>\n\nПользователей: {users}\nПробный период брали: {trials}\n"
        f"Оплаченных заказов: {paid_n} на {revenue} ₽\nОжидают подтверждения: {pending}\n"
        f"Сумма на балансах: {balances} ₽",
        reply_markup=kb([[("⬅️ Назад", "adm:menu")]]),
    )


@router.callback_query(F.data == "adm:orders")
async def admin_orders(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    rows = db.execute("SELECT id, user_id, plan, devices, price, status FROM orders ORDER BY id DESC LIMIT 10").fetchall()
    lines = [f"№{i} · <code>{u}</code> · {PERIODS.get(p, (p,))[0]} · {d} устр. · {pr} ₽ · {st}" for i, u, p, d, pr, st in rows]
    await c.message.edit_text(
        "📋 <b>Последние заказы</b>\n\n" + ("\n".join(lines) or "Пока пусто"),
        reply_markup=kb([[("⬅️ Назад", "adm:menu")]]),
    )


@router.callback_query(F.data.in_({"adm:give", "adm:bal", "adm:find", "adm:del", "adm:cast"}))
async def admin_prompt(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    action = c.data.split(":")[1]
    await state.set_state(Admin.wait)
    await state.update_data(action=action)
    await c.message.edit_text(PROMPTS[action], reply_markup=kb([[("✖️ Отмена", "adm:menu")]]))


@router.message(Admin.wait)
async def admin_input(m: Message, state: FSMContext, bot: Bot):
    if not is_admin(m.from_user.id):
        return
    action = (await state.get_data()).get("action")
    await state.clear()
    text = (m.text or "").strip()
    try:
        if action == "give":
            uid, days, devices = [int(x) for x in text.split()]
            client = await panel.grant(uname(uid), days, PAID_GB, devices)
            ensure_user(uid)
            result = f"✅ Выдано: <code>{uid}</code>, {days} дн., {devices} устр."
            try:
                await bot.send_message(
                    uid, f"🎁 Вам выдана подписка на {days} дн.!\n\n<code>{panel.sub_link(client)}</code>")
            except Exception:
                result += "\n⚠️ Пользователю написать не удалось (он не запускал бота)."
        elif action == "bal":
            uid, amount = [int(x) for x in text.split()]
            ensure_user(uid)
            db.execute("UPDATE users SET balance=MAX(balance+?,0) WHERE id=?", (amount, uid))
            db.commit()
            result = f"✅ Баланс <code>{uid}</code>: {get_balance(uid)} ₽"
            try:
                await bot.send_message(uid, f"💰 Ваш баланс изменён на {amount:+d} ₽. Сейчас: {get_balance(uid)} ₽")
            except Exception:
                pass
        elif action == "find":
            uid = int(text)
            row = db.execute("SELECT trial_used, balance FROM users WHERE id=?", (uid,)).fetchone()
            paid_n, revenue = db.execute(
                "SELECT COUNT(*), COALESCE(SUM(price),0) FROM orders WHERE user_id=? AND status='paid'", (uid,)).fetchone()
            client = await panel.get_client(uname(uid))
            result = (
                f"👤 <code>{uid}</code>\n"
                f"В боте: {'да' if row else 'нет'}\nБаланс: {row[1] if row else 0} ₽\n"
                f"Пробный период: {'использован' if row and row[0] else 'нет'}\n"
                f"Оплачено заказов: {paid_n} на {revenue} ₽\n"
            )
            if client:
                exp = panel.ts(client.get("expires_at"))
                exp_txt = datetime.fromtimestamp(exp).strftime("%d.%m.%Y %H:%M") if exp else "бессрочно"
                result += (f"Подписка до: {exp_txt}\nУстройства: {client.get('devices_count', 0)} / "
                           f"{client.get('device_limit') or '∞'}\n<code>{panel.sub_link(client)}</code>")
            else:
                result += "Подписки в панели нет"
        elif action == "del":
            uid = int(text)
            result = "🗑 Подписка удалена" if await panel.delete(uname(uid)) else "Клиент в панели не найден"
        elif action == "cast":
            ids = [r[0] for r in db.execute("SELECT id FROM users").fetchall()]
            ok = fail = 0
            for uid in ids:
                try:
                    await bot.send_message(uid, m.html_text)
                    ok += 1
                except Exception:
                    fail += 1
                await asyncio.sleep(0.05)
            result = f"📢 Рассылка завершена: доставлено {ok}, не доставлено {fail}"
        else:
            result = "Действие не выбрано"
    except ValueError:
        result = "⚠️ Неверный формат. Откройте пункт меню ещё раз и посмотрите пример."
    except Exception as e:
        log.exception("admin action failed")
        result = f"⚠️ Ошибка: {e}"
    await m.answer(result, reply_markup=admin_menu())


# ───────────────────────── Веб-сервер (для Render + будущий вебхук оплаты) ─────────────────────────
async def health(_):
    return web.Response(text="Barbe VPN bot is running")


async def run_web(bot: Bot):
    app = web.Application()
    app.router.add_get("/", health)

    # TODO: сюда добавим приём уведомлений от Rollypay:
    # async def rollypay_hook(request): ... проверить подпись ... await fulfill(order_id, bot)
    # app.router.add_post("/rollypay", rollypay_hook)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main():
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    try:
        await run_web(bot)  # нужен только для Render/вебхуков; на обычном хостинге можно без него
    except OSError as e:
        log.warning("Веб-сервер не запущен (порт занят/недоступен): %s", e)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
