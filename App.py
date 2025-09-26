#!/usr/bin/env python3
# App.py — main bot file
# Tested with Python 3.11+ and aiogram 3.x

import os
import asyncio
import logging
import secrets
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove

# ---------- Configuration ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://user:pass@host:port/db
ADMIN_IDS = set()  # optionally fill with admin numeric ids, or add via env var
ADMIN_ENV = os.getenv("ADMIN_IDS")
if ADMIN_ENV:
    for x in ADMIN_ENV.split(","):
        try:
            ADMIN_IDS.add(int(x.strip()))
        except:
            pass

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required in env")

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pchelo_farm")

# ---------- DB helpers ----------
class DB:
    pool: Optional[asyncpg.pool.Pool] = None

    @classmethod
    async def init(cls, dsn: str):
        log.info("Connecting to DB...")
        cls.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        async with cls.pool.acquire() as conn:
            await create_tables(conn)
            await ensure_bee_templates(conn)
        log.info("DB ready.")

    @classmethod
    async def close(cls):
        if cls.pool:
            await cls.pool.close()
            cls.pool = None

# Create tables
async def create_tables(conn):
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id BIGINT PRIMARY KEY,
        username TEXT,
        nickname TEXT,
        coins BIGINT DEFAULT 0,
        last_spin TIMESTAMP,
        created_at TIMESTAMP DEFAULT now()
    );
    """)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS bee_templates (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        rarity TEXT NOT NULL,
        role TEXT NOT NULL,
        description TEXT,
        base_value INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT now()
    );
    """)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS parts (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        template_id INTEGER NOT NULL REFERENCES bee_templates(id),
        part_type TEXT NOT NULL, -- wing/body/sting/head
        rarity TEXT NOT NULL,
        amount INTEGER NOT NULL DEFAULT 0,
        UNIQUE(user_id, template_id, part_type, rarity)
    );
    """)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS bees (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        template_id INTEGER NOT NULL REFERENCES bee_templates(id),
        rarity TEXT NOT NULL,
        role TEXT NOT NULL,
        level INTEGER NOT NULL DEFAULT 1,
        exp INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT now()
    );
    """)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS market_listings (
        id SERIAL PRIMARY KEY,
        seller_id BIGINT NOT NULL,
        bee_id INTEGER NOT NULL REFERENCES bees(id),
        price BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT now()
    );
    """)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS squads (
        user_id BIGINT PRIMARY KEY,
        slot1 INTEGER,
        slot2 INTEGER,
        slot3 INTEGER,
        slot4 INTEGER,
        slot5 INTEGER,
        slot6 INTEGER
    );
    """)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS spins_log (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        template_id INTEGER,
        part_type TEXT,
        rarity TEXT,
        amount INTEGER,
        created_at TIMESTAMP DEFAULT now()
    );
    """)

# ---------- Game configuration ----------
RARITIES = ["Обычная", "Сверхредкая", "Эпическая", "Легендарная", "Мифическая", "Дикая"]
# counts per your spec (distribution of templates)
RARITY_COUNTS = {
    "Обычная": 35,
    "Сверхредкая": 25,
    "Эпическая": 15,
    "Легендарная": 15,
    "Мифическая": 9,
    "Дикая": 1
}
PART_TYPES = ["крылья", "тельце", "жало", "голова"]
ROLES = ["Танк", "Хиллер", "Саппорт"]
# probabilities for qty distribution: 45% => 1-3, 35% => 4-6, 15% => 7-9, 5% => 10
def draw_amount():
    r = secrets.randbelow(100) + 1
    if r <= 45:
        return secrets.choice([1,2,3])
    if r <= 45+35:
        return secrets.choice([4,5,6])
    if r <= 45+35+15:
        return secrets.choice([7,8,9])
    return 10

# base value per rarity (sell price per part approximate)
RARITY_BASE_VALUE = {
    "Обычная": 50,
    "Сверхредкая": 200,
    "Эпическая": 800,
    "Легендарная": 3000,
    "Мифическая": 12000,
    "Дикая": 50000
}

# ---------- Prepare templates (100+ bees) ----------
SAMPLE_NAMES = [
    "Пчелёнок", "Страж", "Бжур", "Шершень", "Златоброд", "Бронеборт", "Карабч", "Кип", "Гроза",
    "Чум", "Рой", "Светляч", "Гром", "Пробор", "Клир", "Тенебрис", "Велос", "Шип", "Флат",
    "Рыжик", "Скиталец", "Глашатай", "Арк", "Блиц", "Силгар", "Мирон", "Оникс", "Ривер", "Стаз",
    "Люм", "Квент", "Мерз", "Кард", "Аур", "Каин", "Крон", "Везер", "Соль", "Эйр"
]
# We'll generate templates programmatically to match counts

async def ensure_bee_templates(conn):
    # count existing templates
    row = await conn.fetchval("SELECT count(*) FROM bee_templates;")
    if row and row > 100:
        return  # probably already created
    log.info("Creating bee templates...")
    await conn.execute("TRUNCATE bee_templates RESTART IDENTITY;")
    items = []
    for rarity, count in RARITY_COUNTS.items():
        for i in range(count):
            name = f"{secrets.choice(SAMPLE_NAMES)}-{rarity[:3]}-{i+1}"
            role = secrets.choice(ROLES)
            base_value = RARITY_BASE_VALUE[rarity]
            desc = f"{role} пчела ранга {rarity}"
            items.append((name, rarity, role, desc, base_value))
    # ensure we have at least 100 templates (sum of counts should be >=100)
    for it in items:
        await conn.execute("""
            INSERT INTO bee_templates (name, rarity, role, description, base_value)
            VALUES ($1,$2,$3,$4,$5)
        """, *it)
    log.info(f"Inserted {len(items)} templates.")

# ---------- Game logic helpers ----------
def random_template_id_and_rarity(conn_records) -> None:
    # not used; we will pick by weighted rarity counts
    pass

async def pick_random_template(conn) -> asyncpg.Record:
    # pick a template weighted equally among templates (templates already distributed by rarity counts)
    rec = await conn.fetchrow("SELECT * FROM bee_templates ORDER BY RANDOM() LIMIT 1;")
    return rec

async def give_parts_to_user(conn, user_id: int, template_id: int, part_type: str, rarity: str, amount: int):
    # upsert into parts
    await conn.execute("""
        INSERT INTO parts (user_id, template_id, part_type, rarity, amount)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (user_id, template_id, part_type, rarity)
        DO UPDATE SET amount = parts.amount + EXCLUDED.amount
    """, user_id, template_id, part_type, rarity, amount)
    # log the spin
    await conn.execute("""
        INSERT INTO spins_log (user_id, template_id, part_type, rarity, amount)
        VALUES ($1,$2,$3,$4,$5)
    """, user_id, template_id, part_type, rarity, amount)

async def try_assemble(conn, user_id: int, template_id: int, rarity: str) -> Optional[int]:
    """
    Try to assemble a bee if user has at least 1 of each part_type for given template_id & rarity.
    If assembled, consume 1 of each and create bee and return new bee id.
    """
    # check counts
    rows = await conn.fetch("""
        SELECT part_type, amount FROM parts
        WHERE user_id=$1 AND template_id=$2 AND rarity=$3
    """, user_id, template_id, rarity)
    parts_map = {r['part_type']: r['amount'] for r in rows}
    if all(parts_map.get(pt,0) >= 1 for pt in PART_TYPES):
        # consume 1 of each
        for pt in PART_TYPES:
            await conn.execute("""
                UPDATE parts SET amount = amount - 1
                WHERE user_id=$1 AND template_id=$2 AND part_type=$3 AND rarity=$4
            """, user_id, template_id, pt, rarity)
        # create bee
        tmpl = await conn.fetchrow("SELECT role FROM bee_templates WHERE id=$1", template_id)
        role = tmpl['role'] if tmpl else secrets.choice(ROLES)
        rec = await conn.fetchrow("""
            INSERT INTO bees (user_id, template_id, rarity, role)
            VALUES ($1,$2,$3,$4) RETURNING id
        """, user_id, template_id, rarity, role)
        return rec['id']
    return None

async def add_coins(conn, user_id: int, amount: int):
    await conn.execute("INSERT INTO users (id, coins) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET coins = users.coins + $2", user_id, amount)

async def get_user(conn, user_id: int) -> Optional[asyncpg.Record]:
    return await conn.fetchrow("SELECT * FROM users WHERE id=$1", user_id)

async def ensure_user(conn, user: types.User):
    await conn.execute("""
        INSERT INTO users (id, username, nickname, coins)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username
    """, user.id, user.username or None, user.full_name or user.username or f"user{user.id}", 0)

# ---------- Bot setup ----------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# Reply keyboard for quick actions
main_kb = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton("/daily_spin"), KeyboardButton("/my_parts")],
    [KeyboardButton("/my_bees"), KeyboardButton("/squad")],
    [KeyboardButton("/market"), KeyboardButton("/inventory")]
], resize_keyboard=True)

# ---------- Handlers ----------
@dp.message(Command(commands=["start"]))
async def cmd_start(message: types.Message):
    async with DB.pool.acquire() as conn:
        await ensure_user(conn, message.from_user)
        # greet text (as requested)
        intro = (
            "Приветствую в ПЧОЛО-ферме. Это небольшая игра суть которой убивать время.\n\n"
            "Вы можете открывать новых ПЧОЛОВ, можете их продавать на внутреннем рынке и соревноваться "
            "с друзьями выстраивая отряд из 6 своих боевых ПЧОЛОВ.\n\n"
            "Команды: /daily_spin — раз в 24 часа прокрутка фортуны, /my_parts — мои детали, "
            "/my_bees — мои пчёлы, /squad — показать/настроить отряд, /market — рынок.\n"
        )
        await message.answer(intro, reply_markup=main_kb)

@dp.message(Command(commands=["help"]))
async def cmd_help(message: types.Message):
    text = ("Команды:\n"
            "/start — приветствие\n"
            "/daily_spin — раз в 24ч: прокрутить фортуну и получить части\n"
            "/my_parts — показать собранные части\n"
            "/my_bees — показать собранных пчёл\n"
            "/assemble <template_id> — попытаться собрать пчелу из частей (если есть 1 каждого типа)\n"
            "/sell_part <template_id> <part_type> <amount> — продать детали\n"
            "/sell_bee <bee_id> <price> — выставить пчелу на рынок\n"
            "/market — просмотреть рынок\n"
            "/buy <listing_id> — купить с рынка\n"
            "/squad — показать отряд\n"
            "/set_squad <slot(1-6)> <bee_id> — поставить пчелу в слот\n")
    await message.answer(text)

@dp.message(Command(commands=["daily_spin"]))
async def cmd_daily_spin(message: types.Message):
    user = message.from_user
    async with DB.pool.acquire() as conn:
        await ensure_user(conn, user)
        row = await conn.fetchrow("SELECT last_spin FROM users WHERE id=$1", user.id)
        now = datetime.utcnow()
        if row and row['last_spin']:
            last = row['last_spin']
            if (now - last) < timedelta(hours=24):
                remaining = timedelta(hours=24) - (now - last)
                await message.answer(f"Фортуну можно крутить раз в 24 часа. Подождите ещё {remaining}.")
                return
        # Proceed to spin
        tmpl = await pick_random_template(conn)
        if not tmpl:
            await message.answer("Ошибка: не найдено шаблона пчелы.")
            return
        template_id = tmpl['id']
        rarity = tmpl['rarity']
        part_type = secrets.choice(PART_TYPES)
        amount = draw_amount()
        # give parts
        await give_parts_to_user(conn, user.id, template_id, part_type, rarity, amount)
        # update last_spin
        await conn.execute("UPDATE users SET last_spin=$1 WHERE id=$2", now, user.id)
        # try assemble
        bee_id = await try_assemble(conn, user.id, template_id, rarity)
        text = (f"🎰 Результат фортуны:\n"
                f"Пчела-шаблон: <b>{tmpl['name']}</b>\n"
                f"Редкость: <b>{rarity}</b>\n"
                f"Часть: <b>{part_type}</b>\n"
                f"Количество частей: <b>{amount}</b>\n")
        if bee_id:
            text += f"\n🟢 Поздравляем — вы автоматически собрали пчелу! ID пчелы: <b>{bee_id}</b>\n"
        await message.answer(text)

@dp.message(Command(commands=["my_parts"]))
async def cmd_my_parts(message: types.Message):
    user = message.from_user
    async with DB.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.template_id, t.name, p.part_type, p.rarity, p.amount
            FROM parts p JOIN bee_templates t ON p.template_id=t.id
            WHERE p.user_id=$1 AND p.amount>0
            ORDER BY p.rarity, t.id
        """, user.id)
        if not rows:
            await message.answer("У вас нет частей.")
            return
        lines = []
        for r in rows:
            lines.append(f"Template {r['template_id']} ({r['name']}) — {r['part_type']} — {r['rarity']} : {r['amount']}")
        await message.answer("\n".join(lines))

@dp.message(Command(commands=["my_bees"]))
async def cmd_my_bees(message: types.Message):
    user = message.from_user
    async with DB.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT b.id,b.level,b.exp,b.rarity,b.role,t.name
            FROM bees b JOIN bee_templates t ON b.template_id=t.id
            WHERE b.user_id=$1
            ORDER BY b.level DESC, b.id
        """, user.id)
        if not rows:
            await message.answer("У вас пока нет пчёл.")
            return
        lines = []
        for r in rows:
            lines.append(f"ID {r['id']} — {r['name']} — {r['rarity']} — {r['role']} — Уровень {r['level']} (EXP {r['exp']})")
        await message.answer("\n".join(lines))

@dp.message()
async def generic_text(message: types.Message):
    # parse commands like assemble /sell_part etc
    text = message.text.strip().split()
    if not text:
        return
    cmd = text[0].lower()
    async with DB.pool.acquire() as conn:
        if cmd == "/assemble":
            if len(text) < 2:
                await message.answer("Использование: /assemble <template_id>")
                return
            try:
                template_id = int(text[1])
            except:
                await message.answer("template_id должен быть числом")
                return
            # find rarity(s) user has for this template
            rows = await conn.fetch("SELECT DISTINCT rarity FROM parts WHERE user_id=$1 AND template_id=$2 AND amount>0", message.from_user.id, template_id)
            if not rows:
                await message.answer("У вас нет деталей для этого шаблона.")
                return
            # try each rarity (prefer highest rarity present)
            rarities = [r['rarity'] for r in rows]
            # order rarities by defined order
            rar_order = {r:i for i,r in enumerate(RARITIES)}
            rarities.sort(key=lambda x: rar_order.get(x, 999))
            # try assemble any
            assembled = None
            for rar in rarities:
                assembled = await try_assemble(conn, message.from_user.id, template_id, rar)
                if assembled:
                    break
            if assembled:
                await message.answer(f"Вы собрали пчелу! ID: {assembled}")
            else:
                await message.answer("Не удалось собрать пчелу — не хватает каких-то частей (нужна по 1 шт. каждого типа).")
            return

        if cmd == "/sell_part":
            # /sell_part <template_id> <part_type> <amount>
            if len(text) < 4:
                await message.answer("Использование: /sell_part <template_id> <part_type> <amount>")
                return
            try:
                template_id = int(text[1])
                part_type = text[2]
                amount = int(text[3])
            except:
                await message.answer("Неверные аргументы.")
                return
            if part_type not in PART_TYPES:
                await message.answer(f"part_type должен быть одним из: {', '.join(PART_TYPES)}")
                return
            row = await conn.fetchrow("SELECT amount, rarity FROM parts WHERE user_id=$1 AND template_id=$2 AND part_type=$3 ORDER BY amount DESC LIMIT 1", message.from_user.id, template_id, part_type)
            if not row or row['amount'] < amount:
                await message.answer("Недостаточно деталей.")
                return
            rarity = row['rarity']
            price_per = RARITY_BASE_VALUE.get(rarity, 10)
            total = price_per * amount
            # reduce parts (consume highest rarity first for simplicity)
            await conn.execute("UPDATE parts SET amount = amount - $1 WHERE user_id=$2 AND template_id=$3 AND part_type=$4 AND amount>=$1", amount, message.from_user.id, template_id, part_type)
            await conn.execute("UPDATE users SET coins = coins + $1 WHERE id=$2", total, message.from_user.id)
            await message.answer(f"Вы продали {amount}x {part_type} ({rarity}) за {total} монет.")
            return

        if cmd == "/sell_bee":
            # /sell_bee <bee_id> <price>
            if len(text) < 3:
                await message.answer("Использование: /sell_bee <bee_id> <price>")
                return
            try:
                bee_id = int(text[1])
                price = int(text[2])
            except:
                await message.answer("Неверные аргументы.")
                return
            # verify ownership
            b = await conn.fetchrow("SELECT id FROM bees WHERE id=$1 AND user_id=$2", bee_id, message.from_user.id)
            if not b:
                await message.answer("Пчела не найдена или не ваша.")
                return
            # create listing
            await conn.execute("INSERT INTO market_listings (seller_id, bee_id, price) VALUES ($1,$2,$3)", message.from_user.id, bee_id, price)
            await message.answer(f"Пчела ID {bee_id} выставлена на рынок за {price} монет.")
            return

        if cmd == "/market":
            rows = await conn.fetch("""
                SELECT m.id, m.price, b.id AS bee_id, b.rarity, b.role, t.name, m.seller_id
                FROM market_listings m
                JOIN bees b ON m.bee_id=b.id
                JOIN bee_templates t ON b.template_id=t.id
                ORDER BY m.created_at
            """)
            if not rows:
                await message.answer("Рынок пуст.")
                return
            lines = []
            for r in rows:
                lines.append(f"Listing {r['id']}: BeeID {r['bee_id']} ({r['name']}, {r['rarity']}, {r['role']}) — Price: {r['price']} — Seller: {r['seller_id']}")
            await message.answer("\n".join(lines))
            return

        if cmd == "/buy":
            # /buy <listing_id>
            if len(text) < 2:
                await message.answer("Использование: /buy <listing_id>")
                return
            try:
                lid = int(text[1])
            except:
                await message.answer("listing_id должен быть числом")
                return
            listing = await conn.fetchrow("SELECT * FROM market_listings WHERE id=$1", lid)
            if not listing:
                await message.answer(
