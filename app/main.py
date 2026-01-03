import os
import asyncio
import json
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    Text,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


DATA_API_BASE = "https://data-api.polymarket.com"


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def build_db(db_path: str) -> Table:
    # SQLite настройки для скорости + надёжности в контейнере
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
        conn.exec_driver_sql("PRAGMA synchronous=NORMAL;")

    md = MetaData()

    activity = Table(
        "polymarket_activity",
        md,
        Column("transactionHash", String, primary_key=True),
        Column("timestamp", Integer, index=True),
        Column("proxyWallet", String, index=True),
        Column("conditionId", String, index=True),
        Column("type", String),
        Column("side", String),
        Column("asset", String, index=True),
        Column("outcome", String),
        Column("outcomeIndex", Integer),
        Column("price", Float),
        Column("size", Float),
        Column("usdcSize", Float),
        Column("title", Text),
        Column("slug", String),
        Column("eventSlug", String),
        Column("icon", Text),
        Column("raw_json", Text),
    )

    md.create_all(engine)
    return activity


async def fetch_activity(client: httpx.AsyncClient, user: str, limit: int) -> List[Dict[str, Any]]:
    params = {
        "limit": str(limit),
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
        "user": user,
    }
    r = await client.get(f"{DATA_API_BASE}/activity", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response shape: {type(data)}")
    return data


async def telegram_send(client: httpx.AsyncClient, bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    r = await client.post(url, data=payload, timeout=20)
    r.raise_for_status()


def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "transactionHash": item.get("transactionHash"),
        "timestamp": item.get("timestamp"),
        "proxyWallet": item.get("proxyWallet"),
        "conditionId": item.get("conditionId"),
        "type": item.get("type"),
        "side": item.get("side"),
        "asset": item.get("asset"),
        "outcome": item.get("outcome"),
        "outcomeIndex": item.get("outcomeIndex"),
        "price": item.get("price"),
        "size": item.get("size"),
        "usdcSize": item.get("usdcSize"),
        "title": item.get("title"),
        "slug": item.get("slug"),
        "eventSlug": item.get("eventSlug"),
        "icon": item.get("icon"),
        "raw_json": json.dumps(item, ensure_ascii=False),
    }


def format_message(r: Dict[str, Any]) -> str:
    title = (r.get("title") or "").strip()
    typ = r.get("type")
    side = r.get("side")
    price = r.get("price")
    size = r.get("size")
    usdc = r.get("usdcSize")
    tx = r.get("transactionHash")

    lines = ["🆕 Polymarket activity"]
    if title:
        lines.append(f"📝 {title}")
    lines.append(f"🔁 {typ} / {side}")
    if size is not None or usdc is not None:
        lines.append(f"📦 size={size} | usdc={usdc}")
    if price is not None:
        lines.append(f"💲 price={price}")
    lines.append(f"🧾 tx={tx}")
    return "\n".join(lines)


async def run() -> None:
    user = env("POLY_USER")
    bot_token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")

    poll_interval = int(env("POLL_INTERVAL_SEC", "15"))
    limit = int(env("LIMIT", "100"))

    db_path = env("DB_PATH", "/data/polymarket.sqlite3")

    activity = build_db(db_path)
    engine = create_engine(f"sqlite:///{db_path}", future=True)

    headers = {"User-Agent": "polymarket-activity-ingestor/1.0"}
    async with httpx.AsyncClient(headers=headers) as http:
        # единый клиент и для телеги, чтобы не создавать каждый раз
        async with httpx.AsyncClient() as tg:
            while True:
                try:
                    items = await fetch_activity(http, user=user, limit=limit)

                    new_rows: List[Dict[str, Any]] = []
                    with engine.begin() as conn:
                        for item in items:
                            row = normalize_item(item)
                            tx = row["transactionHash"]
                            if not tx:
                                continue

                            stmt = sqlite_insert(activity).values(**row).on_conflict_do_nothing(
                                index_elements=["transactionHash"]
                            )
                            res = conn.execute(stmt)
                            if res.rowcount == 1:
                                new_rows.append(row)

                    # Чтобы не “переворачивать” события, шлём от старых к новым
                    new_rows.sort(key=lambda x: x.get("timestamp") or 0)

                    # Если новых много — можно объединить, но пока шлём по одному
                    for r in new_rows:
                        await telegram_send(tg, bot_token, chat_id, format_message(r))

                except Exception as e:
                    # не урони сервис из-за одной ошибки
                    try:
                        await telegram_send(tg, bot_token, chat_id, f"⚠️ Error: {type(e).__name__}: {e}")
                    except Exception:
                        pass

                await asyncio.sleep(poll_interval)


if __name__ == "__main__":
    asyncio.run(run())
