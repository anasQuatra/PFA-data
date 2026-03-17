import requests
import uuid
import os
from pathlib import Path
from dotenv import load_dotenv
import os
from supabase import create_client
from datetime import datetime, timezone

# Load .env when present (local dev), but don't fail in CI where vars come from env.
load_dotenv(".env")

def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    value = value.strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

supabase = create_client(
    get_required_env("SUPABASE_URL"),
    get_required_env("SUPABASE_KEY")
)

API_KEY = get_required_env("API_KEY")
USER_KEY = get_required_env("USER_KEY")
BASE_URL = "https://public-api.etoro.com/api/v1"

# ==============================
# Resolve Symbol → Instrument ID
# ==============================

def resolve_instrument_id(symbol: str) -> int:
    """
    Resolve a ticker symbol (e.g., 'AAPL', 'BTC')
    to its numeric instrumentId.
    """
    url = f"{BASE_URL}/market-data/search"

    headers = {
        "x-request-id": str(uuid.uuid4()),
        "x-api-key": API_KEY,
        "x-user-key": USER_KEY,
    }

    params = {
        "internalSymbolFull": symbol
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"Error resolving instrument: {response.text}")

    data = response.json()

    instrument = next(
        (item for item in data.get("items", [])
         if item.get("internalSymbolFull") == symbol),
        None
    )

    if not instrument:
        raise ValueError(f"Instrument '{symbol}' not found.")

    return instrument["instrumentId"]

def fetch_candles(
    instrument_id: int,
    interval: str = "OneMinute",
    candles_count: int = 10,
    direction: str = "asc"
):
    """
    Fetch historical OHLCV candles.

    interval options:
        OneMinute, FiveMinutes, TenMinutes,
        FifteenMinutes, ThirtyMinutes,  
        OneHour, FourHours,
        OneDay, OneWeek

    direction:
        asc  -> oldest to newest
        desc -> newest to oldest
    """

    if candles_count > 1000:
        raise ValueError("candles_count cannot exceed 1000")

    url = (
        f"{BASE_URL}/market-data/instruments/"
        f"{instrument_id}/history/candles/"
        f"{direction}/{interval}/{candles_count}"
    )

    headers = {
        "x-request-id": str(uuid.uuid4()),
        "x-api-key": API_KEY,
        "x-user-key": USER_KEY,
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise RuntimeError(f"Error fetching candles: {response.text}")

    data = response.json()
    instrument_block = data["candles"][0]
    candles = instrument_block["candles"]
    return candles

def insert_candles(candles: list[dict], table_name: str = "fetches") -> int:
    """Insert each candle as a separate row in Supabase."""
    inserted = 0
    for candle in candles:
        supabase.table(table_name).insert({"payload": candle}).execute()
        inserted += 1
    return inserted

if __name__ == "__main__":

    symbol = "AAPL"   # "BTC", "ETH", "TSLA"
    instrument_id = resolve_instrument_id(symbol)
    candles_payload = fetch_candles(instrument_id,candles_count=10)
    inserted_count = insert_candles(candles_payload)
    print(f"Inserted {inserted_count} candles into 'fetches'.")