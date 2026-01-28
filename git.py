import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import aiohttp
from datetime import datetime

# --- Configuration ---
st.set_page_config(page_title="CoinDCX Futures Tracker", layout="wide")

# Hide standard menus
st.markdown("""
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- 1. Dynamic Symbol Fetching (CoinDCX) ---

async def get_coindcx_futures_symbols():
    url = "https://api.coindcx.com/exchange/v1/markets"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Filter for symbols ending in USDT and simplify them
                    # CoinDCX usually returns simple names or formatted ones.
                    # We look for valid pairs.
                    symbols = []
                    for item in data:
                        # Logic: Check if it looks like a futures pair or standard pair we want
                        # CoinDCX 'symbol' example: "BTCUSDT" or "B-BTC_USDT"
                        # We will try to extract the base currency.
                        s = item.get('symbol', '')
                        if s.endswith('USDT'):
                            # Create a standard CCXT format: "BTC/USDT"
                            # Remove "B-" prefix if present (common in your previous list)
                            clean = s.replace("B-", "").replace("_", "") 
                            base = clean[:-4] # Remove last 4 chars (USDT)
                            if base:
                                symbols.append(f"{base}/USDT")
                    
                    # Remove duplicates and sort
                    return sorted(list(set(symbols)))
    except Exception as e:
        st.error(f"Error fetching CoinDCX symbols: {e}")
        return []
    return []

# --- 2. Multi-Exchange Fetching Logic ---

def calculate_time_aligned_change(ohlcv, interval_hours):
    if not ohlcv: return -9999
    ms_per_hour = 3600 * 1000
    ms_interval = interval_hours * ms_per_hour
    last_ts = ohlcv[-1][0]
    current_block_start = last_ts - (last_ts % ms_interval)
    target_open_ts = current_block_start - ms_interval
    
    open_price = None
    close_price = None

    for candle in ohlcv:
        if candle[0] == target_open_ts:
            open_price = candle[1]
            break
            
    current_block_index = -1
    for i, candle in enumerate(ohlcv):
        if candle[0] == current_block_start:
            current_block_index = i
            break
            
    if open_price is not None and current_block_index > 0:
        close_price = ohlcv[current_block_index - 1][4]
        return ((close_price - open_price) / open_price) * 100
        
    return -9999

async def fetch_ohlcv_from_exchange(exchange, symbol):
    """Try to fetch data from a specific exchange instance."""
    try:
        # Check if exchange supports the symbol
        # Note: loading markets takes time, so we might skip strict checking 
        # and just try-catch the fetch.
        
        # Limit 100 is enough for our calcs
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='15m', limit=100)
        return ohlcv
    except Exception:
        return None

async def fetch_single_symbol_data(sessions, symbol):
    """
    Iterates through exchanges in priority order:
    1. Binance US
    2. Binance (World)
    3. Bybit
    4. MEXC
    """
    ohlcv = None
    source = "Unknown"

    # Priority 1: Binance US
    if 'binanceus' in sessions:
        ohlcv = await fetch_ohlcv_from_exchange(sessions['binanceus'], symbol)
        if ohlcv: source = "BinanceUS"

    # Priority 2: Binance (Global)
    if not ohlcv and 'binance' in sessions:
        ohlcv = await fetch_ohlcv_from_exchange(sessions['binance'], symbol)
        if ohlcv: source = "Binance"

    # Priority 3: Bybit
    if not ohlcv and 'bybit' in sessions:
        ohlcv = await fetch_ohlcv_from_exchange(sessions['bybit'], symbol)
        if ohlcv: source = "Bybit"

    # Priority 4: MEXC
    if not ohlcv and 'mexc' in sessions:
        ohlcv = await fetch_ohlcv_from_exchange(sessions['mexc'], symbol)
        if ohlcv: source = "MEXC"

    # Process Data if found
    if ohlcv and len(ohlcv) >= 2:
        last_closed_candle = ohlcv[-2]
        current_price = last_closed_candle[4]
        open_15m = last_closed_candle[1]
        change_15m = ((current_price - open_15m) / open_15m) * 100
        change_1h = calculate_time_aligned_change(ohlcv, 1)
        change_4h = calculate_time_aligned_change(ohlcv, 4)
        change_24h = calculate_time_aligned_change(ohlcv, 24)

        return {
            "Symbol": symbol,
            "Price": current_price,
            "Source": source,
            "15m": change_15m,
            "1h": change_1h if change_1h != -9999 else None,
            "4h": change_4h if change_4h != -9999 else None,
            "24h": change_24h if change_24h != -9999 else None,
        }
    return None

async def get_all_data():
    # 1. Get Symbols from CoinDCX
    symbols = await get_coindcx_futures_symbols()
    if not symbols:
        return []

    # 2. Initialize Exchanges
    # We create instances once to reuse connections
    exchanges = {}
    try:
        exchanges['binanceus'] = ccxt.binanceus({'enableRateLimit': True})
        exchanges['binance'] = ccxt.binance({'enableRateLimit': True})
        exchanges['bybit'] = ccxt.bybit({'enableRateLimit': True})
        exchanges['mexc'] = ccxt.mexc({'enableRateLimit': True})

        # 3. Fetch Data in Batches
        batch_size = 20 # Smaller batch size because we are hitting multiple exchanges
        all_results = []
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            tasks = [fetch_single_symbol_data(exchanges, sym) for sym in batch]
            results = await asyncio.gather(*tasks)
            all_results.extend([r for r in results if r is not None])
            # Sleep to respect rate limits
            await asyncio.sleep(0.5)

    except Exception as e:
        st.error(f"Critical Error: {e}")
    finally:
        # Close all exchange connections
        for ex in exchanges.values():
            await ex.close()

    return all_results

# --- 3. Display Logic ---

st.title("🌐 Multi-Exchange Crypto Tracker")
st.caption("Symbols: CoinDCX | Data: Binance US -> Binance -> Bybit -> MEXC")

@st.fragment(run_every=60) # Increased to 60s because this is a HEAVY operation
def show_live_data():
    with st.spinner("Fetching data across exchanges... (This takes time)"):
        try:
            data = asyncio.run(get_all_data())
        except Exception as e:
            st.error(f"Async Error: {e}")
            return

    if not data:
        st.warning("No data found. Retrying...")
        return

    df = pd.DataFrame(data)
    
    if df.empty:
         st.warning("Dataframe is empty.")
         return

    # Sort
    df = df.sort_values(by="15m", ascending=False)
    df.reset_index(drop=True, inplace=True)
    df.index += 1
    df.index.name = "Sr"

    # Formatting
    def format_pct(val):
        if val is None or val == -9999: return "N/A"
        return "{:.2f}%".format(val)
    
    def format_price(val):
        if val is None: return "N/A"
        if val < 0.1: return "${:.6f}".format(val) # More decimals for small coins
        return "${:.2f}".format(val)

    def color_map(val):
        if val is None or val == -9999: return ""
        color = '#4CAF50' if val > 0 else '#FF5252'
        return f'color: {color}; font-weight: bold;'

    st.dataframe(
        df.style.map(color_map, subset=['15m', '1h', '4h', '24h'])
            .format({
                "Price": format_price, 
                "15m": format_pct, 
                "1h": format_pct, 
                "4h": format_pct, 
                "24h": format_pct
            }),
        use_container_width=True, 
        height=800
    )
    st.caption(f"Last Updated: {datetime.now().strftime('%H:%M:%S')} | Total Pairs: {len(df)}")

show_live_data()
