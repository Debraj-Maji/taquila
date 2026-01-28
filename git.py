import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import aiohttp
from datetime import datetime, timedelta

# --- Configuration ---
st.set_page_config(page_title="Crypto Futures Tracker", layout="wide")

# Hide standard menus
st.markdown("""
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- Session State ---
if 'crypto_data' not in st.session_state:
    st.session_state.crypto_data = None
if 'last_fetch_time' not in st.session_state:
    st.session_state.last_fetch_time = None
if 'total_symbols_count' not in st.session_state:
    st.session_state.total_symbols_count = 0
if 'missing_symbols' not in st.session_state:
    st.session_state.missing_symbols = []

# --- 1. Dynamic Symbol Fetching (CoinDCX) ---
async def get_coindcx_futures_symbols():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=INR"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    symbols = []
                    for s in data:
                        if isinstance(s, str) and s.endswith('USDT'):
                            clean = s.replace("B-", "").replace("_", "/")
                            symbols.append(clean)
                    return sorted(list(set(symbols)))
    except Exception:
        return []
    return []

# --- 2. Optimized Fetching Logic ---

def calculate_time_aligned_change(ohlcv, interval_hours):
    """Calculates change based on aligned clock blocks (1h, 4h)"""
    if not ohlcv: return -9999
    ms_per_hour = 3600 * 1000
    ms_interval = interval_hours * ms_per_hour
    last_ts = ohlcv[-1][0]
    current_block_start = last_ts - (last_ts % ms_interval)
    target_open_ts = current_block_start - ms_interval
    
    open_price = None
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

def calculate_day_change(ohlcv):
    """
    Calculates change from TODAY 00:00 UTC to Last Completed Candle Close.
    """
    if not ohlcv or len(ohlcv) < 2: return -9999
    
    last_ts = ohlcv[-1][0] 
    ms_per_day = 86400000 # 24 * 60 * 60 * 1000
    start_of_day_ts = last_ts - (last_ts % ms_per_day)
    
    day_open_price = None
    for candle in ohlcv:
        if candle[0] == start_of_day_ts:
            day_open_price = candle[1]
            break
            
    last_completed_candle = ohlcv[-2]
    last_completed_ts = last_completed_candle[0]
    last_completed_close = last_completed_candle[4]
    
    if last_completed_ts < start_of_day_ts:
        return 0.0
        
    if day_open_price is not None:
        return ((last_completed_close - day_open_price) / day_open_price) * 100
        
    return -9999

async def fetch_ohlcv_direct(exchange, symbol, source_name):
    """Fetch directly from the known valid exchange"""
    try:
        # Fetch 100 candles (Enough for 24h calculation)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='15m', limit=100)
        if not ohlcv or len(ohlcv) < 2: return None

        last_closed_candle = ohlcv[-2]
        current_price = last_closed_candle[4]
        open_15m = last_closed_candle[1]
        
        return {
            "Symbol": symbol,
            "Price": current_price,
            "Source": source_name,
            "15m": ((current_price - open_15m) / open_15m) * 100,
            "1h": calculate_time_aligned_change(ohlcv, 1),
            "4h": calculate_time_aligned_change(ohlcv, 4),
            "24h": calculate_day_change(ohlcv), # <--- UPDATED LOGIC HERE
        }
    except Exception:
        return None

async def safe_load_markets(exchange, name):
    try:
        await exchange.load_markets()
        return True
    except Exception:
        return False

async def get_all_data():
    # 1. Get Target List from CoinDCX
    target_symbols = await get_coindcx_futures_symbols()
    st.session_state.total_symbols_count = len(target_symbols)
    if not target_symbols: return []

    # 2. Init Only Allowed Exchanges
    all_exchanges = {
        'BinanceUS': ccxt.binanceus({'enableRateLimit': True}),
        'MEXC': ccxt.mexc({'enableRateLimit': True}) 
    }

    active_exchanges = {}

    try:
        # 3. Load Markets
        tasks = [safe_load_markets(ex, name) for name, ex in all_exchanges.items()]
        results = await asyncio.gather(*tasks)

        for (name, ex), success in zip(all_exchanges.items(), results):
            if success:
                active_exchanges[name] = ex
            else:
                await ex.close()

        if not active_exchanges:
            st.error("Could not connect to BinanceUS or MEXC.")
            return []

        # 4. Map Symbols (Priority: BinanceUS -> MEXC)
        valid_map = {} 
        priority_order = ['BinanceUS', 'MEXC'] # Check BinanceUS first, then MEXC
        
        # Only check available exchanges
        final_priority = [p for p in priority_order if p in active_exchanges]

        for symbol in target_symbols:
            for name in final_priority:
                ex = active_exchanges[name]
                if symbol in ex.markets:
                    valid_map[symbol] = (name, ex)
                    break 
        
        # 5. Fetch Data
        # MEXC allows many requests, so we can use a larger batch size
        batch_size = 100 
        all_results = []
        
        found_keys = valid_map.keys()
        st.session_state.missing_symbols = [s for s in target_symbols if s not in found_keys]

        tasks = []
        for symbol, (name, ex) in valid_map.items():
            tasks.append(fetch_ohlcv_direct(ex, symbol, name))

        progress_bar = st.progress(0)
        status_text = st.empty()
        total_tasks = len(tasks)
        
        if total_tasks > 0:
            for i in range(0, total_tasks, batch_size):
                batch = tasks[i:i+batch_size]
                status_text.text(f"Fetching batch {i//batch_size + 1}...")
                results = await asyncio.gather(*batch)
                all_results.extend([r for r in results if r is not None])
                
                progress_bar.progress(min((i + batch_size) / total_tasks, 1.0))
                await asyncio.sleep(0.5)

        progress_bar.empty()
        status_text.empty()
        return all_results

    except Exception as e:
        st.error(f"Error: {e}")
        return []
    finally:
        for ex in active_exchanges.values():
            await ex.close()

# --- 3. UI & Logic ---

st.title("🌐 CoinDCX Tracker (Day Change)")

@st.fragment(run_every=60)
def auto_scheduler():
    now = datetime.now()
    current_minute = now.minute
    
    col1, col2 = st.columns([3, 1])
    with col1:
        minutes_to_next = 15 - (current_minute % 15)
        next_run = (now + timedelta(minutes=minutes_to_next)).strftime('%H:%M')
        st.caption(f"🕒 Time: {now.strftime('%H:%M')} | Next: {next_run}")
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.session_state.last_fetch_time = None
            st.rerun()

    should_fetch = False
    if st.session_state.crypto_data is None:
        should_fetch = True
    elif current_minute % 15 == 0:
        last = st.session_state.last_fetch_time
        if last is None or last.minute != current_minute:
            should_fetch = True

    if should_fetch:
        with st.spinner("🚀 Fetching 15m Candles..."):
            new_data = asyncio.run(get_all_data())
            if new_data:
                st.session_state.crypto_data = new_data
                st.session_state.last_fetch_time = now

    if st.session_state.crypto_data:
        df = pd.DataFrame(st.session_state.crypto_data)
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Total List", st.session_state.total_symbols_count)
        m2.metric("Fetched", len(df))
        m3.metric("Missing", len(st.session_state.missing_symbols), delta_color="inverse")
        
        if st.session_state.missing_symbols:
            with st.expander("Show Missing Symbols"):
                st.write(", ".join(st.session_state.missing_symbols))
        
        st.divider()

        df = df.sort_values(by="15m", ascending=False)
        df.reset_index(drop=True, inplace=True)
        df.index += 1
        df.index.name = "Sr"

        def fmt_pct(v): return "N/A" if v == -9999 or v is None else f"{v:.2f}%"
        def fmt_prc(v): return "N/A" if v is None else (f"${v:.6f}" if v < 0.1 else f"${v:.2f}")
        def color(v): 
            if v == -9999 or v is None: return ""
            return f'color: {"#4CAF50" if v > 0 else "#FF5252"}; font-weight: bold;'

        st.dataframe(
            df.style.map(color, subset=['15m', '1h', '4h', '24h']).format({
                "Price": fmt_prc, "15m": fmt_pct, "1h": fmt_pct, "4h": fmt_pct, "24h": fmt_pct
            }),
            use_container_width=True, height=800, on_select="ignore"
        )
    else:
        st.info("Waiting for scheduled fetch...")

auto_scheduler()
