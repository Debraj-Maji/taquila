import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import aiohttp
import time
from datetime import datetime, timedelta

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

# --- Session State ---
if 'crypto_data' not in st.session_state:
    st.session_state.crypto_data = None
if 'last_fetch_time' not in st.session_state:
    st.session_state.last_fetch_time = None
if 'total_symbols_count' not in st.session_state:
    st.session_state.total_symbols_count = 0
if 'missing_symbols' not in st.session_state:
    st.session_state.missing_symbols = []
if 'fetch_logs' not in st.session_state:
    st.session_state.fetch_logs = []
if 'market_map' not in st.session_state:
    st.session_state.market_map = None 
# NEW: Stats Counters
if 'source_counts' not in st.session_state:
    st.session_state.source_counts = {"Bitget": 0, "BinanceUS": 0, "MEXC": 0}
if 'fetch_duration' not in st.session_state:
    st.session_state.fetch_duration = 0.0

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

# --- 2. Calculation Logic ---

def calculate_time_aligned_change(ohlcv, interval_hours):
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
    """Calculates change from TODAY 00:00 UTC to Last Completed Candle Close."""
    if not ohlcv or len(ohlcv) < 2: return -9999
    
    last_ts = ohlcv[-1][0] 
    ms_per_day = 86400000 
    start_of_day_ts = last_ts - (last_ts % ms_per_day)
    
    day_open_price = None
    for candle in ohlcv:
        if candle[0] == start_of_day_ts:
            day_open_price = candle[1]
            break
            
    last_completed_candle = ohlcv[-2]
    last_completed_close = last_completed_candle[4]
    
    if day_open_price is not None and day_open_price > 0:
        return ((last_completed_close - day_open_price) / day_open_price) * 100
        
    return -9999

async def fetch_ohlcv_direct(exchange, symbol, source_name):
    try:
        # Limit 200 ensures we find the 00:00 UTC candle
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='15m', limit=200)
        
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
            "24h": calculate_day_change(ohlcv),
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
    t_total_start = time.time() # START TIMER
    logs = []
    
    # 1. Get CoinDCX List
    target_symbols = await get_coindcx_futures_symbols()
    st.session_state.total_symbols_count = len(target_symbols)
    if not target_symbols: return [], {}, 0

    # 2. Init Exchanges
    all_exchanges = {
        'Bitget': ccxt.bitget({'options': {'defaultType': 'swap'}, 'enableRateLimit': True}),
        'MEXC': ccxt.mexc({'enableRateLimit': True}),
        'BinanceUS': ccxt.binanceus({'enableRateLimit': True})
    }
    
    active_exchanges = {}

    try:
        # 3. Load Markets & Map Symbols
        if st.session_state.market_map is None:
            tasks = [safe_load_markets(ex, name) for name, ex in all_exchanges.items()]
            results = await asyncio.gather(*tasks)

            for (name, ex), success in zip(all_exchanges.items(), results):
                if success:
                    active_exchanges[name] = ex
                else:
                    await ex.close()

            # Build Map: Priority Bitget > BinanceUS > MEXC
            market_map = {} 
            valid_symbols = []
            priority_order = [n for n in ['Bitget', 'BinanceUS', 'MEXC'] if n in active_exchanges]
            
            for sym in target_symbols:
                for name in priority_order:
                    ex = active_exchanges[name]
                    if sym in ex.markets:
                        market_map[sym] = name 
                        valid_symbols.append(sym)
                        break
            
            st.session_state.market_map = market_map
            
        else:
            # Restore connections
            tasks = [safe_load_markets(ex, name) for name, ex in all_exchanges.items()]
            await asyncio.gather(*tasks)
            active_exchanges = all_exchanges 
            market_map = st.session_state.market_map
            valid_symbols = list(market_map.keys())

        # Identify missing
        st.session_state.missing_symbols = [s for s in target_symbols if s not in valid_symbols]

        # 4. Fetch Data
        batch_size = 50 
        all_results = []
        
        # Prepare Tasks
        tasks = []
        for sym in valid_symbols:
            exchange_name = market_map[sym]
            if exchange_name in active_exchanges:
                ex_obj = active_exchanges[exchange_name]
                tasks.append(fetch_ohlcv_direct(ex_obj, sym, exchange_name))

        # Run Batches
        progress_bar = st.progress(0)
        status_text = st.empty()
        total_tasks = len(tasks)
        
        if total_tasks > 0:
            for i in range(0, total_tasks, batch_size):
                batch = tasks[i:i+batch_size]
                results = await asyncio.gather(*batch)
                all_results.extend([r for r in results if r is not None])
                
                progress_bar.progress(min((i + batch_size) / total_tasks, 1.0))
                await asyncio.sleep(0.5)

        progress_bar.empty()
        status_text.empty()
        
        # 5. Calculate Stats
        t_total_end = time.time()
        total_duration = t_total_end - t_total_start
        st.session_state.fetch_logs = logs
        
        # Count Sources
        counts = {"Bitget": 0, "BinanceUS": 0, "MEXC": 0}
        for item in all_results:
            src = item.get("Source", "Unknown")
            counts[src] = counts.get(src, 0) + 1
            
        return all_results, counts, total_duration

    except Exception as e:
        st.error(f"Error: {e}")
        return [], {}, 0
    finally:
        for ex in all_exchanges.values():
            await ex.close()

# --- 3. UI & Logic ---

st.title("🌐 CoinDCX Futures Tracker")

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
            st.session_state.crypto_data = None
            st.session_state.last_fetch_time = None
            st.session_state.source_counts = {"Bitget": 0, "BinanceUS": 0, "MEXC": 0} # Reset stats
            st.rerun()

    should_fetch = False
    if st.session_state.crypto_data is None:
        should_fetch = True
    elif current_minute % 15 == 0:
        last = st.session_state.last_fetch_time
        if last is None or last.minute != current_minute:
            should_fetch = True

    if should_fetch:
        with st.spinner("🚀 Fetching Data from Bitget, BinanceUS, MEXC..."):
            # Unpack the 3 return values
            new_data, new_counts, duration = asyncio.run(get_all_data())
            
            if new_data:
                st.session_state.crypto_data = new_data
                st.session_state.source_counts = new_counts
                st.session_state.fetch_duration = duration
                st.session_state.last_fetch_time = now

    if st.session_state.crypto_data:
        df = pd.DataFrame(st.session_state.crypto_data)
        
        # --- ROW 1: General Stats ---
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total List", st.session_state.total_symbols_count)
        m2.metric("Fetched", len(df))
        m3.metric("Missing", len(st.session_state.missing_symbols), delta_color="inverse")
        m4.metric("⏱️ Total Time", f"{st.session_state.fetch_duration:.1f}s")
        
        # --- ROW 2: Source Breakdown ---
        s1, s2, s3, s4 = st.columns(4)
        counts = st.session_state.source_counts
        s1.metric("🔵 Bitget", counts.get("Bitget", 0))
        s2.metric("🟡 BinanceUS", counts.get("BinanceUS", 0))
        s3.metric("🟢 MEXC", counts.get("MEXC", 0))
        s4.empty() # Spacer
        
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
        
        # Footer Expander
        with st.expander("⚠️ Missing Symbols List"):
             st.write(", ".join(st.session_state.missing_symbols) if st.session_state.missing_symbols else "None")

    else:
        st.info("Initializing...")

auto_scheduler()

