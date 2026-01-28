import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import aiohttp
from datetime import datetime, timedelta

# --- Configuration ---
st.set_page_config(page_title="CoinDCX Futures 15m Tracker", layout="wide")

# Hide standard menus and footer
st.markdown("""
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- Session State Initialization ---
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
                            # Clean: "B-BTC_USDT" -> "BTC/USDT"
                            clean = s.replace("B-", "").replace("_", "/")
                            symbols.append(clean)
                    return sorted(list(set(symbols)))
    except Exception:
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
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe='15m', limit=100)
    except Exception:
        return None

async def fetch_single_symbol_data(sessions, symbol):
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

    # Priority 3: MEXC
    if not ohlcv and 'mexc' in sessions:
        ohlcv = await fetch_ohlcv_from_exchange(sessions['mexc'], symbol)
        if ohlcv: source = "MEXC"
            
    # Priority 4: Bybit
    if not ohlcv and 'bybit' in sessions:
        ohlcv = await fetch_ohlcv_from_exchange(sessions['bybit'], symbol)
        if ohlcv: source = "Bybit"



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
    symbols = await get_coindcx_futures_symbols()
    
    st.session_state.total_symbols_count = len(symbols)
    
    if not symbols:
        return []

    exchanges = {}
    try:
        exchanges['binanceus'] = ccxt.binanceus({'enableRateLimit': True})
        exchanges['binance'] = ccxt.binance({'enableRateLimit': True})
        exchanges['bybit'] = ccxt.bybit({'enableRateLimit': True})
        exchanges['mexc'] = ccxt.mexc({'enableRateLimit': True})

        batch_size = 20
        all_results = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_batches = (len(symbols) // batch_size) + 1
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            
            status_text.text(f"Fetching batch {i//batch_size + 1}/{total_batches}...")
            
            tasks = [fetch_single_symbol_data(exchanges, sym) for sym in batch]
            results = await asyncio.gather(*tasks)
            all_results.extend([r for r in results if r is not None])
            
            progress_bar.progress(min(((i // batch_size) + 1) / total_batches, 1.0))
            await asyncio.sleep(0.5)
            
        progress_bar.empty()
        status_text.empty()
        
        # --- NEW: Calculate Missing Symbols ---
        found_symbols = [item['Symbol'] for item in all_results]
        missing = [s for s in symbols if s not in found_symbols]
        st.session_state.missing_symbols = missing

    except Exception as e:
        st.error(f"Error: {e}")
    finally:
        for ex in exchanges.values():
            await ex.close()

    return all_results

# --- 3. Display & Scheduling Logic ---

st.title("🌐 CoinDCX Futures (15m Interval)")

@st.fragment(run_every=60)
def auto_scheduler():
    now = datetime.now()
    current_minute = now.minute
    
    # --- Layout for Controls ---
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Calculate Next Run
        minutes_to_next = 15 - (current_minute % 15)
        next_run_time = now + timedelta(minutes=minutes_to_next)
        next_run_time = next_run_time.replace(second=0, microsecond=0)
        st.caption(f"🕒 Current Time: {now.strftime('%H:%M')} | Next Auto-Fetch: {next_run_time.strftime('%H:%M')} (in {minutes_to_next} min)")

    with col2:
        # 1. REFRESH BUTTON
        if st.button("Refresh Now 🔄", use_container_width=True):
            st.session_state.last_fetch_time = None # Force reset
            st.rerun() # Restart script to trigger fetch immediately

    # --- FETCH LOGIC ---
    should_fetch = False
    
    if st.session_state.crypto_data is None:
        should_fetch = True
    elif current_minute % 15 == 0:
        last_run = st.session_state.last_fetch_time
        if last_run is None or last_run.minute != current_minute:
            should_fetch = True

    if should_fetch:
        with st.spinner(f"It's {now.strftime('%H:%M')}. Fetching 15m candle data..."):
            try:
                new_data = asyncio.run(get_all_data())
                if new_data:
                    st.session_state.crypto_data = new_data
                    st.session_state.last_fetch_time = now
            except Exception as e:
                st.error(f"Fetch failed: {e}")
    
    # --- DISPLAY METRICS ---
    if st.session_state.crypto_data:
        df = pd.DataFrame(st.session_state.crypto_data)
        
        total_fetched = len(df)
        total_list = st.session_state.total_symbols_count
        not_found_count = len(st.session_state.missing_symbols)
        
        # Metrics Row
        m1, m2, m3 = st.columns(3)
        m1.metric("Total CoinDCX List", total_list)
        m2.metric("Successfully Fetched", total_fetched)
        m3.metric("Not Found / Offline", not_found_count, delta_color="inverse")
        
        # 2. NOT FOUND LIST (Expander)
        if not_found_count > 0:
            with st.expander(f"⚠️ Show {not_found_count} Missing Symbols"):
                st.write(", ".join(st.session_state.missing_symbols))
        
        st.divider()

        # Sort and Format
        df = df.sort_values(by="15m", ascending=False)
        df.reset_index(drop=True, inplace=True)
        df.index += 1
        df.index.name = "Sr"

        def format_pct(val):
            if val is None or val == -9999: return "N/A"
            return "{:.2f}%".format(val)
        
        def format_price(val):
            if val is None: return "N/A"
            if val < 0.1: return "${:.6f}".format(val)
            return "${:.2f}".format(val)

        def color_map(val):
            if val is None or val == -9999: return ""
            color = '#4CAF50' if val > 0 else '#FF5252'
            return f'color: {color}; font-weight: bold;'

        # 3. CLICKABLE TABLE (Disabled row selection, kept header sorting)
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
            height=800,
            # This ensures standard sorting behavior without highlighting full rows
            on_select="ignore" 
        )
    else:
        st.info("Initializing... Waiting for data.")

# Start
auto_scheduler()
