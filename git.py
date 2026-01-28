import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import aiohttp
import time
from datetime import datetime, timedelta

# --- Configuration ---
st.set_page_config(page_title="CoinDCX Futures (Bitget Data)", layout="wide")

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

# --- 2. Bitget Fetching Logic (Matches your Local Script) ---

def calculate_time_aligned_change(ohlcv, interval_hours):
    """
    Exact copy of your local script logic.
    Calculates change based on aligned clock times (rolling windows).
    """
    if not ohlcv: return -9999
    
    ms_per_hour = 3600 * 1000
    ms_interval = interval_hours * ms_per_hour
    last_ts = ohlcv[-1][0]
    
    # Calculate the Timestamp for the Start of the PREVIOUS interval
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
        # The close price is the close of the candle BEFORE the current block starts
        close_price = ohlcv[current_block_index - 1][4]
        return ((close_price - open_price) / open_price) * 100
        
    return -9999

async def fetch_ohlcv_bitget(exchange, symbol):
    try:
        # Fetch 110 candles (Matches your local script limit)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='15m', limit=110)
        
        if not ohlcv or len(ohlcv) < 2: return None

        # Matches your local script logic exactly
        last_closed_candle = ohlcv[-2]
        current_price = last_closed_candle[4]
        open_15m = last_closed_candle[1]
        
        return {
            "Symbol": symbol,
            "Price": current_price,
            "Source": "Bitget",
            "15m": ((current_price - open_15m) / open_15m) * 100,
            "1h": calculate_time_aligned_change(ohlcv, 1),
            "4h": calculate_time_aligned_change(ohlcv, 4),
            "24h": calculate_time_aligned_change(ohlcv, 24), # Used your local logic here
        }
    except Exception:
        return None

async def get_all_data():
    logs = []
    
    # 1. Get List
    t_start = time.time()
    target_symbols = await get_coindcx_futures_symbols()
    t_end = time.time()
    logs.append(f"Fetched List: {len(target_symbols)} symbols in {t_end - t_start:.2f}s")
    
    st.session_state.total_symbols_count = len(target_symbols)
    if not target_symbols: return []

    # 2. Init Bitget (Exactly like local script)
    exchange = ccxt.bitget({
        'options': {'defaultType': 'swap'}, 
        'enableRateLimit': True
    })

    try:
        # 3. Load Markets to Map Symbols
        t_start = time.time()
        try:
            await exchange.load_markets()
            logs.append(f"Loaded Bitget Markets in {time.time() - t_start:.2f}s")
        except Exception as e:
            st.error(f"Bitget Connection Error: {e}")
            await exchange.close()
            return []

        # 4. Filter Valid Symbols
        valid_symbols = []
        for sym in target_symbols:
            if sym in exchange.markets:
                valid_symbols.append(sym)
        
        st.session_state.missing_symbols = [s for s in target_symbols if s not in valid_symbols]
        
        # 5. Fetch Data
        batch_size = 50 
        all_results = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        total_valid = len(valid_symbols)
        
        if total_valid > 0:
            for i in range(0, total_valid, batch_size):
                b_start = time.time()
                
                batch = valid_symbols[i:i+batch_size]
                batch_num = i//batch_size + 1
                status_text.text(f"Fetching batch {batch_num}...")
                
                tasks = [fetch_ohlcv_bitget(exchange, sym) for sym in batch]
                results = await asyncio.gather(*tasks)
                all_results.extend([r for r in results if r is not None])
                
                b_end = time.time()
                logs.append(f"Batch {batch_num}: {b_end - b_start:.2f}s")
                
                progress_bar.progress(min((i + batch_size) / total_valid, 1.0))
                # Bitget rate limits are stricter than MEXC
                await asyncio.sleep(0.5)

        progress_bar.empty()
        status_text.empty()
        
        st.session_state.fetch_logs = logs
        return all_results

    except Exception as e:
        st.error(f"Error: {e}")
        return []
    finally:
        await exchange.close()

# --- 3. UI & Logic ---

st.title("🌐 CoinDCX Futures (Bitget Source)")

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
        with st.spinner("🚀 Fetching Bitget Data..."):
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
        
        with st.expander("⏱️ Logs & Missing Symbols"):
            tab1, tab2 = st.tabs(["Missing", "Logs"])
            with tab1:
                st.write(", ".join(st.session_state.missing_symbols) if st.session_state.missing_symbols else "None")
            with tab2:
                for log in st.session_state.fetch_logs: st.text(log)
        
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
