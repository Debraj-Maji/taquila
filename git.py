import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import aiohttp
from datetime import datetime, timedelta

# --- Configuration ---
st.set_page_config(page_title="CoinDCX Futures Ultra-Fast", layout="wide")
st.markdown("""<style>#MainMenu {visibility: hidden;} footer {visibility: hidden;} header {visibility: hidden;}</style>""", unsafe_allow_html=True)

# --- Session State ---
if 'crypto_data' not in st.session_state: st.session_state.crypto_data = None
if 'last_update' not in st.session_state: st.session_state.last_update = None

# --- 1. Get CoinDCX List ---
async def get_coindcx_symbols():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=INR"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Clean "B-BTC_USDT" -> "BTC/USDT"
                    return sorted(list(set([s.replace("B-", "").replace("_", "/") for s in data if s.endswith('USDT')])))
    except: return []
    return []

# --- 2. Ultra-Fast Bulk Fetch ---
async def fetch_bulk_data():
    target_symbols = await get_coindcx_symbols()
    if not target_symbols: return []

    # Initialize Exchanges
    # We use 'enableRateLimit': False because we are making only ~1 call per exchange!
    ex_binance = ccxt.binance({'options': {'defaultType': 'swap'}}) # USDT-M Futures
    ex_bybit = ccxt.bybit({'options': {'defaultType': 'swap'}})
    ex_mexc = ccxt.mexc({'options': {'defaultType': 'swap'}})

    final_data = {} # { 'BTC/USDT': { '15m': ..., 'Price': ... } }

    try:
        # --- A. BINANCE (The Heavy Lifter) ---
        # We fetch 4 different "Windows" of data for ALL coins in parallel
        # This is 4 API calls TOTAL.
        
        async def fetch_binance_window(window_size):
            # params={"windowSize": "15m"} asks Binance for 15m stats for ALL coins
            return await ex_binance.fetch_tickers(params={"windowSize": window_size})

        tasks = [
            fetch_binance_window("15m"),
            fetch_binance_window("1h"),
            fetch_binance_window("4h"),
            ex_binance.fetch_tickers() # Standard 24h
        ]
        
        # Run all 4 Binance calls at once
        b_15m, b_1h, b_4h, b_24h = await asyncio.gather(*tasks, return_exceptions=True)

        # Process Binance Data
        if isinstance(b_24h, dict):
            for symbol, ticker in b_24h.items():
                if symbol not in final_data: final_data[symbol] = {}
                final_data[symbol]['Price'] = ticker.get('last')
                final_data[symbol]['24h'] = ticker.get('percentage')
                final_data[symbol]['Source'] = 'Binance'

        # Helper to inject window stats
        def inject_window(dataset, key_name):
            if isinstance(dataset, dict):
                for symbol, ticker in dataset.items():
                    if symbol in final_data:
                        # Binance returns "priceChangePercent" which is the % change
                        final_data[symbol][key_name] = ticker.get('percentage')

        inject_window(b_15m, '15m')
        inject_window(b_1h, '1h')
        inject_window(b_4h, '4h')

        # --- B. BYBIT & MEXC (Fill in the gaps) ---
        # Only fetch standard 24h ticker for these, as they don't support clean bulk "15m" windows
        
        # Identify what's missing
        missing_symbols = [s for s in target_symbols if s not in final_data]
        
        if missing_symbols:
            # Fetch ALL Bybit tickers in 1 call
            try:
                bybit_tickers = await ex_bybit.fetch_tickers()
                for symbol in missing_symbols:
                    if symbol in bybit_tickers:
                        t = bybit_tickers[symbol]
                        final_data[symbol] = {
                            'Price': t.get('last'),
                            '24h': t.get('percentage'),
                            'Source': 'Bybit'
                            # 15m/1h/4h will be None (N/A)
                        }
            except: pass

        # Re-check missing
        missing_symbols = [s for s in target_symbols if s not in final_data]
        
        if missing_symbols:
            # Fetch ALL MEXC tickers in 1 call
            try:
                mexc_tickers = await ex_mexc.fetch_tickers()
                for symbol in missing_symbols:
                    if symbol in mexc_tickers:
                        t = mexc_tickers[symbol]
                        final_data[symbol] = {
                            'Price': t.get('last'),
                            '24h': t.get('percentage'),
                            'Source': 'MEXC'
                        }
            except: pass

    except Exception as e:
        st.error(f"Bulk Fetch Error: {e}")
    finally:
        await ex_binance.close()
        await ex_bybit.close()
        await ex_mexc.close()

    # --- C. Filter & Format ---
    # We gathered data for ALL Binance coins (thousands). 
    # Now we filter down to ONLY the 400 CoinDCX coins.
    
    clean_results = []
    for sym in target_symbols:
        if sym in final_data:
            d = final_data[sym]
            clean_results.append({
                "Symbol": sym,
                "Price": d.get('Price'),
                "15m": d.get('15m'),
                "1h": d.get('1h'),
                "4h": d.get('4h'),
                "24h": d.get('24h'),
                "Source": d.get('Source')
            })
    
    return clean_results

# --- 3. UI & Scheduler ---

st.title("⚡ CoinDCX Speed Tracker (Bulk Mode)")

@st.fragment(run_every=60)
def ui_fragment():
    now = datetime.now()
    
    # Refresh Logic
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption(f"Last Update: {st.session_state.last_update} | Mode: Ultra-Fast (Bulk API)")
    with col2:
        if st.button("🔄 Force Refresh"):
            st.session_state.crypto_data = None
            st.rerun()

    # Auto-Fetch Trigger (Every 15m or First Load)
    should_fetch = False
    if st.session_state.crypto_data is None: should_fetch = True
    elif now.minute % 15 == 0:
        # Simple debounce: Don't fetch if we just fetched in this same minute
        last = st.session_state.last_update
        if not last or last.split(":")[1] != str(now.minute):
            should_fetch = True

    if should_fetch:
        with st.spinner("🚀 Bulk fetching 15m/1h/4h stats for ALL coins..."):
            data = asyncio.run(fetch_bulk_data())
            if data:
                st.session_state.crypto_data = data
                st.session_state.last_update = now.strftime("%H:%M")

    # Display Table
    if st.session_state.crypto_data:
        df = pd.DataFrame(st.session_state.crypto_data)
        
        # Metrics
        found = len(df)
        total = 408 # Approx
        missing = total - found
        
        m1, m2 = st.columns(2)
        m1.metric("Tracked Pairs", found)
        m2.metric("Missing/Offline", missing, delta_color="inverse")
        
        st.divider()
        
        # Formatting
        df = df.sort_values(by="15m", ascending=False)
        df.reset_index(drop=True, inplace=True)
        df.index += 1
        df.index.name = "Sr"

        def fmt_pct(v): return "N/A" if pd.isna(v) or v is None else f"{v:.2f}%"
        def fmt_prc(v): return "N/A" if pd.isna(v) or v is None else (f"${v:.6f}" if v < 0.1 else f"${v:.2f}")
        def color(v): 
            if pd.isna(v) or v is None: return ""
            return f'color: {"#4CAF50" if v > 0 else "#FF5252"}; font-weight: bold;'

        st.dataframe(
            df.style.map(color, subset=['15m', '1h', '4h', '24h'])
                .format({"Price": fmt_prc, "15m": fmt_pct, "1h": fmt_pct, "4h": fmt_pct, "24h": fmt_pct}),
            use_container_width=True, height=800, on_select="ignore"
        )

ui_fragment()
