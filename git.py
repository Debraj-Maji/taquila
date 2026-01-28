import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import time
from datetime import datetime

# --- 1. Configuration ---
st.set_page_config(page_title="Crypto Tracker", layout="wide")

# Hide standard Streamlit menu for a cleaner "App-like" look
hide_menu_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(hide_menu_style, unsafe_allow_html=True)

RAW_SYMBOLS = [
    "B-SAHARA_USDT","B-FLOW_USDT","B-BCH_USDT","B-XRP_USDT","B-LTC_USDT","B-TRX_USDT","B-SANTOS_USDT","B-PUMP_USDT","B-MMT_USDT","B-LINK_USDT","B-XLM_USDT","B-XMR_USDT","B-DASH_USDT","B-ZEC_USDT","B-BNB_USDT","B-SWELL_USDT","B-IOTA_USDT","B-BAT_USDT","B-IOST_USDT","B-PNUT_USDT","B-KNC_USDT","B-ZRX_USDT","B-BAND_USDT","B-HIPPO_USDT","B-RLC_USDT","B-CRV_USDT","B-FARTCOIN_USDT","B-TRB_USDT","B-RUNE_USDT","B-SOL_USDT","B-THE_USDT","B-MORPHO_USDT","B-PYTH_USDT","B-ICX_USDT","B-KAIA_USDT","B-STORJ_USDT","B-ACX_USDT","B-ORCA_USDT","B-UNI_USDT","B-ETC_USDT","B-KOMA_USDT","B-VIRTUAL_USDT","B-SPX_USDT","B-ENJ_USDT","B-AVA_USDT","B-ASTR_USDT","B-1000RATS_USDT","B-ERA_USDT","B-VELODROME_USDT","B-ACE_USDT","B-VANA_USDT","B-PENGU_USDT","B-ADA_USDT","B-AAVE_USDT","B-FIL_USDT","B-IMX_USDT","B-RSR_USDT","B-A2Z_USDT","B-USUAL_USDT","B-AIXBT_USDT","B-KMNO_USDT","B-CGPT_USDT","B-BEL_USDT","B-ATOM_USDT","B-HIVE_USDT","B-ONDO_USDT","B-DEXE_USDT","B-1000PEPE_USDT","B-PHA_USDT","B-WAL_USDT","B-TOWNS_USDT","B-PROVE_USDT","B-GRT_USDT","B-ALGO_USDT","B-1INCH_USDT","B-ZIL_USDT","B-SAND_USDT","B-BNT_USDT","B-RVN_USDT","B-INIT_USDT","B-SFP_USDT","B-BIO_USDT","B-COOKIE_USDT","B-AWE_USDT","B-COTI_USDT","B-API3_USDT","B-SONIC_USDT","B-PORTAL_USDT","B-PROM_USDT","B-S_USDT","B-SOLV_USDT","B-EGLD_USDT","B-MANA_USDT","B-ARC_USDT","B-GMT_USDT","B-AVAAI_USDT","B-RARE_USDT","B-AVAX_USDT","B-TRUMP_USDT","B-KSM_USDT","B-C98_USDT","B-MASK_USDT","B-DYDX_USDT","B-GALA_USDT","B-MELANIA_USDT","B-NEO_USDT","B-ANIME_USDT","B-VINE_USDT","B-SKL_USDT","B-OP_USDT","B-INJ_USDT","B-CHZ_USDT","B-1000LUNC_USDT","B-KITE_USDT","B-FET_USDT","B-FXS_USDT","B-HOT_USDT","B-MINA_USDT","B-PHB_USDT","B-GMX_USDT","B-MITO_USDT","B-PIPPIN_USDT","B-HBAR_USDT","B-CFX_USDT","B-MTL_USDT","B-ACH_USDT","B-SSV_USDT","B-CKB_USDT","B-IOTX_USDT","B-ONG_USDT","B-TRU_USDT","B-OGN_USDT","B-LQTY_USDT","B-ID_USDT","B-BLUR_USDT","B-ETHW_USDT","B-GTC_USDT","B-VVV_USDT","B-EDU_USDT","B-SUI_USDT","B-JTO_USDT","B-HAEDAL_USDT","B-ATA_USDT","B-ARPA_USDT","B-AUCTION_USDT","B-APE_USDT","B-UMA_USDT","B-HEMI_USDT","B-JASMY_USDT","B-GRIFFAIN_USDT","B-AI_USDT","B-FIS_USDT","B-PENDLE_USDT","B-ARKM_USDT","B-QNT_USDT","B-XAI_USDT","B-MAGIC_USDT","B-2Z_USDT","B-BERA_USDT","B-T_USDT","B-ZETA_USDT","B-OXT_USDT","B-BIGTIME_USDT","B-OG_USDT","B-LAYER_USDT","B-MOVE_USDT","B-SUSHI_USDT","B-BSV_USDT","B-RONIN_USDT","B-DYM_USDT","B-GAS_USDT","B-SOPH_USDT","B-IP_USDT","B-HYPE_USDT","B-POWR_USDT","B-ROSE_USDT","B-MEME_USDT","B-COMP_USDT","B-ARB_USDT","B-TOKEN_USDT","B-JOE_USDT","B-1000SHIB_USDT","B-AEVO_USDT","B-VANRY_USDT","B-SEI_USDT","B-BOME_USDT","B-TNSR_USDT","B-CYBER_USDT","B-GPS_USDT","B-ZKC_USDT","B-BRETT_USDT","B-POPCAT_USDT","B-POLYX_USDT","B-SHELL_USDT","B-TIA_USDT","B-KAITO_USDT","B-MBOX_USDT","B-CAKE_USDT","B-EPIC_USDT","B-BMT_USDT","B-FORM_USDT","B-TUT_USDT","B-ORDI_USDT","B-BROCCOLI714_USDT","B-KAS_USDT","B-1000FLOKI_USDT","B-1000BONK_USDT","B-BANANAS31_USDT","B-SPELL_USDT","B-NIL_USDT","B-FLUX_USDT","B-APT_USDT","B-MAVIA_USDT","B-PERP_USDT","B-RPL_USDT","B-FIDA_USDT","B-FIO_USDT","B-HMSTR_USDT","B-REI_USDT","B-EIGEN_USDT","B-1000CAT_USDT","B-GOAT_USDT","B-MOODENG_USDT","B-PAXG_USDT","B-ZEREBRO_USDT","B-WAXP_USDT","B-LSK_USDT","B-ALT_USDT","B-MLN_USDT","B-NTRN_USDT","B-ATH_USDT","B-STEEM_USDT","B-JUP_USDT","B-XCN_USDT","B-ILV_USDT","B-SAFE_USDT","B-OM_USDT","B-STO_USDT","B-KERNEL_USDT","B-WCT_USDT","B-JST_USDT","B-TON_USDT","B-PUNDIX_USDT","B-DOLO_USDT","B-EDEN_USDT","B-NOM_USDT","B-SXT_USDT","B-YGG_USDT","B-1000SATS_USDT","B-ASR_USDT","B-ALPINE_USDT","B-SYRUP_USDT","B-NXPC_USDT","B-AXL_USDT","B-HUMA_USDT","B-PONKE_USDT","B-GIGGLE_USDT","B-A_USDT","B-SCR_USDT","B-GLM_USDT","B-LA_USDT","B-HOME_USDT","B-DOGE_USDT","B-VIC_USDT","B-RESOLV_USDT","B-USTC_USDT","B-SPK_USDT","B-YB_USDT","B-F_USDT","B-ENA_USDT","B-GUN_USDT","B-NEWT_USDT","B-EUL_USDT","B-ENSO_USDT","B-BABY_USDT","B-ZBT_USDT","B-TURTLE_USDT","B-CC_USDT","B-RIF_USDT","B-SXP_USDT","B-ALLO_USDT","B-TAO_USDT","B-ARK_USDT","B-BB_USDT","B-RED_USDT","B-NOT_USDT","B-MEW_USDT","B-NFP_USDT","B-CELR_USDT","B-MOCA_USDT","B-RENDER_USDT","B-BANANA_USDT","B-KAVA_USDT","B-PLUME_USDT","B-QUICK_USDT","B-POL_USDT","B-TREE_USDT","B-ENS_USDT","B-XVS_USDT","B-SYN_USDT","B-OPEN_USDT","B-SKY_USDT","B-AVNT_USDT","B-ETHFI_USDT","B-IO_USDT","B-LISTA_USDT","B-ZRO_USDT","B-HOLO_USDT","B-BARD_USDT","B-NMR_USDT","B-CHR_USDT","B-SUN_USDT","B-XTZ_USDT","B-HOOK_USDT","B-DOGS_USDT","B-FF_USDT","B-AR_USDT","B-CHILLGUY_USDT","B-YFI_USDT","B-HIGH_USDT","B-COS_USDT","B-LDO_USDT","B-DIA_USDT","B-SIGN_USDT","B-MOVR_USDT","B-WIF_USDT","B-CETUS_USDT","B-1000000MOG_USDT","B-ALICE_USDT","B-GRASS_USDT","B-C_USDT","B-DEGEN_USDT","B-DRIFT_USDT","B-ZK_USDT","B-XPL_USDT","B-WLFI_USDT","B-SOMI_USDT","B-RDNT_USDT","B-NKN_USDT","B-COW_USDT","B-HYPER_USDT","B-LINEA_USDT","B-0G_USDT","B-BAN_USDT","B-STX_USDT","B-VET_USDT","B-DENT_USDT","B-ASTER_USDT","B-MIRA_USDT","B-LUMIA_USDT","B-DOT_USDT","B-HFT_USDT","B-THETA_USDT","B-WLD_USDT","B-LRC_USDT","B-1MBABYDOGE_USDT","B-ZEN_USDT","B-NEAR_USDT","B-PARTI_USDT","B-REZ_USDT","B-SNX_USDT","B-MUBARAK_USDT","B-MANTA_USDT","B-AKT_USDT","B-LPT_USDT","B-AERO_USDT","B-ICP_USDT","B-AGLD_USDT","B-SUPER_USDT","B-TLM_USDT","B-SYS_USDT","B-TWT_USDT","B-LUNA2_USDT","B-ACT_USDT","B-PIXEL_USDT","B-SAGA_USDT","B-CELO_USDT","B-HEI_USDT","B-CTSI_USDT","B-WOO_USDT","B-MAV_USDT","B-DUSK_USDT","B-ME_USDT","B-SWARMS_USDT","B-AXS_USDT","B-USDC_USDT","B-BTC_USDT","B-VTHO_USDT","B-QTUM_USDT","B-W_USDT","B-SCRT_USDT","B-METIS_USDT","B-STRK_USDT","B-CATI_USDT","B-ETH_USDT","B-FLM_USDT","B-G_USDT","B-BICO_USDT","B-ONT_USDT","B-PEOPLE_USDT","B-CHESS_USDT","B-B3_USDT","B-ONE_USDT","B-STG_USDT","B-ALCH_USDT"
]

def clean_symbol(s):
    return s.replace("B-", "").replace("_", "/")

CLEANED_SYMBOLS = [clean_symbol(s) for s in RAW_SYMBOLS]

# --- 2. Logic (Copied exactly from your script) ---

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

async def fetch_single_pair(exchange, symbol, original_name):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='15m', limit=110)
        if len(ohlcv) < 2: return None
        
        last_closed_candle = ohlcv[-2]
        current_price = last_closed_candle[4]
        open_15m = last_closed_candle[1]
        change_15m = ((current_price - open_15m) / open_15m) * 100
        change_1h = calculate_time_aligned_change(ohlcv, 1)
        change_4h = calculate_time_aligned_change(ohlcv, 4)
        change_24h = calculate_time_aligned_change(ohlcv, 24)

        return {
            "Symbol": original_name.replace("B-", ""),
            "Price": current_price,
            "15m": round(change_15m, 2),
            "1h": round(change_1h, 2) if change_1h != -9999 else None,
            "4h": round(change_4h, 2) if change_4h != -9999 else None,
            "24h": round(change_24h, 2) if change_24h != -9999 else None,
        }
    except Exception:
        return None

async def get_crypto_data():
    exchange = ccxt.bitget({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})
    tasks = [fetch_single_pair(exchange, sym, raw) for sym, raw in zip(CLEANED_SYMBOLS, RAW_SYMBOLS)]
    results = await asyncio.gather(*tasks)
    await exchange.close()
    return [r for r in results if r is not None]

# --- 3. Run Loop in Streamlit ---

# --- 3. The New Display Logic (No Blur) ---

st.title("🚀 Crypto Futures Tracker")
st.caption("Live Bitget Data | Aligned to Clock Time | Updates every 10s")

# @st.fragment keeps this part independent. It updates without reloading the whole page.
@st.fragment(run_every=10)
def show_live_data():
    # 1. Setup Async Loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # 2. Fetch Data
    try:
        data = loop.run_until_complete(get_crypto_data())
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return
    finally:
        loop.close()

    if not data:
        st.warning("Fetching data...")
        return

    # 3. Process Data
    df = pd.DataFrame(data)
    df = df.sort_values(by="15m", ascending=False)
    df.reset_index(drop=True, inplace=True)
    df.index += 1
    df.index.name = "Sr"

    # 4. Style Data
    def color_map(val):
        if val is None: return ""
        # Green for positive, Red for negative
        color = '#4CAF50' if val > 0 else '#FF5252'
        return f'color: {color}; font-weight: bold;'

    # 5. Show Data
    st.dataframe(
        df.style.map(color_map, subset=['15m', '1h', '4h', '24h'])
            .format({"Price": "${:.4f}", "15m": "{:.2f}%", "1h": "{:.2f}%", "4h": "{:.2f}%", "24h": "{:.2f}%"}),
        use_container_width=True,
        height=800
    )
    st.caption(f"Last Updated: {datetime.now().strftime('%H:%M:%S')}")

# Start the tracker
show_live_data()
st.rerun()
