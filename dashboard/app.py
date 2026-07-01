"""TradePulse dashboard (Phase 5).

Read-only Streamlit dashboard over the Postgres candles/alerts tables. It shows,
per symbol, a candlestick + volume chart of the most recent candles, a live KPI
row (price, per-candle move, pipeline health), and a combined alerts feed.

No pipeline logic here: this only reads existing tables. The data-fetching and
render live inside an st.fragment(run_every="10s") so the page refreshes itself
without a full reload.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

# --- Config ----------------------------------------------------------------

SYMBOLS = [s.strip() for s in os.getenv("PRODUCT_IDS", "BTC-USD,ETH-USD,SOL-USD").split(",") if s.strip()]

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "tradepulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "tradepulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")

CANDLE_LIMIT = 60
ALERT_LIMIT = 12

# Palette (matches the project banner).
UP = "#34d399"
DOWN = "#f87171"
INK = "#e6edf7"
MUTED = "#8da0c2"
GRID = "#1b2a4a"
CARD = "#131c30"

st.set_page_config(page_title="TradePulse", page_icon="📈", layout="wide")


# --- Data access -----------------------------------------------------------


@st.cache_resource
def get_engine():
    url = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(url, pool_pre_ping=True)


def latest_candle_time(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT max(window_end) FROM candles")).scalar()


def candles_for(engine, symbol: str, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    sql = text(
        "SELECT window_start, window_end, open, high, low, close, volume "
        "FROM candles WHERE symbol = :s ORDER BY window_start DESC LIMIT :n"
    )
    df = pd.read_sql(sql, engine, params={"s": symbol, "n": limit})
    return df.sort_values("window_start").reset_index(drop=True)


def latest_two_per_symbol(engine) -> pd.DataFrame:
    """Latest two candles per symbol, for current price + per-candle delta."""
    sql = text(
        "SELECT symbol, close, open, window_start FROM ("
        "  SELECT symbol, close, open, window_start,"
        "         row_number() OVER (PARTITION BY symbol ORDER BY window_start DESC) rn"
        "  FROM candles) t WHERE rn <= 2"
    )
    return pd.read_sql(sql, engine)


def recent_alerts(engine, limit: int = ALERT_LIMIT) -> pd.DataFrame:
    sql = text(
        "SELECT symbol, ts, price, pct_change, message "
        "FROM alerts ORDER BY ts DESC LIMIT :n"
    )
    return pd.read_sql(sql, engine, params={"n": limit})


# --- Styling ---------------------------------------------------------------


def inject_css():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', 'Segoe UI', sans-serif; }
        .block-container { padding-top: 1.6rem; padding-bottom: 2rem; max-width: 1500px; }
        #MainMenu, header[data-testid="stHeader"], footer { visibility: hidden; }

        .tp-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:0.4rem; }
        .tp-brand { font-size:1.9rem; font-weight:800; letter-spacing:0.5px; }
        .tp-brand .g { color:#34d399; }
        .tp-brand .w { color:#f8fafc; }
        .tp-sub { color:#8da0c2; font-size:0.86rem; margin-top:-2px; }
        .tp-live { display:flex; align-items:center; gap:8px; color:#8da0c2; font-size:0.8rem; }
        .tp-dot { width:9px; height:9px; border-radius:50%; background:#34d399; box-shadow:0 0 0 0 rgba(52,211,153,0.7); animation:pulse 1.8s infinite; }
        @keyframes pulse { 0%{box-shadow:0 0 0 0 rgba(52,211,153,0.6);} 70%{box-shadow:0 0 0 8px rgba(52,211,153,0);} 100%{box-shadow:0 0 0 0 rgba(52,211,153,0);} }

        .tp-card { background:#131c30; border:1px solid #23324f; border-radius:16px; padding:16px 18px; height:100%; }
        .tp-card .label { color:#8da0c2; font-size:0.78rem; font-weight:600; text-transform:uppercase; letter-spacing:0.6px; }
        .tp-card .value { color:#f8fafc; font-size:1.65rem; font-weight:700; margin-top:4px; line-height:1.1; }
        .tp-card .delta { font-size:0.9rem; font-weight:600; margin-top:6px; }
        .up { color:#34d399; } .down { color:#f87171; } .muted { color:#8da0c2; }

        .tp-status { display:inline-flex; align-items:center; gap:7px; font-weight:700; font-size:1.25rem; }
        .tp-status .sdot { width:11px; height:11px; border-radius:50%; }

        .tp-alert { display:grid; grid-template-columns:64px 96px 120px 1fr; align-items:center;
                    gap:10px; padding:10px 14px; border:1px solid #23324f; border-radius:12px;
                    background:#131c30; margin-bottom:8px; }
        .tp-alert .sym { font-weight:700; color:#f8fafc; }
        .tp-alert .time { color:#8da0c2; font-size:0.85rem; }
        .tp-alert .msg { color:#cdd8ee; font-size:0.9rem; }
        .tp-badge { font-weight:700; text-align:center; border-radius:8px; padding:3px 0; font-size:0.9rem; }
        .tp-badge.up { background:rgba(52,211,153,0.14); }
        .tp-badge.down { background:rgba(248,113,113,0.14); }

        .tp-section { color:#f8fafc; font-size:1.05rem; font-weight:700; margin:0.4rem 0 0.6rem; }
        .tp-wait { text-align:center; padding:70px 20px; color:#8da0c2; }
        .tp-wait .big { font-size:1.4rem; font-weight:700; color:#e6edf7; margin-bottom:8px; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def header():
    st.markdown(
        """
        <div class="tp-header">
          <div>
            <div class="tp-brand"><span class="w">Trade</span><span class="g">Pulse</span></div>
            <div class="tp-sub">real-time crypto trade streaming pipeline</div>
          </div>
          <div class="tp-live"><span class="tp-dot"></span> LIVE · auto-refresh 10s</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# --- Rendering -------------------------------------------------------------


def price_card(symbol: str, price: float, delta_pct):
    if delta_pct is None:
        delta_html = '<div class="delta muted">first candle</div>'
    else:
        cls = "up" if delta_pct >= 0 else "down"
        arrow = "▲" if delta_pct >= 0 else "▼"
        delta_html = f'<div class="delta {cls}">{arrow} {delta_pct:+.2f}% <span class="muted">vs prev candle</span></div>'
    return (
        f'<div class="tp-card"><div class="label">{symbol}</div>'
        f'<div class="value">${price:,.2f}</div>{delta_html}</div>'
    )


def health_card(age_seconds: float):
    if age_seconds <= 180:
        color, label = UP, "Live"
    elif age_seconds <= 360:
        color, label = "#fbbf24", "Lagging"
    else:
        color, label = DOWN, "Stale"
    mins, secs = divmod(int(age_seconds), 60)
    ago = f"{mins}m {secs}s" if mins else f"{secs}s"
    return (
        f'<div class="tp-card"><div class="label">Pipeline health</div>'
        f'<div class="value"><span class="tp-status"><span class="sdot" style="background:{color}"></span>{label}</span></div>'
        f'<div class="delta muted">last candle {ago} ago</div></div>'
    )


def render_metrics(engine, last_ts):
    two = latest_two_per_symbol(engine)
    cols = st.columns(len(SYMBOLS) + 1)
    for col, symbol in zip(cols, SYMBOLS):
        rows = two[two["symbol"] == symbol].sort_values("window_start", ascending=False)
        if rows.empty:
            col.markdown(
                f'<div class="tp-card"><div class="label">{symbol}</div>'
                f'<div class="value muted">no data</div>'
                f'<div class="delta muted">waiting</div></div>',
                unsafe_allow_html=True,
            )
            continue
        price = float(rows.iloc[0]["close"])
        if len(rows) >= 2:
            prev = float(rows.iloc[1]["close"])
            delta = (price - prev) / prev * 100 if prev else 0.0
        else:
            delta = None
        col.markdown(price_card(symbol, price, delta), unsafe_allow_html=True)

    age = (datetime.now(timezone.utc) - last_ts).total_seconds()
    cols[-1].markdown(health_card(age), unsafe_allow_html=True)


def candle_figure(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.74, 0.26], vertical_spacing=0.03,
    )
    x = df["window_start"]
    fig.add_trace(
        go.Candlestick(
            x=x, open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            increasing_line_color=UP, increasing_fillcolor=UP,
            decreasing_line_color=DOWN, decreasing_fillcolor=DOWN,
            line_width=1, whiskerwidth=0.4, name="", showlegend=False,
        ),
        row=1, col=1,
    )
    vol_colors = [UP if c >= o else DOWN for o, c in zip(df["open"], df["close"])]
    fig.add_trace(
        go.Bar(x=x, y=df["volume"], marker_color=vol_colors, marker_line_width=0,
               opacity=0.55, name="", showlegend=False, hovertemplate="vol %{y:.4f}<extra></extra>"),
        row=2, col=1,
    )
    last_close = float(df["close"].iloc[-1])
    fig.add_hline(y=last_close, line_dash="dot", line_color=MUTED, line_width=1,
                  opacity=0.5, row=1, col=1)

    fig.update_layout(
        height=560,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=MUTED, family="Inter, Segoe UI, sans-serif", size=12),
        hovermode="x unified",
        xaxis_rangeslider_visible=False,
        dragmode=False,
        bargap=0.25,
    )
    fig.update_xaxes(gridcolor=GRID, zeroline=False, showgrid=True, tickformat="%H:%M")
    fig.update_yaxes(gridcolor=GRID, zeroline=False, side="right", tickprefix="$", row=1, col=1)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, side="right", row=2, col=1)
    return fig


def render_chart(engine):
    symbol = st.segmented_control(
        "Symbol", SYMBOLS, default=SYMBOLS[0], key="symbol",
        selection_mode="single", label_visibility="collapsed",
    ) or SYMBOLS[0]

    df = candles_for(engine, symbol)
    if df.empty:
        st.info(f"No candles for {symbol} yet. They appear about 2 minutes after the job starts.")
        return

    last = df.iloc[-1]
    move = (float(last["close"]) - float(last["open"])) / float(last["open"]) * 100 if last["open"] else 0.0
    cls = "up" if move >= 0 else "down"
    st.markdown(
        f'<div class="tp-sub"><b style="color:#f8fafc;font-size:1rem">{symbol}</b> &nbsp; '
        f'${float(last["close"]):,.2f} &nbsp; '
        f'<span class="{cls}">{move:+.2f}% last candle</span> &nbsp;·&nbsp; '
        f'{len(df)} candles</div>',
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        candle_figure(df), width="stretch",
        config={"displayModeBar": False, "scrollZoom": False},
    )


def render_alerts(engine):
    st.markdown('<div class="tp-section">Recent volatility alerts</div>', unsafe_allow_html=True)
    alerts = recent_alerts(engine)
    if alerts.empty:
        st.markdown('<div class="tp-sub">No alerts yet.</div>', unsafe_allow_html=True)
        return
    html = []
    for _, a in alerts.iterrows():
        up = a["pct_change"] >= 0
        cls = "up" if up else "down"
        arrow = "▲" if up else "▼"
        ts = pd.to_datetime(a["ts"]).strftime("%H:%M")
        html.append(
            f'<div class="tp-alert">'
            f'<div class="tp-badge {cls}"><span class="{cls}">{arrow} {a["pct_change"]:+.2f}%</span></div>'
            f'<div class="sym">{a["symbol"]}</div>'
            f'<div class="time">{ts} · ${float(a["price"]):,.2f}</div>'
            f'<div class="msg">{a["message"]}</div>'
            f'</div>'
        )
    st.markdown("".join(html), unsafe_allow_html=True)


def render_waiting():
    st.markdown(
        '<div class="tp-wait"><div class="big">Waiting for data</div>'
        'The pipeline is running. The first 1-minute candles land about two '
        'minutes after startup. This view refreshes automatically.</div>',
        unsafe_allow_html=True,
    )


# --- App --------------------------------------------------------------------


@st.fragment(run_every="10s")
def dashboard_body():
    try:
        engine = get_engine()
        last_ts = latest_candle_time(engine)
    except Exception as exc:  # noqa: BLE001 - surface connection issues, don't crash
        st.error(f"Cannot reach the database yet: {exc}")
        return

    updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    st.markdown(f'<div class="tp-sub">Updated {updated}</div>', unsafe_allow_html=True)

    if last_ts is None:
        render_waiting()
        return

    render_metrics(engine, last_ts)
    st.write("")
    render_chart(engine)
    st.divider()
    render_alerts(engine)


def main():
    inject_css()
    header()
    dashboard_body()


if __name__ == "__main__":
    main()
