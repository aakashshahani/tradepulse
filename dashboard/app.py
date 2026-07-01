"""TradePulse dashboard (Phase 5).

Read-only Streamlit dashboard over the Postgres candles/alerts tables. Per
symbol it shows a candlestick + volume chart with moving-average and VWAP
overlays, a live KPI row (last close, window change, sparkline, pipeline
health), and a combined alerts feed.

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

# Chart timeframe options (label -> lookback minutes; None means all history).
TIMEFRAMES = {"30m": 30, "1h": 60, "3h": 180, "All": None}
KPI_LOOKBACK_MIN = 20   # minutes used for the KPI window-change + sparkline
MA_FAST = 10
MA_SLOW = 20
ALERT_LIMIT = 12

# Shown on the alerts panel so the active threshold is never implied silently.
# Kept in sync with the spark_job service via the same env var.
ALERT_THRESHOLD_PCT = float(os.getenv("ALERT_THRESHOLD_PCT", "0.3"))

# Palette (matches the project banner).
UP = "#34d399"
DOWN = "#f87171"
INK = "#e6edf7"
MUTED = "#8da0c2"
GRID = "#1b2a4a"
MA_FAST_C = "#22d3ee"
MA_SLOW_C = "#fbbf24"
VWAP_C = "#a78bfa"

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


def malformed_dropped(engine) -> int:
    """Cumulative count of malformed records the Spark job has dropped."""
    with engine.connect() as conn:
        v = conn.execute(
            text("SELECT value FROM pipeline_metrics WHERE metric = 'malformed_dropped'")
        ).scalar()
    return int(v) if v is not None else 0


def candles_for(engine, symbol: str, minutes) -> pd.DataFrame:
    """Candles for a symbol within the last `minutes` (None means all history).

    Filtering by time (not row count) is what makes 30m/1h/3h actually differ,
    and it drops old orphan candles from earlier runs so gaps don't draw long
    diagonal MA/VWAP lines across empty time.
    """
    base = (
        "SELECT window_start, window_end, open, high, low, close, volume "
        "FROM candles WHERE symbol = :s"
    )
    if minutes is None:
        sql = text(base + " ORDER BY window_start DESC LIMIT 2000")
        params = {"s": symbol}
    else:
        sql = text(
            base + " AND window_start >= now() - (:mins * interval '1 minute') "
            "ORDER BY window_start"
        )
        params = {"s": symbol, "mins": minutes}
    df = pd.read_sql(sql, engine, params=params)
    return df.sort_values("window_start").reset_index(drop=True)


def kpi_frame(engine, minutes: int = KPI_LOOKBACK_MIN) -> pd.DataFrame:
    """Candles per symbol within the last `minutes`, for KPI change/sparkline."""
    sql = text(
        "SELECT symbol, open, close, window_start FROM candles "
        "WHERE window_start >= now() - (:mins * interval '1 minute') "
        "ORDER BY symbol, window_start"
    )
    return pd.read_sql(sql, engine, params={"mins": minutes})


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

        .tp-card { background:#131c30; border:1px solid #23324f; border-radius:16px; padding:14px 18px; height:100%; }
        .tp-card .label { color:#8da0c2; font-size:0.76rem; font-weight:600; text-transform:uppercase; letter-spacing:0.6px; }
        .tp-card .value { color:#f8fafc; font-size:1.6rem; font-weight:700; margin-top:2px; line-height:1.1; }
        .tp-card .sublabel { color:#5f7196; font-size:0.68rem; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; }
        .tp-card .delta { font-size:0.86rem; font-weight:600; margin-top:5px; }
        .tp-card .spark { margin-top:8px; }
        .up { color:#34d399; } .down { color:#f87171; } .muted { color:#8da0c2; }

        .tp-status { display:inline-flex; align-items:center; gap:7px; font-weight:700; font-size:1.2rem; }
        .tp-status .sdot { width:11px; height:11px; border-radius:50%; }

        .tp-alert { display:grid; grid-template-columns:96px 96px 150px 1fr; align-items:center;
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


# --- Small helpers ----------------------------------------------------------


def sparkline_svg(values, color: str, w: int = 150, h: int = 36) -> str:
    """Inline SVG sparkline of closes, with a soft area fill and an end dot."""
    vals = [float(v) for v in values if v is not None]
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1.0
    n = len(vals)
    pts = []
    for i, v in enumerate(vals):
        x = 2 + i / (n - 1) * (w - 4)
        y = (h - 3) - (v - lo) / rng * (h - 6)
        pts.append((x, y))
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area = f"2,{h} " + line + f" {w - 2},{h}"
    ex, ey = pts[-1]
    return (
        f'<svg class="spark" width="{w}" height="{h}" viewBox="0 0 {w} {h}">'
        f'<polygon points="{area}" fill="{color}" opacity="0.10"/>'
        f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="1.8" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{ex:.1f}" cy="{ey:.1f}" r="2.6" fill="{color}"/></svg>'
    )


# --- Rendering -------------------------------------------------------------


def price_card(symbol: str, price: float, change_pct, minutes: int, spark_html: str):
    if change_pct is None:
        delta_html = '<div class="delta muted">first candle</div>'
    else:
        cls = "up" if change_pct >= 0 else "down"
        arrow = "▲" if change_pct >= 0 else "▼"
        delta_html = (
            f'<div class="delta {cls}">{arrow} {change_pct:+.2f}% '
            f'<span class="muted">last {minutes}m</span></div>'
        )
    return (
        f'<div class="tp-card"><div class="label">{symbol}</div>'
        f'<div class="value">${price:,.2f}</div>'
        f'<div class="sublabel">last close</div>'
        f"{delta_html}{spark_html}</div>"
    )


def health_card(age_seconds: float, dropped: int):
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
        f'<div class="sublabel">watermark lag is normal</div>'
        f'<div class="delta muted">last candle {ago} ago · {dropped:,} malformed dropped</div></div>'
    )


def render_metrics(engine, last_ts):
    kf = kpi_frame(engine)
    cols = st.columns(len(SYMBOLS) + 1)
    for col, symbol in zip(cols, SYMBOLS):
        rows = kf[kf["symbol"] == symbol]
        if rows.empty:
            col.markdown(
                f'<div class="tp-card"><div class="label">{symbol}</div>'
                f'<div class="value muted">no data</div>'
                f'<div class="sublabel">waiting</div></div>',
                unsafe_allow_html=True,
            )
            continue
        closes = rows["close"].tolist()
        price = float(closes[-1])
        first_open = float(rows["open"].iloc[0])
        change = (price - first_open) / first_open * 100 if first_open else None
        if len(closes) < 2:
            change = None
        color = UP if (change is None or change >= 0) else DOWN
        spark = sparkline_svg(closes, color)
        col.markdown(
            price_card(symbol, price, change, KPI_LOOKBACK_MIN, spark),
            unsafe_allow_html=True,
        )

    age = (datetime.now(timezone.utc) - last_ts).total_seconds()
    cols[-1].markdown(health_card(age, malformed_dropped(engine)), unsafe_allow_html=True)


def candle_figure(df: pd.DataFrame) -> go.Figure:
    df = df.copy()
    # Reindex onto a complete 1-minute grid so missing minutes become explicit
    # NaN rows. With connectgaps=False this breaks the MA/VWAP lines across data
    # gaps instead of drawing a straight interpolation over a period that had no
    # candles (and no trades).
    if len(df) > 1:
        df = df.set_index("window_start")
        grid = pd.date_range(df.index.min(), df.index.max(), freq="1min")
        df = df.reindex(grid)
        df.index.name = "window_start"
        df = df.reset_index()

    df["ma_fast"] = df["close"].rolling(MA_FAST, min_periods=MA_FAST).mean()
    df["ma_slow"] = df["close"].rolling(MA_SLOW, min_periods=MA_SLOW).mean()
    # Session VWAP over the visible window, from candle typical prices. cumsum
    # skips NaN, so gap rows stay NaN (line breaks) rather than interpolating.
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    df["vwap"] = (typical * df["volume"]).cumsum() / df["volume"].cumsum().replace(0, pd.NA)

    x = df["window_start"]
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.74, 0.26], vertical_spacing=0.03,
    )

    fig.add_trace(
        go.Candlestick(
            x=x, open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            increasing_line_color=UP, increasing_fillcolor=UP,
            decreasing_line_color=DOWN, decreasing_fillcolor=DOWN,
            line_width=1, whiskerwidth=0.4, name="price", showlegend=False,
        ),
        row=1, col=1,
    )
    for col_name, color, label in (
        ("ma_fast", MA_FAST_C, f"MA{MA_FAST}"),
        ("ma_slow", MA_SLOW_C, f"MA{MA_SLOW}"),
        ("vwap", VWAP_C, "VWAP"),
    ):
        dash = "dot" if col_name == "vwap" else "solid"
        fig.add_trace(
            go.Scatter(
                x=x, y=df[col_name], mode="lines", name=label,
                line=dict(color=color, width=1.5, dash=dash),
                opacity=0.9, connectgaps=False,
                hovertemplate=label + " $%{y:,.2f}<extra></extra>",
            ),
            row=1, col=1,
        )

    vol_colors = [UP if c >= o else DOWN for o, c in zip(df["open"], df["close"])]
    fig.add_trace(
        go.Bar(x=x, y=df["volume"], marker_color=vol_colors, marker_line_width=0,
               opacity=0.55, name="volume", showlegend=False,
               hovertemplate="vol %{y:.4f}<extra></extra>"),
        row=2, col=1,
    )

    valid = df.dropna(subset=["close", "open"])
    last_close = float(valid["close"].iloc[-1])
    last_up = last_close >= float(valid["open"].iloc[-1])
    tag_color = UP if last_up else DOWN
    fig.add_hline(y=last_close, line_dash="dot", line_color=tag_color, line_width=1,
                  opacity=0.6, row=1, col=1)
    fig.add_annotation(
        xref="x domain", x=1.0, yref="y", y=last_close,
        text=f" ${last_close:,.2f} ", showarrow=False,
        xanchor="left", yanchor="middle",
        font=dict(color="#0b1220", size=11, family="Inter"),
        bgcolor=tag_color, borderpad=2, row=1, col=1,
    )

    fig.update_layout(
        height=580,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=MUTED, family="Inter, Segoe UI, sans-serif", size=12),
        hovermode="x unified",
        xaxis_rangeslider_visible=False,
        dragmode=False,
        bargap=0.25,
        legend=dict(
            orientation="h", yanchor="top", y=0.99, xanchor="left", x=0.01,
            bgcolor="rgba(19,28,48,0.65)", bordercolor="#23324f", borderwidth=1,
            font=dict(size=11, color="#cdd8ee"),
        ),
    )
    fig.update_xaxes(gridcolor=GRID, zeroline=False, showgrid=True, tickformat="%H:%M", row=1, col=1)
    fig.update_xaxes(gridcolor=GRID, zeroline=False, tickformat="%H:%M",
                     title_text="time (UTC)", title_font=dict(size=11), row=2, col=1)
    lo, hi = float(df["low"].min()), float(df["high"].max())
    pad = (hi - lo) * 0.08 or 1.0
    fig.update_yaxes(gridcolor=GRID, zeroline=False, side="right", tickprefix="$",
                     range=[lo - pad, hi + pad], row=1, col=1)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, side="right", title_text="vol", row=2, col=1)
    return fig


def render_chart(engine):
    c1, c2 = st.columns([3, 2])
    with c1:
        symbol = st.segmented_control(
            "Symbol", SYMBOLS, default=SYMBOLS[0], key="symbol",
            selection_mode="single",
        ) or SYMBOLS[0]
    with c2:
        tf_label = st.segmented_control(
            "Range", list(TIMEFRAMES), default="1h", key="tf",
            selection_mode="single",
        ) or "1h"

    df = candles_for(engine, symbol, TIMEFRAMES[tf_label])
    if df.empty:
        st.info(f"No candles for {symbol} yet. They appear about 2 minutes after the job starts.")
        return

    first_open = float(df["open"].iloc[0])
    last_close = float(df["close"].iloc[-1])
    move = (last_close - first_open) / first_open * 100 if first_open else 0.0
    cls = "up" if move >= 0 else "down"
    st.markdown(
        f'<div class="tp-sub"><b style="color:#f8fafc;font-size:1rem">{symbol}</b> &nbsp; '
        f'${last_close:,.2f} &nbsp; '
        f'<span class="{cls}">{move:+.2f}% over {tf_label}</span> &nbsp;·&nbsp; '
        f'{len(df)} candles · times UTC</div>',
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        candle_figure(df), width="stretch",
        config={"displayModeBar": False, "scrollZoom": False},
    )


def render_alerts(engine):
    st.markdown(
        f'<div class="tp-section">Recent volatility alerts '
        f'<span class="tp-sub">(moves over {ALERT_THRESHOLD_PCT:g}% · times UTC)</span></div>',
        unsafe_allow_html=True,
    )
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
