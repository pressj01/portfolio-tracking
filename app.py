from flask import Flask, render_template, redirect, url_for, flash, request, jsonify
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import json
import datetime
import calendar as _cal

from config import get_connection
from import_data import (
    import_from_excel, import_weekly_payouts, import_monthly_payouts,
    import_monthly_payout_tickers, ensure_tables_exist,
)
from normalize import (
    populate_holdings,
    populate_dividends,
    populate_income_tracking,
    populate_pillar_weights,
)

app = Flask(__name__)
app.secret_key = "portfolio-tracking-secret-key"


PILLAR_COLS = [
    "hedged_anchor", "anchor", "gold_silver", "booster", "juicer", "bdc", "growth",
]

DOLLAR_COLS = {
    "price_paid", "current_price", "purchase_value", "current_value",
    "gain_or_loss", "div", "dividend_paid", "estim_payment_per_year",
    "approx_monthly_income", "withdraw_8pct_cost_annually", "withdraw_8pct_per_month",
    "cash_not_reinvested", "total_cash_reinvested", "dollars_per_hour",
    "ytd_divs", "total_divs_received",
}

PCT_COLS = {
    "percent_change", "gain_or_loss_percentage", "annual_yield_on_cost",
    "current_annual_yield", "percent_of_account", "account_yield_on_cost",
    "current_yield_of_account", "paid_for_itself",
}

DECIMAL_COLS = {
    "quantity", "shares_bought_from_dividend", "shares_bought_in_year", "shares_in_month",
}


def format_dashboard(df):
    """Drop pillar columns and format numbers for display."""
    df = df.drop(columns=[c for c in PILLAR_COLS if c in df.columns], errors="ignore")

    for col in df.columns:
        if col in DOLLAR_COLS:
            df[col] = df[col].apply(lambda v: f"${v:,.2f}" if pd.notna(v) else "")
        elif col in PCT_COLS:
            df[col] = df[col].apply(lambda v: f"{v * 100:.2f}%" if pd.notna(v) else "")
        elif col in DECIMAL_COLS:
            df[col] = df[col].apply(lambda v: f"{v:.3f}" if pd.notna(v) else "")

    # Blank out any remaining NaN / "nan" in unformatted columns (e.g. div_frequency, classification_type)
    df = df.fillna("").astype(str)
    df = df.replace({"nan": "", "None": "", "NaT": ""})
    return df


def _yf_div_pay_date(ticker):
    """Fetch next dividend pay date from yfinance. Returns date or None."""
    try:
        import yfinance as yf
        cal = yf.Ticker(ticker).calendar
        if not cal or not isinstance(cal, dict):
            return None
        d = cal.get("Dividend Date")
        if d is None:
            return None
        if hasattr(d, "date"):
            d = d.date()
        elif isinstance(d, str):
            from datetime import datetime
            d = datetime.strptime(d[:10], "%Y-%m-%d").date()
        import datetime as _dt
        if not isinstance(d, _dt.date):
            return None
        # Reject stale dates (>18 months old)
        if d < _dt.date.today() - _dt.timedelta(days=548):
            return None
        return d
    except Exception:
        return None


def _build_cal_events(conn):
    """Shared helper: read ex-div events from all_account_info and return sorted list of dicts."""
    from datetime import datetime, timedelta
    FREQ_LABEL = {
        "M":  "monthly",  "52": "weekly",  "W":  "weekly",
        "Q":  "quarterly", "SA": "semi-annual", "A": "annual",
    }
    FREQ_COLOR = {
        "M":  "#00c9a7", "52": "#FFD700", "W":  "#FFD700",
        "Q":  "#7ecfff", "SA": "#f0a0ff", "A":  "#f0a0ff",
    }
    # Fallback days-after-ex-div for non-weekly freqs (used when yfinance has no data)
    FREQ_OFFSET = {"M": 10, "Q": 14, "SA": 21, "A": 21}

    try:
        df = pd.read_sql("""
            SELECT ticker, description, ex_div_date, div, div_frequency
            FROM dbo.all_account_info
            WHERE ex_div_date IS NOT NULL
              AND ex_div_date NOT IN ('', '--')
              AND current_price IS NOT NULL
              AND ISNULL(quantity, 0) > 0
            ORDER BY ticker
        """, conn)
    except Exception:
        return []

    # Pre-fetch yfinance pay dates for non-weekly tickers (fast: 404s return immediately)
    yf_pay_dates = {}
    for tkr in df.loc[~df["div_frequency"].isin(["52", "W"]), "ticker"].tolist():
        pd_date = _yf_div_pay_date(tkr)
        if pd_date:
            yf_pay_dates[tkr] = pd_date

    events = []
    for _, row in df.iterrows():
        raw = str(row["ex_div_date"]).strip()
        dt = None
        for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw, fmt).date()
                break
            except ValueError:
                continue
        if dt is None:
            continue

        freq   = str(row["div_frequency"]).strip() if pd.notna(row["div_frequency"]) else ""
        amount = float(row["div"]) if pd.notna(row["div"]) and row["div"] else None
        ticker = row["ticker"]

        # ── Determine pay date ────────────────────────────────────────────
        pay_estimated = True

        def _next_biz(base, days):
            """Add `days` calendar days then push forward past any weekend."""
            d = base + timedelta(days=days)
            if d.weekday() == 5:    # Saturday → Monday
                d += timedelta(days=2)
            elif d.weekday() == 6:  # Sunday → Monday
                d += timedelta(days=1)
            return d

        if freq in ("52", "W"):
            # Weekly: pay 1 business day after ex-div.
            # Friday ex-div → Monday pay (weekend settlement window).
            pay_dt = _next_biz(dt, 1)
        elif ticker in yf_pay_dates:
            # Confirmed pay date from Yahoo Finance (stocks/BDCs)
            pay_dt = yf_pay_dates[ticker]
            pay_estimated = False
        elif freq == "M":
            # Monthly ETF distributions: pay within 2 days of ex-div
            pay_dt = _next_biz(dt, 2)
        else:
            offset = FREQ_OFFSET.get(freq, 10)
            pay_dt = dt + timedelta(days=offset)

        events.append({
            "ticker":        ticker,
            "description":   str(row["description"]) if pd.notna(row["description"]) else "",
            "date":          dt.isoformat(),
            "day":           str(dt.day),
            "month":         dt.strftime("%b"),
            "weekday":       dt.strftime("%a"),
            "amount":        round(amount, 4) if amount is not None else None,
            "freq":          freq,
            "freq_label":    FREQ_LABEL.get(freq, freq.lower() if freq else ""),
            "color":         FREQ_COLOR.get(freq, "#8899aa"),
            "pay_date":      pay_dt.isoformat(),
            "pay_month":     pay_dt.strftime("%b"),
            "pay_day":       str(pay_dt.day),
            "pay_estimated": pay_estimated,
        })
    # Sort by estimated pay date so cards are in payment order
    events.sort(key=lambda e: e["pay_date"])
    return events


@app.route("/")
def index():
    """Dashboard page with import button, dividend calendar preview, and data table."""
    from datetime import date as date_type, timedelta
    conn = get_connection()
    ensure_tables_exist(conn)

    try:
        df = pd.read_sql("""
            SELECT * FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
            ORDER BY ticker
        """, conn)
    except Exception:
        df = pd.DataFrame()

    cal_events = _build_cal_events(conn)
    conn.close()

    today     = date_type.today()
    today_str = today.isoformat()
    window_start = (today - timedelta(days=7)).isoformat()
    window_end   = (today + timedelta(days=60)).isoformat()
    preview_events = [e for e in cal_events
                      if window_start <= e["date"] <= window_end]

    # ── Portfolio-level stats (calculated, not stored) ────────────────────
    port_stats = {}
    HIDDEN_COLS = ["account_yield_on_cost", "current_yield_of_account", "dollars_per_hour"]
    if not df.empty:
        total_purchase  = pd.to_numeric(df["purchase_value"],       errors="coerce").sum()
        total_current   = pd.to_numeric(df["current_value"],        errors="coerce").sum()
        total_annual    = pd.to_numeric(df["estim_payment_per_year"],errors="coerce").sum()
        total_divs      = pd.to_numeric(df["total_divs_received"],  errors="coerce").sum()
        total_gain      = pd.to_numeric(df["gain_or_loss"],         errors="coerce").sum()

        total_return_dollar = total_gain + total_divs
        total_return_pct    = round(total_return_dollar / total_purchase * 100, 2) if total_purchase else None

        port_stats = {
            "yield_on_cost":       round(total_annual / total_purchase * 100, 2) if total_purchase else None,
            "current_yield":       round(total_annual / total_current  * 100, 2) if total_current  else None,
            "dollars_per_hour":    round(total_annual / 2080, 4)                 if total_annual   else None,
            "total_annual":        round(total_annual, 2),
            "total_divs":          round(total_divs, 2),
            "total_gain":          round(total_gain, 2),
            "total_purchase":      round(total_purchase, 2),
            "total_current":       round(total_current, 2),
            "total_return_dollar": round(total_return_dollar, 2),
            "total_return_pct":    total_return_pct,
        }

    row_count = len(df)
    if not df.empty:
        df = format_dashboard(df)
        df = df.drop(columns=[c for c in HIDDEN_COLS if c in df.columns])

    return render_template(
        "index.html",
        data=df,
        row_count=row_count,
        cal_events=preview_events,
        today=today_str,
        port_stats=port_stats,
    )


@app.route("/import", methods=["POST"])
def do_import():
    """Handle the Import Data button."""
    try:
        row_count, message = import_from_excel()
        flash(message, "success")
    except Exception as e:
        flash(f"Import failed: {e}", "error")
    return redirect(url_for("index"))


@app.route("/manage")
def manage():
    """Data management page with normalize buttons."""
    return render_template("manage.html")


@app.route("/populate/<table>", methods=["POST"])
def populate(table):
    """Handle populate buttons for normalized tables."""
    funcs = {
        "holdings": populate_holdings,
        "dividends": populate_dividends,
        "income_tracking": populate_income_tracking,
        "pillar_weights": populate_pillar_weights,
    }
    func = funcs.get(table)
    if not func:
        flash(f"Unknown table: {table}", "error")
        return redirect(url_for("manage"))

    try:
        row_count, message = func()
        flash(message, "success")
    except Exception as e:
        flash(f"Failed to populate {table}: {e}", "error")
    return redirect(url_for("manage"))


# ── Dividend Analysis page ─────────────────────────────────────

def _da_build_charts_and_totals(df, conn):
    """Build charts_da and totals for dividend analysis from a (possibly filtered) DataFrame."""
    charts_da = {}
    totals    = {}

    def _add_m(d, n):
        mo = d.month - 1 + n
        return datetime.date(d.year + mo // 12, mo % 12 + 1, min(d.day, 28))

    if df.empty:
        return charts_da, totals

    df_c = df.copy()
    for _col in ['ytd_divs', 'total_divs_received', 'estim_payment_per_year',
                 'approx_monthly_income', 'purchase_value']:
        df_c[_col] = pd.to_numeric(df_c[_col], errors='coerce').fillna(0)
    df_c['description']   = df_c['description'].fillna('').astype(str)
    df_c['div_frequency'] = df_c['div_frequency'].fillna('').astype(str).str.strip()

    totals['ytd_divs']               = df_c['ytd_divs'].sum()
    totals['total_divs_received']    = df_c['total_divs_received'].sum()
    totals['dividend_paid']          = pd.to_numeric(df_c['dividend_paid'], errors='coerce').fillna(0).sum()
    totals['estim_payment_per_year'] = df_c['estim_payment_per_year'].sum()
    totals['approx_monthly_income']  = df_c['approx_monthly_income'].sum()

    # Chart 1: Est. Annual Income by Ticker (top 20)
    c1 = df_c[df_c['estim_payment_per_year'] > 0].nlargest(20, 'estim_payment_per_year')
    if not c1.empty:
        fig1 = go.Figure(go.Bar(
            x=c1['ticker'].tolist(), y=c1['estim_payment_per_year'].tolist(),
            marker_color='#a855f7',
            text=[f"${v:,.0f}" for v in c1['estim_payment_per_year'].tolist()],
            textposition='outside', customdata=c1['description'].tolist(),
            hovertemplate='<b>%{x}</b><br>%{customdata}<br>Est. Annual: <b>$%{y:,.2f}</b><extra></extra>',
        ))
        fig1.update_layout(
            title='Est. Annual Income by Ticker (Top 20)', template='plotly_dark',
            xaxis_tickangle=-45,
            yaxis=dict(title='Est. Annual Income ($)', range=[0, c1['estim_payment_per_year'].max() * 1.2]),
            margin=dict(t=50, b=100, l=60, r=20),
            paper_bgcolor='#1a1f2e', plot_bgcolor='rgba(255,255,255,0.03)',
        )
        charts_da['annual_income'] = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

    # Chart 4: Total Divs Received by Ticker
    c4 = df_c[df_c['total_divs_received'] > 0].sort_values('total_divs_received', ascending=False)
    if not c4.empty:
        fig4 = go.Figure(go.Bar(
            x=c4['ticker'].tolist(), y=c4['total_divs_received'].tolist(),
            marker_color='#38bdf8',
            text=[f"${v:,.0f}" for v in c4['total_divs_received'].tolist()],
            textposition='outside', customdata=c4['description'].tolist(),
            hovertemplate='<b>%{x}</b><br>%{customdata}<br>Total Divs: <b>$%{y:,.2f}</b><extra></extra>',
        ))
        fig4.update_layout(
            title='Total Dividends Received by Ticker', template='plotly_dark',
            xaxis_tickangle=-45,
            yaxis=dict(title='Total Divs Received ($)', range=[0, c4['total_divs_received'].max() * 1.2]),
            margin=dict(t=50, b=100, l=60, r=20),
            paper_bgcolor='#1a1f2e', plot_bgcolor='rgba(255,255,255,0.03)',
        )
        charts_da['total_divs_ticker'] = json.dumps(fig4, cls=plotly.utils.PlotlyJSONEncoder)

    # Chart 5: Paid For Itself
    paid_df = df_c.copy()
    paid_df['paid_for_itself']     = pd.to_numeric(paid_df['paid_for_itself'], errors='coerce')
    paid_df['classification_type'] = paid_df['classification_type'].fillna('Other').astype(str)
    paid_df = paid_df[paid_df['paid_for_itself'] > 0].copy()
    paid_df['paid_pct'] = (paid_df['paid_for_itself'] * 100).round(1)
    paid_df = paid_df.sort_values('paid_pct', ascending=False)
    if not paid_df.empty:
        fig5 = go.Figure()
        for ctype in paid_df['classification_type'].unique().tolist():
            g = paid_df[paid_df['classification_type'] == ctype]
            fig5.add_trace(go.Bar(
                x=g['ticker'].tolist(), y=g['paid_pct'].tolist(), name=ctype,
                text=[f"{v:.1f}%" for v in g['paid_pct'].tolist()],
                textposition='auto', customdata=g['description'].tolist(),
                hovertemplate='<b>%{x}</b><br>%{customdata}<br>Cost Recovered: <b>%{y:.1f}%</b><extra></extra>',
            ))
        fig5.add_hline(y=100, line_dash='dash', line_color='gold',
                       annotation_text='100% — Fully Paid For Itself', annotation_font_color='gold')
        _pfi_max = paid_df['paid_pct'].max()
        fig5.update_layout(
            title='Paid For Itself — % of Cost Recovered via Dividends',
            template='plotly_dark', xaxis_tickangle=-45,
            yaxis=dict(title='% of Cost Recovered', range=[0, max(_pfi_max * 1.15, 110)]),
            legend_title='Asset Type', margin=dict(t=60, b=100, l=60, r=20),
            paper_bgcolor='#1a1f2e', plot_bgcolor='rgba(255,255,255,0.03)',
        )
        charts_da['paid_for_itself'] = json.dumps(fig5, cls=plotly.utils.PlotlyJSONEncoder)

    # Chart 6: Total Dividends by Asset Type (pie)
    type_df  = df_c.copy()
    type_df['classification_type'] = type_df['classification_type'].fillna('Other').astype(str)
    type_grp = type_df.groupby('classification_type')['total_divs_received'].sum().reset_index()
    type_grp = type_grp[type_grp['total_divs_received'] > 0]
    if not type_grp.empty:
        fig6 = go.Figure(go.Pie(
            labels=type_grp['classification_type'].tolist(),
            values=type_grp['total_divs_received'].tolist(),
            hovertemplate='<b>%{label}</b><br>Total Dividends: $%{value:,.2f}<br>Share: %{percent}<extra></extra>',
            textinfo='label+percent',
        ))
        fig6.update_layout(title='Total Dividends Received by Asset Type',
            template='plotly_dark', paper_bgcolor='#1a1f2e')
        charts_da['by_type'] = json.dumps(fig6, cls=plotly.utils.PlotlyJSONEncoder)

    # Chart 2: Projected Monthly Income — next 12 months (with DRIP compounding)
    today_d     = datetime.date.today()
    month_start = today_d.replace(day=1)
    future_months = [_add_m(month_start, i) for i in range(12)]
    proj_key    = {m: 0.0 for m in future_months}

    def _parse_ex_date(s):
        if not s or str(s).strip() in ('--', 'nan', 'None', ''):
            return None
        for fmt in ('%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d'):
            try:
                return datetime.datetime.strptime(str(s).strip(), fmt).date()
            except ValueError:
                continue
        return None

    for _, row in df_c.iterrows():
        annual = float(row['estim_payment_per_year'] or 0)
        if annual <= 0:
            continue
        freq = str(row['div_frequency'] or '').strip()
        if freq in ('--', ''):
            continue
        ex_date       = _parse_ex_date(row.get('ex_div_date'))
        drip          = str(row.get('reinvest') or '').strip().upper() == 'Y'
        div_per_share = float(row.get('div') or 0)
        price         = float(row.get('current_price') or 0)
        g = (div_per_share / price) if (drip and price > 0 and div_per_share > 0) else 0

        if freq == '52':
            nd = ex_date or month_start
            while nd >= month_start: nd -= datetime.timedelta(days=7)
            nd += datetime.timedelta(days=7)
            pay_n = 0
            while nd.replace(day=1) <= future_months[-1]:
                mk = nd.replace(day=1)
                if mk in proj_key: proj_key[mk] += (annual / 52) * ((1 + g) ** pay_n)
                pay_n += 1; nd += datetime.timedelta(days=7)
        elif freq == 'M':
            nd = ex_date or month_start
            while nd >= month_start: nd = _add_m(nd, -1)
            nd = _add_m(nd, 1)
            pay_n = 0
            while nd.replace(day=1) <= future_months[-1]:
                mk = nd.replace(day=1)
                if mk in proj_key: proj_key[mk] += (annual / 12) * ((1 + g) ** pay_n)
                pay_n += 1; nd = _add_m(nd, 1)
        elif freq == 'Q':
            per_occ = annual / 4
            nd = _add_m(ex_date, 1) if ex_date else month_start
            while nd >= month_start: nd = _add_m(nd, -3)
            nd = _add_m(nd, 3)
            for pay_n in range(5):
                mk = nd.replace(day=1)
                if mk in proj_key: proj_key[mk] += per_occ * ((1 + g) ** pay_n)
                nd = _add_m(nd, 3)
        elif freq in ('S', 'SA'):
            per_occ = annual / 2
            nd = _add_m(ex_date, 1) if ex_date else month_start
            while nd >= month_start: nd = _add_m(nd, -6)
            nd = _add_m(nd, 6)
            for pay_n in range(3):
                mk = nd.replace(day=1)
                if mk in proj_key: proj_key[mk] += per_occ * ((1 + g) ** pay_n)
                nd = _add_m(nd, 6)
        elif freq == 'A':
            nd = ex_date or month_start
            while nd >= month_start: nd = _add_m(nd, -12)
            nd = _add_m(nd, 12)
            mk = nd.replace(day=1)
            if mk in proj_key: proj_key[mk] += annual
        else:
            for m in future_months: proj_key[m] += annual / 12

    try:
        cur_actual = pd.read_sql(
            "SELECT amount FROM dbo.monthly_payouts WHERE year=? AND month=?",
            conn, params=[month_start.year, month_start.month]
        )
        if not cur_actual.empty:
            actual_floor = float(cur_actual.iloc[0]['amount'])
            if proj_key.get(month_start, 0) < actual_floor:
                proj_key[month_start] = actual_floor
    except Exception:
        pass

    proj_labels = [m.strftime('%b') for m in future_months]
    proj_vals   = [round(proj_key[m], 2) for m in future_months]
    total_12m   = sum(proj_vals)
    fig2 = go.Figure(go.Bar(
        x=proj_labels, y=proj_vals, marker_color='#22d3ee',
        text=[f"${v:,.0f}" for v in proj_vals], textposition='outside',
        hovertemplate='<b>%{x}</b><br>Projected: <b>$%{y:,.2f}</b><extra></extra>',
    ))
    _proj_max = max(proj_vals) if proj_vals else 1
    fig2.update_layout(
        title=(f'Projected Monthly Income — Next 12 Months'
               f'  |  Est. Annual: ${total_12m:,.0f}'
               f'  |  Monthly Avg: ${total_12m/12:,.0f}'),
        template='plotly_dark',
        yaxis=dict(title='Projected Income ($)', range=[0, _proj_max * 1.2]),
        margin=dict(t=60, b=60, l=60, r=20),
        paper_bgcolor='#1a1f2e', plot_bgcolor='rgba(255,255,255,0.03)',
    )
    charts_da['projected_monthly'] = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    # Chart 3: Monthly Dividends Received (always portfolio-wide)
    try:
        da_window     = [_add_m(month_start, i) for i in range(12)]
        win_start_key = da_window[0].year  * 100 + da_window[0].month
        win_end_key   = da_window[-1].year * 100 + da_window[-1].month
        mp_df = pd.read_sql("""
            SELECT year, month, amount FROM dbo.monthly_payouts
            WHERE (year * 100 + month) >= ? AND (year * 100 + month) <= ?
            ORDER BY year, month
        """, conn, params=[win_start_key, win_end_key])
        actuals = {(int(r['year']), int(r['month'])): float(r['amount']) for _, r in mp_df.iterrows()}
        rx_labels  = [m.strftime('%b') for m in da_window]
        rx_vals    = [actuals.get((m.year, m.month), 0.0) for m in da_window]
        total_rx   = sum(rx_vals)
        bar_colors = ['#a855f7' if actuals.get((m.year, m.month)) else '#3d1f5e' for m in da_window]
        fig3 = go.Figure(go.Bar(
            x=rx_labels, y=rx_vals, marker_color=bar_colors,
            text=[f"${v:,.0f}" if v else '' for v in rx_vals], textposition='outside',
            hovertemplate='<b>%{x}</b><br>Received: <b>$%{y:,.2f}</b><extra></extra>',
        ))
        _rx_max = max((v for v in rx_vals if v), default=1000)
        win_start, win_end = da_window[0], da_window[-1]
        fig3.update_layout(
            title=(f'Monthly Dividends Received  |  {win_start.strftime("%b %Y")} – '
                   f'{win_end.strftime("%b %Y")}  |  Total to date: ${total_rx:,.0f}'),
            template='plotly_dark',
            yaxis=dict(title='Amount ($)', range=[0, _rx_max * 1.2]),
            margin=dict(t=60, b=60, l=60, r=20),
            paper_bgcolor='#1a1f2e', plot_bgcolor='rgba(255,255,255,0.03)',
        )
        charts_da['monthly_received'] = json.dumps(fig3, cls=plotly.utils.PlotlyJSONEncoder)
    except Exception:
        pass

    return charts_da, totals


@app.route("/dividend_analysis")
def dividend_analysis():
    """Per-ticker dividend summary with filterable charts by asset type."""
    conn = get_connection()
    ensure_tables_exist(conn)
    try:
        df = pd.read_sql("""
            SELECT
                ticker, description, classification_type,
                ytd_divs, total_divs_received, paid_for_itself,
                dividend_paid, estim_payment_per_year, approx_monthly_income,
                annual_yield_on_cost, current_annual_yield,
                purchase_value, current_value, gain_or_loss,
                div_frequency, ex_div_date,
                reinvest, div, current_price
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND ISNULL(quantity, 0) > 0
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn)
    except Exception:
        df = pd.DataFrame()

    all_types = sorted(df['classification_type'].dropna().unique().tolist()) if not df.empty else []
    charts_da, totals = _da_build_charts_and_totals(df, conn)
    conn.close()
    return render_template("dividend_analysis.html",
                           data=df, totals=totals, charts_da=charts_da, all_types=all_types)


@app.route("/dividend_analysis/data")
def dividend_analysis_data():
    """AJAX: filtered charts + totals for dividend analysis category filter."""
    types_param = request.args.get('types', '').strip()
    type_list   = [t.strip() for t in types_param.split(',') if t.strip()] if types_param else []
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT
                ticker, description, classification_type,
                ytd_divs, total_divs_received, paid_for_itself,
                dividend_paid, estim_payment_per_year, approx_monthly_income,
                annual_yield_on_cost, current_annual_yield,
                purchase_value, current_value, gain_or_loss,
                div_frequency, ex_div_date,
                reinvest, div, current_price
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND ISNULL(quantity, 0) > 0
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn)
    except Exception:
        df = pd.DataFrame()
    if type_list and not df.empty:
        df = df[df['classification_type'].isin(type_list)]
    charts_da, totals = _da_build_charts_and_totals(df, conn)
    conn.close()
    safe_totals = {k: (float(v) if v == v else 0.0) for k, v in totals.items()}
    return jsonify({'charts': charts_da, 'totals': safe_totals})




# ── Dividend Charts page ───────────────────────────────────────

@app.route("/dividend_charts")
def dividend_charts():
    """Charts for YTD Divs, Total Divs Received, and Paid For Itself."""
    conn = get_connection()
    ensure_tables_exist(conn)

    charts = {}

    try:
        df = pd.read_sql("""
            SELECT ticker, description, classification_type,
                   ytd_divs, total_divs_received, paid_for_itself,
                   purchase_value, estim_payment_per_year
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn)

        if not df.empty:
            # Coerce all numeric columns — handles '--' and other non-numeric cells
            for col in ["ytd_divs", "total_divs_received", "paid_for_itself",
                        "purchase_value", "estim_payment_per_year"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df["description"] = df["description"].fillna("").astype(str)
            df["classification_type"] = df["classification_type"].fillna("Other").astype(str)

            def _bar_traces(frame, y_col, hover_label, fmt="$"):
                """Add one Bar trace per asset type so bars are color-coded."""
                traces = []
                for ctype in frame["classification_type"].unique().tolist():
                    g = frame[frame["classification_type"] == ctype]
                    vals = g[y_col].tolist()
                    if fmt == "$":
                        texts = [f"${v:,.2f}" for v in vals]
                        htmpl = (f"<b>%{{x}}</b><br>%{{customdata}}<br>"
                                 f"{hover_label}: <b>$%{{y:,.2f}}</b><extra></extra>")
                    else:  # percent
                        texts = [f"{v:.1f}%" for v in vals]
                        htmpl = (f"<b>%{{x}}</b><br>%{{customdata}}<br>"
                                 f"{hover_label}: <b>%{{y:.1f}}%</b><extra></extra>")
                    traces.append(go.Bar(
                        x=g["ticker"].tolist(),
                        y=vals,
                        name=ctype,
                        text=texts,
                        textposition="auto",
                        customdata=g["description"].tolist(),
                        hovertemplate=htmpl,
                    ))
                return traces

            def _bar_layout(title, yaxis_title):
                return dict(
                    title=title,
                    template="plotly_dark",
                    xaxis_title="Ticker",
                    yaxis_title=yaxis_title,
                    xaxis_tickangle=-45,
                    legend_title="Asset Type",
                    uniformtext_minsize=8,
                    uniformtext_mode="hide",
                    margin=dict(t=60, b=120),
                )

            # 1. Total Divs Received by Ticker (top 25)
            top25 = df[df["total_divs_received"] > 0].nlargest(25, "total_divs_received").copy()
            if not top25.empty:
                fig1 = go.Figure(_bar_traces(top25, "total_divs_received", "Total Dividends Received"))
                fig1.update_layout(**_bar_layout(
                    "Total Dividends Received by Ticker (Top 25)",
                    "Total Dividends Received ($)",
                ))
                charts["total_divs"] = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

            # 2. YTD Divs by Ticker (top 25)
            ytd_df = df[df["ytd_divs"] > 0].nlargest(25, "ytd_divs").copy()
            if not ytd_df.empty:
                fig2 = go.Figure(_bar_traces(ytd_df, "ytd_divs", "YTD Dividends"))
                fig2.update_layout(**_bar_layout(
                    "YTD Dividends by Ticker (Top 25)",
                    "YTD Dividends ($)",
                ))
                charts["ytd_divs"] = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

            # 3. Paid For Itself % by Ticker
            paid_df = df[df["paid_for_itself"] > 0].copy()
            paid_df = paid_df.sort_values("paid_for_itself", ascending=False)
            if not paid_df.empty:
                paid_df["paid_pct"] = (paid_df["paid_for_itself"] * 100).round(1)
                fig3 = go.Figure(_bar_traces(paid_df, "paid_pct", "Cost Recovered", fmt="%"))
                fig3.add_hline(y=100, line_dash="dash", line_color="gold",
                               annotation_text="100% — Fully Paid For Itself",
                               annotation_font_color="gold")
                fig3.update_layout(**_bar_layout(
                    "Paid For Itself — % of Cost Recovered via Dividends",
                    "% of Cost Recovered",
                ))
                charts["paid_for_itself"] = json.dumps(fig3, cls=plotly.utils.PlotlyJSONEncoder)

            # 4. YTD vs Total Divs comparison (grouped bar, top 20)
            cmp_df = df[df["total_divs_received"] > 0].nlargest(20, "total_divs_received").copy()
            cmp_df["ytd_divs"] = cmp_df["ytd_divs"].fillna(0)
            if not cmp_df.empty:
                fig4 = go.Figure()
                fig4.add_trace(go.Bar(
                    x=cmp_df["ticker"].tolist(),
                    y=cmp_df["total_divs_received"].tolist(),
                    name="Total Divs Received",
                    marker_color="#636EFA",
                    text=[f"${v:,.2f}" for v in cmp_df["total_divs_received"]],
                    textposition="auto",
                    customdata=cmp_df["description"].tolist(),
                    hovertemplate="<b>%{x}</b><br>%{customdata}<br>Total Divs Received: <b>$%{y:,.2f}</b><extra></extra>",
                ))
                fig4.add_trace(go.Bar(
                    x=cmp_df["ticker"].tolist(),
                    y=cmp_df["ytd_divs"].tolist(),
                    name="YTD Divs",
                    marker_color="#EF553B",
                    text=[f"${v:,.2f}" for v in cmp_df["ytd_divs"]],
                    textposition="auto",
                    customdata=cmp_df["description"].tolist(),
                    hovertemplate="<b>%{x}</b><br>%{customdata}<br>YTD Dividends: <b>$%{y:,.2f}</b><extra></extra>",
                ))
                fig4.update_layout(
                    title="YTD Divs vs Total Divs Received (Top 20 by Total)",
                    barmode="group",
                    template="plotly_dark",
                    xaxis_title="Ticker",
                    yaxis_title="Amount ($)",
                    xaxis_tickangle=-45,
                    legend_title="Series",
                    uniformtext_minsize=8,
                    uniformtext_mode="hide",
                    margin=dict(t=60, b=120),
                )
                charts["comparison"] = json.dumps(fig4, cls=plotly.utils.PlotlyJSONEncoder)

            # 5. Total Divs by Classification Type (pie)
            type_df = df.groupby("classification_type")["total_divs_received"].sum().reset_index()
            type_df = type_df[type_df["total_divs_received"] > 0].copy()
            if not type_df.empty:
                fig5 = go.Figure(go.Pie(
                    labels=type_df["classification_type"].tolist(),
                    values=type_df["total_divs_received"].tolist(),
                    hovertemplate="<b>%{label}</b><br>Total Dividends: $%{value:,.2f}<br>Portfolio Share: %{percent}<extra></extra>",
                    textinfo="label+percent",
                ))
                fig5.update_layout(
                    title="Total Dividends Received by Asset Type",
                    template="plotly_dark",
                )
                charts["by_type"] = json.dumps(fig5, cls=plotly.utils.PlotlyJSONEncoder)

    except Exception as e:
        flash(f"Chart error: {e}", "error")

    conn.close()
    return render_template("dividend_charts.html", charts=charts)


# ── Payouts page ──────────────────────────────────────────────

@app.route("/payouts")
def payouts():
    """Payouts page with weekly/monthly tables and per-ticker total divs received."""
    conn = get_connection()
    ensure_tables_exist(conn)

    weekly = pd.read_sql("""
        SELECT id, pay_date, week_of_month, amount,
               SUM(amount) OVER (ORDER BY pay_date) AS running_total
        FROM dbo.weekly_payouts
        ORDER BY pay_date
    """, conn)

    monthly = pd.read_sql("""
        SELECT id, year, month, amount,
               SUM(amount) OVER (ORDER BY year, month) AS running_total
        FROM dbo.monthly_payouts
        ORDER BY year, month
    """, conn)

    # Per-ticker detail for weekly payers
    weekly_tickers = pd.read_sql("""
        SELECT ticker, shares, distribution, total_dividend
        FROM dbo.weekly_payout_tickers
        ORDER BY ticker
    """, conn)

    # Per-ticker detail for monthly payers
    monthly_tickers = pd.read_sql("""
        SELECT ticker, pay_month
        FROM dbo.monthly_payout_tickers
        ORDER BY ticker, pay_month
    """, conn)

    # Per-ticker total divs received from main table — all real holdings
    try:
        ticker_divs = pd.read_sql("""
            SELECT ticker, description, ytd_divs, total_divs_received, paid_for_itself
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn)
    except Exception:
        ticker_divs = pd.DataFrame()

    # Build a dict: month_number → list of tickers
    month_to_tickers = {}
    for _, row in monthly_tickers.iterrows():
        m = int(row["pay_month"])
        month_to_tickers.setdefault(m, []).append(row["ticker"])

    conn.close()
    return render_template(
        "payouts.html",
        weekly=weekly, monthly=monthly,
        weekly_tickers=weekly_tickers, month_to_tickers=month_to_tickers,
        ticker_divs=ticker_divs,
    )


@app.route("/payouts/weekly/import", methods=["POST"])
def import_weekly():
    try:
        count, msg = import_weekly_payouts()
        flash(msg, "success")
    except Exception as e:
        flash(f"Weekly import failed: {e}", "error")
    return redirect(url_for("payouts"))


@app.route("/payouts/monthly/import", methods=["POST"])
def import_monthly():
    try:
        count, msg = import_monthly_payouts()
        flash(msg, "success")
        count2, msg2 = import_monthly_payout_tickers()
        flash(msg2, "success")
    except Exception as e:
        flash(f"Monthly import failed: {e}", "error")
    return redirect(url_for("payouts"))


@app.route("/payouts/weekly/add", methods=["POST"])
def add_weekly():
    pay_date = request.form["pay_date"]
    week = request.form.get("week_of_month") or None
    amount = request.form["amount"]
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO dbo.weekly_payouts (pay_date, week_of_month, amount) VALUES (?, ?, ?)",
            pay_date, int(week) if week else None, float(amount),
        )
        conn.commit()
        conn.close()
        flash("Weekly payout added.", "success")
    except Exception as e:
        flash(f"Failed to add: {e}", "error")
    return redirect(url_for("payouts"))


@app.route("/payouts/monthly/add", methods=["POST"])
def add_monthly():
    year = request.form["year"]
    month = request.form["month"]
    amount = request.form["amount"]
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO dbo.monthly_payouts (year, month, amount) VALUES (?, ?, ?)",
            int(year), int(month), float(amount),
        )
        conn.commit()
        conn.close()
        flash("Monthly payout added.", "success")
    except Exception as e:
        flash(f"Failed to add: {e}", "error")
    return redirect(url_for("payouts"))


@app.route("/payouts/weekly/<int:row_id>/edit", methods=["POST"])
def edit_weekly(row_id):
    pay_date = request.form["pay_date"]
    week = request.form.get("week_of_month") or None
    amount = request.form["amount"]
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE dbo.weekly_payouts SET pay_date=?, week_of_month=?, amount=? WHERE id=?",
            pay_date, int(week) if week else None, float(amount), row_id,
        )
        conn.commit()
        conn.close()
        flash("Weekly payout updated.", "success")
    except Exception as e:
        flash(f"Failed to update: {e}", "error")
    return redirect(url_for("payouts"))


@app.route("/payouts/monthly/<int:row_id>/edit", methods=["POST"])
def edit_monthly(row_id):
    year = request.form["year"]
    month = request.form["month"]
    amount = request.form["amount"]
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE dbo.monthly_payouts SET year=?, month=?, amount=? WHERE id=?",
            int(year), int(month), float(amount), row_id,
        )
        conn.commit()
        conn.close()
        flash("Monthly payout updated.", "success")
    except Exception as e:
        flash(f"Failed to update: {e}", "error")
    return redirect(url_for("payouts"))


@app.route("/payouts/weekly/<int:row_id>/delete", methods=["POST"])
def delete_weekly(row_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dbo.weekly_payouts WHERE id=?", row_id)
        conn.commit()
        conn.close()
        flash("Weekly payout deleted.", "success")
    except Exception as e:
        flash(f"Failed to delete: {e}", "error")
    return redirect(url_for("payouts"))


@app.route("/payouts/monthly/<int:row_id>/delete", methods=["POST"])
def delete_monthly(row_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dbo.monthly_payouts WHERE id=?", row_id)
        conn.commit()
        conn.close()
        flash("Monthly payout deleted.", "success")
    except Exception as e:
        flash(f"Failed to delete: {e}", "error")
    return redirect(url_for("payouts"))


# ── Charts page ────────────────────────────────────────────────

@app.route("/charts")
def charts():
    """Income and payout trend charts."""
    conn = get_connection()
    chart_data = {}

    try:
        df = pd.read_sql("""
            SELECT import_date,
                   SUM(approx_monthly_income) AS total_monthly_income,
                   SUM(estim_payment_per_year) AS total_annual_income,
                   SUM(ytd_divs) AS total_ytd_divs,
                   SUM(total_divs_received) AS total_divs_received
            FROM dbo.income_tracking
            GROUP BY import_date
            ORDER BY import_date
        """, conn)

        if not df.empty:
            dates = df["import_date"].tolist()

            fig1 = go.Figure()
            fig1.add_trace(go.Bar(
                x=dates,
                y=df["total_monthly_income"].tolist(),
                name="Monthly Income",
                hovertemplate="<b>%{x}</b><br>Monthly Income: $%{y:,.2f}<extra></extra>",
            ))
            fig1.update_layout(
                title="Monthly Dividend Income Over Time",
                template="plotly_dark",
                xaxis_title="Import Date",
                yaxis_title="Monthly Income ($)",
            )
            chart_data["monthly"] = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=dates,
                y=df["total_annual_income"].tolist(),
                name="Annual Income",
                hovertemplate="<b>%{x}</b><br>Annual Income: $%{y:,.2f}<extra></extra>",
            ))
            fig2.update_layout(
                title="Estimated Annual Income Over Time",
                template="plotly_dark",
                xaxis_title="Import Date",
                yaxis_title="Annual Income ($)",
            )
            chart_data["annual"] = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

            # YTD and Total Divs trend over imports
            ytd_col = df["total_ytd_divs"] if "total_ytd_divs" in df.columns else None
            total_col = df["total_divs_received"] if "total_divs_received" in df.columns else None
            if ytd_col is not None and total_col is not None and ytd_col.notna().any():
                fig3 = go.Figure()
                fig3.add_trace(go.Bar(
                    x=df["import_date"], y=df["total_ytd_divs"].fillna(0),
                    name="YTD Divs Total",
                ))
                fig3.add_trace(go.Bar(
                    x=df["import_date"], y=df["total_divs_received"].fillna(0),
                    name="Total Divs Received",
                ))
                fig3.update_layout(
                    title="YTD Divs & Total Divs Received Over Import Dates",
                    barmode="group",
                    template="plotly_dark",
                    xaxis_title="Import Date",
                    yaxis_title="Amount ($)",
                )
                chart_data["div_trend"] = json.dumps(fig3, cls=plotly.utils.PlotlyJSONEncoder)

    except Exception:
        pass

    try:
        wdf = pd.read_sql("""
            SELECT pay_date, amount,
                   SUM(amount) OVER (ORDER BY pay_date) AS running_total
            FROM dbo.weekly_payouts ORDER BY pay_date
        """, conn)
        if not wdf.empty:
            fig_w = go.Figure()
            fig_w.add_trace(go.Bar(x=wdf["pay_date"], y=wdf["amount"], name="Weekly Payout"))
            fig_w.add_trace(go.Scatter(x=wdf["pay_date"], y=wdf["running_total"],
                                       name="Running Total", mode="lines+markers"))
            fig_w.update_layout(title="Weekly Payouts", template="plotly_dark",
                                xaxis_title="Date", yaxis_title="Amount ($)")
            chart_data["weekly"] = json.dumps(fig_w, cls=plotly.utils.PlotlyJSONEncoder)
    except Exception:
        pass

    try:
        mdf = pd.read_sql("""
            SELECT year, month, amount,
                   SUM(amount) OVER (ORDER BY year, month) AS running_total
            FROM dbo.monthly_payouts ORDER BY year, month
        """, conn)
        if not mdf.empty:
            mdf["label"] = mdf.apply(lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1)
            fig_m = go.Figure()
            fig_m.add_trace(go.Bar(x=mdf["label"], y=mdf["amount"], name="Monthly Payout"))
            fig_m.add_trace(go.Scatter(x=mdf["label"], y=mdf["running_total"],
                                       name="Running Total", mode="lines+markers"))
            fig_m.update_layout(title="Monthly Payouts", template="plotly_dark",
                                xaxis_title="Month", yaxis_title="Amount ($)")
            chart_data["monthly_payout"] = json.dumps(fig_m, cls=plotly.utils.PlotlyJSONEncoder)
    except Exception:
        pass

    conn.close()
    return render_template("charts.html", charts=chart_data)


# ── Total Return page ──────────────────────────────────────────

@app.route("/total_return")
def total_return():
    """Total return dashboard: DB-calculated returns (scatter/table) + AJAX yfinance charts."""
    conn = get_connection()
    ensure_tables_exist(conn)

    try:
        port = pd.read_sql("""
            SELECT ticker, description, classification_type,
                   price_paid, current_price, quantity,
                   purchase_value, current_value,
                   gain_or_loss, gain_or_loss_percentage,
                   total_divs_received, ytd_divs,
                   estim_payment_per_year, annual_yield_on_cost
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
            ORDER BY ticker
        """, conn)
    except Exception:
        port = pd.DataFrame()
    conn.close()

    scatter_json = None
    scatter_error = None

    if not port.empty:
        port["total_divs_received"] = port["total_divs_received"].fillna(0)
        port["total_return_dollar"] = port["gain_or_loss"].fillna(0) + port["total_divs_received"]
        port["total_return_pct"]    = (port["total_return_dollar"] / port["purchase_value"]) * 100
        port["price_return_pct"]    = (port["gain_or_loss"].fillna(0) / port["purchase_value"]) * 100

        # Scatter: all holdings, yield on cost vs total return (DB / all-time since purchase)
        scatter_error = None
        try:
            scatter_df = port.copy()
            scatter_df["yield_on_cost_pct"] = scatter_df["annual_yield_on_cost"].fillna(0) * 100
            scatter_df["classification_type"] = scatter_df["classification_type"].fillna("Other")
            max_pv = float(scatter_df["purchase_value"].max())
            scatter_df["bubble_size"] = ((scatter_df["purchase_value"] / max_pv * 35) + 8).clip(8, 43)

            fig_scatter = go.Figure()
            for ctype, grp in scatter_df.groupby("classification_type"):
                fig_scatter.add_trace(go.Scatter(
                    x=grp["yield_on_cost_pct"].tolist(),
                    y=grp["total_return_pct"].tolist(),
                    mode="markers+text",
                    name=str(ctype),
                    text=grp["ticker"].tolist(),
                    textposition="top center",
                    textfont=dict(size=9),
                    marker=dict(size=grp["bubble_size"].tolist(), opacity=0.8),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Total Ret: %{y:.2f}%<br>"
                        "Yield on Cost: %{x:.2f}%"
                        "<extra>" + str(ctype) + "</extra>"
                    ),
                ))

            fig_scatter.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            fig_scatter.update_layout(
                title="Total Return % vs Annual Yield on Cost — Since Purchase (All Time)",
                template="plotly_dark",
                height=520,
                xaxis_title="Annual Yield on Cost (%)",
                yaxis_title="Total Return % (Since Purchase)",
                legend_title="Type",
            )
            scatter_json = json.dumps(fig_scatter, cls=plotly.utils.PlotlyJSONEncoder)
        except Exception as _scatter_exc:
            import traceback
            scatter_error = traceback.format_exc(limit=3)
            scatter_json = None

    table_df = port.sort_values("total_return_pct", ascending=False) if not port.empty else port

    return render_template(
        "total_return.html",
        table=table_df,
        scatter=scatter_json,
        scatter_error=scatter_error,
    )


@app.route("/total_return/data")
def total_return_data():
    """AJAX endpoint: yfinance bar + price-history charts for a given period."""
    from flask import jsonify
    import warnings, traceback, math
    from datetime import date as date_type
    warnings.filterwarnings("ignore")

    period = request.args.get("period", "1y")

    period_map = {
        "1mo":  (dict(period="1mo"),  "1d"),
        "3mo":  (dict(period="3mo"),  "1d"),
        "6mo":  (dict(period="6mo"),  "1d"),
        "ytd":  (dict(period="ytd"),  "1d"),
        "1y":   (dict(period="1y"),   "1wk"),
        "2y":   (dict(period="2y"),   "1wk"),
        "5y":   (dict(period="5y"),   "1mo"),
        "max":  (dict(period="max"),  "1mo"),
    }

    if period.isdigit() and len(period) == 4:
        yr = int(period)
        today = date_type.today()
        start = f"{yr}-01-01"
        end   = today.strftime("%Y-%m-%d") if yr == today.year else f"{yr}-12-31"
        yf_kwargs   = dict(start=start, end=end)
        yf_interval = "1d" if yr == today.year else "1wk"
    else:
        yf_range, yf_interval = period_map.get(period, (dict(period="1y"), "1wk"))
        yf_kwargs = yf_range

    period_labels = {
        "1mo": "1 Month", "3mo": "3 Months", "6mo": "6 Months",
        "ytd": "Year to Date", "1y": "1 Year", "2y": "2 Years",
        "5y": "5 Years", "max": "All Available",
        "2021": "Calendar 2021", "2022": "Calendar 2022",
        "2023": "Calendar 2023", "2024": "Calendar 2024",
        "2025": "Calendar 2025",
    }
    period_label = period_labels.get(period, period)

    conn = get_connection()
    try:
        port = pd.read_sql("""
            SELECT ticker, description, classification_type, purchase_value
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
            ORDER BY ticker
        """, conn)
    except Exception:
        port = pd.DataFrame()
    conn.close()

    if port.empty:
        return jsonify({"error": "No portfolio data found."}), 404

    try:
        import yfinance as yf

        tickers_list = port["ticker"].tolist()
        raw = yf.download(
            " ".join(tickers_list + ["SPY"]),
            **yf_kwargs,
            interval=yf_interval,
            progress=False,
            auto_adjust=True,
        )

        if raw.empty or "Close" not in raw:
            return jsonify({"error": "No price data returned from Yahoo Finance."}), 500

        close = raw["Close"]

        # Normalize each ticker to 100 at first valid close
        norm = close.copy()
        for col in norm.columns:
            first_valid = norm[col].first_valid_index()
            if first_valid is not None:
                base = norm.loc[first_valid, col]
                norm[col] = (norm[col] / base) * 100 if base and base != 0 else None

        # Period return % per ticker
        returns = {}
        for col in norm.columns:
            s = norm[col].dropna()
            returns[col] = round(float(s.iloc[-1] - 100), 2) if len(s) >= 2 else None

        spy_ret = returns.get("SPY")

        # ── Bar chart ───────────────────────────────────────────
        ret_df = port.copy()
        ret_df["period_return"] = ret_df["ticker"].map(returns)
        ret_df = ret_df[ret_df["period_return"].notna()].sort_values("period_return", ascending=True)

        if ret_df.empty:
            return jsonify({"error": f"No return data available for period: {period_label}"}), 404

        colors = ["#4dff91" if v >= 0 else "#ff6b6b" for v in ret_df["period_return"]]

        fig_bar = go.Figure(go.Bar(
            x=ret_df["period_return"],
            y=ret_df["ticker"],
            orientation="h",
            marker_color=colors,
            text=ret_df["period_return"].apply(lambda v: f"{v:.1f}%"),
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Return: %{x:.2f}%<extra></extra>",
        ))

        if spy_ret is not None:
            fig_bar.add_vline(
                x=spy_ret,
                line_dash="dash", line_color="#FFD700",
                annotation_text=f"SPY: {spy_ret:.1f}%",
                annotation_position="top",
            )

        fig_bar.update_layout(
            title=f"Total Return % by Ticker — {period_label} (dividend-adjusted via Yahoo Finance)",
            template="plotly_dark",
            xaxis_title="Total Return (%)",
            yaxis_title="",
            height=max(500, len(ret_df) * 20),
            margin=dict(l=70, r=80, t=60, b=50),
        )

        # ── Price history line chart ────────────────────────────
        dates = [str(d)[:10] for d in norm.index]
        top10_set = set(
            ret_df.nlargest(5, "period_return")["ticker"].tolist()
            + ret_df.nsmallest(5, "period_return")["ticker"].tolist()
        )

        fig_hist = go.Figure()
        if "SPY" in norm.columns:
            fig_hist.add_trace(go.Scatter(
                x=dates, y=norm["SPY"].tolist(),
                name="SPY (Benchmark)",
                line=dict(color="#FFD700", width=3, dash="dash"),
                visible=True,
            ))

        for tkr in tickers_list:
            if tkr not in norm.columns:
                continue
            vals = norm[tkr].tolist()
            desc_vals = port.loc[port["ticker"] == tkr, "description"].values
            hover_name = desc_vals[0][:35] if len(desc_vals) else tkr
            visible = True if tkr in top10_set else "legendonly"
            fig_hist.add_trace(go.Scatter(
                x=dates, y=vals,
                name=tkr,
                visible=visible,
                hovertemplate=f"<b>{tkr}</b> — {hover_name}<br>Normalized: %{{y:.1f}}<br>Date: %{{x}}<extra></extra>",
                line=dict(width=1.5),
            ))

        fig_hist.update_layout(
            title=f"Price Performance — {period_label} (normalized to 100, dividend-adjusted)",
            template="plotly_dark",
            xaxis_title="Date",
            yaxis_title="Normalized Price (100 = start)",
            height=550,
            legend=dict(orientation="v", x=1.01, y=1, font=dict(size=10)),
            hovermode="x unified",
        )

        return jsonify({
            "bar":          json.loads(json.dumps(fig_bar,  cls=plotly.utils.PlotlyJSONEncoder)),
            "history":      json.loads(json.dumps(fig_hist, cls=plotly.utils.PlotlyJSONEncoder)),
            "spy_ret":      spy_ret,
            "period_label": period_label,
        })

    except Exception as e:
        return jsonify({"error": str(e), "detail": traceback.format_exc(limit=3)}), 500


# ── Single ETF Total Return page ───────────────────────────────

@app.route("/single_etf_return")
def single_etf_return():
    """Interactive single-ticker total return vs SPY comparison page."""
    conn = get_connection()
    ensure_tables_exist(conn)
    try:
        tickers_df = pd.read_sql("""
            SELECT ticker, description, classification_type,
                   price_paid, current_price, quantity,
                   purchase_value, current_value, gain_or_loss,
                   total_divs_received, annual_yield_on_cost
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
            ORDER BY ticker
        """, conn)
    except Exception:
        tickers_df = pd.DataFrame()
    conn.close()

    # Pre-compute total return so we can show it in the dropdown
    if not tickers_df.empty:
        tickers_df["total_divs_received"] = tickers_df["total_divs_received"].fillna(0)
        tickers_df["total_return_pct"] = (
            (tickers_df["gain_or_loss"].fillna(0) + tickers_df["total_divs_received"])
            / tickers_df["purchase_value"] * 100
        )

    return render_template("single_etf_return.html", portfolio=tickers_df)


@app.route("/single_etf_return/data")
def single_etf_data():
    """AJAX endpoint — up to 3 independent tickers (t1/t2/t3), any combination.
    mode: total | price | both
    t1 = portfolio slot, t2 = custom slot, t3 = compare slot (all optional).
    """
    from flask import jsonify
    import warnings, math
    from datetime import date as date_type
    warnings.filterwarnings("ignore")

    t1     = request.args.get("t1",  "").strip().upper()
    t2     = request.args.get("t2",  "").strip().upper()
    t3     = request.args.get("t3",  "").strip().upper()
    period = request.args.get("period", "1y")
    mode   = request.args.get("mode",   "total")   # total | price | both

    # Ordered list of (symbol, slot_key) for each enabled non-empty ticker
    slots = [(sym, lbl) for sym, lbl in [(t1, "t1"), (t2, "t2"), (t3, "t3")] if sym]
    if not slots:
        return jsonify({"error": "No tickers selected."}), 400

    period_map = {
        "1mo":  (dict(period="1mo"),  "1d"),
        "3mo":  (dict(period="3mo"),  "1d"),
        "6mo":  (dict(period="6mo"),  "1d"),
        "ytd":  (dict(period="ytd"),  "1d"),
        "1y":   (dict(period="1y"),   "1wk"),
        "2y":   (dict(period="2y"),   "1wk"),
        "5y":   (dict(period="5y"),   "1mo"),
        "max":  (dict(period="max"),  "1mo"),
    }
    period_labels = {
        "1mo": "1 Month", "3mo": "3 Months", "6mo": "6 Months",
        "ytd": "Year to Date", "1y": "1 Year", "2y": "2 Years",
        "5y": "5 Years", "max": "All Available",
        "2021": "Calendar 2021", "2022": "Calendar 2022",
        "2023": "Calendar 2023", "2024": "Calendar 2024",
        "2025": "Calendar 2025",
    }

    if period.isdigit() and len(period) == 4:
        yr = int(period)
        today = date_type.today()
        start = f"{yr}-01-01"
        end   = today.strftime("%Y-%m-%d") if yr == today.year else f"{yr}-12-31"
        yf_kwargs   = dict(start=start, end=end)
        yf_interval = "1d" if yr == today.year else "1wk"
    else:
        yf_range, yf_interval = period_map.get(period, (dict(period="1y"), "1wk"))
        yf_kwargs = yf_range

    period_label = period_labels.get(period, period)

    # ── Color / line-style per slot ──────────────────────────────────
    SLOT_STYLES = {
        "t1": dict(color="#7ecfff", price_color="#5ba8d0", line_dash="solid", width=3),
        "t2": dict(color="#a0f0c0", price_color="#70c090", line_dash="solid", width=2.5),
        "t3": dict(color="#FFD700", price_color="#c0a000", line_dash="dash",  width=2.5),
    }

    # ── Helpers ──────────────────────────────────────────────────────
    def dl(symbols, adj):
        sym_str = " ".join(symbols) if isinstance(symbols, list) else symbols
        raw = __import__("yfinance").download(
            sym_str, **yf_kwargs, interval=yf_interval,
            progress=False, auto_adjust=adj,
        )
        if raw.empty or "Close" not in raw:
            return pd.DataFrame()
        c = raw["Close"]
        return c.to_frame() if isinstance(c, pd.Series) else c

    def get_col(df, sym):
        if sym in df.columns:
            return df[sym].dropna()
        if df.shape[1] == 1:
            return df.iloc[:, 0].dropna()
        return None

    def norm(s):
        s = s.dropna()
        return ((s / s.iloc[0]) * 100).round(4) if len(s) >= 2 else None

    def pct_ret(ns):
        return round(float(ns.iloc[-1] - 100), 4) if ns is not None and len(ns) >= 2 else None

    def ann_ret(ns, raw_s):
        if ns is None or raw_s is None or len(ns) < 2:
            return None
        n_days = (raw_s.dropna().index[-1] - raw_s.dropna().index[0]).days
        if n_days <= 0:
            return None
        years = n_days / 365.25
        try:
            a = ((ns.iloc[-1] / 100) ** (1 / years) - 1) * 100
            return round(a, 4) if math.isfinite(a) else None
        except Exception:
            return None

    def max_dd(s):
        s = s.dropna()
        if len(s) < 2:
            return None
        peak = s.expanding().max()
        return round(float(((s - peak) / peak * 100).min()), 4)

    try:
        import yfinance as yf  # noqa

        # ── DB snapshot helper (portfolio tickers with no Yahoo data) ────────
        def db_snapshot(sym):
            """Return {price_ret, div_pct, total_ret} from all_account_info, or None."""
            c2 = get_connection()
            try:
                r2 = pd.read_sql("""
                    SELECT purchase_value, gain_or_loss, total_divs_received
                    FROM dbo.all_account_info WHERE ticker = ?
                """, c2, params=[sym])
            except Exception:
                r2 = pd.DataFrame()
            finally:
                c2.close()
            if r2.empty:
                return None
            r  = r2.iloc[0]
            pv = float(r["purchase_value"])      if pd.notna(r["purchase_value"])      else 0.0
            gl = float(r["gain_or_loss"])         if pd.notna(r["gain_or_loss"])         else 0.0
            dv = float(r["total_divs_received"]) if pd.notna(r["total_divs_received"]) else 0.0
            if pv == 0:
                return None
            return {
                "price_ret": round(gl          / pv * 100, 4),
                "div_pct":   round(dv          / pv * 100, 4),
                "total_ret": round((gl + dv)   / pv * 100, 4),
            }

        # ── Download price data for all active tickers at once ───────────────
        all_syms    = [sym for sym, _ in slots]
        close_adj   = dl(all_syms, adj=True)  if mode in ("total", "both") else pd.DataFrame()
        close_unadj = dl(all_syms, adj=False) if mode in ("price",  "both") else pd.DataFrame()

        fig          = go.Figure()
        stats_list   = []   # per-ticker stat dicts
        any_db       = False

        # Reference date span — from first ticker that has usable time-series data
        ref_dates = None
        for sym, _ in slots:
            for df in [close_adj, close_unadj]:
                if df.empty:
                    continue
                s = get_col(df, sym)
                if s is not None and len(s) >= 2:
                    ref_dates = [str(d)[:10] for d in s.index]
                    break
            if ref_dates:
                break

        # ── Per-ticker trace building ────────────────────────────────────────
        for sym, slot in slots:
            sty     = SLOT_STYLES[slot]
            s_adj   = get_col(close_adj,   sym) if not close_adj.empty   else None
            s_unadj = get_col(close_unadj, sym) if not close_unadj.empty else None
            has_adj   = s_adj   is not None and len(s_adj)   >= 2
            has_unadj = s_unadj is not None and len(s_unadj) >= 2

            # Determine if we need Yahoo data and whether it's missing
            need_adj   = mode in ("total", "both")
            need_unadj = mode == "price"

            if (need_adj and not has_adj) or (need_unadj and not has_unadj):
                snap = db_snapshot(sym)
                if snap is None:
                    stats_list.append({
                        "label": sym, "ret": None, "price_ret": None,
                        "div_contrib": None, "ann": None, "mdd": None,
                        "note": "No data available",
                    })
                    continue

                # Draw horizontal reference lines at the DB return levels
                any_db  = True
                x_span  = [ref_dates[0], ref_dates[-1]] if ref_dates else ["Start", "Now"]

                if mode in ("total", "both"):
                    lvl = 100 + snap["total_ret"]
                    fig.add_trace(go.Scatter(
                        x=x_span, y=[lvl, lvl],
                        name=f"{sym} Total (DB)",
                        mode="lines+markers",
                        line=dict(color=sty["color"], width=2, dash="dashdot"),
                        marker=dict(symbol="diamond", size=8),
                        hovertemplate=(f"<b>{sym} DB snapshot</b>  Total: "
                                       f"{snap['total_ret']:+.2f}%<extra>all-time</extra>"),
                    ))
                if mode in ("price", "both"):
                    lvl = 100 + snap["price_ret"]
                    fig.add_trace(go.Scatter(
                        x=x_span, y=[lvl, lvl],
                        name=f"{sym} Price (DB)",
                        mode="lines+markers",
                        line=dict(color=sty["price_color"], width=2, dash="dot"),
                        marker=dict(symbol="circle-open", size=6),
                        hovertemplate=(f"<b>{sym} DB snapshot</b>  Price: "
                                       f"{snap['price_ret']:+.2f}%<extra>all-time</extra>"),
                    ))

                stats_list.append({
                    "label":       sym,
                    "ret":         snap["total_ret"] if mode != "price" else snap["price_ret"],
                    "price_ret":   snap["price_ret"] if mode == "both" else None,
                    "div_contrib": snap["div_pct"]   if mode == "both" else None,
                    "ann":  None, "mdd": None,
                    "note": "DB snapshot — no Yahoo price history",
                })
                continue

            # ── Yahoo data is available — add traces ─────────────────────────
            if mode == "total":
                n = norm(s_adj)
                if n is None:
                    continue
                fig.add_trace(go.Scatter(
                    x=[str(d)[:10] for d in n.index], y=n.tolist(),
                    name=sym,
                    line=dict(color=sty["color"], width=sty["width"],
                              dash=sty["line_dash"]),
                    hovertemplate=(f"<b>{sym}</b>: %{{y:.2f}}<br>%{{x}}"
                                   f"<extra>Total Return</extra>"),
                ))
                stats_list.append({
                    "label": sym, "ret": pct_ret(n), "price_ret": None,
                    "div_contrib": None,
                    "ann": ann_ret(n, s_adj), "mdd": max_dd(s_adj), "note": None,
                })

            elif mode == "price":
                n = norm(s_unadj)
                if n is None:
                    continue
                fig.add_trace(go.Scatter(
                    x=[str(d)[:10] for d in n.index], y=n.tolist(),
                    name=f"{sym} (Price)",
                    line=dict(color=sty["price_color"], width=sty["width"],
                              dash=sty["line_dash"]),
                    hovertemplate=(f"<b>{sym} Price</b>: %{{y:.2f}}<br>%{{x}}"
                                   f"<extra>Price Only</extra>"),
                ))
                stats_list.append({
                    "label": f"{sym} (Price)", "ret": pct_ret(n), "price_ret": None,
                    "div_contrib": None,
                    "ann": ann_ret(n, s_unadj), "mdd": max_dd(s_unadj), "note": None,
                })

            else:  # both
                n_adj   = norm(s_adj)   if has_adj   else None
                n_unadj = norm(s_unadj) if has_unadj else None

                # Price-only trace first (drawn behind)
                if n_unadj is not None:
                    fig.add_trace(go.Scatter(
                        x=[str(d)[:10] for d in n_unadj.index], y=n_unadj.tolist(),
                        name=f"{sym} (Price)",
                        line=dict(color=sty["price_color"],
                                  width=max(sty["width"] - 1, 1.5), dash="dot"),
                        hovertemplate=(f"<b>{sym} Price</b>: %{{y:.2f}}<br>%{{x}}"
                                       f"<extra></extra>"),
                    ))
                # Total return on top
                if n_adj is not None:
                    fig.add_trace(go.Scatter(
                        x=[str(d)[:10] for d in n_adj.index], y=n_adj.tolist(),
                        name=f"{sym} (Total)",
                        line=dict(color=sty["color"], width=sty["width"],
                                  dash=sty["line_dash"]),
                        hovertemplate=(f"<b>{sym} Total</b>: %{{y:.2f}}<br>%{{x}}"
                                       f"<extra></extra>"),
                    ))

                tr = pct_ret(n_adj)
                pr = pct_ret(n_unadj)
                stats_list.append({
                    "label": sym, "ret": tr, "price_ret": pr,
                    "div_contrib": (round(tr - pr, 4)
                                    if tr is not None and pr is not None else None),
                    "ann": ann_ret(n_adj, s_adj), "mdd": max_dd(s_adj), "note": None,
                })

        # ── Pairwise alphas ──────────────────────────────────────────────────
        alphas = []
        rets = [(t["label"], t["ret"])
                for t in stats_list if t.get("ret") is not None]
        for i in range(len(rets)):
            for j in range(i + 1, len(rets)):
                a, b = rets[i], rets[j]
                alphas.append({
                    "label": f"{a[0]} vs {b[0]}",
                    "value": round(a[1] - b[1], 4),
                })

        # ── Chart layout ─────────────────────────────────────────────────────
        sym_list    = " | ".join(sym for sym, _ in slots)
        mode_suffix = {"total": "", "price": " — Price Only",
                       "both": " — Total & Price Return"}
        title = f"{sym_list} — {period_label}{mode_suffix.get(mode, '')}"
        if any_db:
            title += "  ⚠ some tickers: DB snapshot"

        fig.add_hline(y=100, line_dash="dot", line_color="gray", opacity=0.4)
        fig.update_layout(
            title=title,
            template="plotly_dark",
            xaxis_title="Date",
            yaxis_title="Normalized Return (100 = start)",
            height=490,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            margin=dict(l=60, r=40, t=80, b=50),
        )
        if any_db:
            fig.add_annotation(
                text=("⚠ Horizontal lines = DB portfolio snapshot (all-time) "
                      "| Time series = Yahoo Finance"),
                xref="paper", yref="paper", x=0.5, y=-0.12,
                showarrow=False, font=dict(size=11, color="#8899aa"),
                xanchor="center",
            )

        stats_out = {
            "tickers": stats_list,
            "alphas":  alphas,
            "period":  period_label,
            "mode":    mode,
        }

        # ── DB position panel — only for t1 (portfolio slot) ─────────────────
        db_stats = {}
        if t1:
            conn = get_connection()
            try:
                row = pd.read_sql("""
                    SELECT price_paid, current_price, quantity, purchase_value,
                           current_value, gain_or_loss, total_divs_received,
                           annual_yield_on_cost, estim_payment_per_year, description
                    FROM dbo.all_account_info WHERE ticker = ?
                """, conn, params=[t1])
            except Exception:
                row = pd.DataFrame()
            conn.close()
            if not row.empty:
                r    = row.iloc[0]
                gl   = float(r["gain_or_loss"])        if pd.notna(r["gain_or_loss"])        else 0
                divs = float(r["total_divs_received"]) if pd.notna(r["total_divs_received"]) else 0
                pv   = float(r["purchase_value"])      if pd.notna(r["purchase_value"])      else 0
                db_stats = {
                    "description":     str(r["description"]) if pd.notna(r["description"]) else t1,
                    "price_paid":      float(r["price_paid"])    if pd.notna(r["price_paid"])    else None,
                    "current_price":   float(r["current_price"]) if pd.notna(r["current_price"]) else None,
                    "shares":          float(r["quantity"])      if pd.notna(r["quantity"])      else None,
                    "purchase_value":  pv,
                    "current_value":   float(r["current_value"]) if pd.notna(r["current_value"]) else None,
                    "price_gain_loss": gl,
                    "total_divs":      divs,
                    "total_return_dollar": gl + divs,
                    "total_return_pct":    round((gl + divs) / pv * 100, 4) if pv else None,
                    "yield_on_cost_pct":   (round(float(r["annual_yield_on_cost"]) * 100, 4)
                                            if pd.notna(r["annual_yield_on_cost"]) else None),
                    "est_annual_income":   (float(r["estim_payment_per_year"])
                                            if pd.notna(r["estim_payment_per_year"]) else None),
                }

        return jsonify({
            "chart": json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)),
            "stats": stats_out,
            "db":    db_stats,
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "detail": traceback.format_exc(limit=3)}), 500


# ── Dividend Calendar ──────────────────────────────────────────────────────

@app.route("/div_calendar")
def div_calendar():
    """Full ex-dividend date calendar — card layout for all portfolio holdings."""
    from datetime import date as date_type
    conn = get_connection()
    events = _build_cal_events(conn)
    conn.close()
    today = date_type.today().isoformat()
    return render_template("div_calendar.html", events=events, today=today)


@app.route("/div_calendar/paydates")
def div_calendar_paydates():
    """AJAX: return pay dates using the same smart logic as _build_cal_events.
    Response: { TICKER: { pay_date: 'YYYY-MM-DD', estimated: bool } }
    freqs param format: TICKER:FREQ:YYYY-MM-DD  (comma-separated)
    """
    from flask import jsonify
    from datetime import datetime, timedelta

    def _next_biz(base, days):
        d = base + timedelta(days=days)
        if d.weekday() == 5:    d += timedelta(days=2)
        elif d.weekday() == 6:  d += timedelta(days=1)
        return d

    # Fallback offsets for non-weekly, non-monthly when yfinance has no data
    FREQ_OFFSET = {"Q": 14, "SA": 21, "A": 21}

    # Parse all pairs first so we can batch yfinance for non-weekly tickers
    parsed = []
    for pair in request.args.get("freqs", "").split(","):
        parts = [p.strip() for p in pair.split(":")]
        if len(parts) < 3:
            continue
        sym, freq, exdiv_str = parts[0].upper(), parts[1], parts[2]
        if not sym or not exdiv_str:
            continue
        try:
            db_ex = datetime.strptime(exdiv_str, "%Y-%m-%d").date()
        except Exception:
            continue
        parsed.append((sym, freq, db_ex))

    # Pre-fetch yfinance for non-weekly, non-monthly tickers
    yf_dates = {}
    for sym, freq, _ in parsed:
        if freq not in ("52", "W", "M"):
            pd_date = _yf_div_pay_date(sym)
            if pd_date:
                yf_dates[sym] = pd_date

    results = {}
    for sym, freq, db_ex in parsed:
        try:
            if freq in ("52", "W"):
                pay = _next_biz(db_ex, 1)
                results[sym] = {"pay_date": pay.isoformat(), "estimated": True}
            elif freq == "M":
                pay = _next_biz(db_ex, 2)
                results[sym] = {"pay_date": pay.isoformat(), "estimated": True}
            elif sym in yf_dates:
                results[sym] = {"pay_date": yf_dates[sym].isoformat(), "estimated": False}
            else:
                offset = FREQ_OFFSET.get(freq, 10)
                pay = db_ex + timedelta(days=offset)
                results[sym] = {"pay_date": pay.isoformat(), "estimated": True}
        except Exception:
            results[sym] = {"pay_date": None, "estimated": True}

    return jsonify(results)



# ── Sector ETF strip (dashboard AJAX) ────────────────────────────────────────

@app.route("/sector_etfs/data")
def sector_etfs_data():
    """AJAX: return price, 1-day change, and 5-indicator signal for the 11 XL* sector ETFs."""
    from flask import jsonify
    import yfinance as yf
    import warnings
    warnings.filterwarnings("ignore")

    SECTORS = [
        ("XLB",  "Materials"),
        ("XLC",  "Comm. Services"),
        ("XLE",  "Energy"),
        ("XLF",  "Financials"),
        ("XLI",  "Industrials"),
        ("XLK",  "Technology"),
        ("XLP",  "Consumer Staples"),
        ("XLRE", "Real Estate"),
        ("XLU",  "Utilities"),
        ("XLV",  "Health Care"),
        ("XLY",  "Consumer Discret."),
    ]
    tickers = [s[0] for s in SECTORS]
    names   = dict(SECTORS)

    try:
        raw = yf.download(
            " ".join(tickers),
            period="1y",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
        if raw.empty:
            return jsonify(error="No data returned from Yahoo Finance.")

        close_df = raw["Close"]
        high_df  = raw["High"]
        low_df   = raw["Low"]
        empty    = pd.Series([], dtype=float)

        results = []
        for ticker in tickers:
            has_data = (ticker in close_df.columns and
                        ticker in high_df.columns and
                        ticker in low_df.columns)
            if has_data:
                close = close_df[ticker].dropna()
                high  = high_df[ticker].dropna()
                low   = low_df[ticker].dropna()
            else:
                close = high = low = empty

            signal = _vote([
                _ao(high, low)[0],
                _rsi(close)[0],
                _macd(close),
                _sma(close, 50)[0],
                _sma(close, 200)[0],
            ])

            price      = float(close.iloc[-1])  if len(close) >= 1 else None
            prev_price = float(close.iloc[-2])  if len(close) >= 2 else None
            change_pct = round((price - prev_price) / prev_price * 100, 2) \
                         if price is not None and prev_price else None

            results.append({
                "ticker":     ticker,
                "name":       names[ticker],
                "price":      round(price, 2) if price is not None else None,
                "change_pct": change_pct,
                "signal":     signal,
            })

        return jsonify(results=results)

    except Exception:
        import traceback
        return jsonify(error=traceback.format_exc(limit=5))


# ── Shared Indicator Helpers (used by Buy/Sell Signals + Watchlist) ───────────

def _ao(high, low):
    """AO = SMA5(mid) - SMA34(mid). Returns (signal, value, direction)."""
    if len(high) < 34:
        return "NEUTRAL", None, ""
    mid = (high + low) / 2
    ao_v = (mid.rolling(5).mean() - mid.rolling(34).mean()).dropna()
    if len(ao_v) < 2:
        return "NEUTRAL", None, ""
    cur, prv = float(ao_v.iloc[-1]), float(ao_v.iloc[-2])
    direction = "Rising" if cur > prv else ("Falling" if cur < prv else "Flat")
    if cur > 0 and cur > prv:
        return "BUY", cur, direction
    if cur < 0 and cur < prv:
        return "SELL", cur, direction
    return "NEUTRAL", cur, direction


def _rsi(close, period=14):
    """RSI(14). BUY < 30, SELL > 70. Returns (signal, value)."""
    if len(close) < period + 1:
        return "NEUTRAL", None
    delta = close.diff()
    avg_gain = delta.clip(lower=0).ewm(com=period - 1, adjust=False, min_periods=period).mean()
    avg_loss = (-delta).clip(lower=0).ewm(com=period - 1, adjust=False, min_periods=period).mean()
    rs  = avg_gain / avg_loss
    val = float((100 - 100 / (1 + rs)).iloc[-1])
    if pd.isna(val):
        return "NEUTRAL", None
    if val < 30:
        return "BUY", val
    if val > 70:
        return "SELL", val
    return "NEUTRAL", val


def _macd(close):
    """MACD line vs signal line. BUY above, SELL below. Returns signal string."""
    if len(close) < 26:
        return "NEUTRAL"
    macd_line = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    sig_line  = macd_line.ewm(span=9, adjust=False).mean()
    m, s = float(macd_line.iloc[-1]), float(sig_line.iloc[-1])
    if pd.isna(m) or pd.isna(s):
        return "NEUTRAL"
    return "BUY" if m > s else "SELL"


def _sma(close, period):
    """Price vs SMA. BUY >1% above, SELL >1% below, NEUTRAL within 1%.
    Returns (signal, sma_value, pct_diff)."""
    if len(close) < period:
        return "NEUTRAL", None, None
    sma_val = close.rolling(period).mean().iloc[-1]
    price   = close.iloc[-1]
    if pd.isna(sma_val) or pd.isna(price) or float(sma_val) == 0:
        return "NEUTRAL", None, None
    sma_f, price_f = float(sma_val), float(price)
    pct = (price_f - sma_f) / sma_f * 100
    if price_f > sma_f * 1.01:
        return "BUY", sma_f, pct
    if price_f < sma_f * 0.99:
        return "SELL", sma_f, pct
    return "NEUTRAL", sma_f, pct


def _vote(signals):
    """≥3 BUY = BUY, ≥3 SELL = SELL, else NEUTRAL."""
    if signals.count("BUY") >= 3:
        return "BUY"
    if signals.count("SELL") >= 3:
        return "SELL"
    return "NEUTRAL"


def _sharpe(close, risk_free_annual=0.05):
    """Annualized Sharpe ratio from daily close prices. Returns float or None."""
    import numpy as np
    try:
        if len(close) < 30:
            return None
        daily_ret = close.pct_change().dropna()
        if len(daily_ret) < 30:
            return None
        std = float(daily_ret.std())
        if std == 0 or np.isnan(std):
            return None
        daily_rf = risk_free_annual / 252
        excess = float(daily_ret.mean()) - daily_rf
        return round(excess / std * np.sqrt(252), 2)
    except Exception:
        return None


def _sortino(close, risk_free_annual=0.05):
    """Annualized Sortino ratio from daily close prices. Returns float or None."""
    import numpy as np
    try:
        if len(close) < 30:
            return None
        daily_ret = close.pct_change().dropna()
        if len(daily_ret) < 30:
            return None
        daily_rf = risk_free_annual / 252
        neg_ret = daily_ret[daily_ret < 0]
        if len(neg_ret) == 0:
            return None
        down_std = float(neg_ret.std())
        if down_std == 0 or np.isnan(down_std):
            return None
        excess = float(daily_ret.mean()) - daily_rf
        return round(excess / down_std * np.sqrt(252), 2)
    except Exception:
        return None


SIGNAL_COLOR = {"BUY": "#00c853", "SELL": "#d50000", "NEUTRAL": "#f9a825"}
SIGNAL_ORDER = {"BUY": 0, "NEUTRAL": 1, "SELL": 2}


# ── Buy/Sell Signals ─────────────────────────────────────────────────────────

@app.route("/buy_sell_signals")
def buy_sell_signals():
    """Buy/sell signal dashboard — shell page; data loaded via AJAX."""
    return render_template("buy_sell_signals.html")


@app.route("/buy_sell_signals/data")
def buy_sell_signals_data():
    """AJAX: compute 5-indicator majority-vote signals for portfolio + watchlist tickers."""
    from flask import jsonify
    import yfinance as yf
    import warnings
    warnings.filterwarnings("ignore")

    WATCHLIST = [
        # State Street Sector ETFs
        "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
        # Alternatives / Managed Futures
        "HDG", "HFGM", "WTMF", "DBMF", "HFMF", "GMOM", "MFUT", "CFIT", "GAA",
        "RPAR", "IRVH", "WTIP", "RLY", "FTLS", "BTAL", "CSM", "WTLS", "ORR",
        "NLSI", "TAIL", "KMLM", "CTA", "MNA",
    ]
    WATCHLIST_NAMES = {
        # State Street Sector ETFs
        "XLB":  "Materials Select Sector SPDR Fund",
        "XLC":  "Communication Services Select Sector SPDR Fund",
        "XLE":  "Energy Select Sector SPDR Fund",
        "XLF":  "Financial Select Sector SPDR Fund",
        "XLI":  "Industrial Select Sector SPDR Fund",
        "XLK":  "Technology Select Sector SPDR Fund",
        "XLP":  "Consumer Staples Select Sector SPDR Fund",
        "XLRE": "Real Estate Select Sector SPDR Fund",
        "XLU":  "Utilities Select Sector SPDR Fund",
        "XLV":  "Health Care Select Sector SPDR Fund",
        "XLY":  "Consumer Discretionary Select Sector SPDR Fund",
        # Alternatives / Managed Futures
        "HDG":  "ProShares Hedge Replication ETF",
        "HFGM": "Unlimited HFGM Global Macro ETF",
        "WTMF": "WisdomTree Managed Futures Strategy Fund",
        "DBMF": "iMGP DBi Managed Futures Strategy ETF",
        "HFMF": "Unlimited HFMF Managed Futures ETF",
        "GMOM": "Cambria Global Momentum ETF",
        "MFUT": "Cambria Managed Futures Strategy ETF",
        "CFIT": "Cambria Fixed Income Trend ETF",
        "GAA":  "Cambria Global Asset Allocation ETF",
        "RPAR": "RPAR Risk Parity ETF",
        "IRVH": "Global X Interest Rate Volatility & Inflation Hedge ETF",
        "WTIP": "WisdomTree Inflation Plus Fund",
        "RLY":  "SPDR SSgA Multi-Asset Real Return ETF",
        "FTLS": "First Trust Long/Short Equity ETF",
        "BTAL": "AGFiQ US Market Neutral Anti-Beta Fund",
        "CSM":  "ProShares Large Cap Core Plus",
        "WTLS": "WisdomTree Efficient Long/Short U.S. Equity Fund",
        "ORR":  "Militia Long/Short Equity ETF",
        "NLSI": "NEOS Long/Short Equity Income ETF",
        "TAIL": "Cambria Tail Risk ETF",
        "KMLM": "KFA Mount Lucas Index Strategy ETF",
        "CTA":  "Simplify Managed Futures Strategy ETF",
        "MNA":  "IQ Merger Arbitrage ETF",
    }
    WATCHLIST_SIZE = 1000

    def _fmt_pct(v):
        if v is None:
            return "\u2014"
        return f"+{v:.1f}%" if v >= 0 else f"{v:.1f}%"

    def _csig(s):
        """Wrap a signal string in a color-coded HTML span for Plotly hover."""
        c = "#00c853" if s == "BUY" else ("#d50000" if s == "SELL" else "#f9a825")
        return f'<span style="color:{c};font-weight:bold">{s}</span>'

    # ── DB query ──────────────────────────────────────────────────────────────

    conn = get_connection()
    try:
        port = pd.read_sql("""
            SELECT ticker, description, classification_type, purchase_value
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
            ORDER BY ticker
        """, conn)
    except Exception:
        port = pd.DataFrame(columns=["ticker", "description", "classification_type", "purchase_value"])
    conn.close()

    port["description"]        = port["description"].fillna("")
    port["classification_type"] = port["classification_type"].fillna("")
    port_sizes = dict(zip(port["ticker"].tolist(), port["purchase_value"].tolist()))
    port_desc  = dict(zip(port["ticker"].tolist(), port["description"].tolist()))
    port_type  = dict(zip(port["ticker"].tolist(), port["classification_type"].tolist()))
    all_tickers = sorted(set(port["ticker"].tolist() + WATCHLIST))

    error    = None
    fig_json = None
    table_rows = []
    counts   = {"BUY": 0, "SELL": 0, "NEUTRAL": 0}

    try:
        raw = yf.download(
            " ".join(all_tickers),
            period="1y",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )

        if raw.empty:
            error = "No price data returned from Yahoo Finance."
        else:
            try:
                high_df  = raw["High"]
                low_df   = raw["Low"]
                close_df = raw["Close"]
            except KeyError:
                high_df = low_df = close_df = None

            if any(df is None for df in [high_df, low_df, close_df]):
                error = "Missing OHLC price data from Yahoo Finance."
            else:
                SECTOR_TICKERS = {
                    "XLB", "XLC", "XLE", "XLF", "XLI",
                    "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
                }

                # Treemap lists — pre-seed the sector group parent node
                labels      = ["State Street Sectors"]
                parents     = [""]
                values      = [0]
                colors      = ["#1a1a3a"]
                hover_texts = [""]
                empty = pd.Series([], dtype=float)

                for ticker in all_tickers:
                    has_data = (ticker in close_df.columns and
                                ticker in high_df.columns and
                                ticker in low_df.columns)
                    if has_data:
                        close = close_df[ticker].dropna()
                        high  = high_df[ticker].dropna()
                        low   = low_df[ticker].dropna()
                    else:
                        close = high = low = empty

                    # Calculate all 5 indicators
                    ao_sig,  ao_val,    ao_dir          = _ao(high, low)
                    rsi_sig, rsi_val                    = _rsi(close)
                    macd_sig                            = _macd(close)
                    sma50_sig,  sma50_v,  sma50_pct     = _sma(close, 50)
                    sma200_sig, sma200_v, sma200_pct    = _sma(close, 200)

                    # Risk-adjusted return ratios
                    sharpe_val  = _sharpe(close)
                    sortino_val = _sortino(close)

                    # Majority vote across all 5
                    signal = _vote([ao_sig, rsi_sig, macd_sig, sma50_sig, sma200_sig])
                    counts[signal] += 1

                    is_portfolio = ticker in port_sizes
                    size = port_sizes.get(ticker, WATCHLIST_SIZE)

                    ao_val_str  = f"{ao_val:.4f}" if ao_val is not None else "\u2014"
                    rsi_val_str = f"{rsi_val:.1f}" if rsi_val is not None else "\u2014"
                    hover_text = (
                        f"<b>Overall: {_csig(signal)}</b><br>"
                        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500<br>"
                        f"AO:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{_csig(ao_sig)}&nbsp;&nbsp;({ao_val_str}, {ao_dir or '\u2014'})<br>"
                        f"RSI:&nbsp;&nbsp;&nbsp;&nbsp;{_csig(rsi_sig)}&nbsp;&nbsp;({rsi_val_str})<br>"
                        f"MACD:&nbsp;&nbsp;&nbsp;{_csig(macd_sig)}<br>"
                        f"SMA&nbsp;50:&nbsp;{_csig(sma50_sig)}&nbsp;&nbsp;({_fmt_pct(sma50_pct)})<br>"
                        f"SMA&nbsp;200:{_csig(sma200_sig)}&nbsp;&nbsp;({_fmt_pct(sma200_pct)})"
                    )

                    is_sector = ticker in SECTOR_TICKERS and not is_portfolio
                    if is_portfolio:
                        labels.append(ticker)
                        parents.append("")
                        values.append(float(size))
                        colors.append(SIGNAL_COLOR[signal])
                        hover_texts.append(hover_text)
                    elif is_sector:
                        labels.append(ticker)
                        parents.append("State Street Sectors")
                        values.append(WATCHLIST_SIZE)
                        colors.append(SIGNAL_COLOR[signal])
                        hover_texts.append(hover_text)

                    table_rows.append({
                        "ticker":       ticker,
                        "desc":         port_desc.get(ticker) or WATCHLIST_NAMES.get(ticker, ""),
                        "ctype":        port_type.get(ticker, ""),
                        "source":       "Portfolio" if is_portfolio else ("Sectors" if is_sector else "Watchlist"),
                        # Overall
                        "signal":       signal,
                        "sig_order":    SIGNAL_ORDER[signal],
                        # AO
                        "ao_sig":       ao_sig,
                        "ao_sig_ord":   SIGNAL_ORDER[ao_sig],
                        "ao_value":     f"{ao_val:.4f}" if ao_val is not None else "\u2014",
                        "ao_val_num":   ao_val if ao_val is not None else "",
                        "ao_dir":       ao_dir,
                        # RSI
                        "rsi_sig":      rsi_sig,
                        "rsi_sig_ord":  SIGNAL_ORDER[rsi_sig],
                        "rsi_value":    f"{rsi_val:.1f}" if rsi_val is not None else "\u2014",
                        "rsi_val_num":  rsi_val if rsi_val is not None else "",
                        # MACD
                        "macd_sig":     macd_sig,
                        "macd_sig_ord": SIGNAL_ORDER[macd_sig],
                        # SMA50
                        "sma50_sig":    sma50_sig,
                        "sma50_sig_ord": SIGNAL_ORDER[sma50_sig],
                        "sma50_pct":    _fmt_pct(sma50_pct),
                        "sma50_pct_num": sma50_pct if sma50_pct is not None else "",
                        # SMA200
                        "sma200_sig":   sma200_sig,
                        "sma200_sig_ord": SIGNAL_ORDER[sma200_sig],
                        "sma200_pct":   _fmt_pct(sma200_pct),
                        "sma200_pct_num": sma200_pct if sma200_pct is not None else "",
                        # Sharpe / Sortino
                        "sharpe_val":     f"{sharpe_val:.2f}" if sharpe_val is not None else "\u2014",
                        "sharpe_val_num": sharpe_val if sharpe_val is not None else "",
                        "sortino_val":    f"{sortino_val:.2f}" if sortino_val is not None else "\u2014",
                        "sortino_val_num": sortino_val if sortino_val is not None else "",
                        # Portfolio
                        "pv_fmt":       f"${size:,.2f}" if is_portfolio else "\u2014",
                        "pv_num":       float(size) if is_portfolio else 0,
                        "src_order":    0 if is_portfolio else (1 if is_sector else 2),
                    })

                # Default sort: Portfolio group first, then by signal, then by value descending
                table_rows.sort(key=lambda r: (r["src_order"], r["sig_order"], -r["pv_num"]))

                fig = go.Figure(go.Treemap(
                    labels=labels,
                    parents=parents,
                    values=values,
                    text=hover_texts,
                    marker=dict(colors=colors),
                    textinfo="label",
                    hovertemplate="<b>%{label}</b><br>%{text}<extra></extra>",
                ))
                fig.update_layout(
                    title="Buy / Sell Signal Dashboard — 5-Indicator Majority Vote",
                    template="plotly_dark",
                    margin=dict(t=50, l=5, r=5, b=5),
                    height=720,
                    hoverlabel=dict(
                        bgcolor="#111124",
                        bordercolor="#3a3a5c",
                        font=dict(color="#e0e0e0", size=13),
                    ),
                )
                fig_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    except Exception:
        import traceback
        error = traceback.format_exc(limit=5)

    # Scrub NaN/NaT from response (pandas NaN → invalid JSON)
    import math
    def _scrub_bss(obj):
        if isinstance(obj, list):
            return [_scrub_bss(v) for v in obj]
        if isinstance(obj, dict):
            return {k: _scrub_bss(v) for k, v in obj.items()}
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if obj is pd.NaT:
            return None
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return obj

    table_rows = _scrub_bss(table_rows)

    return jsonify(fig_json=fig_json, error=error, table_rows=table_rows, counts=counts)


# ── NAV Erosion Back-Tester ───────────────────────────────────────────────────

@app.route("/nav_erosion")
def nav_erosion():
    """NAV erosion back-tester — shell page; data loaded via AJAX."""
    return render_template("nav_erosion.html")


@app.route("/nav_erosion/data")
def nav_erosion_data():
    """AJAX: back-test a ticker for NAV erosion over a custom date range."""
    from flask import jsonify
    import yfinance as yf
    import warnings
    warnings.filterwarnings("ignore")

    sym = request.args.get("ticker", "").strip().upper()
    try:
        initial_investment = float(request.args.get("amount", 0))
    except (TypeError, ValueError):
        initial_investment = 0.0
    start_date = request.args.get("start", "")
    end_date   = request.args.get("end", "")
    try:
        reinvest_pct = float(request.args.get("reinvest", 0))
    except (TypeError, ValueError):
        reinvest_pct = 0.0

    if not sym:
        return jsonify(error="Please enter a ticker symbol.")
    if initial_investment <= 0:
        return jsonify(error="Initial investment must be greater than zero.")
    if not start_date or not end_date:
        return jsonify(error="Please select both start and end dates.")

    error    = None
    warning  = None
    fig_json = None
    rows     = []
    summary  = {}

    try:
        from datetime import datetime as _dt
        hist = yf.Ticker(sym).history(
            start=start_date, end=end_date,
            interval="1d", auto_adjust=False, actions=True,
        )

        if hist.empty:
            return jsonify(error=f"No data found for ticker {sym}. Check the symbol and date range.")

        # Detect if data starts later than requested (ETF didn't exist yet)
        requested_start = _dt.strptime(start_date, "%Y-%m-%d").date()
        actual_start    = hist.index[0].date()
        if (actual_start - requested_start).days > 30:
            warning = (
                f"{sym} only has data going back to {actual_start.strftime('%B %d, %Y')}. "
                f"Results are shown from that date — your requested start "
                f"({requested_start.strftime('%B %d, %Y')}) predates when this ETF existed."
            )

        monthly_close = hist["Close"].resample("ME").last()
        monthly_divs  = hist["Dividends"].resample("ME").sum()

        df = pd.DataFrame({"price": monthly_close, "div": monthly_divs}).dropna(subset=["price"])
        df["div"] = df["div"].fillna(0.0)

        if df.empty:
            return jsonify(error=f"No usable monthly data for {sym}.")

        initial_price = float(df["price"].iloc[0])
        if initial_price == 0:
            return jsonify(error=f"Initial price for {sym} is zero — cannot calculate.")

        current_shares           = initial_investment / initial_price
        cumulative_dist          = 0.0
        cumulative_reinvested    = 0.0
        cumulative_shares_bought = 0.0

        for dt, row in df.iterrows():
            price         = float(row["price"])
            div_per_share = float(row["div"])
            total_dist    = div_per_share * current_shares
            reinvest_amt  = total_dist * reinvest_pct / 100.0
            shares_bought = (reinvest_amt / price) if price > 0 else 0.0
            current_shares += shares_bought
            portfolio_val  = current_shares * price
            breakeven_sh   = (initial_investment / price) if price > 0 else 0.0
            shares_deficit = breakeven_sh - current_shares
            price_delta_pct = (price - initial_price) / initial_price * 100 if initial_price else 0.0
            cumulative_dist          += total_dist
            cumulative_reinvested    += reinvest_amt
            cumulative_shares_bought += shares_bought

            rows.append({
                "date":            dt.strftime("%b %Y"),
                "price":           round(price, 4),
                "price_delta_pct": round(price_delta_pct, 2),
                "div_per_share":   round(div_per_share, 4),
                "total_dist":      round(total_dist, 2),
                "reinvested":      round(reinvest_amt, 2),
                "shares_bought":   round(shares_bought, 4),
                "total_shares":    round(current_shares, 4),
                "portfolio_val":   round(portfolio_val, 2),
                "breakeven_sh":    round(breakeven_sh, 4),
                "shares_deficit":  round(shares_deficit, 4),
            })

        final_row = rows[-1]
        summary = {
            "total_dist":          round(cumulative_dist, 2),
            "total_shares_bought": round(cumulative_shares_bought, 4),
            "total_reinvested":    round(cumulative_reinvested, 2),
            "final_value":      final_row["portfolio_val"],
            "price_chg_pct":    final_row["price_delta_pct"],
            "has_erosion":      final_row["shares_deficit"] > 0,
            "final_deficit":    final_row["shares_deficit"],
        }

        dates_list     = [r["date"]          for r in rows]
        prices_list    = [r["price"]          for r in rows]
        vals_list      = [r["portfolio_val"]  for r in rows]
        breakeven_list = [initial_investment] * len(rows)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates_list, y=prices_list,
            name="Share Price",
            line=dict(color="#7ecfff", width=2),
            yaxis="y1",
            hovertemplate="<b>%{x}</b><br>Price: $%{y:.2f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=dates_list, y=vals_list,
            name="Portfolio Value",
            line=dict(color="#00e89a", width=2),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Portfolio Value: $%{y:,.2f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=dates_list, y=breakeven_list,
            name="Initial Investment",
            line=dict(color="#888", width=1.5, dash="dash"),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Break-Even: $%{y:,.2f}<extra></extra>",
        ))
        fig.update_layout(
            title=f"{sym} — NAV Erosion Back-Test ({rows[0]['date']} → {rows[-1]['date']})",
            template="plotly_dark",
            margin=dict(t=50, l=60, r=60, b=50),
            height=420,
            legend=dict(orientation="h", y=1.08, x=0),
            hoverlabel=dict(
                bgcolor="#111124",
                bordercolor="#3a3a5c",
                font=dict(color="#e0e0e0", size=13),
            ),
            yaxis=dict(title="Share Price ($)", tickprefix="$", side="left"),
            yaxis2=dict(
                title="Portfolio Value ($)", tickprefix="$",
                overlaying="y", side="right", showgrid=False,
            ),
            hovermode="x unified",
        )
        fig_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    except Exception:
        import traceback
        error = traceback.format_exc(limit=5)

    return jsonify(fig_json=fig_json, error=error, warning=warning, rows=rows, summary=summary)


@app.route("/nav_erosion_portfolio")
def nav_erosion_portfolio():
    """NAV Erosion Portfolio Screener — shell page; data loaded via AJAX."""
    return render_template("nav_erosion_portfolio.html")


@app.route("/nav_erosion_portfolio/list", methods=["GET", "POST"])
def nav_erosion_portfolio_list():
    from flask import jsonify
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()

    if request.method == "GET":
        cursor.execute(
            "SELECT ticker, amount, reinvest_pct FROM dbo.nav_erosion_portfolio_list ORDER BY sort_order"
        )
        rows = [{"ticker": r[0], "amount": r[1], "reinvest_pct": r[2]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(rows=rows)

    # POST — replace list
    data = request.get_json(force=True, silent=True) or {}
    rows = data.get("rows", [])

    if len(rows) > 150:
        conn.close()
        return jsonify(error="Maximum 150 ETFs allowed.")

    validated = []
    for i, r in enumerate(rows):
        ticker = str(r.get("ticker", "")).strip().upper()
        if not ticker:
            conn.close()
            return jsonify(error=f"Row {i+1}: ticker is required.")
        try:
            amount = float(r.get("amount", 0))
        except (TypeError, ValueError):
            conn.close()
            return jsonify(error=f"Row {i+1}: invalid amount.")
        if amount <= 0:
            conn.close()
            return jsonify(error=f"Row {i+1}: amount must be greater than 0.")
        try:
            reinvest_pct = float(r.get("reinvest_pct", 0))
        except (TypeError, ValueError):
            conn.close()
            return jsonify(error=f"Row {i+1}: invalid reinvest %.")
        if reinvest_pct < 0 or reinvest_pct > 100:
            conn.close()
            return jsonify(error=f"Row {i+1}: reinvest % must be 0–100.")
        validated.append((ticker, amount, reinvest_pct, i))

    cursor.execute("DELETE FROM dbo.nav_erosion_portfolio_list")
    for ticker, amount, reinvest_pct, sort_order in validated:
        cursor.execute(
            "INSERT INTO dbo.nav_erosion_portfolio_list (ticker, amount, reinvest_pct, sort_order) "
            "VALUES (?, ?, ?, ?)",
            ticker, amount, reinvest_pct, sort_order,
        )
    conn.commit()
    conn.close()
    return jsonify(ok=True)


@app.route("/nav_erosion_portfolio/saved", methods=["GET", "POST"])
def nav_erosion_portfolio_saved():
    from flask import jsonify
    import json as _json
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()

    if request.method == "GET":
        cursor.execute(
            "SELECT id, name, created_at, start_date, end_date "
            "FROM dbo.nav_erosion_saved_backtests ORDER BY created_at DESC"
        )
        saved = [
            {
                "id":         r[0],
                "name":       r[1],
                "created_at": r[2].strftime("%Y-%m-%d %H:%M") if r[2] else "",
                "start_date": r[3],
                "end_date":   r[4],
            }
            for r in cursor.fetchall()
        ]
        conn.close()
        return jsonify(saved=saved)

    # POST — save a new named backtest
    data = request.get_json(force=True, silent=True) or {}
    name = str(data.get("name", "")).strip()
    if not name:
        conn.close()
        return jsonify(error="Name is required.")
    if len(name) > 200:
        conn.close()
        return jsonify(error="Name must be 200 characters or less.")
    rows_input = data.get("rows", [])
    start = str(data.get("start", ""))
    end   = str(data.get("end", ""))
    rows_json = _json.dumps(rows_input)
    cursor.execute(
        "INSERT INTO dbo.nav_erosion_saved_backtests (name, start_date, end_date, rows_json) "
        "OUTPUT INSERTED.id "
        "VALUES (?, ?, ?, ?)",
        name, start, end, rows_json,
    )
    new_id = int(cursor.fetchone()[0])
    conn.commit()
    conn.close()
    return jsonify(ok=True, id=new_id)


@app.route("/nav_erosion_portfolio/saved/<int:saved_id>", methods=["GET", "PUT", "DELETE"])
def nav_erosion_portfolio_saved_item(saved_id):
    from flask import jsonify
    import json as _json
    conn = get_connection()
    cursor = conn.cursor()

    if request.method == "DELETE":
        cursor.execute("DELETE FROM dbo.nav_erosion_saved_backtests WHERE id = ?", saved_id)
        conn.commit()
        conn.close()
        return jsonify(ok=True)

    if request.method == "PUT":
        data = request.get_json(force=True, silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            conn.close()
            return jsonify(error="Name is required.")
        if len(name) > 200:
            conn.close()
            return jsonify(error="Name must be 200 characters or less.")
        rows_input = data.get("rows", [])
        start = str(data.get("start", ""))
        end   = str(data.get("end", ""))
        rows_json = _json.dumps(rows_input)
        cursor.execute(
            "UPDATE dbo.nav_erosion_saved_backtests "
            "SET name=?, start_date=?, end_date=?, rows_json=?, created_at=GETDATE() "
            "WHERE id=?",
            name, start, end, rows_json, saved_id,
        )
        conn.commit()
        conn.close()
        return jsonify(ok=True)

    # GET — load one saved backtest
    cursor.execute(
        "SELECT name, start_date, end_date, rows_json "
        "FROM dbo.nav_erosion_saved_backtests WHERE id = ?",
        saved_id,
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return jsonify(error="Not found."), 404
    return jsonify(
        name=row[0],
        start=row[1],
        end=row[2],
        rows=_json.loads(row[3]),
    )


@app.route("/portfolio_income_sim")
def portfolio_income_sim():
    """Portfolio Income Simulator — shell page; data loaded via AJAX."""
    return render_template("portfolio_income_sim.html")


@app.route("/portfolio_income_sim/portfolio_tickers")
def portfolio_income_sim_portfolio_tickers():
    """Return all live portfolio tickers for the picker modal."""
    from flask import jsonify
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, description, classification_type,
               purchase_value, reinvest, current_annual_yield
        FROM dbo.all_account_info
        WHERE current_price IS NOT NULL
        ORDER BY ticker
    """)
    tickers = [
        {
            "ticker":        r[0],
            "description":   r[1] or "",
            "type":          r[2] or "",
            "amount":        round(float(r[3] or 0), 2),
            "drip":          (r[4] or "").upper() == "Y",
            "current_yield": round(float(r[5] or 0), 2),
        }
        for r in cursor.fetchall()
    ]
    conn.close()
    return jsonify(tickers=tickers)


@app.route("/portfolio_income_sim/list", methods=["GET", "POST"])
def portfolio_income_sim_list():
    from flask import jsonify
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()

    if request.method == "GET":
        cursor.execute(
            "SELECT ticker, amount, reinvest_pct, yield_override "
            "FROM dbo.portfolio_income_sim_list ORDER BY sort_order"
        )
        rows = [
            {"ticker": r[0], "amount": r[1], "reinvest_pct": r[2], "yield_override": r[3]}
            for r in cursor.fetchall()
        ]
        conn.close()
        return jsonify(rows=rows)

    # POST — replace list
    data = request.get_json(force=True, silent=True) or {}
    rows = data.get("rows", [])

    if len(rows) > 150:
        conn.close()
        return jsonify(error="Maximum 150 ETFs allowed.")

    validated = []
    for i, r in enumerate(rows):
        ticker = str(r.get("ticker", "")).strip().upper()
        if not ticker:
            conn.close()
            return jsonify(error=f"Row {i+1}: ticker is required.")
        try:
            amount = float(r.get("amount", 0))
        except (TypeError, ValueError):
            conn.close()
            return jsonify(error=f"Row {i+1}: invalid amount.")
        if amount <= 0:
            conn.close()
            return jsonify(error=f"Row {i+1}: amount must be greater than 0.")
        try:
            reinvest_pct = float(r.get("reinvest_pct", 0))
        except (TypeError, ValueError):
            conn.close()
            return jsonify(error=f"Row {i+1}: invalid reinvest %.")
        if reinvest_pct < 0 or reinvest_pct > 100:
            conn.close()
            return jsonify(error=f"Row {i+1}: reinvest % must be 0–100.")
        yield_override = r.get("yield_override")
        if yield_override is not None:
            try:
                yield_override = float(yield_override)
            except (TypeError, ValueError):
                yield_override = None
            if yield_override is not None and (yield_override < 0 or yield_override > 100):
                conn.close()
                return jsonify(error=f"Row {i+1}: yield override must be 0–100.")
        validated.append((ticker, amount, reinvest_pct, yield_override, i))

    cursor.execute("DELETE FROM dbo.portfolio_income_sim_list")
    for ticker, amount, reinvest_pct, yield_override, sort_order in validated:
        cursor.execute(
            "INSERT INTO dbo.portfolio_income_sim_list (ticker, amount, reinvest_pct, yield_override, sort_order) "
            "VALUES (?, ?, ?, ?, ?)",
            ticker, amount, reinvest_pct, yield_override, sort_order,
        )
    conn.commit()
    conn.close()
    return jsonify(ok=True)


@app.route("/portfolio_income_sim/saved", methods=["GET", "POST"])
def portfolio_income_sim_saved():
    from flask import jsonify
    import json as _json
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()

    if request.method == "GET":
        cursor.execute(
            "SELECT id, name, created_at, mode, start_date, end_date, market_type, duration_months "
            "FROM dbo.portfolio_income_sim_saved ORDER BY created_at DESC"
        )
        saved = [
            {
                "id":              r[0],
                "name":            r[1],
                "created_at":      r[2].strftime("%Y-%m-%d %H:%M") if r[2] else "",
                "mode":            r[3],
                "start_date":      r[4],
                "end_date":        r[5],
                "market_type":     r[6],
                "duration_months": r[7],
            }
            for r in cursor.fetchall()
        ]
        conn.close()
        return jsonify(saved=saved)

    # POST — save a new named simulation
    try:
        data = request.get_json(force=True, silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            conn.close()
            return jsonify(error="Name is required.")
        if len(name) > 200:
            conn.close()
            return jsonify(error="Name must be 200 characters or less.")
        rows_input      = data.get("rows", [])
        mode            = str(data.get("mode", "historical"))
        start           = str(data.get("start", ""))
        end             = str(data.get("end", ""))
        market_type     = str(data.get("market_type", ""))
        duration_months = data.get("duration_months")
        if duration_months is not None:
            try:
                duration_months = int(duration_months)
            except (TypeError, ValueError):
                duration_months = None
        rows_json = _json.dumps(rows_input)
        cursor.execute(
            "INSERT INTO dbo.portfolio_income_sim_saved "
            "(name, mode, start_date, end_date, market_type, duration_months, rows_json) "
            "OUTPUT INSERTED.id "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            name, mode, start, end, market_type, duration_months, rows_json,
        )
        new_id = int(cursor.fetchone()[0])
        conn.commit()
        conn.close()
        return jsonify(ok=True, id=new_id)
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return jsonify(error=str(e)), 500


@app.route("/portfolio_income_sim/saved/<int:saved_id>", methods=["GET", "PUT", "DELETE"])
def portfolio_income_sim_saved_item(saved_id):
    from flask import jsonify
    import json as _json
    conn = get_connection()
    cursor = conn.cursor()

    if request.method == "DELETE":
        cursor.execute("DELETE FROM dbo.portfolio_income_sim_saved WHERE id = ?", saved_id)
        conn.commit()
        conn.close()
        return jsonify(ok=True)

    if request.method == "PUT":
        try:
            data = request.get_json(force=True, silent=True) or {}
            name = str(data.get("name", "")).strip()
            if not name:
                conn.close()
                return jsonify(error="Name is required.")
            if len(name) > 200:
                conn.close()
                return jsonify(error="Name must be 200 characters or less.")
            rows_input      = data.get("rows", [])
            mode            = str(data.get("mode", "historical"))
            start           = str(data.get("start", ""))
            end             = str(data.get("end", ""))
            market_type     = str(data.get("market_type", ""))
            duration_months = data.get("duration_months")
            if duration_months is not None:
                try:
                    duration_months = int(duration_months)
                except (TypeError, ValueError):
                    duration_months = None
            rows_json = _json.dumps(rows_input)
            cursor.execute(
                "UPDATE dbo.portfolio_income_sim_saved "
                "SET name=?, mode=?, start_date=?, end_date=?, market_type=?, "
                "    duration_months=?, rows_json=?, created_at=GETDATE() "
                "WHERE id=?",
                name, mode, start, end, market_type, duration_months, rows_json, saved_id,
            )
            conn.commit()
            conn.close()
            return jsonify(ok=True)
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            return jsonify(error=str(e)), 500

    # GET — load one saved simulation
    cursor.execute(
        "SELECT name, mode, start_date, end_date, market_type, duration_months, rows_json "
        "FROM dbo.portfolio_income_sim_saved WHERE id = ?",
        saved_id,
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return jsonify(error="Not found."), 404
    return jsonify(
        name=row[0],
        mode=row[1],
        start=row[2],
        end=row[3],
        market_type=row[4],
        duration_months=row[5],
        rows=_json.loads(row[6]),
    )


@app.route("/portfolio_income_sim/run", methods=["POST"])
def portfolio_income_sim_run():
    from flask import jsonify
    import yfinance as yf
    import numpy as np
    import warnings
    import traceback as _tb
    warnings.filterwarnings("ignore")

    try:
        return _portfolio_income_sim_run_inner()
    except Exception as _e:
        return jsonify(error=f"Server error: {str(_e)}", detail=_tb.format_exc())


def _portfolio_income_sim_run_inner():
    from flask import jsonify
    import yfinance as yf
    import numpy as np
    import warnings
    warnings.filterwarnings("ignore")

    data        = request.get_json(force=True, silent=True) or {}
    mode        = data.get("mode", "historical")
    rows_input  = data.get("rows", [])

    if not rows_input:
        return jsonify(error="No ETFs provided.")
    if len(rows_input) > 150:
        return jsonify(error="Maximum 150 ETFs allowed.")

    # Validate rows
    validated = []
    for i, r in enumerate(rows_input):
        ticker = str(r.get("ticker", "")).strip().upper()
        if not ticker:
            return jsonify(error=f"Row {i+1}: ticker is required.")
        try:
            amount = float(r.get("amount", 0))
        except (TypeError, ValueError):
            return jsonify(error=f"Row {i+1}: invalid amount.")
        if amount <= 0:
            return jsonify(error=f"Row {i+1}: amount must be greater than 0.")
        try:
            reinvest_pct = float(r.get("reinvest_pct", 0))
        except (TypeError, ValueError):
            return jsonify(error=f"Row {i+1}: invalid reinvest %.")
        if reinvest_pct < 0 or reinvest_pct > 100:
            return jsonify(error=f"Row {i+1}: reinvest % must be 0–100.")
        yield_override = r.get("yield_override")
        if yield_override is not None and yield_override != "":
            try:
                yield_override = float(yield_override)
                if yield_override <= 0:
                    yield_override = None
            except (TypeError, ValueError):
                yield_override = None
        else:
            yield_override = None
        validated.append({
            "ticker": ticker, "amount": amount,
            "reinvest_pct": reinvest_pct, "yield_override": yield_override,
        })

    unique_tickers = list(dict.fromkeys(r["ticker"] for r in validated))

    # ── HISTORICAL MODE ──────────────────────────────────────────────────────
    if mode == "historical":
        start_date = data.get("start", "")
        end_date   = data.get("end", "")
        if not start_date or not end_date:
            return jsonify(error="Please select both start and end dates.")

        try:
            from datetime import datetime as _dt
            raw = yf.download(
                unique_tickers,
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=False,
                actions=True,
                group_by="ticker",
                progress=False,
            )
        except Exception as e:
            return jsonify(error=f"Failed to fetch data: {str(e)}")

        requested_start = _dt.strptime(start_date, "%Y-%m-%d").date()

        def get_ticker_df(sym):
            try:
                if len(unique_tickers) == 1:
                    close = raw["Close"]
                    divs  = raw["Dividends"] if "Dividends" in raw.columns else pd.Series(0.0, index=raw.index)
                else:
                    if sym not in raw.columns.get_level_values(0):
                        return None, None
                    close = raw[sym]["Close"]
                    divs  = raw[sym]["Dividends"] if "Dividends" in raw[sym].columns else pd.Series(0.0, index=raw[sym].index)
            except Exception:
                return None, None
            return close, divs

        results = []
        for r in validated:
            sym          = r["ticker"]
            amount       = r["amount"]
            reinvest_pct = r["reinvest_pct"]

            close, divs = get_ticker_df(sym)

            if close is None or close.dropna().empty:
                results.append({"ticker": sym, "amount": amount, "reinvest_pct": reinvest_pct,
                                 "error": f"No data found for {sym}.", "monthly_prices": [], "monthly_portfolio_vals": []})
                continue

            monthly_close = close.resample("ME").last()
            monthly_divs  = divs.resample("ME").sum()
            df = pd.DataFrame({"price": monthly_close, "div": monthly_divs}).dropna(subset=["price"])
            df["div"] = df["div"].fillna(0.0)

            if df.empty:
                results.append({"ticker": sym, "amount": amount, "reinvest_pct": reinvest_pct,
                                 "error": f"No usable monthly data for {sym}.", "monthly_prices": [], "monthly_portfolio_vals": []})
                continue

            actual_start = df.index[0].date()
            warning = None
            if (actual_start - requested_start).days > 30:
                warning = (
                    f"{sym} only has data going back to {actual_start.strftime('%B %d, %Y')}. "
                    f"Results start from that date."
                )

            initial_price = float(df["price"].iloc[0])
            if initial_price == 0:
                results.append({"ticker": sym, "amount": amount, "reinvest_pct": reinvest_pct,
                                 "error": f"Initial price for {sym} is zero.", "monthly_prices": [], "monthly_portfolio_vals": []})
                continue

            current_shares   = amount / initial_price
            cumul_dist       = 0.0
            cumul_reinvested = 0.0
            monthly_prices        = []
            monthly_portfolio_vals = []

            for dt, row in df.iterrows():
                price         = float(row["price"])
                div_per_share = float(row["div"])
                total_dist_m  = div_per_share * current_shares
                reinvest_amt  = total_dist_m * reinvest_pct / 100.0
                shares_bought = (reinvest_amt / price) if price > 0 else 0.0
                current_shares   += shares_bought
                cumul_dist       += total_dist_m
                cumul_reinvested += reinvest_amt
                monthly_prices.append(round(price, 4))
                monthly_portfolio_vals.append(round(current_shares * price, 2))

            final_price     = float(df["price"].iloc[-1])
            portfolio_val   = current_shares * final_price
            breakeven_final = (amount / final_price) if final_price > 0 else 0.0
            final_deficit   = breakeven_final - current_shares
            price_delta_pct = (final_price - initial_price) / initial_price * 100 if initial_price else 0.0
            gain_loss_dollar = portfolio_val - amount
            gain_loss_pct    = gain_loss_dollar / amount * 100 if amount else 0.0
            effective_yield_pct = cumul_dist / amount * 100 if amount else 0.0

            # Date labels for x-axis
            date_labels = [dt.strftime("%Y-%m") for dt in df.index]

            results.append({
                "ticker":               sym,
                "amount":               round(amount, 2),
                "reinvest_pct":         round(reinvest_pct, 1),
                "start_price":          round(initial_price, 4),
                "end_price":            round(final_price, 4),
                "price_delta_pct":      round(price_delta_pct, 2),
                "total_dist":           round(cumul_dist, 2),
                "total_reinvested":     round(cumul_reinvested, 2),
                "final_value":          round(portfolio_val, 2),
                "gain_loss_dollar":     round(gain_loss_dollar, 2),
                "gain_loss_pct":        round(gain_loss_pct, 2),
                "effective_yield_pct":  round(effective_yield_pct, 2),
                "ttm_yield_pct":        None,
                "has_erosion":          final_deficit > 0,
                "final_deficit":        round(final_deficit, 4),
                "monthly_prices":       monthly_prices,
                "monthly_portfolio_vals": monthly_portfolio_vals,
                "date_labels":          date_labels,
                "warning":              warning,
                "error":                None,
            })

        return jsonify(results=results)

    # ── SIMULATION MODE ──────────────────────────────────────────────────────
    if mode == "simulate":
        market_type     = data.get("market_type", "neutral")
        duration_months = int(data.get("duration_months", 36))
        if duration_months < 1 or duration_months > 600:
            return jsonify(error="Duration must be between 1 and 600 months.")

        bias_map     = {"bullish": +0.010, "bearish": -0.015, "neutral": 0.0}
        vol_mult_map = {"bullish": 0.9,    "bearish": 1.2,    "neutral": 1.0}
        bias     = bias_map.get(market_type, 0.0)
        vol_mult = vol_mult_map.get(market_type, 1.0)

        results = []
        for r in validated:
            sym          = r["ticker"]
            amount       = r["amount"]
            reinvest_pct = r["reinvest_pct"]
            yo           = r["yield_override"]

            # Fetch per-ticker — more reliable than batch group_by download
            try:
                hist = yf.Ticker(sym).history(period="1y", auto_adjust=False, actions=True)
            except Exception as e:
                results.append({"ticker": sym, "amount": amount, "reinvest_pct": reinvest_pct,
                                 "error": f"Failed to fetch data for {sym}: {str(e)}",
                                 "monthly_prices": [], "monthly_portfolio_vals": []})
                continue

            if hist is None or hist.empty:
                results.append({"ticker": sym, "amount": amount, "reinvest_pct": reinvest_pct,
                                 "error": f"No data found for {sym}. Check the ticker symbol.",
                                 "monthly_prices": [], "monthly_portfolio_vals": []})
                continue

            close = hist["Close"]
            divs  = hist["Dividends"] if "Dividends" in hist.columns else pd.Series(0.0, index=hist.index)

            if close.dropna().empty:
                results.append({"ticker": sym, "amount": amount, "reinvest_pct": reinvest_pct,
                                 "error": f"No data found for {sym}. Check the ticker symbol.",
                                 "monthly_prices": [], "monthly_portfolio_vals": []})
                continue

            current_price = float(close.dropna().iloc[-1])
            if current_price <= 0:
                results.append({"ticker": sym, "amount": amount, "reinvest_pct": reinvest_pct,
                                 "error": f"Could not determine current price for {sym}.", "monthly_prices": [], "monthly_portfolio_vals": []})
                continue

            # TTM yield
            if yo is not None:
                ttm_yield = yo / 100.0
            else:
                ttm_divs_sum = float(divs.sum()) if divs is not None else 0.0
                ttm_yield = ttm_divs_sum / current_price if current_price > 0 else 0.0

            # Historical monthly volatility
            monthly_returns = close.dropna().resample("ME").last().pct_change().dropna()
            if len(monthly_returns) >= 2:
                hist_sigma = float(monthly_returns.std())
            else:
                hist_sigma = 0.05  # fallback 5%

            mu    = bias
            sigma = hist_sigma * vol_mult

            # Cap monthly sigma at 25% (~87% annualised).
            # Penny stocks / low-liquidity tickers can have 50-100%+ monthly σ from
            # historical data; uncapped, that produces nonsensical single-path outcomes.
            SIGMA_CAP = 0.25
            sigma_capped = sigma > SIGMA_CAP
            sigma = min(sigma, SIGMA_CAP)

            # Monte-Carlo GBM — run N_PATHS and take the median price path.
            # A single random path always has outlier tickers; the median of many
            # paths converges to exp(drift × n) × price, which is deterministic and
            # realistic: bearish high-vol stocks reliably decline, neutral stays flat.
            N_PATHS = 300
            drift   = mu - 0.5 * sigma ** 2
            np.random.seed(None)
            # Z shape: (N_PATHS, duration_months)
            Z            = np.random.normal(0.0, 1.0, (N_PATHS, duration_months))
            log_rets     = drift + sigma * Z                        # (N_PATHS, months)
            # prepend zeros so index 0 = starting price
            cum_log      = np.cumsum(
                               np.hstack([np.zeros((N_PATHS, 1)), log_rets]), axis=1
                           )                                        # (N_PATHS, months+1)
            # Floor = 0.01% of starting price (avoids division-by-zero without
            # distorting sub-penny stocks where an absolute $0.01 floor would be
            # orders of magnitude above the actual price).
            price_floor  = max(current_price * 0.0001, 1e-10)
            price_matrix = np.maximum(current_price * np.exp(cum_log), price_floor)
            # median across all paths at each time step
            prices = [float(v) for v in np.median(price_matrix, axis=0)]  # length months+1

            # Simulate month-by-month
            initial_shares   = amount / current_price
            current_shares   = initial_shares
            cumul_dist       = 0.0
            cumul_reinvested = 0.0
            monthly_prices        = []
            monthly_portfolio_vals = []

            for price in prices[1:]:
                pct_chg = (price - current_price) / current_price * 100
                if pct_chg >= 10:
                    factor = min(1.0 + (pct_chg - 10) * 0.02, 1.30)
                elif pct_chg >= -10:
                    factor = 1.0
                elif pct_chg >= -20:
                    factor = 0.85
                elif pct_chg >= -30:
                    factor = 0.70
                else:
                    factor = max(0.40, 1.0 + pct_chg * 0.02)

                dist_per_share   = (ttm_yield / 12) * price * factor
                total_dist_m     = dist_per_share * current_shares
                reinvest_amt     = total_dist_m * (reinvest_pct / 100)
                shares_bought    = reinvest_amt / price if price > 0 else 0.0
                current_shares  += shares_bought
                cumul_dist       += total_dist_m
                cumul_reinvested += reinvest_amt
                monthly_prices.append(round(price, 4))
                monthly_portfolio_vals.append(round(current_shares * price, 2))

            final_price      = prices[-1]
            final_value      = current_shares * final_price
            breakeven_final  = amount / final_price if final_price else 0.0
            final_deficit    = breakeven_final - current_shares
            gain_loss_dollar = final_value - amount
            gain_loss_pct    = gain_loss_dollar / amount * 100 if amount else 0.0
            eff_yield_pct    = cumul_dist / amount * 100 if amount else 0.0
            price_delta_pct  = (final_price - current_price) / current_price * 100 if current_price else 0.0

            date_labels = [f"Month {i+1}" for i in range(duration_months)]

            results.append({
                "ticker":               sym,
                "amount":               round(amount, 2),
                "reinvest_pct":         round(reinvest_pct, 1),
                "start_price":          round(current_price, 4),
                "end_price":            round(final_price, 4),
                "price_delta_pct":      round(price_delta_pct, 2),
                "total_dist":           round(cumul_dist, 2),
                "total_reinvested":     round(cumul_reinvested, 2),
                "final_value":          round(final_value, 2),
                "gain_loss_dollar":     round(gain_loss_dollar, 2),
                "gain_loss_pct":        round(gain_loss_pct, 2),
                "effective_yield_pct":  round(eff_yield_pct, 2),
                "ttm_yield_pct":        round(ttm_yield * 100, 2),
                "has_erosion":          bool(final_deficit > 0),
                "final_deficit":        round(final_deficit, 4),
                "monthly_prices":       monthly_prices,
                "monthly_portfolio_vals": monthly_portfolio_vals,
                "date_labels":          date_labels,
                "warning":              (
                    f"{sym} has extreme historical volatility; monthly sigma was capped "
                    f"at 25% (raw: {hist_sigma * vol_mult * 100:.0f}%) to prevent "
                    f"unrealistic simulation outcomes."
                ) if sigma_capped else None,
                "error":                None,
            })

        return jsonify(results=results)

    return jsonify(error=f"Unknown mode: {mode}")


@app.route("/nav_erosion_portfolio/data", methods=["POST"])
def nav_erosion_portfolio_data():
    from flask import jsonify
    import yfinance as yf
    import warnings
    warnings.filterwarnings("ignore")

    data = request.get_json(force=True, silent=True) or {}
    start_date = data.get("start", "")
    end_date   = data.get("end", "")
    rows_input = data.get("rows", [])

    if not start_date or not end_date:
        return jsonify(error="Please select both start and end dates.")
    if not rows_input:
        return jsonify(error="No ETFs provided.")
    if len(rows_input) > 150:
        return jsonify(error="Maximum 150 ETFs allowed.")

    # Validate rows and collect unique tickers
    validated = []
    for i, r in enumerate(rows_input):
        ticker = str(r.get("ticker", "")).strip().upper()
        if not ticker:
            return jsonify(error=f"Row {i+1}: ticker is required.")
        try:
            amount = float(r.get("amount", 0))
        except (TypeError, ValueError):
            return jsonify(error=f"Row {i+1}: invalid amount.")
        if amount <= 0:
            return jsonify(error=f"Row {i+1}: amount must be greater than 0.")
        try:
            reinvest_pct = float(r.get("reinvest_pct", 0))
        except (TypeError, ValueError):
            return jsonify(error=f"Row {i+1}: invalid reinvest %.")
        if reinvest_pct < 0 or reinvest_pct > 100:
            return jsonify(error=f"Row {i+1}: reinvest % must be 0–100.")
        validated.append({"ticker": ticker, "amount": amount, "reinvest_pct": reinvest_pct})

    unique_tickers = list(dict.fromkeys(r["ticker"] for r in validated))

    # Batch download all tickers
    try:
        from datetime import datetime as _dt
        raw = yf.download(
            unique_tickers,
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
            actions=True,
            group_by="ticker",
            progress=False,
        )
    except Exception as e:
        return jsonify(error=f"Failed to fetch data: {str(e)}")

    requested_start = _dt.strptime(start_date, "%Y-%m-%d").date()

    def get_ticker_df(sym):
        """Extract Close + Dividends for one ticker from the batch download."""
        try:
            if len(unique_tickers) == 1:
                close = raw["Close"]
                divs  = raw["Dividends"] if "Dividends" in raw.columns else pd.Series(0.0, index=raw.index)
            else:
                if sym not in raw.columns.get_level_values(0):
                    return None, None
                close = raw[sym]["Close"]
                divs  = raw[sym]["Dividends"] if "Dividends" in raw[sym].columns else pd.Series(0.0, index=raw[sym].index)
        except Exception:
            return None, None
        return close, divs

    results = []
    for r in validated:
        sym         = r["ticker"]
        amount      = r["amount"]
        reinvest_pct = r["reinvest_pct"]

        close, divs = get_ticker_df(sym)

        if close is None or close.dropna().empty:
            results.append({
                "ticker":        sym,
                "amount":        amount,
                "reinvest_pct":  reinvest_pct,
                "error":         f"No data found for {sym}.",
            })
            continue

        monthly_close = close.resample("ME").last()
        monthly_divs  = divs.resample("ME").sum()

        df = pd.DataFrame({"price": monthly_close, "div": monthly_divs}).dropna(subset=["price"])
        df["div"] = df["div"].fillna(0.0)

        if df.empty:
            results.append({
                "ticker":       sym,
                "amount":       amount,
                "reinvest_pct": reinvest_pct,
                "error":        f"No usable monthly data for {sym}.",
            })
            continue

        # Detect truncated start
        actual_start = df.index[0].date()
        warning = None
        if (actual_start - requested_start).days > 30:
            warning = (
                f"{sym} only has data going back to {actual_start.strftime('%B %d, %Y')}. "
                f"Results start from that date."
            )

        initial_price = float(df["price"].iloc[0])
        if initial_price == 0:
            results.append({
                "ticker":       sym,
                "amount":       amount,
                "reinvest_pct": reinvest_pct,
                "error":        f"Initial price for {sym} is zero.",
            })
            continue

        current_shares        = amount / initial_price
        cumul_dist            = 0.0
        cumul_reinvested      = 0.0
        has_erosion_at_end    = False
        final_deficit         = 0.0

        for dt, row in df.iterrows():
            price         = float(row["price"])
            div_per_share = float(row["div"])
            total_dist    = div_per_share * current_shares
            reinvest_amt  = total_dist * reinvest_pct / 100.0
            shares_bought = (reinvest_amt / price) if price > 0 else 0.0
            current_shares += shares_bought
            breakeven_sh   = (amount / price) if price > 0 else 0.0
            shares_deficit = breakeven_sh - current_shares
            cumul_dist       += total_dist
            cumul_reinvested += reinvest_amt

        final_price        = float(df["price"].iloc[-1])
        portfolio_val      = current_shares * final_price
        breakeven_final    = (amount / final_price) if final_price > 0 else 0.0
        final_deficit      = breakeven_final - current_shares
        has_erosion_at_end = final_deficit > 0
        price_delta_pct    = (final_price - initial_price) / initial_price * 100 if initial_price else 0.0
        gain_loss_dollar   = portfolio_val - amount
        gain_loss_pct      = gain_loss_dollar / amount * 100 if amount else 0.0

        results.append({
            "ticker":          sym,
            "amount":          round(amount, 2),
            "reinvest_pct":    round(reinvest_pct, 1),
            "start_price":     round(initial_price, 4),
            "end_price":       round(final_price, 4),
            "price_delta_pct": round(price_delta_pct, 2),
            "total_dist":      round(cumul_dist, 2),
            "total_reinvested":round(cumul_reinvested, 2),
            "final_value":     round(portfolio_val, 2),
            "gain_loss_dollar":round(gain_loss_dollar, 2),
            "gain_loss_pct":   round(gain_loss_pct, 2),
            "has_erosion":     has_erosion_at_end,
            "final_deficit":   round(final_deficit, 4),
            "warning":         warning,
            "error":           None,
        })

    return jsonify(results=results)


# ── Watchlist ─────────────────────────────────────────────────────────────────

@app.route("/watchlist")
def watchlist():
    """Watchlist — shell page; data loaded via AJAX."""
    return render_template("watchlist.html")


@app.route("/watchlist/watching", methods=["GET", "POST"])
def watchlist_watching_list():
    """GET: return watching rows.  POST: bulk-replace watching list."""
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()

    if request.method == "POST":
        data = request.get_json(force=True)
        rows = data.get("rows", [])
        cursor.execute("DELETE FROM dbo.watchlist_watching")
        for i, r in enumerate(rows):
            ticker = str(r.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            notes = str(r.get("notes", ""))[:500]
            cursor.execute(
                "INSERT INTO dbo.watchlist_watching (ticker, notes, sort_order) VALUES (?, ?, ?)",
                (ticker, notes, i),
            )
        conn.commit()
        conn.close()
        return jsonify(ok=True)

    # GET
    rows = pd.read_sql(
        "SELECT ticker, notes, added_date FROM dbo.watchlist_watching ORDER BY sort_order, id",
        conn,
    )
    conn.close()
    rows["added_date"] = rows["added_date"].astype(str)
    rows["notes"] = rows["notes"].fillna("")
    return jsonify(rows=rows.to_dict(orient="records"))


@app.route("/watchlist/sold", methods=["GET", "POST"])
def watchlist_sold_list():
    """GET: return sold rows.  POST: add or delete a sold position."""
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()

    if request.method == "POST":
        data = request.get_json(force=True)
        action = data.get("action", "add")

        if action == "delete":
            row_id = data.get("id")
            if row_id is not None:
                cursor.execute("DELETE FROM dbo.watchlist_sold WHERE id = ?", (int(row_id),))
            conn.commit()
            conn.close()
            return jsonify(ok=True)

        # action == "add"
        ticker = str(data.get("ticker", "")).strip().upper()
        if not ticker:
            conn.close()
            return jsonify(ok=False, error="Ticker is required"), 400
        cursor.execute(
            """INSERT INTO dbo.watchlist_sold
               (ticker, buy_price, sell_price, shares_sold, sell_date, divs_received, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                ticker,
                data.get("buy_price"),
                data.get("sell_price"),
                data.get("shares_sold"),
                data.get("sell_date") or None,
                data.get("divs_received"),
                str(data.get("notes", ""))[:500],
            ),
        )
        conn.commit()
        conn.close()
        return jsonify(ok=True)

    # GET
    rows = pd.read_sql(
        """SELECT id, ticker, buy_price, sell_price, shares_sold, sell_date,
                  divs_received, notes, added_date
           FROM dbo.watchlist_sold ORDER BY sort_order, id""",
        conn,
    )
    conn.close()
    rows["notes"] = rows["notes"].fillna("")
    records = rows.to_dict(orient="records")
    import math
    for rec in records:
        for k, v in rec.items():
            if v is pd.NaT or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                rec[k] = None
            elif hasattr(v, 'isoformat'):      # datetime → string
                rec[k] = v.isoformat()
            elif isinstance(v, str) and v in ("NaT", "nan", "None", "NaN"):
                rec[k] = None
    return jsonify(rows=records)


@app.route("/watchlist/sold/<int:row_id>", methods=["PUT"])
def watchlist_sold_edit(row_id):
    """Update a single sold position by id."""
    data = request.get_json(force=True)
    conn = get_connection()
    cursor = conn.cursor()
    fields = []
    values = []
    for col in ["ticker", "buy_price", "sell_price", "shares_sold", "sell_date", "divs_received", "notes"]:
        if col in data:
            fields.append(f"{col} = ?")
            val = data[col]
            if col == "ticker":
                val = str(val).strip().upper()
            elif col == "notes":
                val = str(val)[:500]
            elif col == "sell_date":
                val = val or None
            values.append(val)
    if fields:
        values.append(row_id)
        cursor.execute(
            f"UPDATE dbo.watchlist_sold SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
    conn.close()
    return jsonify(ok=True)


@app.route("/watchlist/data")
def watchlist_data():
    """AJAX: analysis data for all watchlist tickers (watching + sold)."""
    import yfinance as yf
    import warnings
    warnings.filterwarnings("ignore")

    conn = get_connection()
    ensure_tables_exist(conn)
    try:
        watching = pd.read_sql(
            "SELECT ticker, notes FROM dbo.watchlist_watching ORDER BY sort_order, id", conn
        )
    except Exception:
        watching = pd.DataFrame(columns=["ticker", "notes"])
    try:
        sold = pd.read_sql(
            """SELECT id, ticker, buy_price, sell_price, shares_sold, divs_received, notes
               FROM dbo.watchlist_sold ORDER BY sort_order, id""",
            conn,
        )
    except Exception:
        sold = pd.DataFrame(columns=["id", "ticker", "buy_price", "sell_price",
                                      "shares_sold", "divs_received", "notes"])
    conn.close()

    watching_tickers = watching["ticker"].tolist() if len(watching) else []
    sold_tickers = sold["ticker"].tolist() if len(sold) else []
    all_tickers = sorted(set(watching_tickers + sold_tickers))

    if not all_tickers:
        return jsonify(watching=[], sold=[], counts={"BUY": 0, "SELL": 0, "NEUTRAL": 0})

    error = None
    watching_rows = []
    sold_rows = []
    ticker_info = {}
    counts = {"BUY": 0, "SELL": 0, "NEUTRAL": 0}

    try:
        raw = yf.download(
            " ".join(all_tickers),
            period="1y",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )

        if raw.empty:
            return jsonify(watching=[], sold=[], counts=counts, error="No price data returned.")

        close_df = raw["Close"]
        high_df = raw["High"]
        low_df = raw["Low"]

        empty = pd.Series([], dtype=float)

        # Pre-compute per-ticker data
        ticker_info = {}
        for ticker in all_tickers:
            has_data = (ticker in close_df.columns and
                        ticker in high_df.columns and
                        ticker in low_df.columns)
            if has_data:
                close = close_df[ticker].dropna()
                high = high_df[ticker].dropna()
                low = low_df[ticker].dropna()
            else:
                close = high = low = empty

            ao_sig, ao_val, ao_dir = _ao(high, low)
            rsi_sig, rsi_val = _rsi(close)
            macd_sig = _macd(close)
            sma50_sig, sma50_v, sma50_pct = _sma(close, 50)
            sma200_sig, sma200_v, sma200_pct = _sma(close, 200)
            signal = _vote([ao_sig, rsi_sig, macd_sig, sma50_sig, sma200_sig])

            # Risk-adjusted return ratios
            sharpe_val  = _sharpe(close)
            sortino_val = _sortino(close)

            # Current price + 1-day change
            price = float(close.iloc[-1]) if len(close) >= 1 else None
            prev_price = float(close.iloc[-2]) if len(close) >= 2 else None
            change_1d = round((price - prev_price) / prev_price * 100, 2) \
                if price is not None and prev_price else None

            # Dividend yield (TTM)
            div_yield = None
            try:
                info = yf.Ticker(ticker).info
                raw_yield = info.get("dividendYield")
                if raw_yield is not None:
                    # yfinance may return as fraction (0.0351) or already as percent (3.51)
                    div_yield = round(raw_yield if raw_yield > 1 else raw_yield * 100, 2)
            except Exception:
                pass

            # 1-year total return %
            one_yr_ret = None
            if len(close) >= 2:
                first_price = float(close.iloc[0])
                if first_price > 0:
                    one_yr_ret = round((price - first_price) / first_price * 100, 2)

            # NAV erosion flag: 1y price < -5% while paying dividends
            nav_erosion = False
            if one_yr_ret is not None and one_yr_ret < -5 and div_yield is not None and div_yield > 0:
                nav_erosion = True

            ticker_info[ticker] = {
                "price": round(price, 2) if price is not None else None,
                "change_1d": change_1d,
                "div_yield": div_yield,
                "signal": signal,
                "ao_sig": ao_sig,
                "ao_val": round(ao_val, 4) if ao_val is not None else None,
                "ao_dir": ao_dir,
                "rsi_sig": rsi_sig,
                "rsi_val": round(rsi_val, 1) if rsi_val is not None else None,
                "macd_sig": macd_sig,
                "sma50_sig": sma50_sig,
                "sma50_pct": round(sma50_pct, 1) if sma50_pct is not None else None,
                "sma200_sig": sma200_sig,
                "sma200_pct": round(sma200_pct, 1) if sma200_pct is not None else None,
                "one_yr_ret": one_yr_ret,
                "nav_erosion": nav_erosion,
                "sharpe": round(sharpe_val, 2) if sharpe_val is not None else None,
                "sortino": round(sortino_val, 2) if sortino_val is not None else None,
            }

        # Build watching rows
        for _, wr in watching.iterrows():
            t = wr["ticker"]
            info = ticker_info.get(t, {})
            watching_rows.append({
                "ticker": t,
                "notes": wr.get("notes", ""),
                **info,
            })

        # Build sold rows — _safe() converts pandas NaN → None for JSON safety
        import math
        def _safe(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v

        for _, sr in sold.iterrows():
            t = sr["ticker"]
            info = ticker_info.get(t, {})
            cur_price = info.get("price")
            sell_price = _safe(sr.get("sell_price"))
            price_since_sold = None
            if cur_price is not None and sell_price is not None and sell_price > 0:
                price_since_sold = round((cur_price - sell_price) / sell_price * 100, 2)
            sold_rows.append({
                "id": int(sr["id"]),
                "ticker": t,
                "buy_price": _safe(sr.get("buy_price")),
                "sell_price": sell_price,
                "shares_sold": _safe(sr.get("shares_sold")),
                "divs_received": _safe(sr.get("divs_received")),
                "notes": sr.get("notes", "") or "",
                "cur_price": cur_price,
                "price_since_sold": price_since_sold,
                **{k: info.get(k) for k in ["signal", "ao_sig", "ao_val", "ao_dir",
                                              "rsi_sig", "rsi_val", "macd_sig",
                                              "sma50_sig", "sma50_pct", "sma200_sig",
                                              "sma200_pct", "one_yr_ret", "div_yield",
                                              "change_1d", "nav_erosion",
                                              "sharpe", "sortino"]},
            })

    except Exception:
        import traceback
        error = traceback.format_exc(limit=5)

    # Scrub NaN/NaT from all response data (pandas NaN → invalid JSON)
    import math
    def _scrub(obj):
        if isinstance(obj, list):
            return [_scrub(v) for v in obj]
        if isinstance(obj, dict):
            return {k: _scrub(v) for k, v in obj.items()}
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if obj is pd.NaT:
            return None
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return obj

    watching_rows = _scrub(watching_rows)
    sold_rows = _scrub(sold_rows)
    ticker_info_clean = _scrub(ticker_info)

    # Recount from finalized watching_rows to guarantee counts match visible table
    counts = {"BUY": 0, "SELL": 0, "NEUTRAL": 0}
    for row in watching_rows:
        sig = row.get("signal", "NEUTRAL")
        counts[sig] = counts.get(sig, 0) + 1

    return jsonify(watching=watching_rows, sold=sold_rows, counts=counts,
                   ticker_info=ticker_info_clean, error=error)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
