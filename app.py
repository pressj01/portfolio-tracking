from flask import Flask, render_template, redirect, url_for, flash, request, jsonify, session, Response
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
    import_monthly_payout_tickers, ensure_tables_exist, import_from_upload,
)
from normalize import (
    populate_holdings,
    populate_dividends,
    populate_income_tracking,
    populate_pillar_weights,
)

app = Flask(__name__)
app.secret_key = "portfolio-tracking-secret-key"


# ── Multi-profile helpers ─────────────────────────────────────────────────────

def get_profile_id():
    """Return the active profile_id from the session (default: 1 = Owner)."""
    return int(session.get('profile_id', 1))


@app.context_processor
def inject_profile():
    """Inject active_profile_id, active_profile_name, and all_profiles into every template."""
    pid = get_profile_id()
    conn = get_connection()
    try:
        row = pd.read_sql(
            "SELECT name FROM dbo.profiles WHERE id = ?", conn, params=[pid]
        )
        pname = row.iloc[0]['name'] if not row.empty else 'Owner'
        all_profiles_df = pd.read_sql(
            "SELECT id, name FROM dbo.profiles ORDER BY id", conn
        )
        profiles_list = all_profiles_df.to_dict('records')
    except Exception:
        pname, profiles_list = 'Owner', [{'id': 1, 'name': 'Owner'}]
    finally:
        conn.close()
    return {
        'active_profile_id':   pid,
        'active_profile_name': pname,
        'all_profiles':        profiles_list,
    }


PILLAR_COLS = [
    "hedged_anchor", "anchor", "gold_silver", "booster", "juicer", "bdc", "growth",
]

SWAP_CANDIDATES = [
    # Monthly covered-call income
    "JEPI", "JEPQ", "XYLD", "QYLD", "RYLD", "DIVO", "SVOL",
    # Daily/weekly options income
    "XDTE", "QDTE", "RDTE",
    # Dividend growth
    "SCHD", "DGRO", "VYM", "DVY", "HDV",
    # BDCs
    "ARCC", "MAIN", "HTGC", "OBDC", "BXSL", "GBDC",
    # CEFs
    "PDI", "PTY", "GOF", "UTF", "ERC",
    # High yield / bonds
    "HYG", "JNK", "BKLN", "ANGL",
    # Real estate / infrastructure
    "O", "STAG", "VICI", "AMT",
    # Preferred / hybrid
    "PFF", "PFFD",
    # Broad income
    "NUSI", "XYLG", "QYLG",
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
        pid = get_profile_id()
        df = pd.read_sql("""
            SELECT ticker, description, ex_div_date, div, div_frequency
            FROM dbo.all_account_info
            WHERE ex_div_date IS NOT NULL
              AND ex_div_date NOT IN ('', '--')
              AND current_price IS NOT NULL
              AND ISNULL(quantity, 0) > 0
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[pid])
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

        # ── Project stored ex-div date forward to next upcoming occurrence ─
        # Non-Owner profiles store the most-recent past ex-div date from
        # yfinance dividend history. Advance it forward so the calendar stays
        # current. Owner (pid==1) dates are kept up-to-date by Excel import
        # and must NOT be projected (that would hide recent "Past 30 Days" events).
        today_d = datetime.today().date()
        threshold = today_d - timedelta(days=1)
        if pid != 1 and dt < threshold and freq:
            def _add_months(d, n):
                m = d.month + n
                y = d.year + (m - 1) // 12
                m = (m - 1) % 12 + 1
                day = min(d.day, _cal.monthrange(y, m)[1])
                return d.replace(year=y, month=m, day=day)

            _period = {
                "M":  lambda d: _add_months(d, 1),
                "Q":  lambda d: _add_months(d, 3),
                "SA": lambda d: _add_months(d, 6),
                "A":  lambda d: _add_months(d, 12),
                "W":  lambda d: d + timedelta(weeks=1),
                "52": lambda d: d + timedelta(weeks=1),
            }.get(freq)
            if _period:
                while dt < threshold:
                    dt = _period(dt)

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

    pid = get_profile_id()
    try:
        df = pd.read_sql("""
            SELECT * FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[pid])
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
        total_return_pct    = round(total_return_dollar / total_purchase * 100, 2) if total_purchase else 0

        port_stats = {
            "yield_on_cost":       round(total_annual / total_purchase * 100, 2) if total_purchase else 0,
            "current_yield":       round(total_annual / total_current  * 100, 2) if total_current  else 0,
            "dollars_per_hour":    round(total_annual / 2080, 4)                 if total_annual   else 0,
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
    pid = get_profile_id()
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
        row_count, message = func(profile_id=pid)
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
        _da_pid = get_profile_id()
        cur_actual = pd.read_sql(
            "SELECT amount FROM dbo.monthly_payouts WHERE year=? AND month=? AND profile_id=?",
            conn, params=[month_start.year, month_start.month, _da_pid]
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
              AND profile_id = ?
            ORDER BY year, month
        """, conn, params=[win_start_key, win_end_key, get_profile_id()])
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
    pid = get_profile_id()
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
              AND profile_id = ?
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn, params=[pid])
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
    pid = get_profile_id()
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
              AND profile_id = ?
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn, params=[pid])
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
    pid = get_profile_id()

    charts = {}

    try:
        df = pd.read_sql("""
            SELECT ticker, description, classification_type,
                   ytd_divs, total_divs_received, paid_for_itself,
                   purchase_value, estim_payment_per_year
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND profile_id = ?
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn, params=[pid])

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
    pid = get_profile_id()

    weekly = pd.read_sql("""
        SELECT id, pay_date, week_of_month, amount,
               SUM(amount) OVER (ORDER BY pay_date) AS running_total
        FROM dbo.weekly_payouts
        WHERE profile_id = ?
        ORDER BY pay_date
    """, conn, params=[pid])

    monthly = pd.read_sql("""
        SELECT id, year, month, amount,
               SUM(amount) OVER (ORDER BY year, month) AS running_total
        FROM dbo.monthly_payouts
        WHERE profile_id = ?
        ORDER BY year, month
    """, conn, params=[pid])

    # Per-ticker detail for weekly payers
    weekly_tickers = pd.read_sql("""
        SELECT ticker, shares, distribution, total_dividend
        FROM dbo.weekly_payout_tickers
        WHERE profile_id = ?
        ORDER BY ticker
    """, conn, params=[pid])

    # Per-ticker detail for monthly payers
    monthly_tickers = pd.read_sql("""
        SELECT ticker, pay_month
        FROM dbo.monthly_payout_tickers
        WHERE profile_id = ?
        ORDER BY ticker, pay_month
    """, conn, params=[pid])

    # Per-ticker total divs received from main table — all real holdings
    try:
        ticker_divs = pd.read_sql("""
            SELECT ticker, description, ytd_divs, total_divs_received, paid_for_itself
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND profile_id = ?
            ORDER BY ISNULL(total_divs_received, 0) DESC, ticker
        """, conn, params=[pid])
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
    pid = get_profile_id()
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO dbo.weekly_payouts (pay_date, week_of_month, amount, profile_id) VALUES (?, ?, ?, ?)",
            pay_date, int(week) if week else None, float(amount), pid,
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
    pid = get_profile_id()
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO dbo.monthly_payouts (year, month, amount, profile_id) VALUES (?, ?, ?, ?)",
            int(year), int(month), float(amount), pid,
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
    pid = get_profile_id()
    chart_data = {}

    try:
        df = pd.read_sql("""
            SELECT import_date,
                   SUM(approx_monthly_income) AS total_monthly_income,
                   SUM(estim_payment_per_year) AS total_annual_income,
                   SUM(ytd_divs) AS total_ytd_divs,
                   SUM(total_divs_received) AS total_divs_received
            FROM dbo.income_tracking
            WHERE profile_id = ?
            GROUP BY import_date
            ORDER BY import_date
        """, conn, params=[pid])

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
            FROM dbo.weekly_payouts
            WHERE profile_id = ?
            ORDER BY pay_date
        """, conn, params=[pid])
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
            FROM dbo.monthly_payouts
            WHERE profile_id = ?
            ORDER BY year, month
        """, conn, params=[pid])
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
    pid = get_profile_id()

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
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[pid])
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

    pid = get_profile_id()
    conn = get_connection()
    try:
        port = pd.read_sql("""
            SELECT ticker, description, classification_type, purchase_value
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[pid])
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
    pid = get_profile_id()
    try:
        tickers_df = pd.read_sql("""
            SELECT ticker, description, classification_type,
                   price_paid, current_price, quantity,
                   purchase_value, current_value, gain_or_loss,
                   total_divs_received, annual_yield_on_cost
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[pid])
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

    from datetime import date as date_type
    return render_template("single_etf_return.html", portfolio=tickers_df,
                           current_year=date_type.today().year)


@app.route("/single_etf_return/data")
def single_etf_data():
    """AJAX endpoint — unlimited tickers via t1 + extra=SYM1,SYM2,...
    mode: total | price | both | income
    t1 = portfolio slot, extra = comma-separated comparison tickers.
    Backwards compat: old t2/t3 params still work.
    """
    from flask import jsonify
    import warnings, math
    from datetime import date as date_type
    warnings.filterwarnings("ignore")

    t1     = request.args.get("t1",  "").strip().upper()
    extra  = request.args.get("extra", "").strip()
    # Backwards compat: old t2/t3 params
    t2     = request.args.get("t2",  "").strip().upper()
    t3     = request.args.get("t3",  "").strip().upper()
    if not extra:
        extra = ",".join(s for s in [t2, t3] if s and s != t1)
    period = request.args.get("period", "1y")
    mode   = request.args.get("mode",   "total")   # total | price | both | all3
    reinvest_pct = max(0, min(100, int(request.args.get("reinvest", 100))))
    reinvest_frac = reinvest_pct / 100.0

    # Ordered list of (symbol, slot_key) for each enabled non-empty ticker
    slots = []
    if t1:
        slots.append((t1, "t1"))
    for i, sym in enumerate(tok.strip().upper() for tok in extra.split(",") if tok.strip()):
        if sym and sym != t1 and not any(ex == sym for ex, _ in slots):
            slots.append((sym, f"x{i}"))
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
    }
    # Dynamic calendar year labels
    period_labels.update(
        {str(y): f"Calendar {y}" for y in range(2021, date_type.today().year + 1)}
    )

    # Custom date range support
    custom_start = request.args.get("start", "").strip()
    custom_end   = request.args.get("end",   "").strip()
    requested_start = None  # for fund-existence warnings
    period_label = None

    if custom_start and custom_end:
        try:
            cs = date_type.fromisoformat(custom_start)
            ce = date_type.fromisoformat(custom_end)
            if cs >= ce:
                return jsonify({"error": "Start date must be before end date."}), 400
            span_days = (ce - cs).days
            yf_kwargs   = dict(start=custom_start, end=custom_end)
            yf_interval = "1d" if span_days <= 180 else ("1wk" if span_days <= 730 else "1mo")
            period_label = f"{custom_start} \u2192 {custom_end}"
            requested_start = cs
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    elif period.isdigit() and len(period) == 4:
        yr = int(period)
        today = date_type.today()
        start = f"{yr}-01-01"
        end   = today.strftime("%Y-%m-%d") if yr == today.year else f"{yr}-12-31"
        yf_kwargs   = dict(start=start, end=end)
        yf_interval = "1d" if yr == today.year else "1wk"
        requested_start = date_type.fromisoformat(start)
    else:
        yf_range, yf_interval = period_map.get(period, (dict(period="1y"), "1wk"))
        yf_kwargs = yf_range
        # Approximate requested_start for standard periods
        _approx_days = {"1mo": 30, "3mo": 90, "6mo": 180, "ytd": None,
                        "1y": 365, "2y": 730, "5y": 1825, "max": None}
        ad = _approx_days.get(period)
        if period == "ytd":
            requested_start = date_type(date_type.today().year, 1, 1)
        elif ad:
            from datetime import timedelta
            requested_start = date_type.today() - timedelta(days=ad)

    if period_label is None:
        period_label = period_labels.get(period, period)

    # ── Color / line-style per slot ──────────────────────────────────
    _EXTRA_COLORS = [
        ("#a0f0c0", "#70c090"), ("#FFD700", "#c0a000"), ("#ff7eb3", "#d05a8a"),
        ("#b39ddb", "#8a73b0"), ("#ff8a65", "#c06040"), ("#4dd0e1", "#30a0b0"),
        ("#aed581", "#80a858"), ("#f48fb1", "#c06080"),
    ]
    _DASH_CYCLE = ["solid", "solid", "dash", "dash", "dot", "dot", "dashdot"]

    # Price+Div gets its own color per slot (warm tones, distinct from total/price)
    _PRICEDIV_COLORS = ["#66ddaa", "#e0c050", "#e88acc", "#c0aaee", "#e0a070", "#66c8d8", "#b8d870", "#e8a0c0"]

    SLOT_STYLES = {}
    for _si, (_sym, _sk) in enumerate(slots):
        if _si == 0:
            # First ticker always gets the primary blue style
            SLOT_STYLES[_sk] = dict(color="#7ecfff", price_color="#5ba8d0",
                                    pricediv_color="#66ddaa", line_dash="solid", width=3)
        else:
            _ci = (_si - 1) % len(_EXTRA_COLORS)
            SLOT_STYLES[_sk] = dict(
                color=_EXTRA_COLORS[_ci][0],
                price_color=_EXTRA_COLORS[_ci][1],
                pricediv_color=_PRICEDIV_COLORS[(_si) % len(_PRICEDIV_COLORS)],
                line_dash=_DASH_CYCLE[(_si - 1) % len(_DASH_CYCLE)],
                width=2.5,
            )

    # ── Helpers ──────────────────────────────────────────────────────
    def dl(symbols, adj):
        sym_str = " ".join(symbols) if isinstance(symbols, list) else symbols
        raw = __import__("yfinance").download(
            sym_str, **yf_kwargs, interval=yf_interval,
            progress=False, auto_adjust=adj,
        )
        if raw.empty:
            return pd.DataFrame()
        try:
            c = raw["Close"]
        except KeyError:
            return pd.DataFrame()
        if isinstance(c, pd.Series):
            name = symbols[0] if isinstance(symbols, list) else symbols
            return c.to_frame(name=name)
        return c

    def dl_income(symbols):
        """Download with actions=True to get Close + Dividends (unadjusted).
        Always uses daily interval so every individual dividend payment is
        captured for accurate reinvestment simulation."""
        sym_str = " ".join(symbols) if isinstance(symbols, list) else symbols
        raw = __import__("yfinance").download(
            sym_str, **yf_kwargs, interval="1d",
            progress=False, auto_adjust=False, actions=True,
        )
        if raw.empty:
            return pd.DataFrame(), pd.DataFrame()
        try:
            close = raw["Close"]
        except KeyError:
            close = pd.DataFrame()
        try:
            divs = raw["Dividends"]
        except KeyError:
            divs = pd.DataFrame()
        # Normalize single-ticker Series to DataFrame
        name = symbols[0] if isinstance(symbols, list) else symbols
        if isinstance(close, pd.Series):
            close = close.to_frame(name=name)
        if isinstance(divs, pd.Series):
            divs = divs.to_frame(name=name)
        return close, divs

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

    def blend_price_drip(close_series, divs_series, frac, track_cash=True):
        """Simulate partial reinvestment.  frac=0 → price+cash divs,
        frac=1 → full DRIP.  In between: frac of each div buys new
        shares (which also appreciate), (1-frac) kept as cash.
        track_cash=True: total wealth (shares*price + cash from un-reinvested divs)
        track_cash=False: invested value only (shares*price, un-reinvested divs withdrawn)
        Returns normalized series (100 = start)."""
        if close_series is None or len(close_series.dropna()) < 2:
            return None
        close = close_series.dropna()
        if divs_series is not None:
            divs = divs_series.reindex(close.index, fill_value=0.0)
        else:
            divs = pd.Series(0.0, index=close.index)
        shares = 1.0
        cash_divs = 0.0
        vals = []
        p0 = close.iloc[0]
        for i in range(len(close)):
            d = divs.iloc[i]
            if d > 0:
                reinvest_amt = d * shares * frac
                if track_cash:
                    cash_divs += d * shares * (1 - frac)
                shares += reinvest_amt / close.iloc[i]
            vals.append(shares * close.iloc[i] + cash_divs)
        result = pd.Series(vals, index=close.index)
        return ((result / result.iloc[0]) * 100).round(4)

    try:
        import yfinance as yf  # noqa

        # ── DB snapshot helper (portfolio tickers with no Yahoo data) ────────
        _snap_pid = get_profile_id()
        def db_snapshot(sym):
            """Return {price_ret, div_pct, total_ret} from all_account_info, or None."""
            c2 = get_connection()
            try:
                r2 = pd.read_sql("""
                    SELECT purchase_value, gain_or_loss, total_divs_received
                    FROM dbo.all_account_info WHERE ticker = ? AND profile_id = ?
                """, c2, params=[sym, _snap_pid])
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
        # Always download unadjusted close (used for price-only and blend_price_drip)
        close_unadj = dl(all_syms, adj=False)
        # Always download dividends for all modes except price-only
        # (auto_adjust=True is unreliable for many ETFs, so we use
        #  blend_price_drip with real dividend data for all total-return calcs)
        if mode != "price":
            _income_close, _income_divs = dl_income(all_syms)
        else:
            _income_close, _income_divs = pd.DataFrame(), pd.DataFrame()

        fig          = go.Figure()
        stats_list   = []   # per-ticker stat dicts
        any_db       = False

        # Reference date span — from first ticker that has usable time-series data
        ref_dates = None
        for sym, _ in slots:
            for df in [close_unadj, _income_close]:
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
            s_unadj = get_col(close_unadj, sym) if not close_unadj.empty else None
            has_unadj = s_unadj is not None and len(s_unadj) >= 2

            # Determine if we need Yahoo data and whether it's missing
            if not has_unadj:
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

                if mode in ("total", "pricediv", "both", "all3", "all4"):
                    lvl = 100 + snap["total_ret"]
                    _lbl = "Total" if mode != "pricediv" else "Price+Div"
                    fig.add_trace(go.Scatter(
                        x=x_span, y=[lvl, lvl],
                        name=f"{sym} {_lbl} (DB)",
                        mode="lines+markers",
                        line=dict(color=sty["color"], width=2, dash="dashdot"),
                        marker=dict(symbol="diamond", size=8),
                        hovertemplate=(f"<b>{sym} DB snapshot</b>  Total: "
                                       f"{snap['total_ret']:+.2f}%<extra>all-time</extra>"),
                    ))
                if mode in ("price", "both", "all3", "all4"):
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
                    "ret":         snap["total_ret"] if mode not in ("price",) else snap["price_ret"],
                    "price_ret":   snap["price_ret"] if mode in ("both", "all3", "all4") else None,
                    "div_contrib": snap["div_pct"]   if mode in ("pricediv", "both", "all3", "all4") else None,
                    "ann":  None, "mdd": None,
                    "note": "DB snapshot — no Yahoo price history",
                })
                continue

            # ── Yahoo data is available — add traces ─────────────────────────
            if mode == "total":
                s_ic = get_col(_income_close, sym)
                s_id = get_col(_income_divs,  sym)
                n = blend_price_drip(s_ic if s_ic is not None else s_unadj, s_id, reinvest_frac)
                if n is None:
                    continue
                ri_label = f" ({reinvest_pct}%)" if reinvest_pct < 100 else ""
                fig.add_trace(go.Scatter(
                    x=[str(d)[:10] for d in n.index], y=n.tolist(),
                    name=f"{sym}{ri_label}",
                    line=dict(color=sty["color"], width=sty["width"],
                              dash=sty["line_dash"]),
                    hovertemplate=(f"<b>{sym}</b>: %{{y:.2f}}<br>%{{x}}"
                                   f"<extra>Return ({reinvest_pct}%)</extra>"),
                ))
                ret_val = pct_ret(n)
                stats_list.append({
                    "label": sym, "ret": ret_val,
                    "price_ret": None,
                    "div_contrib": None,
                    "ann": ann_ret(n, s_unadj), "mdd": max_dd(n), "note": None,
                })

            elif mode == "pricediv":
                # Price + Dividends (cash, no reinvestment)
                s_close_pd = get_col(_income_close, sym)
                s_divs_pd  = get_col(_income_divs,  sym)
                if s_close_pd is None or len(s_close_pd.dropna()) < 2:
                    # Fall back to unadjusted close if dl_income didn't work
                    s_close_pd = s_unadj
                if s_close_pd is None or len(s_close_pd.dropna()) < 2:
                    continue
                s_close_pd = s_close_pd.dropna()
                if s_divs_pd is not None:
                    s_divs_pd = s_divs_pd.reindex(s_close_pd.index, fill_value=0.0)
                    cumul_divs = s_divs_pd.cumsum()
                else:
                    cumul_divs = pd.Series(0.0, index=s_close_pd.index)
                total_val = s_close_pd + cumul_divs
                n = ((total_val / total_val.iloc[0]) * 100).round(4)
                # Also compute price-only for div contribution stat
                n_price = norm(s_close_pd)
                fig.add_trace(go.Scatter(
                    x=[str(d)[:10] for d in n.index], y=n.tolist(),
                    name=f"{sym} (Price+Div)",
                    line=dict(color=sty["pricediv_color"], width=sty["width"],
                              dash=sty["line_dash"]),
                    hovertemplate=(f"<b>{sym} Price+Div</b>: %{{y:.2f}}%<br>%{{x}}"
                                   f"<extra>Price + Cash Divs</extra>"),
                ))
                ret_val = pct_ret(n)
                pr = pct_ret(n_price)
                stats_list.append({
                    "label": sym, "ret": ret_val,
                    "price_ret": pr,
                    "div_contrib": (round(ret_val - pr, 4)
                                    if ret_val is not None and pr is not None else None),
                    "ann": ann_ret(n, s_close_pd), "mdd": max_dd(s_close_pd),
                    "note": None,
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

            elif mode in ("all3", "all4"):
                # all3: Price Only (dotted) + Custom Blend (dashed) + 100% DRIP (solid)
                # all4: adds Price+Dividends (long dash) between Price and Custom Blend
                n_unadj = norm(s_unadj) if has_unadj else None
                _ic = get_col(_income_close, sym)
                _id = get_col(_income_divs,  sym)

                # 100% DRIP via blend_price_drip (uses real dividend data, not auto_adjust)
                n_drip = blend_price_drip(_ic if _ic is not None else s_unadj, _id, 1.0)

                # 1) Price-only trace (bottom — dotted)
                if n_unadj is not None:
                    fig.add_trace(go.Scatter(
                        x=[str(d)[:10] for d in n_unadj.index], y=n_unadj.tolist(),
                        name=f"{sym} (Price)",
                        line=dict(color=sty["price_color"],
                                  width=max(sty["width"] - 1, 1.5), dash="dot"),
                        hovertemplate=(f"<b>{sym} Price</b>: %{{y:.2f}}%<br>%{{x}}"
                                       f"<extra></extra>"),
                    ))

                # 2) Price + Cash Dividends trace (all4 only — long dash)
                n_pricediv = None
                pricediv_ret = None
                if mode == "all4":
                    s_close_pd = _ic if _ic is not None else s_unadj
                    s_divs_pd  = _id
                    if s_close_pd is not None and len(s_close_pd.dropna()) >= 2:
                        s_close_pd = s_close_pd.dropna()
                        if s_divs_pd is not None:
                            s_divs_pd = s_divs_pd.reindex(s_close_pd.index, fill_value=0.0)
                            cumul_divs = s_divs_pd.cumsum()
                        else:
                            cumul_divs = pd.Series(0.0, index=s_close_pd.index)
                        total_val = s_close_pd + cumul_divs
                        n_pricediv = ((total_val / total_val.iloc[0]) * 100).round(4)
                        pricediv_ret = pct_ret(n_pricediv)
                        fig.add_trace(go.Scatter(
                            x=[str(d)[:10] for d in n_pricediv.index], y=n_pricediv.tolist(),
                            name=f"{sym} (Price+Div)",
                            line=dict(color=sty["pricediv_color"],
                                      width=max(sty["width"] - 0.5, 1.5), dash="longdash"),
                            hovertemplate=(f"<b>{sym} Price+Div</b>: %{{y:.2f}}%<br>%{{x}}"
                                           f"<extra></extra>"),
                        ))

                # 3) Custom blend trace (dashed) — skip if 0% or 100%
                # track_cash=False: shows invested-only value (divs not reinvested
                # are treated as withdrawn), ensuring this line is always between
                # Price Only and 100% DRIP for clear visual separation.
                n_blend = blend_price_drip(_ic if _ic is not None else s_unadj, _id, reinvest_frac, track_cash=False)
                if reinvest_pct > 0 and reinvest_pct < 100 and n_blend is not None:
                    fig.add_trace(go.Scatter(
                        x=[str(d)[:10] for d in n_blend.index], y=n_blend.tolist(),
                        name=f"{sym} ({reinvest_pct}%)",
                        line=dict(color=sty["color"], width=sty["width"], dash="dash"),
                        hovertemplate=(f"<b>{sym} {reinvest_pct}%</b>: %{{y:.2f}}%<br>%{{x}}"
                                       f"<extra></extra>"),
                    ))

                # 4) 100% DRIP reference trace (top — solid)
                if n_drip is not None:
                    fig.add_trace(go.Scatter(
                        x=[str(d)[:10] for d in n_drip.index], y=n_drip.tolist(),
                        name=f"{sym} (100% DRIP)",
                        line=dict(color=sty["color"], width=sty["width"],
                                  dash="solid"),
                        hovertemplate=(f"<b>{sym} 100% DRIP</b>: %{{y:.2f}}%<br>%{{x}}"
                                       f"<extra></extra>"),
                    ))

                tr = pct_ret(n_drip)
                pr = pct_ret(n_unadj)
                custom_ret  = pct_ret(n_blend) if n_blend is not None else None
                drip_div_c  = round(tr - pr, 4) if tr is not None and pr is not None else None
                stat_entry = {
                    "label": sym,
                    "ret":         tr,          # 100% DRIP total
                    "price_ret":   pr,          # price only
                    "div_contrib": drip_div_c,  # DRIP div contribution
                    "custom_ret":  custom_ret,  # custom blend %
                    "ann": ann_ret(n_drip, s_unadj) if n_drip is not None else None,
                    "mdd": max_dd(n_drip) if n_drip is not None else max_dd(s_unadj),
                    "note": None,
                }
                if mode == "all4":
                    stat_entry["pricediv_ret"] = pricediv_ret
                stats_list.append(stat_entry)

            else:  # both — price-only + blended return
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
                # Blended return on top (always use blend_price_drip with real divs)
                _ic = get_col(_income_close, sym)
                _id = get_col(_income_divs,  sym)
                n_blend = blend_price_drip(_ic if _ic is not None else s_unadj, _id, reinvest_frac)
                ri_label = f" ({reinvest_pct}%)" if reinvest_pct < 100 else ""
                if n_blend is not None:
                    fig.add_trace(go.Scatter(
                        x=[str(d)[:10] for d in n_blend.index], y=n_blend.tolist(),
                        name=f"{sym} (Return{ri_label})",
                        line=dict(color=sty["color"], width=sty["width"],
                                  dash=sty["line_dash"]),
                        hovertemplate=(f"<b>{sym} Return</b>: %{{y:.2f}}<br>%{{x}}"
                                       f"<extra>{reinvest_pct}%</extra>"),
                    ))

                tr = pct_ret(n_blend)
                pr = pct_ret(n_unadj)
                stats_list.append({
                    "label": sym, "ret": tr, "price_ret": pr,
                    "div_contrib": (round(tr - pr, 4)
                                    if tr is not None and pr is not None else None),
                    "ann": ann_ret(n_blend, s_unadj) if n_blend is not None else None,
                    "mdd": max_dd(n_blend) if n_blend is not None else max_dd(s_unadj),
                    "note": None,
                })

        # ── Fund existence warnings ──────────────────────────────────────────
        warnings = []
        if requested_start is not None:
            from datetime import timedelta
            tol = timedelta(days=7)
            for sym, _ in slots:
                for df in [close_unadj, _income_close]:
                    if df.empty:
                        continue
                    s = get_col(df, sym)
                    if s is not None and len(s) >= 2:
                        first_date = s.index[0]
                        # Convert to date for comparison
                        fd = first_date.date() if hasattr(first_date, 'date') else first_date
                        if fd > requested_start + tol:
                            warnings.append(
                                f"{sym}: data starts {fd.isoformat()} — "
                                f"requested range begins earlier ({requested_start.isoformat()})"
                            )
                        break

        # ── Pairwise alphas (only when ≤5 tickers) ────────────────────────────
        alphas = []
        alpha_note = None
        rets = [(t["label"], t["ret"])
                for t in stats_list if t.get("ret") is not None]
        if len(rets) > 5:
            alpha_note = "Too many tickers for pairwise comparison"
        else:
            for i in range(len(rets)):
                for j in range(i + 1, len(rets)):
                    a, b = rets[i], rets[j]
                    alphas.append({
                        "label": f"{a[0]} vs {b[0]}",
                        "value": round(a[1] - b[1], 4),
                    })

        # ── Chart layout ─────────────────────────────────────────────────────
        sym_list    = " | ".join(sym for sym, _ in slots)
        ri_suffix   = f" ({reinvest_pct}% reinvest)" if reinvest_pct < 100 and mode not in ("price", "pricediv") else ""
        mode_suffix = {"total": "", "price": " — Price Only",
                       "pricediv": " — Price + Cash Dividends",
                       "both": " — Return & Price",
                       "all3": " — Price vs Custom vs DRIP",
                       "all4": " — All Four Modes"}
        title = f"{sym_list} — {period_label}{mode_suffix.get(mode, '')}{ri_suffix}"
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
            margin=dict(l=60, r=80, t=80, b=50),
        )
        if any_db:
            fig.add_annotation(
                text=("⚠ Horizontal lines = DB portfolio snapshot (all-time) "
                      "| Time series = Yahoo Finance"),
                xref="paper", yref="paper", x=0.5, y=-0.12,
                showarrow=False, font=dict(size=11, color="#8899aa"),
                xanchor="center",
            )

        # ── End-of-line return % annotations ─────────────────────────────────
        for trace in fig.data:
            if trace.y and len(trace.y) >= 2 and trace.x and len(trace.x) >= 2:
                last_y = trace.y[-1] if isinstance(trace.y, list) else list(trace.y)[-1]
                last_x = trace.x[-1] if isinstance(trace.x, list) else list(trace.x)[-1]
                ret_pct = last_y - 100
                sign = "+" if ret_pct >= 0 else ""
                fig.add_annotation(
                    x=last_x, y=last_y,
                    text=f"<b>{sign}{ret_pct:.1f}%</b>",
                    showarrow=False,
                    xanchor="left", yanchor="middle",
                    xshift=6,
                    font=dict(size=10, color=trace.line.color if trace.line and trace.line.color else "#cdd9e5"),
                )

        stats_out = {
            "tickers":      stats_list,
            "alphas":       alphas,
            "alpha_note":   alpha_note,
            "period":       period_label,
            "mode":         mode,
            "reinvest_pct": reinvest_pct,
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
                    FROM dbo.all_account_info WHERE ticker = ? AND profile_id = ?
                """, conn, params=[t1, _snap_pid])
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
            "warnings": warnings,
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


def _letter_grade(score):
    """Convert numeric score (0-100) to letter grade."""
    if score >= 97:
        return "A+"
    if score >= 93:
        return "A"
    if score >= 90:
        return "A-"
    if score >= 87:
        return "B+"
    if score >= 83:
        return "B"
    if score >= 80:
        return "B-"
    if score >= 77:
        return "C+"
    if score >= 73:
        return "C"
    if score >= 70:
        return "C-"
    if score >= 67:
        return "D+"
    if score >= 63:
        return "D"
    if score >= 60:
        return "D-"
    return "F"


def _grade_portfolio(returns_df, weights_arr, bench_ret=None):
    """Compute composite portfolio grade dict given returns + weights.

    Args:
        returns_df: DataFrame of daily returns (columns = tickers)
        weights_arr: numpy array of weights (will be normalized)
        bench_ret: optional Series of benchmark daily returns

    Returns:
        dict with sharpe, sortino, calmar, omega, max_drawdown,
        effective_n, top_weight, up/down_capture, and grade sub-dict.
    """
    import numpy as np
    import math

    def _safe(v):
        if v is None:
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    def _sm(val, thr):
        """Score a metric where higher is better."""
        if val is None:
            return None
        exc, good, fair, poor = thr
        if val >= exc:
            return 100.0
        if val >= good:
            return 80 + 20 * (val - good) / (exc - good)
        if val >= fair:
            return 60 + 20 * (val - fair) / (good - fair)
        if val >= poor:
            return 40 + 20 * (val - poor) / (fair - poor)
        return max(0.0, 40 * val / poor) if poor != 0 else 0.0

    def _sl(val, thr):
        """Score a metric where lower is better."""
        if val is None:
            return None
        exc, good, fair, poor = thr
        if val <= exc:
            return 100.0
        if val <= good:
            return 80 + 20 * (good - val) / (good - exc)
        if val <= fair:
            return 60 + 20 * (fair - val) / (fair - good)
        if val <= poor:
            return 40 + 20 * (poor - val) / (poor - fair)
        return max(0.0, 20.0)

    w = np.array(weights_arr, dtype=float)
    w_sum = w.sum()
    if w_sum > 0:
        w = w / w_sum

    port_daily = returns_df.dot(w)
    port_cum = (1 + port_daily).cumprod()

    port_mdd = _safe(_max_drawdown(port_cum))
    metrics = {
        "sharpe": _safe(_sharpe(port_cum)),
        "sortino": _safe(_sortino(port_cum)),
        "calmar": _safe(_calmar(port_cum)),
        "omega": _safe(_omega(port_daily)),
        "max_drawdown": port_mdd,
    }

    if bench_ret is not None:
        aligned = pd.concat([port_daily, bench_ret], axis=1).dropna()
        if len(aligned) > 30:
            aligned.columns = ["port", "bench"]
            uc, dc = _capture_ratios(aligned["port"], aligned["bench"])
            metrics["up_capture"] = _safe(uc)
            metrics["down_capture"] = _safe(dc)

    wt_sorted = sorted(w, reverse=True)
    metrics["top_weight"] = round(float(wt_sorted[0]) * 100, 2) if len(wt_sorted) else 0
    hhi = float(sum(wi ** 2 for wi in w))
    metrics["effective_n"] = round(1.0 / hhi, 1) if hhi > 0 else 0

    mdd_pct = port_mdd * 100 if port_mdd is not None else None
    sub = {
        "sharpe":        _sm(metrics.get("sharpe"),        (1.5, 1.0, 0.5, 0.0)),
        "sortino":       _sm(metrics.get("sortino"),       (2.0, 1.5, 1.0, 0.5)),
        "calmar":        _sm(metrics.get("calmar"),        (1.5, 1.0, 0.5, 0.2)),
        "omega":         _sm(metrics.get("omega"),         (2.0, 1.5, 1.2, 1.0)),
        "max_drawdown":  _sl(abs(mdd_pct) if mdd_pct is not None else None, (10, 20, 30, 40)),
        "down_capture":  _sl(metrics.get("down_capture"),  (80, 90, 100, 120)),
        "diversification": _sm(metrics.get("effective_n"), (20, 12, 6, 3)),
    }

    gw = {"sharpe": 25, "sortino": 15, "calmar": 10, "omega": 10,
          "max_drawdown": 20, "down_capture": 10, "diversification": 10}
    label_map = {
        "sharpe": "Sharpe Ratio", "sortino": "Sortino Ratio",
        "calmar": "Calmar Ratio", "omega": "Omega Ratio",
        "max_drawdown": "Max Drawdown", "down_capture": "Downside Capture",
        "diversification": "Diversification",
    }

    total_w = total_s = 0.0
    breakdown = []
    for key, wt in gw.items():
        sc = sub.get(key)
        if sc is not None:
            total_w += wt
            total_s += sc * wt
            breakdown.append({
                "category": label_map[key],
                "score": round(sc, 1),
                "weight": wt,
                "grade": _letter_grade(sc),
            })

    overall = round(total_s / total_w, 1) if total_w > 0 else 0.0
    metrics["grade"] = {
        "overall": _letter_grade(overall),
        "score": overall,
        "breakdown": breakdown,
    }
    return metrics


def _calmar(close, risk_free_annual=0.0):
    """Calmar ratio: annualized return / abs(max drawdown). Returns float or None."""
    import numpy as np
    try:
        if len(close) < 30:
            return None
        ann_ret = (close.iloc[-1] / close.iloc[0]) ** (252 / len(close)) - 1
        running_max = close.cummax()
        drawdowns = (close - running_max) / running_max
        mdd = float(drawdowns.min())
        if mdd == 0 or np.isnan(mdd):
            return None
        return round(float(ann_ret) / abs(mdd), 2)
    except Exception:
        return None


def _omega(daily_returns, threshold=0.0):
    """Omega ratio: sum of gains above threshold / sum of losses below. Returns float or None."""
    import numpy as np
    try:
        if len(daily_returns) < 30:
            return None
        excess = daily_returns - threshold / 252
        gains = float(excess[excess > 0].sum())
        losses = abs(float(excess[excess <= 0].sum()))
        if losses == 0 or np.isnan(losses):
            return None
        return round(gains / losses, 2)
    except Exception:
        return None


def _capture_ratios(ticker_returns, bench_returns):
    """Upside/downside capture ratios. Returns (up_capture, down_capture) or (None, None)."""
    import numpy as np
    try:
        if len(ticker_returns) < 30:
            return None, None
        up_days = bench_returns > 0
        down_days = bench_returns < 0
        if up_days.sum() == 0 or down_days.sum() == 0:
            return None, None
        bench_up_mean = float(bench_returns[up_days].mean())
        bench_down_mean = float(bench_returns[down_days].mean())
        if bench_up_mean == 0 or bench_down_mean == 0:
            return None, None
        up_cap = round(float(ticker_returns[up_days].mean()) / bench_up_mean * 100, 1)
        down_cap = round(float(ticker_returns[down_days].mean()) / bench_down_mean * 100, 1)
        return up_cap, down_cap
    except Exception:
        return None, None


def _max_drawdown(close):
    """Maximum drawdown as a fraction (negative). Returns float or None."""
    try:
        running_max = close.cummax()
        drawdowns = (close - running_max) / running_max
        return float(drawdowns.min())
    except Exception:
        return None


def _portfolio_sharpe(weights, returns_df, risk_free_annual=0.05):
    """Portfolio Sharpe from weight vector and returns DataFrame."""
    import numpy as np
    port_ret = returns_df.dot(weights)
    daily_rf = risk_free_annual / 252
    excess = float(port_ret.mean()) - daily_rf
    std = float(port_ret.std())
    if std == 0:
        return 0.0
    return excess / std * np.sqrt(252)


def _portfolio_max_dd(weights, returns_df):
    """Portfolio max drawdown from weight vector and returns DataFrame."""
    port_ret = returns_df.dot(weights)
    cum = (1 + port_ret).cumprod()
    running_max = cum.cummax()
    dd = (cum - running_max) / running_max
    return float(dd.min())


def _optimize_sharpe(returns_df):
    """Find weight vector maximizing portfolio Sharpe ratio using scipy SLSQP."""
    import numpy as np
    from scipy.optimize import minimize

    n = returns_df.shape[1]
    init = np.ones(n) / n

    def neg_sharpe(w):
        return -_portfolio_sharpe(w, returns_df)

    bounds = [(0, 1)] * n
    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1}]
    result = minimize(neg_sharpe, init, method="SLSQP", bounds=bounds,
                      constraints=constraints, options={"maxiter": 1000})
    return result.x if result.success else init


def _optimize_income(returns_df, yields, min_sharpe=0.5, max_dd=-0.30):
    """Maximize weighted dividend yield subject to Sharpe and drawdown constraints."""
    import numpy as np
    from scipy.optimize import minimize

    n = returns_df.shape[1]
    init = np.ones(n) / n
    yields_arr = np.array(yields)

    def neg_yield(w):
        return -float(w.dot(yields_arr))

    constraints = [
        {"type": "eq", "fun": lambda w: w.sum() - 1},
        {"type": "ineq", "fun": lambda w: _portfolio_sharpe(w, returns_df) - min_sharpe},
        {"type": "ineq", "fun": lambda w: _portfolio_max_dd(w, returns_df) - max_dd},
    ]
    bounds = [(0, 1)] * n
    result = minimize(neg_yield, init, method="SLSQP", bounds=bounds,
                      constraints=constraints, options={"maxiter": 1000})
    return result.x if result.success else init


def _optimize_balanced(returns_df, yields, balance=0.5):
    """Maximize a blend of portfolio yield (income) and Sharpe (safety).
    balance=1.0: pure income; balance=0.0: pure Sharpe.
    """
    import numpy as np
    from scipy.optimize import minimize

    n = returns_df.shape[1]
    mu = returns_df.mean().values * 252
    cov = returns_df.cov().values * 252
    yields_arr = np.array(yields)
    max_yield = float(yields_arr.max()) if yields_arr.max() > 0 else 0.10

    def neg_objective(w):
        port_yield = float(yields_arr @ w)
        port_ret = float(mu @ w)
        port_vol = float(np.sqrt(w @ cov @ w))
        port_sharpe = (port_ret - 0.05) / port_vol if port_vol > 1e-6 else 0
        norm_yield = port_yield / max_yield if max_yield > 0 else 0
        norm_sharpe = min(max(port_sharpe, 0), 3) / 3.0
        return -(balance * norm_yield + (1.0 - balance) * norm_sharpe)

    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1}]
    bounds = [(0.01, 0.40)] * n
    best = None
    rng = np.random.default_rng(42)
    for _ in range(20):
        w0 = rng.dirichlet(np.ones(n))
        try:
            res = minimize(neg_objective, w0, method="SLSQP", bounds=bounds,
                           constraints=constraints, options={"maxiter": 1000, "ftol": 1e-9})
            if res.success and (best is None or res.fun < best.fun):
                best = res
        except Exception:
            pass
    if best is None:
        return np.ones(n) / n
    w = np.maximum(best.x, 0)
    w[w < 0.01] = 0
    return w / w.sum() if w.sum() > 0 else np.ones(n) / n


def _build_efficient_frontier(returns_df, n_points=30):
    """Generate (volatility, return) points for the efficient frontier."""
    import numpy as np
    from scipy.optimize import minimize

    n = returns_df.shape[1]
    mean_ret = returns_df.mean().values * 252
    cov = returns_df.cov().values * 252

    ret_range = np.linspace(mean_ret.min(), mean_ret.max(), n_points)
    frontier = []

    for target in ret_range:
        def portfolio_vol(w):
            return np.sqrt(w @ cov @ w)

        constraints = [
            {"type": "eq", "fun": lambda w: w.sum() - 1},
            {"type": "eq", "fun": lambda w: w @ mean_ret - target},
        ]
        bounds = [(0, 1)] * n
        init = np.ones(n) / n
        result = minimize(portfolio_vol, init, method="SLSQP", bounds=bounds,
                          constraints=constraints, options={"maxiter": 500})
        if result.success:
            vol = float(np.sqrt(result.x @ cov @ result.x))
            ret = float(result.x @ mean_ret)
            frontier.append({"vol": round(vol * 100, 2), "ret": round(ret * 100, 2)})

    return frontier


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
    _bss_pid = get_profile_id()
    try:
        port = pd.read_sql("""
            SELECT ticker, description, classification_type, purchase_value
            FROM dbo.all_account_info
            WHERE purchase_value IS NOT NULL AND purchase_value > 0
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[_bss_pid])
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
    pid = get_profile_id()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, description, classification_type,
               purchase_value, reinvest, current_annual_yield
        FROM dbo.all_account_info
        WHERE current_price IS NOT NULL AND profile_id = ?
        ORDER BY ticker
    """, pid)
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
                if isinstance(raw.columns, pd.MultiIndex):
                    top_keys = raw.columns.get_level_values(0).unique()
                    if sym in top_keys:
                        sub = raw[sym]
                        close = sub["Close"] if "Close" in sub.columns else None
                        divs  = sub["Dividends"] if "Dividends" in sub.columns else pd.Series(0.0, index=raw.index)
                    elif "Close" in top_keys:
                        close = raw["Close"][sym] if sym in raw["Close"].columns else raw["Close"].iloc[:, 0]
                        divs_df = raw["Dividends"] if "Dividends" in top_keys else None
                        if divs_df is not None:
                            divs = divs_df[sym] if sym in divs_df.columns else pd.Series(0.0, index=raw.index)
                        else:
                            divs = pd.Series(0.0, index=raw.index)
                    else:
                        return None, None
                else:
                    close = raw["Close"] if "Close" in raw.columns else None
                    divs  = raw["Dividends"] if "Dividends" in raw.columns else pd.Series(0.0, index=raw.index)
            except Exception:
                return None, None
            if close is None:
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
            if isinstance(raw.columns, pd.MultiIndex):
                # MultiIndex — try ticker-first (group_by="ticker") then column-first
                top_keys = raw.columns.get_level_values(0).unique()
                if sym in top_keys:
                    # group_by="ticker": top level is ticker name
                    sub = raw[sym]
                    close = sub["Close"] if "Close" in sub.columns else None
                    divs  = sub["Dividends"] if "Dividends" in sub.columns else pd.Series(0.0, index=raw.index)
                elif "Close" in top_keys:
                    # group_by="column" (default): top level is column name
                    close = raw["Close"][sym] if sym in raw["Close"].columns else raw["Close"].iloc[:, 0]
                    divs_df = raw["Dividends"] if "Dividends" in top_keys else None
                    if divs_df is not None:
                        divs = divs_df[sym] if sym in divs_df.columns else pd.Series(0.0, index=raw.index)
                    else:
                        divs = pd.Series(0.0, index=raw.index)
                else:
                    return None, None
            else:
                # Flat columns (single ticker, no MultiIndex)
                close = raw["Close"] if "Close" in raw.columns else None
                divs  = raw["Dividends"] if "Dividends" in raw.columns else pd.Series(0.0, index=raw.index)
        except Exception:
            return None, None
        if close is None:
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


# ── Portfolio Analytics & Optimization ────────────────────────────────────────

@app.route("/portfolio_analytics")
def portfolio_analytics():
    """Portfolio analytics shell page — metrics + optimization via AJAX."""
    pid = get_profile_id()
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT ticker, current_value, estim_payment_per_year
            FROM dbo.all_account_info
            WHERE current_value IS NOT NULL AND current_value > 0
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[pid])
    except Exception:
        df = pd.DataFrame()
    conn.close()
    tickers = df["ticker"].tolist() if not df.empty else []
    return render_template("portfolio_analytics.html", tickers=tickers)


@app.route("/portfolio_analytics/data")
def portfolio_analytics_data():
    """AJAX: compute risk metrics and/or optimization for given tickers."""
    import warnings, math
    import numpy as np
    warnings.filterwarnings("ignore")

    tickers_param = request.args.get("tickers", "")
    benchmark = request.args.get("benchmark", "SPY")
    period = request.args.get("period", "1y")
    mode = request.args.get("mode", "metrics")
    min_sharpe = float(request.args.get("min_sharpe", "0.5"))
    max_dd = float(request.args.get("max_dd", "-0.30"))

    if not tickers_param:
        return jsonify({"error": "No tickers provided."}), 400

    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()]
    if not tickers:
        return jsonify({"error": "No valid tickers."}), 400

    # Period mapping (same pattern as total_return_data)
    from datetime import date as date_type
    period_map = {
        "1mo": dict(period="1mo"), "3mo": dict(period="3mo"),
        "6mo": dict(period="6mo"), "ytd": dict(period="ytd"),
        "1y": dict(period="1y"), "2y": dict(period="2y"),
        "5y": dict(period="5y"), "max": dict(period="max"),
    }
    if period.isdigit() and len(period) == 4:
        yr = int(period)
        today = date_type.today()
        start = f"{yr}-01-01"
        end = today.strftime("%Y-%m-%d") if yr == today.year else f"{yr}-12-31"
        yf_kwargs = dict(start=start, end=end)
    else:
        yf_kwargs = period_map.get(period, dict(period="1y"))

    # Fetch price data
    import yfinance as yf
    all_tickers = list(set(tickers + [benchmark]))
    try:
        raw = yf.download(" ".join(all_tickers), **yf_kwargs, progress=False, auto_adjust=True)
        if raw.empty:
            return jsonify({"error": "No price data returned."}), 500
    except Exception as e:
        return jsonify({"error": f"yfinance error: {str(e)}"}), 500

    # Extract close prices
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw["Close"].dropna(how="all")
    else:
        close = raw[["Close"]].dropna(how="all")
        close.columns = all_tickers[:1]

    # Get DB weights and yields
    _pa_pid = get_profile_id()
    conn = get_connection()
    try:
        db_df = pd.read_sql("""
            SELECT ticker, current_value, estim_payment_per_year
            FROM dbo.all_account_info
            WHERE current_value IS NOT NULL AND current_value > 0
              AND profile_id = ?
        """, conn, params=[_pa_pid])
    except Exception:
        db_df = pd.DataFrame(columns=["ticker", "current_value", "estim_payment_per_year"])
    conn.close()

    total_val = db_df["current_value"].sum() if not db_df.empty else 1
    weight_map = {}
    yield_map = {}
    for _, r in db_df.iterrows():
        t = r["ticker"]
        weight_map[t] = float(r["current_value"]) / total_val if total_val > 0 else 0
        cv = float(r["current_value"]) if r["current_value"] and r["current_value"] > 0 else 1
        ep = float(r["estim_payment_per_year"]) if pd.notna(r["estim_payment_per_year"]) else 0
        yield_map[t] = ep / cv

    bench_close = close[benchmark] if benchmark in close.columns else None
    bench_ret = bench_close.pct_change().dropna() if bench_close is not None else None

    def safe(v):
        if v is None:
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    # Compute per-ticker metrics
    metrics = []
    available_tickers = []
    for t in tickers:
        if t not in close.columns:
            continue
        tc = close[t].dropna()
        if len(tc) < 30:
            continue
        available_tickers.append(t)
        tr = tc.pct_change().dropna()
        up_cap, down_cap = _capture_ratios(tr, bench_ret) if bench_ret is not None else (None, None)
        annual_ret = round(float(tr.mean() * 252) * 100, 2)
        annual_vol = round(float(tr.std() * np.sqrt(252)) * 100, 2)
        metrics.append({
            "ticker": t,
            "sharpe": safe(_sharpe(tc)),
            "sortino": safe(_sortino(tc)),
            "calmar": safe(_calmar(tc)),
            "omega": safe(_omega(tr)),
            "up_capture": safe(up_cap),
            "down_capture": safe(down_cap),
            "weight": round(weight_map.get(t, 0) * 100, 2),
            "annual_ret": annual_ret,
            "annual_vol": annual_vol,
            "annual_income": round(float(db_df.loc[db_df["ticker"] == t, "estim_payment_per_year"].values[0])
                                   if t in db_df["ticker"].values and
                                      pd.notna(db_df.loc[db_df["ticker"] == t, "estim_payment_per_year"].values[0])
                                   else 0, 2),
        })

    # Portfolio weighted metrics
    port_metrics = {}
    result_corr = None
    result_dd = None
    if len(available_tickers) >= 2:
        returns_df = close[available_tickers].pct_change().dropna()
        weights_arr = np.array([weight_map.get(t, 1 / len(available_tickers)) for t in available_tickers])
        w_sum = weights_arr.sum()
        if w_sum > 0:
            weights_arr = weights_arr / w_sum

        port_daily = returns_df.dot(weights_arr)
        port_cum = (1 + port_daily).cumprod()

        port_mdd = safe(_max_drawdown(port_cum))
        port_metrics = {
            "sharpe": safe(_sharpe(port_cum)),
            "sortino": safe(_sortino(port_cum)),
            "calmar": safe(_calmar(port_cum)),
            "omega": safe(_omega(port_daily)),
            "max_drawdown": port_mdd,
        }
        if bench_ret is not None:
            aligned = pd.concat([port_daily, bench_ret], axis=1).dropna()
            if len(aligned) > 30:
                aligned.columns = ["port", "bench"]
                up_cap, down_cap = _capture_ratios(aligned["port"], aligned["bench"])
                port_metrics["up_capture"] = safe(up_cap)
                port_metrics["down_capture"] = safe(down_cap)

        # Diversification: top-holding concentration and effective N
        wt_arr_sorted = sorted(weights_arr, reverse=True)
        port_metrics["top_weight"] = round(wt_arr_sorted[0] * 100, 2) if len(wt_arr_sorted) else 0
        hhi = float(sum(w ** 2 for w in weights_arr))
        port_metrics["effective_n"] = round(1 / hhi, 1) if hhi > 0 else 0
        port_metrics["n_holdings"] = len(available_tickers)

        # ── Portfolio Grade ──
        def _score_metric(val, thresholds):
            """Score 0–100 given (excellent, good, fair, poor) thresholds (descending)."""
            if val is None:
                return None
            exc, good, fair, poor = thresholds
            if val >= exc:
                return 100
            if val >= good:
                return 80 + 20 * (val - good) / (exc - good)
            if val >= fair:
                return 60 + 20 * (val - fair) / (good - fair)
            if val >= poor:
                return 40 + 20 * (val - poor) / (fair - poor)
            return max(0, 40 * val / poor) if poor != 0 else 0

        def _score_lower_better(val, thresholds):
            """Score 0–100 where lower values are better. Thresholds ascending."""
            if val is None:
                return None
            exc, good, fair, poor = thresholds
            if val <= exc:
                return 100
            if val <= good:
                return 80 + 20 * (good - val) / (good - exc)
            if val <= fair:
                return 60 + 20 * (fair - val) / (fair - good)
            if val <= poor:
                return 40 + 20 * (poor - val) / (poor - fair)
            return max(0, 20)

        sub_scores = {}
        # Sharpe: >1.5 excellent, >1.0 good, >0.5 fair, >0.0 poor
        sub_scores["sharpe"] = _score_metric(port_metrics.get("sharpe"), (1.5, 1.0, 0.5, 0.0))
        # Sortino: >2.0 excellent, >1.5 good, >1.0 fair, >0.5 poor
        sub_scores["sortino"] = _score_metric(port_metrics.get("sortino"), (2.0, 1.5, 1.0, 0.5))
        # Calmar: >1.5 excellent, >1.0 good, >0.5 fair, >0.2 poor
        sub_scores["calmar"] = _score_metric(port_metrics.get("calmar"), (1.5, 1.0, 0.5, 0.2))
        # Omega: >2.0 excellent, >1.5 good, >1.2 fair, >1.0 poor
        sub_scores["omega"] = _score_metric(port_metrics.get("omega"), (2.0, 1.5, 1.2, 1.0))
        # Max drawdown (as %): >-10 excellent, >-20 good, >-30 fair, >-40 poor
        mdd_pct = port_mdd * 100 if port_mdd is not None else None
        sub_scores["max_drawdown"] = _score_lower_better(
            abs(mdd_pct) if mdd_pct is not None else None, (10, 20, 30, 40))
        # Downside capture: <80 excellent, <90 good, <100 fair, <120 poor
        sub_scores["down_capture"] = _score_lower_better(
            port_metrics.get("down_capture"), (80, 90, 100, 120))
        # Diversification via effective N: >20 excellent, >12 good, >6 fair, >3 poor
        sub_scores["diversification"] = _score_metric(
            port_metrics.get("effective_n"), (20, 12, 6, 3))

        # Weighted average (skip None scores)
        weights_grade = {
            "sharpe": 25, "sortino": 15, "calmar": 10, "omega": 10,
            "max_drawdown": 20, "down_capture": 10, "diversification": 10,
        }
        total_w = 0
        total_score = 0
        grade_breakdown = []
        for key, w in weights_grade.items():
            sc = sub_scores.get(key)
            label_map = {
                "sharpe": "Sharpe Ratio", "sortino": "Sortino Ratio",
                "calmar": "Calmar Ratio", "omega": "Omega Ratio",
                "max_drawdown": "Max Drawdown", "down_capture": "Downside Capture",
                "diversification": "Diversification",
            }
            if sc is not None:
                total_w += w
                total_score += sc * w
                grade_breakdown.append({
                    "category": label_map.get(key, key),
                    "score": round(sc, 1),
                    "weight": w,
                    "grade": _letter_grade(sc),
                })

        overall_score = round(total_score / total_w, 1) if total_w > 0 else 0
        overall_grade = _letter_grade(overall_score)

        port_metrics["grade"] = {
            "overall": overall_grade,
            "score": overall_score,
            "breakdown": grade_breakdown,
        }

        # Correlation matrix
        corr = returns_df.corr()
        result_corr = {
            "labels": available_tickers,
            "matrix": [[round(float(corr.loc[a, b]), 3) for b in available_tickers]
                       for a in available_tickers],
        }

        # Portfolio drawdown series (capped at ~200 points)
        roll_max = port_cum.cummax()
        drawdown_s = (port_cum / roll_max - 1)
        step = max(1, len(drawdown_s) // 200)
        dd_sampled = drawdown_s.iloc[::step]
        result_dd = {
            "dates": [d.strftime("%Y-%m-%d") for d in dd_sampled.index],
            "values": [round(float(v) * 100, 2) for v in dd_sampled.values],
        }

    result = {"metrics": metrics, "portfolio_metrics": port_metrics}
    if result_corr:
        result["correlation"] = result_corr
        result["drawdown_series"] = result_dd

    # Optimization
    if mode in ("optimize_returns", "optimize_income", "optimize_balanced") and len(available_tickers) >= 2:
        returns_df = close[available_tickers].pct_change().dropna()

        current_weights = np.array([weight_map.get(t, 0) for t in available_tickers])
        cw_sum = current_weights.sum()
        if cw_sum > 0:
            current_weights = current_weights / cw_sum

        if mode == "optimize_returns":
            opt_w = _optimize_sharpe(returns_df)
            frontier = _build_efficient_frontier(returns_df)

            # Optimal portfolio stats
            opt_sharpe = _portfolio_sharpe(opt_w, returns_df)
            mean_ret = float(returns_df.mean().values @ opt_w) * 252
            cov = returns_df.cov().values * 252
            opt_vol = float(np.sqrt(opt_w @ cov @ opt_w))

            weights_out = []
            for i, t in enumerate(available_tickers):
                weights_out.append({
                    "ticker": t,
                    "current_pct": round(current_weights[i] * 100, 2),
                    "optimal_pct": round(opt_w[i] * 100, 2),
                })

            # Current portfolio point for frontier chart
            curr_ret = float(returns_df.mean().values @ current_weights) * 252
            curr_vol = float(np.sqrt(current_weights @ cov @ current_weights))

            result["optimization"] = {
                "weights": weights_out,
                "frontier": frontier,
                "optimal_point": {"vol": round(opt_vol * 100, 2), "ret": round(mean_ret * 100, 2)},
                "current_point": {"vol": round(curr_vol * 100, 2), "ret": round(curr_ret * 100, 2)},
                "summary": {
                    "sharpe": round(opt_sharpe, 2),
                    "expected_return": round(mean_ret * 100, 2),
                    "expected_vol": round(opt_vol * 100, 2),
                },
            }

        elif mode == "optimize_income":
            yields_list = [yield_map.get(t, 0) for t in available_tickers]
            opt_w = _optimize_income(returns_df, yields_list, min_sharpe=min_sharpe, max_dd=max_dd)

            opt_yield = float(opt_w.dot(np.array(yields_list)))
            curr_yield = float(current_weights.dot(np.array(yields_list)))

            opt_sharpe_val = _portfolio_sharpe(opt_w, returns_df)
            opt_dd_val = _portfolio_max_dd(opt_w, returns_df)

            # Estimate annual income
            total_portfolio_val = total_val
            curr_income = curr_yield * total_portfolio_val
            opt_income = opt_yield * total_portfolio_val

            weights_out = []
            for i, t in enumerate(available_tickers):
                weights_out.append({
                    "ticker": t,
                    "current_pct": round(current_weights[i] * 100, 2),
                    "optimal_pct": round(opt_w[i] * 100, 2),
                    "yield_pct": round(yields_list[i] * 100, 2),
                })

            # Yield vs risk scatter data
            scatter_data = []
            for i, t in enumerate(available_tickers):
                tc = close[t].dropna()
                tr = tc.pct_change().dropna()
                vol = float(tr.std() * np.sqrt(252)) if len(tr) > 1 else 0
                scatter_data.append({
                    "ticker": t,
                    "yield_pct": round(yields_list[i] * 100, 2),
                    "vol_pct": round(vol * 100, 2),
                    "is_optimal": float(opt_w[i]) > 0.01,
                })

            result["optimization"] = {
                "weights": weights_out,
                "scatter": scatter_data,
                "summary": {
                    "sharpe": round(opt_sharpe_val, 2),
                    "max_dd": round(opt_dd_val * 100, 2),
                    "opt_yield": round(opt_yield * 100, 2),
                    "curr_yield": round(curr_yield * 100, 2),
                    "opt_income": round(opt_income, 2),
                    "curr_income": round(curr_income, 2),
                },
            }

        elif mode == "optimize_balanced":
            balance = float(request.args.get("balance", "0.5"))
            yields_list = [yield_map.get(t, 0) for t in available_tickers]
            opt_w = _optimize_balanced(returns_df, yields_list, balance=balance)

            opt_yield = float(opt_w.dot(np.array(yields_list)))
            curr_yield = float(current_weights.dot(np.array(yields_list)))
            opt_sharpe_val = _portfolio_sharpe(opt_w, returns_df)
            curr_sharpe_val = _portfolio_sharpe(current_weights, returns_df)
            opt_dd_val = _portfolio_max_dd(opt_w, returns_df)
            opt_income = opt_yield * total_val
            curr_income = curr_yield * total_val

            weights_out = []
            for i, t in enumerate(available_tickers):
                weights_out.append({
                    "ticker": t,
                    "current_pct": round(current_weights[i] * 100, 2),
                    "optimal_pct": round(opt_w[i] * 100, 2),
                    "yield_pct": round(yields_list[i] * 100, 2),
                })

            # Scatter: yield vs vol, sized by Sharpe, coloured by in-optimal
            scatter_data = []
            for i, t in enumerate(available_tickers):
                tc = close[t].dropna()
                tr = tc.pct_change().dropna()
                vol = float(tr.std() * np.sqrt(252)) if len(tr) > 1 else 0
                sharpe_t = safe(_sharpe(tc))
                scatter_data.append({
                    "ticker": t,
                    "yield_pct": round(yields_list[i] * 100, 2),
                    "vol_pct": round(vol * 100, 2),
                    "sharpe": sharpe_t if sharpe_t is not None else 0,
                    "is_optimal": float(opt_w[i]) > 0.01,
                })

            result["optimization"] = {
                "weights": weights_out,
                "scatter": scatter_data,
                "summary": {
                    "opt_yield": round(opt_yield * 100, 2),
                    "curr_yield": round(curr_yield * 100, 2),
                    "opt_income": round(opt_income, 2),
                    "curr_income": round(curr_income, 2),
                    "opt_sharpe": round(opt_sharpe_val, 2),
                    "curr_sharpe": round(curr_sharpe_val, 2),
                    "max_dd": round(opt_dd_val * 100, 2),
                    "balance": round(balance * 100),
                },
            }

    return jsonify(result)


# ── Portfolio Swap Advisor ─────────────────────────────────────────────────────

@app.route("/portfolio_optimizer")
def portfolio_optimizer():
    """Swap Advisor shell page."""
    pid = get_profile_id()
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT ticker, quantity, current_value, estim_payment_per_year
            FROM dbo.all_account_info
            WHERE current_value IS NOT NULL AND current_value > 0
              AND profile_id = ?
            ORDER BY ticker
        """, conn, params=[pid])
    except Exception:
        df = pd.DataFrame()
    # Load user-saved candidates from DB
    try:
        saved_df = pd.read_sql(
            "SELECT ticker FROM dbo.swap_candidates WHERE profile_id = ? ORDER BY added_at",
            conn, params=[pid]
        )
        saved_candidates = saved_df["ticker"].tolist()
    except Exception:
        saved_candidates = []
    conn.close()
    tickers = df["ticker"].tolist() if not df.empty else []
    portfolio_set = set(tickers)
    db_holdings = []
    if not df.empty:
        for _, r in df.iterrows():
            if pd.notna(r.get("quantity")) and r["quantity"] > 0:
                db_holdings.append({
                    "ticker": r["ticker"],
                    "shares": round(float(r["quantity"]), 4),
                })
    # Merge defaults with user-saved (defaults first, then user-added extras)
    saved_set = set(saved_candidates)
    default_set = set(SWAP_CANDIDATES)
    user_only = [t for t in saved_candidates if t not in default_set]
    return render_template(
        "portfolio_optimizer.html",
        tickers=tickers,
        candidates=SWAP_CANDIDATES,
        saved_candidates=saved_set,
        user_only_candidates=user_only,
        portfolio_set=portfolio_set,
        db_holdings=db_holdings,
    )


@app.route("/portfolio_optimizer/candidates", methods=["POST"])
def swap_candidates_save():
    """Save one or more tickers to the user's swap candidate list."""
    pid = get_profile_id()
    data = request.get_json(silent=True) or {}
    tickers_raw = data.get("tickers", [])
    if isinstance(tickers_raw, str):
        tickers_raw = [tickers_raw]
    tickers = [t.strip().upper() for t in tickers_raw if t.strip()]
    if not tickers:
        return jsonify({"error": "No tickers provided"}), 400
    conn = get_connection()
    cursor = conn.cursor()
    saved = []
    for ticker in tickers:
        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM dbo.swap_candidates WHERE profile_id = ? AND ticker = ?
                )
                INSERT INTO dbo.swap_candidates (profile_id, ticker) VALUES (?, ?)
            """, pid, ticker, pid, ticker)
            saved.append(ticker)
        except Exception:
            pass
    conn.commit()
    conn.close()
    return jsonify({"saved": saved})


@app.route("/portfolio_optimizer/candidates/<ticker>", methods=["DELETE"])
def swap_candidates_delete(ticker):
    """Remove a ticker from the user's swap candidate list."""
    pid = get_profile_id()
    ticker = ticker.strip().upper()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM dbo.swap_candidates WHERE profile_id = ? AND ticker = ?",
        pid, ticker
    )
    conn.commit()
    conn.close()
    return jsonify({"deleted": ticker})


@app.route("/portfolio_optimizer/data")
def portfolio_optimizer_data():
    """AJAX: mode=score scores all holdings; mode=swaps finds replacements."""
    import warnings, math
    import numpy as np
    warnings.filterwarnings("ignore")
    import yfinance as yf

    mode = request.args.get("mode", "score")
    benchmark = request.args.get("benchmark", "SPY")
    period = request.args.get("period", "1y")
    income_floor = float(request.args.get("income_floor", "0.95"))

    from datetime import date as date_type
    period_map = {
        "1mo": dict(period="1mo"), "3mo": dict(period="3mo"),
        "6mo": dict(period="6mo"), "ytd": dict(period="ytd"),
        "1y": dict(period="1y"), "2y": dict(period="2y"),
        "5y": dict(period="5y"), "max": dict(period="max"),
    }
    if period.isdigit() and len(period) == 4:
        yr = int(period)
        today = date_type.today()
        start = f"{yr}-01-01"
        end = today.strftime("%Y-%m-%d") if yr == today.year else f"{yr}-12-31"
        yf_kwargs = dict(start=start, end=end)
    else:
        yf_kwargs = period_map.get(period, dict(period="1y"))

    # Fetch portfolio data from DB
    _po_pid = get_profile_id()
    conn = get_connection()
    try:
        db_df = pd.read_sql("""
            SELECT ticker, current_value, estim_payment_per_year
            FROM dbo.all_account_info
            WHERE current_value IS NOT NULL AND current_value > 0
              AND profile_id = ?
        """, conn, params=[_po_pid])
    except Exception:
        db_df = pd.DataFrame(columns=["ticker", "current_value", "estim_payment_per_year"])
    conn.close()

    if db_df.empty:
        return jsonify({"error": "No portfolio data found."}), 400

    total_val = float(db_df["current_value"].sum())
    weight_map, income_map, value_map, yield_map = {}, {}, {}, {}
    for _, r in db_df.iterrows():
        t = r["ticker"]
        cv = float(r["current_value"]) if r["current_value"] > 0 else 1.0
        ep = float(r["estim_payment_per_year"]) if pd.notna(r["estim_payment_per_year"]) else 0.0
        weight_map[t] = cv / total_val
        income_map[t] = ep
        value_map[t] = cv
        yield_map[t] = ep / cv if cv > 0 else 0.0

    port_tickers = db_df["ticker"].tolist()

    # Custom portfolio override
    custom_port_param = request.args.get("custom_portfolio", "")
    if custom_port_param:
        custom_entries = {}
        for entry in custom_port_param.split(","):
            parts = entry.strip().split(":")
            if len(parts) == 2:
                try:
                    t, s = parts[0].strip().upper(), float(parts[1].strip())
                    if s > 0:
                        custom_entries[t] = s
                except ValueError:
                    pass
        if custom_entries:
            try:
                price_raw = yf.download(
                    " ".join(custom_entries.keys()),
                    period="1d", progress=False, auto_adjust=True,
                )
            except Exception:
                price_raw = pd.DataFrame()
            prices = {}
            if not price_raw.empty:
                if isinstance(price_raw.columns, pd.MultiIndex):
                    for t in custom_entries:
                        if "Close" in price_raw.columns.get_level_values(0) and t in price_raw["Close"].columns:
                            s = price_raw["Close"][t].dropna()
                            if not s.empty:
                                prices[t] = float(s.iloc[-1])
                elif "Close" in price_raw.columns:
                    first_t = list(custom_entries.keys())[0]
                    s = price_raw["Close"].dropna()
                    if not s.empty:
                        prices[first_t] = float(s.iloc[-1])
            db_income_by_ticker = {
                r["ticker"]: float(r["estim_payment_per_year"] or 0)
                for _, r in db_df.iterrows() if pd.notna(r.get("estim_payment_per_year"))
            }
            custom_values = {t: custom_entries[t] * prices.get(t, 0) for t in custom_entries}
            total_custom = sum(custom_values.values()) or 1.0
            weight_map = {t: custom_values[t] / total_custom for t in custom_entries}
            value_map = custom_values
            income_map = {t: db_income_by_ticker.get(t, 0.0) for t in custom_entries}
            yield_map = {
                t: income_map[t] / value_map[t] if value_map.get(t, 0) > 0 else 0.0
                for t in custom_entries
            }
            port_tickers = list(custom_entries.keys())

    def safe(v):
        if v is None:
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    def _ticker_score(tc, tr, bench_ret_s):
        """Compute individual ticker risk score (0–100)."""
        def sm(val, thr):
            if val is None:
                return None
            exc, good, fair, poor = thr
            if val >= exc: return 100.0
            if val >= good: return 80 + 20 * (val - good) / (exc - good)
            if val >= fair: return 60 + 20 * (val - fair) / (good - fair)
            if val >= poor: return 40 + 20 * (val - poor) / (fair - poor)
            return max(0.0, 40 * val / poor) if poor != 0 else 0.0

        def sl(val, thr):
            if val is None:
                return None
            exc, good, fair, poor = thr
            if val <= exc: return 100.0
            if val <= good: return 80 + 20 * (good - val) / (good - exc)
            if val <= fair: return 60 + 20 * (fair - val) / (fair - good)
            if val <= poor: return 40 + 20 * (poor - val) / (poor - fair)
            return max(0.0, 20.0)

        sharpe_v = safe(_sharpe(tc))
        sortino_v = safe(_sortino(tc))
        calmar_v = safe(_calmar(tc))
        omega_v = safe(_omega(tr))
        mdd_v = safe(_max_drawdown(tc))
        _, dc = _capture_ratios(tr, bench_ret_s) if bench_ret_s is not None else (None, None)

        mdd_pct = mdd_v * 100 if mdd_v is not None else None
        sub = {
            "sharpe":       sm(sharpe_v,  (1.5, 1.0, 0.5, 0.0)),
            "sortino":      sm(sortino_v, (2.0, 1.5, 1.0, 0.5)),
            "calmar":       sm(calmar_v,  (1.5, 1.0, 0.5, 0.2)),
            "omega":        sm(omega_v,   (2.0, 1.5, 1.2, 1.0)),
            "max_drawdown": sl(abs(mdd_pct) if mdd_pct is not None else None, (10, 20, 30, 40)),
            "down_capture": sl(dc, (80, 90, 100, 120)),
        }
        gw = {"sharpe": 30, "sortino": 20, "calmar": 15,
              "omega": 15, "max_drawdown": 15, "down_capture": 5}
        tw = ts = 0.0
        for k, w in gw.items():
            sc = sub.get(k)
            if sc is not None:
                tw += w
                ts += sc * w
        ticker_score = round(ts / tw, 1) if tw > 0 else 0.0
        return ticker_score, sharpe_v, sortino_v, calmar_v, omega_v, mdd_v, dc

    # ── mode=score ──
    if mode == "score":
        all_dl = list(set(port_tickers + [benchmark]))
        try:
            raw = yf.download(" ".join(all_dl), **yf_kwargs, progress=False, auto_adjust=True)
            if raw.empty:
                return jsonify({"error": "No price data returned."}), 500
        except Exception as e:
            return jsonify({"error": f"yfinance error: {str(e)}"}), 500

        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"].dropna(how="all")
        else:
            close = raw[["Close"]].dropna(how="all")
            close.columns = [all_dl[0]]

        bench_close = close[benchmark] if benchmark in close.columns else None
        bench_ret = bench_close.pct_change().dropna() if bench_close is not None else None

        holdings_out = []
        for t in port_tickers:
            if t not in close.columns:
                continue
            tc = close[t].dropna()
            if len(tc) < 30:
                continue
            tr = tc.pct_change().dropna()
            t_score, sharpe_v, sortino_v, calmar_v, omega_v, mdd_v, dc = _ticker_score(tc, tr, bench_ret)
            up_cap, _ = _capture_ratios(tr, bench_ret) if bench_ret is not None else (None, None)

            holdings_out.append({
                "ticker": t,
                "score": t_score,
                "grade": _letter_grade(t_score),
                "sharpe": safe(sharpe_v),
                "sortino": safe(sortino_v),
                "calmar": safe(calmar_v),
                "omega": safe(omega_v),
                "max_drawdown": round(mdd_v * 100, 2) if mdd_v is not None else None,
                "up_capture": safe(up_cap),
                "down_capture": safe(dc),
                "weight": round(weight_map.get(t, 0) * 100, 2),
                "annual_income": round(income_map.get(t, 0), 2),
                "yield_pct": round(yield_map.get(t, 0) * 100, 2),
                "current_value": round(value_map.get(t, 0), 2),
            })

        holdings_out.sort(key=lambda x: x["score"])

        # Portfolio-level grade
        avail = [h["ticker"] for h in holdings_out]
        port_grade = {}
        port_stats = {}
        if len(avail) >= 2:
            returns_df = close[avail].pct_change().dropna()
            weights_arr = np.array([weight_map.get(t, 1.0 / len(avail)) for t in avail])
            pm = _grade_portfolio(returns_df, weights_arr, bench_ret)
            port_grade = pm.get("grade", {})
            port_stats = {
                "sharpe": safe(pm.get("sharpe")),
                "sortino": safe(pm.get("sortino")),
                "calmar": safe(pm.get("calmar")),
                "omega": safe(pm.get("omega")),
                "max_drawdown": round(pm["max_drawdown"] * 100, 2) if pm.get("max_drawdown") is not None else None,
                "effective_n": pm.get("effective_n"),
                "top_weight": pm.get("top_weight"),
                "up_capture": safe(pm.get("up_capture")),
                "down_capture": safe(pm.get("down_capture")),
                "n_holdings": len(avail),
            }

        scored_tickers = [h["ticker"] for h in holdings_out]
        skipped_tickers = [t for t in port_tickers if t not in scored_tickers]
        return jsonify({
            "portfolio_grade": port_grade,
            "portfolio_stats": port_stats,
            "holdings": holdings_out,
            "skipped_tickers": skipped_tickers,
        })

    # ── mode=swaps ──
    elif mode == "swaps":
        weak_param = request.args.get("weak", "")
        cands_param = request.args.get("candidates", "")

        weak_tickers = [t.strip().upper() for t in weak_param.split(",") if t.strip()]
        cand_tickers = [t.strip().upper() for t in cands_param.split(",") if t.strip()]

        if not weak_tickers:
            return jsonify({"error": "No weak tickers specified."}), 400
        if not cand_tickers:
            return jsonify({"error": "No candidates specified."}), 400

        all_dl = list(set(port_tickers + weak_tickers + cand_tickers + [benchmark]))
        try:
            raw = yf.download(" ".join(all_dl), **yf_kwargs, progress=False, auto_adjust=True, actions=True)
            if raw.empty:
                return jsonify({"error": "No price data returned."}), 500
        except Exception as e:
            return jsonify({"error": f"yfinance error: {str(e)}"}), 500

        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"].dropna(how="all")
            dividends = raw["Dividends"].fillna(0) if "Dividends" in raw.columns.get_level_values(0) else pd.DataFrame(0, index=raw.index, columns=close.columns)
        else:
            close = raw[["Close"]].dropna(how="all")
            close.columns = [all_dl[0]]
            if "Dividends" in raw.columns:
                dividends = raw[["Dividends"]].fillna(0)
                dividends.columns = [all_dl[0]]
            else:
                dividends = pd.DataFrame(0, index=raw.index, columns=[all_dl[0]])

        bench_close = close[benchmark] if benchmark in close.columns else None
        bench_ret = bench_close.pct_change().dropna() if bench_close is not None else None

        # Base portfolio (tickers with enough data)
        avail_port = [t for t in port_tickers if t in close.columns and len(close[t].dropna()) >= 30]
        if len(avail_port) < 2:
            return jsonify({"error": "Not enough portfolio price data."}), 400

        base_returns_df = close[avail_port].pct_change().dropna()
        base_weights = np.array([weight_map.get(t, 1.0 / len(avail_port)) for t in avail_port])
        base_pm = _grade_portfolio(base_returns_df, base_weights, bench_ret)
        base_score = base_pm["grade"]["score"] if "grade" in base_pm else 0.0
        base_grade = base_pm["grade"]["overall"] if "grade" in base_pm else "--"

        # Candidate yields — use DB yield_map first, then trailing dividend history (annualized)
        # Calculate the span of the download in years for annualizing dividends
        _div_span_days = (close.index[-1] - close.index[0]).days if len(close) >= 2 else 365
        _div_span_years = max(_div_span_days / 365.25, 0.1)  # floor at ~5 weeks

        cand_yields = {}
        for c in cand_tickers:
            if c in yield_map:
                cand_yields[c] = yield_map[c]
            elif c in close.columns and c in dividends.columns:
                try:
                    div_series = dividends[c]
                    total_divs = div_series[div_series > 0].sum()
                    annual_divs = total_divs / _div_span_years
                    last_price = close[c].dropna().iloc[-1] if len(close[c].dropna()) > 0 else 0
                    dy = (annual_divs / last_price) if last_price > 0 else 0.0
                    cand_yields[c] = min(dy, 0.50)  # clamp to 50% max
                except Exception:
                    cand_yields[c] = 0.0
            else:
                cand_yields[c] = 0.0

        swaps_out = {}
        for wt in weak_tickers:
            if wt not in close.columns:
                continue

            current_income = income_map.get(wt, 0.0)
            current_value_wt = value_map.get(wt, 0.0)

            # Individual score for the weak ticker
            tc_wt = close[wt].dropna()
            wt_score = 0.0
            if len(tc_wt) >= 30:
                tr_wt = tc_wt.pct_change().dropna()
                wt_score, *_ = _ticker_score(tc_wt, tr_wt, bench_ret)

            candidates_out = []
            for c in cand_tickers:
                if c not in close.columns:
                    continue
                cc = close[c].dropna()
                if len(cc) < 30:
                    continue

                # Income floor check
                c_yield = cand_yields.get(c, 0.0)
                new_income = c_yield * current_value_wt
                if current_income > 0:
                    income_ratio = new_income / current_income
                    if income_ratio < income_floor:
                        continue
                else:
                    income_ratio = 1.0

                # Build swapped portfolio returns
                if wt in avail_port:
                    swap_idx = avail_port.index(wt)
                    swap_tickers = list(avail_port)
                    swap_tickers[swap_idx] = c
                    cols = []
                    for i, t in enumerate(avail_port):
                        if i == swap_idx:
                            cols.append(cc.rename(c))
                        else:
                            cols.append(close[t])
                    swapped_close = pd.concat(cols, axis=1)
                    swapped_close.columns = swap_tickers
                else:
                    swap_tickers = avail_port + [c]
                    swapped_close = pd.concat([close[avail_port], cc.rename(c)], axis=1)
                    swap_tickers = list(swapped_close.columns)

                swapped_close = swapped_close.dropna(how="all")
                if len(swapped_close) < 30:
                    continue

                swapped_returns = swapped_close.pct_change().dropna()
                swapped_weights = np.array([
                    weight_map.get(t, 1.0 / len(swap_tickers)) for t in swap_tickers
                ])
                try:
                    new_pm = _grade_portfolio(swapped_returns, swapped_weights, bench_ret)
                    new_score = new_pm["grade"]["score"] if "grade" in new_pm else 0.0
                    new_grade = new_pm["grade"]["overall"] if "grade" in new_pm else "--"
                except Exception:
                    continue

                improvement = round(new_score - base_score, 1)

                cr = cc.pct_change().dropna()
                candidates_out.append({
                    "ticker": c,
                    "score_after": round(new_score, 1),
                    "grade_after": new_grade,
                    "improvement": improvement,
                    "new_income": round(new_income, 2),
                    "income_ratio": round(income_ratio, 3),
                    "sharpe": safe(_sharpe(cc)),
                    "sortino": safe(_sortino(cc)),
                    "yield_pct": round(c_yield * 100, 2),
                })

            candidates_out.sort(key=lambda x: x["improvement"], reverse=True)
            swaps_out[wt] = {
                "current_score": wt_score,
                "current_grade": _letter_grade(wt_score),
                "current_income": round(current_income, 2),
                "current_value": round(current_value_wt, 2),
                "candidates": candidates_out[:5],
            }

        return jsonify({
            "base_score": round(base_score, 1),
            "base_grade": base_grade,
            "portfolio_tickers_used": avail_port,
            "swaps": swaps_out,
        })

    # ── mode=eval_custom_swap ── evaluate a single custom ticker as swap for a weak ticker
    elif mode == "eval_custom_swap":
        weak_ticker = request.args.get("weak", "").strip().upper()
        cand_ticker = request.args.get("candidate", "").strip().upper()
        if not weak_ticker or not cand_ticker:
            return jsonify({"error": "Both weak and candidate tickers are required."}), 400

        all_dl = list(set(port_tickers + [weak_ticker, cand_ticker, benchmark]))
        try:
            raw = yf.download(" ".join(all_dl), **yf_kwargs, progress=False, auto_adjust=True, actions=True)
            if raw.empty:
                return jsonify({"error": "No price data returned."}), 500
        except Exception as e:
            return jsonify({"error": f"yfinance error: {str(e)}"}), 500

        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"].dropna(how="all")
            dividends = raw["Dividends"].fillna(0) if "Dividends" in raw.columns.get_level_values(0) else pd.DataFrame(0, index=raw.index, columns=close.columns)
        else:
            close = raw[["Close"]].dropna(how="all")
            close.columns = [all_dl[0]]
            if "Dividends" in raw.columns:
                dividends = raw[["Dividends"]].fillna(0)
                dividends.columns = [all_dl[0]]
            else:
                dividends = pd.DataFrame(0, index=raw.index, columns=[all_dl[0]])

        if cand_ticker not in close.columns:
            return jsonify({"error": f"No price data for {cand_ticker}."}), 400

        bench_close = close[benchmark] if benchmark in close.columns else None
        bench_ret = bench_close.pct_change().dropna() if bench_close is not None else None

        avail_port = [t for t in port_tickers if t in close.columns and len(close[t].dropna()) >= 30]
        if len(avail_port) < 2:
            return jsonify({"error": "Not enough portfolio price data."}), 400

        base_returns_df = close[avail_port].pct_change().dropna()
        base_weights = np.array([weight_map.get(t, 1.0 / len(avail_port)) for t in avail_port])
        base_pm = _grade_portfolio(base_returns_df, base_weights, bench_ret)
        base_score = base_pm["grade"]["score"] if "grade" in base_pm else 0.0

        # Candidate yield (annualized)
        _div_span_days = (close.index[-1] - close.index[0]).days if len(close) >= 2 else 365
        _div_span_years = max(_div_span_days / 365.25, 0.1)

        if cand_ticker in yield_map:
            c_yield = yield_map[cand_ticker]
        elif cand_ticker in dividends.columns:
            try:
                ds = dividends[cand_ticker]
                total_d = ds[ds > 0].sum()
                annual_d = total_d / _div_span_years
                lp = close[cand_ticker].dropna().iloc[-1] if len(close[cand_ticker].dropna()) > 0 else 0
                c_yield = min((annual_d / lp) if lp > 0 else 0.0, 0.50)
            except Exception:
                c_yield = 0.0
        else:
            c_yield = 0.0

        current_income = income_map.get(weak_ticker, 0.0)
        current_value_wt = value_map.get(weak_ticker, 0.0)
        new_income = c_yield * current_value_wt
        if current_income > 0:
            income_ratio = new_income / current_income
        else:
            income_ratio = 1.0

        cc = close[cand_ticker].dropna()
        if len(cc) < 30:
            return jsonify({"error": f"Not enough price history for {cand_ticker} (need 30 days)."}), 400

        # Build swapped portfolio
        if weak_ticker in avail_port:
            swap_idx = avail_port.index(weak_ticker)
            swap_tickers = list(avail_port)
            swap_tickers[swap_idx] = cand_ticker
            cols = []
            for i, t in enumerate(avail_port):
                if i == swap_idx:
                    cols.append(cc.rename(cand_ticker))
                else:
                    cols.append(close[t])
            swapped_close = pd.concat(cols, axis=1)
            swapped_close.columns = swap_tickers
        else:
            swap_tickers = avail_port + [cand_ticker]
            swapped_close = pd.concat([close[avail_port], cc.rename(cand_ticker)], axis=1)
            swap_tickers = list(swapped_close.columns)

        swapped_close = swapped_close.dropna(how="all")
        if len(swapped_close) < 30:
            return jsonify({"error": f"Not enough overlapping data for swap evaluation."}), 400

        swapped_returns = swapped_close.pct_change().dropna()
        swapped_weights = np.array([
            weight_map.get(t, 1.0 / len(swap_tickers)) for t in swap_tickers
        ])
        try:
            new_pm = _grade_portfolio(swapped_returns, swapped_weights, bench_ret)
            new_score = new_pm["grade"]["score"] if "grade" in new_pm else 0.0
            new_grade = new_pm["grade"]["overall"] if "grade" in new_pm else "--"
        except Exception:
            return jsonify({"error": "Failed to grade swapped portfolio."}), 500

        improvement = round(new_score - base_score, 1)

        return jsonify({
            "candidate": {
                "ticker": cand_ticker,
                "score_after": round(new_score, 1),
                "grade_after": new_grade,
                "improvement": improvement,
                "new_income": round(new_income, 2),
                "income_ratio": round(income_ratio, 3),
                "sharpe": safe(_sharpe(cc)),
                "sortino": safe(_sortino(cc)),
                "yield_pct": round(c_yield * 100, 2),
            },
            "base_score": round(base_score, 1),
        })

    return jsonify({"error": "Unknown mode."}), 400


# ── Portfolio Builder routes ──────────────────────────────────────────────────

@app.route("/portfolio_builder")
def portfolio_builder():
    """Portfolio Builder — create, save, and compare hypothetical portfolios."""
    pid = get_profile_id()
    conn = get_connection()
    ensure_tables_exist(conn)
    try:
        portfolios = pd.read_sql("""
            SELECT bp.id, bp.name, bp.notes, bp.updated_at,
                   COUNT(bh.id) AS holding_count
            FROM dbo.builder_portfolios bp
            LEFT JOIN dbo.builder_holdings bh ON bh.portfolio_id = bp.id
            WHERE bp.profile_id = ?
            GROUP BY bp.id, bp.name, bp.notes, bp.updated_at
            ORDER BY bp.updated_at DESC
        """, conn, params=[pid])
    except Exception:
        portfolios = pd.DataFrame(columns=["id", "name", "notes", "updated_at", "holding_count"])
    conn.close()
    port_list = portfolios.to_dict("records") if not portfolios.empty else []
    return render_template("portfolio_builder.html", portfolios=port_list)


@app.route("/portfolio_builder/portfolios", methods=["POST"])
def pb_create_portfolio():
    """Create a new named portfolio."""
    pid = get_profile_id()
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    notes = (data.get("notes") or "").strip()
    if not name:
        return jsonify({"error": "Name is required."}), 400
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO dbo.builder_portfolios (profile_id, name, notes)
            VALUES (?, ?, ?)
        """, pid, name, notes or None)
        conn.commit()
        cursor.execute(
            "SELECT id FROM dbo.builder_portfolios WHERE profile_id=? AND name=?",
            pid, name
        )
        row = cursor.fetchone()
        new_id = row[0] if row else None
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"id": new_id, "name": name, "notes": notes})


@app.route("/portfolio_builder/portfolios/<int:pid_>", methods=["PATCH"])
def pb_update_portfolio(pid_):
    """Rename a portfolio or update its notes."""
    pid = get_profile_id()
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    notes = (data.get("notes") or "").strip()
    if not name:
        return jsonify({"error": "Name is required."}), 400
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE dbo.builder_portfolios
            SET name=?, notes=?, updated_at=GETDATE()
            WHERE id=? AND profile_id=?
        """, name, notes or None, pid_, pid)
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"id": pid_, "name": name, "notes": notes})


@app.route("/portfolio_builder/portfolios/<int:pid_>", methods=["DELETE"])
def pb_delete_portfolio(pid_):
    """Delete a portfolio and all its holdings."""
    pid = get_profile_id()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM dbo.builder_holdings WHERE portfolio_id=?", pid_
        )
        cursor.execute(
            "DELETE FROM dbo.builder_portfolios WHERE id=? AND profile_id=?", pid_, pid
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"deleted": pid_})


@app.route("/portfolio_builder/portfolios/<int:pid_>/holdings", methods=["GET"])
def pb_get_holdings(pid_):
    """Return the holdings list for a portfolio (id check against profile)."""
    pid = get_profile_id()
    conn = get_connection()
    try:
        row = pd.read_sql(
            "SELECT id FROM dbo.builder_portfolios WHERE id=? AND profile_id=?",
            conn, params=[pid_, pid]
        )
        if row.empty:
            conn.close()
            return jsonify({"error": "Not found."}), 404
        df = pd.read_sql("""
            SELECT ticker, dollar_amount
            FROM dbo.builder_holdings
            WHERE portfolio_id=?
            ORDER BY added_at
        """, conn, params=[pid_])
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"holdings": df.to_dict("records")})


@app.route("/portfolio_builder/portfolios/<int:pid_>/holdings", methods=["POST"])
def pb_add_holding(pid_):
    """Add or update a holding (ticker + dollar_amount)."""
    pid = get_profile_id()
    data = request.get_json(force=True)
    ticker = (data.get("ticker") or "").strip().upper()
    try:
        dollar_amount = float(data.get("dollar_amount", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid dollar amount."}), 400
    if not ticker:
        return jsonify({"error": "Ticker is required."}), 400
    if dollar_amount <= 0:
        return jsonify({"error": "Dollar amount must be positive."}), 400
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Verify portfolio belongs to this profile
        cursor.execute(
            "SELECT id FROM dbo.builder_portfolios WHERE id=? AND profile_id=?", pid_, pid
        )
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": "Not found."}), 404
        # Upsert
        cursor.execute("""
            MERGE dbo.builder_holdings AS tgt
            USING (SELECT ? AS portfolio_id, ? AS ticker, ? AS dollar_amount) AS src
              ON tgt.portfolio_id = src.portfolio_id AND tgt.ticker = src.ticker
            WHEN MATCHED THEN
              UPDATE SET dollar_amount = src.dollar_amount
            WHEN NOT MATCHED THEN
              INSERT (portfolio_id, ticker, dollar_amount) VALUES (src.portfolio_id, src.ticker, src.dollar_amount);
        """, pid_, ticker, dollar_amount)
        # Touch updated_at on portfolio
        cursor.execute(
            "UPDATE dbo.builder_portfolios SET updated_at=GETDATE() WHERE id=?", pid_
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"portfolio_id": pid_, "ticker": ticker, "dollar_amount": dollar_amount})


@app.route("/portfolio_builder/portfolios/<int:pid_>/holdings/<ticker>", methods=["DELETE"])
def pb_delete_holding(pid_, ticker):
    """Remove a single holding from a portfolio."""
    pid = get_profile_id()
    ticker = ticker.strip().upper()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Verify ownership
        cursor.execute(
            "SELECT id FROM dbo.builder_portfolios WHERE id=? AND profile_id=?", pid_, pid
        )
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": "Not found."}), 404
        cursor.execute(
            "DELETE FROM dbo.builder_holdings WHERE portfolio_id=? AND ticker=?", pid_, ticker
        )
        cursor.execute(
            "UPDATE dbo.builder_portfolios SET updated_at=GETDATE() WHERE id=?", pid_
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"deleted": ticker})


@app.route("/portfolio_builder/portfolios/<int:pid_>/analyze")
def pb_analyze(pid_):
    """AJAX: score all holdings in a builder portfolio and return full metrics."""
    import warnings, math
    import numpy as np
    warnings.filterwarnings("ignore")
    import yfinance as yf

    pid = get_profile_id()
    benchmark = request.args.get("benchmark", "SPY")
    period = request.args.get("period", "1y")

    from datetime import date as date_type
    period_map = {
        "3mo": dict(period="3mo"), "6mo": dict(period="6mo"),
        "1y": dict(period="1y"), "2y": dict(period="2y"), "5y": dict(period="5y"),
        "ytd": dict(period="ytd"), "max": dict(period="max"),
    }
    if period.isdigit() and len(period) == 4:
        yr = int(period)
        today = date_type.today()
        start = f"{yr}-01-01"
        end = today.strftime("%Y-%m-%d") if yr == today.year else f"{yr}-12-31"
        yf_kwargs = dict(start=start, end=end)
    else:
        yf_kwargs = period_map.get(period, dict(period="1y"))

    conn = get_connection()
    try:
        # Ownership check
        meta = pd.read_sql(
            "SELECT id, name FROM dbo.builder_portfolios WHERE id=? AND profile_id=?",
            conn, params=[pid_, pid]
        )
        if meta.empty:
            conn.close()
            return jsonify({"error": "Portfolio not found."}), 404
        holdings_df = pd.read_sql(
            "SELECT ticker, dollar_amount FROM dbo.builder_holdings WHERE portfolio_id=? ORDER BY added_at",
            conn, params=[pid_]
        )
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()

    if holdings_df.empty:
        return jsonify({"error": "No holdings in this portfolio."}), 400

    tickers = holdings_df["ticker"].tolist()
    dollar_map = {r["ticker"]: float(r["dollar_amount"]) for _, r in holdings_df.iterrows()}
    total_dollars = sum(dollar_map.values())

    all_fetch = list(set(tickers + [benchmark]))

    # ── Fetch price + actions data ──────────────────────────────────────────
    try:
        raw_adj = yf.download(" ".join(all_fetch), **yf_kwargs,
                              progress=False, auto_adjust=True)
        raw_unadj = yf.download(" ".join(all_fetch), **yf_kwargs,
                                progress=False, auto_adjust=False, actions=True)
        if raw_adj.empty:
            return jsonify({"error": "No price data returned."}), 500
    except Exception as e:
        return jsonify({"error": f"yfinance error: {str(e)}"}), 500

    def _get_col(raw, col_name, ticker):
        if isinstance(raw.columns, pd.MultiIndex):
            if col_name in raw.columns.get_level_values(0) and ticker in raw.columns.get_level_values(1):
                return raw[col_name][ticker].dropna()
        else:
            if col_name in raw.columns:
                return raw[col_name].dropna()
        return pd.Series(dtype=float)

    # Benchmark returns
    bench_close = _get_col(raw_adj, "Close", benchmark)
    bench_ret = bench_close.pct_change().dropna() if len(bench_close) > 1 else None

    def safe(v):
        if v is None:
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return round(float(v), 4)

    # ── Per-ticker metrics ──────────────────────────────────────────────────
    holdings_out = []
    available_tickers = []
    returns_dict = {}

    for t in tickers:
        tc = _get_col(raw_adj, "Close", t)
        if len(tc) < 30:
            # Include row but metrics blank
            holdings_out.append({
                "ticker": t, "dollar_amount": dollar_map[t],
                "shares": None, "current_price": None, "weight_pct": None,
                "score": None, "grade": "--",
                "sharpe": None, "sortino": None, "calmar": None, "omega": None,
                "up_capture": None, "down_capture": None,
                "annual_ret": None, "annual_vol": None, "max_drawdown": None,
                "week52_high": None, "week52_low": None,
                "annual_yield_pct": None, "annual_income": None, "monthly_income": None,
                "error": "Insufficient price data"
            })
            continue

        current_price = float(tc.iloc[-1])
        shares = dollar_map[t] / current_price if current_price > 0 else None
        weight_pct = round(dollar_map[t] / total_dollars * 100, 2) if total_dollars > 0 else 0

        tr = tc.pct_change().dropna()
        up_cap, down_cap = _capture_ratios(tr, bench_ret) if bench_ret is not None else (None, None)
        annual_ret = round(float(tr.mean() * 252) * 100, 2) if len(tr) > 0 else None
        annual_vol = round(float(tr.std() * np.sqrt(252)) * 100, 2) if len(tr) > 0 else None
        mdd = _max_drawdown(tc)

        # 52-week high/low from unadjusted High/Low
        high_s = _get_col(raw_unadj, "High", t)
        low_s = _get_col(raw_unadj, "Low", t)
        w52_high = round(float(high_s.max()), 2) if len(high_s) > 0 else None
        w52_low = round(float(low_s.min()), 2) if len(low_s) > 0 else None

        # Dividend income from unadjusted Dividends column
        div_s = _get_col(raw_unadj, "Dividends", t)
        annual_income = 0.0
        annual_yield_pct = None
        if len(div_s) > 0:
            total_divs = float(div_s[div_s > 0].sum()) if (div_s > 0).any() else 0.0
            # Annualize based on period coverage
            n_days = max((tc.index[-1] - tc.index[0]).days, 1)
            annual_income = round(total_divs / n_days * 365.25 * (shares or 0), 2)
            if current_price > 0 and total_divs > 0:
                ann_yield = total_divs / n_days * 365.25
                annual_yield_pct = round(ann_yield / current_price * 100, 2)
                annual_yield_pct = min(annual_yield_pct, 50.0)  # cap
        monthly_income = round(annual_income / 12, 2)

        # Pre-compute metrics for scoring + table
        sharpe_v = safe(_sharpe(tc))
        sortino_v = safe(_sortino(tc))
        calmar_v = safe(_calmar(tc))
        omega_v = safe(_omega(tr))

        # Inline per-ticker scoring (mirrors _ticker_score logic; that fn is nested in optimizer)
        def _sm(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val >= exc: return 100.0
            if val >= good: return 80 + 20 * (val - good) / (exc - good)
            if val >= fair: return 60 + 20 * (val - fair) / (good - fair)
            if val >= poor: return 40 + 20 * (val - poor) / (fair - poor)
            return max(0.0, 40 * val / poor) if poor != 0 else 0.0

        def _sl(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val <= exc: return 100.0
            if val <= good: return 80 + 20 * (good - val) / (good - exc)
            if val <= fair: return 60 + 20 * (fair - val) / (fair - good)
            if val <= poor: return 40 + 20 * (poor - val) / (poor - fair)
            return max(0.0, 20.0)

        mdd_pct = safe(mdd) * 100 if safe(mdd) is not None else None
        sub_scores = {
            "sharpe":       _sm(sharpe_v,  (1.5, 1.0, 0.5, 0.0)),
            "sortino":      _sm(sortino_v, (2.0, 1.5, 1.0, 0.5)),
            "calmar":       _sm(calmar_v,  (1.5, 1.0, 0.5, 0.2)),
            "omega":        _sm(omega_v,   (2.0, 1.5, 1.2, 1.0)),
            "max_drawdown": _sl(abs(mdd_pct) if mdd_pct is not None else None, (10, 20, 30, 40)),
            "down_capture": _sl(safe(down_cap), (80, 90, 100, 120)),
        }
        gw = {"sharpe": 30, "sortino": 20, "calmar": 15,
              "omega": 15, "max_drawdown": 15, "down_capture": 5}
        tw = ts_acc = 0.0
        for k, w in gw.items():
            sc_val = sub_scores.get(k)
            if sc_val is not None:
                tw += w
                ts_acc += sc_val * w
        t_score = round(ts_acc / tw, 1) if tw > 0 else 0.0

        available_tickers.append(t)
        returns_dict[t] = tr

        holdings_out.append({
            "ticker": t,
            "dollar_amount": dollar_map[t],
            "shares": round(shares, 4) if shares is not None else None,
            "current_price": round(current_price, 2),
            "weight_pct": weight_pct,
            "score": t_score,
            "grade": _letter_grade(t_score),
            "sharpe": sharpe_v,
            "sortino": sortino_v,
            "calmar": calmar_v,
            "omega": omega_v,
            "up_capture": safe(up_cap),
            "down_capture": safe(down_cap),
            "annual_ret": annual_ret,
            "annual_vol": annual_vol,
            "max_drawdown": safe(mdd),
            "week52_high": w52_high,
            "week52_low": w52_low,
            "annual_yield_pct": annual_yield_pct,
            "annual_income": annual_income,
            "monthly_income": monthly_income,
        })

    # ── Portfolio-level metrics ─────────────────────────────────────────────
    port_metrics = {}
    result_corr = None
    result_dd = None

    if len(available_tickers) >= 1:
        returns_df = pd.DataFrame({t: returns_dict[t] for t in available_tickers}).dropna()
        weights_arr = np.array([dollar_map[t] for t in available_tickers])
        w_sum = weights_arr.sum()
        if w_sum > 0:
            weights_arr = weights_arr / w_sum

        port_daily = returns_df.dot(weights_arr) if len(available_tickers) > 1 else returns_df[available_tickers[0]]
        port_cum = (1 + port_daily).cumprod()
        port_mdd = safe(_max_drawdown(port_cum))

        port_metrics = {
            "sharpe": safe(_sharpe(port_cum)),
            "sortino": safe(_sortino(port_cum)),
            "calmar": safe(_calmar(port_cum)),
            "omega": safe(_omega(port_daily)),
            "max_drawdown": port_mdd,
        }

        if bench_ret is not None:
            aligned = pd.concat([port_daily, bench_ret], axis=1).dropna()
            if len(aligned) > 30:
                aligned.columns = ["port", "bench"]
                up_c, dn_c = _capture_ratios(aligned["port"], aligned["bench"])
                port_metrics["up_capture"] = safe(up_c)
                port_metrics["down_capture"] = safe(dn_c)

        wt_sorted = sorted(weights_arr, reverse=True)
        port_metrics["top_weight"] = round(float(wt_sorted[0]) * 100, 2) if len(wt_sorted) else 0
        hhi = float(sum(w ** 2 for w in weights_arr))
        port_metrics["effective_n"] = round(1 / hhi, 1) if hhi > 0 else 0
        port_metrics["n_holdings"] = len(available_tickers)
        port_metrics["total_value"] = round(total_dollars, 2)
        port_metrics["monthly_income"] = round(sum(h["monthly_income"] for h in holdings_out if h.get("monthly_income")), 2)
        port_metrics["annual_income"] = round(sum(h["annual_income"] for h in holdings_out if h.get("annual_income")), 2)

        # ── 7-factor grade (matching portfolio_analytics.html weights) ────
        def _sm(val, thr):
            if val is None:
                return None
            exc, good, fair, poor = thr
            if val >= exc: return 100.0
            if val >= good: return 80.0 + 20.0 * (val - good) / (exc - good)
            if val >= fair: return 60.0 + 20.0 * (val - fair) / (good - fair)
            if val >= poor: return 40.0 + 20.0 * (val - poor) / (fair - poor)
            return max(0.0, 40.0 * val / poor) if poor != 0 else 0.0

        def _sl(val, thr):
            if val is None:
                return None
            exc, good, fair, poor = thr
            if val <= exc: return 100.0
            if val <= good: return 80.0 + 20.0 * (good - val) / (good - exc)
            if val <= fair: return 60.0 + 20.0 * (fair - val) / (fair - good)
            if val <= poor: return 40.0 + 20.0 * (poor - val) / (poor - fair)
            return max(0.0, 20.0)

        mdd_pct = abs(float(port_mdd) * 100) if port_mdd is not None else None
        sub_scores = {
            "sharpe":          _sm(port_metrics.get("sharpe"),        (1.5, 1.0, 0.5, 0.0)),
            "sortino":         _sm(port_metrics.get("sortino"),       (2.0, 1.5, 1.0, 0.5)),
            "calmar":          _sm(port_metrics.get("calmar"),        (1.5, 1.0, 0.5, 0.2)),
            "omega":           _sm(port_metrics.get("omega"),         (2.0, 1.5, 1.2, 1.0)),
            "max_drawdown":    _sl(mdd_pct,                           (10,  20,  30,  40)),
            "down_capture":    _sl(port_metrics.get("down_capture"),  (80,  90,  100, 120)),
            "diversification": _sm(port_metrics.get("effective_n"),   (20,  12,  6,   3)),
        }
        grade_weights = {
            "sharpe": 25, "sortino": 15, "calmar": 10, "omega": 10,
            "max_drawdown": 20, "down_capture": 10, "diversification": 10,
        }
        label_map = {
            "sharpe": "Sharpe Ratio", "sortino": "Sortino Ratio",
            "calmar": "Calmar Ratio", "omega": "Omega Ratio",
            "max_drawdown": "Max Drawdown", "down_capture": "Downside Capture",
            "diversification": "Diversification",
        }
        total_w, total_sc = 0.0, 0.0
        breakdown = []
        for key, w in grade_weights.items():
            sc = sub_scores.get(key)
            if sc is not None:
                total_w += w
                total_sc += sc * w
                breakdown.append({
                    "category": label_map[key],
                    "score": round(sc, 1),
                    "weight": w,
                    "grade": _letter_grade(sc),
                    "key": key,
                })
        overall_score = round(total_sc / total_w, 1) if total_w > 0 else 0
        port_metrics["grade"] = {
            "overall": _letter_grade(overall_score),
            "score": overall_score,
            "breakdown": breakdown,
        }

        # Correlation matrix
        if len(available_tickers) >= 2:
            corr = returns_df.corr()
            result_corr = {
                "labels": available_tickers,
                "matrix": [[round(float(corr.loc[a, b]), 3) for b in available_tickers]
                           for a in available_tickers],
            }

            # Drawdown series (≤200 pts)
            roll_max = port_cum.cummax()
            dd_s = (port_cum / roll_max - 1)
            step = max(1, len(dd_s) // 200)
            dd_sampled = dd_s.iloc[::step]
            result_dd = {
                "dates": [d.strftime("%Y-%m-%d") for d in dd_sampled.index],
                "values": [round(float(v) * 100, 2) for v in dd_sampled.values],
            }

    result = {
        "portfolio_id": pid_,
        "portfolio_name": meta["name"].iloc[0],
        "portfolio_metrics": port_metrics,
        "holdings": holdings_out,
    }
    if result_corr:
        result["correlation"] = result_corr
    if result_dd:
        result["drawdown_series"] = result_dd

    return jsonify(result)


@app.route("/portfolio_builder/compare", methods=["POST"])
def pb_compare():
    """AJAX: compare multiple builder portfolios — returns metrics for charts."""
    import warnings, math
    import numpy as np
    warnings.filterwarnings("ignore")
    import yfinance as yf

    pid = get_profile_id()
    data = request.get_json(force=True)
    portfolio_ids = data.get("portfolio_ids", [])
    period = data.get("period", "1y")
    benchmark = data.get("benchmark", "SPY")

    if not portfolio_ids:
        return jsonify({"error": "No portfolio IDs provided."}), 400

    period_map = {
        "3mo": dict(period="3mo"), "6mo": dict(period="6mo"),
        "1y": dict(period="1y"), "2y": dict(period="2y"), "5y": dict(period="5y"),
        "ytd": dict(period="ytd"), "max": dict(period="max"),
    }
    yf_kwargs = period_map.get(period, dict(period="1y"))

    conn = get_connection()
    results = []

    for port_id in portfolio_ids:
        try:
            meta = pd.read_sql(
                "SELECT id, name FROM dbo.builder_portfolios WHERE id=? AND profile_id=?",
                conn, params=[port_id, pid]
            )
            if meta.empty:
                continue
            holdings_df = pd.read_sql(
                "SELECT ticker, dollar_amount FROM dbo.builder_holdings WHERE portfolio_id=?",
                conn, params=[port_id]
            )
        except Exception:
            continue

        if holdings_df.empty:
            continue

        tickers = holdings_df["ticker"].tolist()
        dollar_map = {r["ticker"]: float(r["dollar_amount"]) for _, r in holdings_df.iterrows()}

        all_fetch = list(set(tickers + [benchmark]))
        try:
            raw_adj = yf.download(" ".join(all_fetch), **yf_kwargs,
                                  progress=False, auto_adjust=True)
            raw_unadj = yf.download(" ".join(all_fetch), **yf_kwargs,
                                    progress=False, auto_adjust=False, actions=True)
            if raw_adj.empty:
                continue
        except Exception:
            continue

        def _gc(raw, col_name, ticker):
            if isinstance(raw.columns, pd.MultiIndex):
                if col_name in raw.columns.get_level_values(0) and ticker in raw.columns.get_level_values(1):
                    return raw[col_name][ticker].dropna()
            else:
                if col_name in raw.columns:
                    return raw[col_name].dropna()
            return pd.Series(dtype=float)

        def safe(v):
            if v is None: return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
            return round(float(v), 4)

        bench_close = _gc(raw_adj, "Close", benchmark)
        bench_ret = bench_close.pct_change().dropna() if len(bench_close) > 1 else None

        available = []
        returns_dict_c = {}
        monthly_income = 0.0

        for t in tickers:
            tc = _gc(raw_adj, "Close", t)
            if len(tc) < 30:
                continue
            tr = tc.pct_change().dropna()
            available.append(t)
            returns_dict_c[t] = tr

            current_price = float(tc.iloc[-1])
            shares = dollar_map[t] / current_price if current_price > 0 else 0
            div_s = _gc(raw_unadj, "Dividends", t)
            if len(div_s) > 0 and (div_s > 0).any():
                total_divs = float(div_s[div_s > 0].sum())
                n_days = max((tc.index[-1] - tc.index[0]).days, 1)
                monthly_income += total_divs / n_days * 365.25 * shares / 12

        if not available:
            continue

        returns_df = pd.DataFrame({t: returns_dict_c[t] for t in available}).dropna()
        weights_arr = np.array([dollar_map[t] for t in available])
        w_sum = weights_arr.sum()
        if w_sum > 0:
            weights_arr = weights_arr / w_sum

        port_daily = returns_df.dot(weights_arr) if len(available) > 1 else returns_df[available[0]]
        port_cum = (1 + port_daily).cumprod()
        port_mdd_raw = _max_drawdown(port_cum)
        mdd_pct = abs(float(port_mdd_raw) * 100) if port_mdd_raw is not None else None

        hhi = float(sum(w ** 2 for w in weights_arr))
        eff_n = round(1 / hhi, 1) if hhi > 0 else 0

        pm = {
            "sharpe": safe(_sharpe(port_cum)),
            "sortino": safe(_sortino(port_cum)),
            "calmar": safe(_calmar(port_cum)),
            "omega": safe(_omega(port_daily)),
            "max_drawdown": safe(port_mdd_raw),
            "effective_n": eff_n,
        }
        if bench_ret is not None:
            aligned = pd.concat([port_daily, bench_ret], axis=1).dropna()
            if len(aligned) > 30:
                aligned.columns = ["port", "bench"]
                _, dn_c = _capture_ratios(aligned["port"], aligned["bench"])
                pm["down_capture"] = safe(dn_c)

        def _sm2(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val >= exc: return 100.0
            if val >= good: return 80.0 + 20.0 * (val - good) / (exc - good)
            if val >= fair: return 60.0 + 20.0 * (val - fair) / (good - fair)
            if val >= poor: return 40.0 + 20.0 * (val - poor) / (fair - poor)
            return max(0.0, 40.0 * val / poor) if poor != 0 else 0.0

        def _sl2(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val <= exc: return 100.0
            if val <= good: return 80.0 + 20.0 * (good - val) / (good - exc)
            if val <= fair: return 60.0 + 20.0 * (fair - val) / (fair - good)
            if val <= poor: return 40.0 + 20.0 * (poor - val) / (poor - fair)
            return max(0.0, 20.0)

        sub_scores = {
            "sharpe":          _sm2(pm.get("sharpe"),        (1.5, 1.0, 0.5, 0.0)),
            "sortino":         _sm2(pm.get("sortino"),       (2.0, 1.5, 1.0, 0.5)),
            "calmar":          _sm2(pm.get("calmar"),        (1.5, 1.0, 0.5, 0.2)),
            "omega":           _sm2(pm.get("omega"),         (2.0, 1.5, 1.2, 1.0)),
            "max_drawdown":    _sl2(mdd_pct,                 (10,  20,  30,  40)),
            "down_capture":    _sl2(pm.get("down_capture"),  (80,  90,  100, 120)),
            "diversification": _sm2(eff_n,                   (20,  12,  6,   3)),
        }
        gw = {"sharpe": 25, "sortino": 15, "calmar": 10, "omega": 10,
              "max_drawdown": 20, "down_capture": 10, "diversification": 10}
        total_w2, total_sc2 = 0.0, 0.0
        for k, w in gw.items():
            sc = sub_scores.get(k)
            if sc is not None:
                total_w2 += w
                total_sc2 += sc * w
        overall_score = round(total_sc2 / total_w2, 1) if total_w2 > 0 else 0

        results.append({
            "id": int(port_id),
            "name": str(meta["name"].iloc[0]),
            "score": overall_score,
            "grade": _letter_grade(overall_score),
            "monthly_income": round(monthly_income, 2),
            "sharpe": pm.get("sharpe"),
            "sortino": pm.get("sortino"),
            "calmar": pm.get("calmar"),
            "omega": pm.get("omega"),
            "max_drawdown": pm.get("max_drawdown"),
            "effective_n": eff_n,
            "breakdown_scores": {k: round(v, 1) if v is not None else None
                                 for k, v in sub_scores.items()},
        })

    conn.close()
    return jsonify({"portfolios": results})


# ── Portfolio Simulator routes ────────────────────────────────────────────────

@app.route("/portfolio_simulator")
def portfolio_simulator():
    """Portfolio Simulator — budget-constrained paper trading simulation."""
    pid = get_profile_id()
    conn = get_connection()
    ensure_tables_exist(conn)
    try:
        portfolios = pd.read_sql("""
            SELECT sp.id, sp.name, sp.notes, sp.budget, sp.updated_at,
                   COUNT(sh.id) AS holding_count
            FROM dbo.simulator_portfolios sp
            LEFT JOIN dbo.simulator_holdings sh ON sh.portfolio_id = sp.id
            WHERE sp.profile_id = ?
            GROUP BY sp.id, sp.name, sp.notes, sp.budget, sp.updated_at
            ORDER BY sp.updated_at DESC
        """, conn, params=[pid])
    except Exception:
        portfolios = pd.DataFrame(columns=["id", "name", "notes", "budget", "updated_at", "holding_count"])
    conn.close()
    port_list = portfolios.to_dict("records") if not portfolios.empty else []
    return render_template("portfolio_simulator.html", portfolios=port_list)


@app.route("/portfolio_simulator/portfolios", methods=["POST"])
def sim_create_portfolio():
    """Create a new named simulator portfolio with optional budget."""
    pid = get_profile_id()
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    notes = (data.get("notes") or "").strip()
    if not name:
        return jsonify({"error": "Name is required."}), 400
    budget_raw = data.get("budget")
    budget = None
    if budget_raw is not None and str(budget_raw).strip() != "":
        try:
            budget = float(budget_raw)
            if budget <= 0:
                budget = None
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid budget value."}), 400
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO dbo.simulator_portfolios (profile_id, name, notes, budget)
            VALUES (?, ?, ?, ?)
        """, pid, name, notes or None, budget)
        conn.commit()
        cursor.execute(
            "SELECT id FROM dbo.simulator_portfolios WHERE profile_id=? AND name=?",
            pid, name
        )
        row = cursor.fetchone()
        new_id = row[0] if row else None
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"id": new_id, "name": name, "notes": notes, "budget": budget})


@app.route("/portfolio_simulator/portfolios/<int:pid_>", methods=["PATCH"])
def sim_update_portfolio(pid_):
    """Rename a simulator portfolio or update its budget/notes."""
    pid = get_profile_id()
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    notes = (data.get("notes") or "").strip()
    if not name:
        return jsonify({"error": "Name is required."}), 400
    budget_raw = data.get("budget")
    budget = None
    if budget_raw is not None and str(budget_raw).strip() != "":
        try:
            budget = float(budget_raw)
            if budget <= 0:
                budget = None
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid budget value."}), 400
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE dbo.simulator_portfolios
            SET name=?, notes=?, budget=?, updated_at=GETDATE()
            WHERE id=? AND profile_id=?
        """, name, notes or None, budget, pid_, pid)
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"id": pid_, "name": name, "notes": notes, "budget": budget})


@app.route("/portfolio_simulator/portfolios/<int:pid_>", methods=["DELETE"])
def sim_delete_portfolio(pid_):
    """Delete a simulator portfolio and all its holdings."""
    pid = get_profile_id()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM dbo.simulator_holdings WHERE portfolio_id=?", pid_
        )
        cursor.execute(
            "DELETE FROM dbo.simulator_portfolios WHERE id=? AND profile_id=?", pid_, pid
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"deleted": pid_})


@app.route("/portfolio_simulator/portfolios/<int:pid_>/holdings", methods=["GET"])
def sim_get_holdings(pid_):
    """Return holdings list + budget + bank_balance for a simulator portfolio."""
    pid = get_profile_id()
    conn = get_connection()
    try:
        port_row = pd.read_sql(
            "SELECT id, budget FROM dbo.simulator_portfolios WHERE id=? AND profile_id=?",
            conn, params=[pid_, pid]
        )
        if port_row.empty:
            conn.close()
            return jsonify({"error": "Not found."}), 404
        budget_val = port_row["budget"].iloc[0]
        budget = float(budget_val) if budget_val is not None and not pd.isna(budget_val) else None
        df = pd.read_sql("""
            SELECT ticker, dollar_amount
            FROM dbo.simulator_holdings
            WHERE portfolio_id=?
            ORDER BY added_at
        """, conn, params=[pid_])
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    total_held = float(df["dollar_amount"].sum()) if not df.empty else 0.0
    bank_balance = round(budget - total_held, 2) if budget is not None else None
    return jsonify({"holdings": df.to_dict("records"), "budget": budget, "bank_balance": bank_balance})


@app.route("/portfolio_simulator/portfolios/<int:pid_>/holdings", methods=["POST"])
def sim_add_holding(pid_):
    """Add or update a holding; enforces budget if set."""
    pid = get_profile_id()
    data = request.get_json(force=True)
    ticker = (data.get("ticker") or "").strip().upper()
    try:
        dollar_amount = float(data.get("dollar_amount", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid dollar amount."}), 400
    if not ticker:
        return jsonify({"error": "Ticker is required."}), 400
    if dollar_amount <= 0:
        return jsonify({"error": "Dollar amount must be positive."}), 400
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT budget FROM dbo.simulator_portfolios WHERE id=? AND profile_id=?", pid_, pid
        )
        port_row = cursor.fetchone()
        if not port_row:
            conn.close()
            return jsonify({"error": "Not found."}), 404
        budget_val = port_row[0]
        budget = float(budget_val) if budget_val is not None else None

        if budget is not None:
            cursor.execute(
                "SELECT ISNULL(SUM(dollar_amount), 0) FROM dbo.simulator_holdings WHERE portfolio_id=?",
                pid_
            )
            current_total = float(cursor.fetchone()[0])
            cursor.execute(
                "SELECT ISNULL(dollar_amount, 0) FROM dbo.simulator_holdings WHERE portfolio_id=? AND ticker=?",
                pid_, ticker
            )
            old_row = cursor.fetchone()
            old_amount = float(old_row[0]) if old_row else 0.0
            projected = current_total - old_amount + dollar_amount
            if projected > budget:
                available = round(budget - (current_total - old_amount), 2)
                conn.close()
                return jsonify({"error": f"Exceeds budget. Available: ${available:,.2f}", "available": available}), 400

        cursor.execute("""
            MERGE dbo.simulator_holdings AS tgt
            USING (SELECT ? AS portfolio_id, ? AS ticker, ? AS dollar_amount) AS src
              ON tgt.portfolio_id = src.portfolio_id AND tgt.ticker = src.ticker
            WHEN MATCHED THEN
              UPDATE SET dollar_amount = src.dollar_amount
            WHEN NOT MATCHED THEN
              INSERT (portfolio_id, ticker, dollar_amount) VALUES (src.portfolio_id, src.ticker, src.dollar_amount);
        """, pid_, ticker, dollar_amount)
        cursor.execute(
            "UPDATE dbo.simulator_portfolios SET updated_at=GETDATE() WHERE id=?", pid_
        )
        conn.commit()

        bank_balance = None
        if budget is not None:
            cursor.execute(
                "SELECT ISNULL(SUM(dollar_amount), 0) FROM dbo.simulator_holdings WHERE portfolio_id=?",
                pid_
            )
            new_total = float(cursor.fetchone()[0])
            bank_balance = round(budget - new_total, 2)
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"portfolio_id": pid_, "ticker": ticker, "dollar_amount": dollar_amount,
                    "budget": budget, "bank_balance": bank_balance})


@app.route("/portfolio_simulator/portfolios/<int:pid_>/holdings/<ticker>", methods=["DELETE"])
def sim_delete_holding(pid_, ticker):
    """Remove a single holding from a simulator portfolio."""
    pid = get_profile_id()
    ticker = ticker.strip().upper()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id FROM dbo.simulator_portfolios WHERE id=? AND profile_id=?", pid_, pid
        )
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": "Not found."}), 404
        cursor.execute(
            "DELETE FROM dbo.simulator_holdings WHERE portfolio_id=? AND ticker=?", pid_, ticker
        )
        cursor.execute(
            "UPDATE dbo.simulator_portfolios SET updated_at=GETDATE() WHERE id=?", pid_
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"deleted": ticker})


@app.route("/portfolio_simulator/portfolios/<int:pid_>/holdings/<ticker>/sell", methods=["POST"])
def sim_sell_holding(pid_, ticker):
    """Sell a percentage (0–100) of a holding; proceeds return to bank."""
    pid = get_profile_id()
    ticker = ticker.strip().upper()
    data = request.get_json(force=True)
    try:
        sell_pct = float(data.get("sell_pct", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid sell_pct."}), 400
    if sell_pct <= 0 or sell_pct > 100:
        return jsonify({"error": "sell_pct must be between 0 and 100."}), 400
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT budget FROM dbo.simulator_portfolios WHERE id=? AND profile_id=?", pid_, pid
        )
        port_row = cursor.fetchone()
        if not port_row:
            conn.close()
            return jsonify({"error": "Not found."}), 404
        budget_val = port_row[0]
        budget = float(budget_val) if budget_val is not None else None

        cursor.execute(
            "SELECT dollar_amount FROM dbo.simulator_holdings WHERE portfolio_id=? AND ticker=?",
            pid_, ticker
        )
        h_row = cursor.fetchone()
        if not h_row:
            conn.close()
            return jsonify({"error": "Holding not found."}), 404
        current_amount = float(h_row[0])
        sold_amount = round(sell_pct / 100.0 * current_amount, 2)

        if sell_pct >= 100:
            cursor.execute(
                "DELETE FROM dbo.simulator_holdings WHERE portfolio_id=? AND ticker=?", pid_, ticker
            )
            new_dollar_amount = None
        else:
            new_dollar_amount = round(current_amount - sold_amount, 2)
            cursor.execute(
                "UPDATE dbo.simulator_holdings SET dollar_amount=? WHERE portfolio_id=? AND ticker=?",
                new_dollar_amount, pid_, ticker
            )
        cursor.execute(
            "UPDATE dbo.simulator_portfolios SET updated_at=GETDATE() WHERE id=?", pid_
        )
        conn.commit()

        bank_balance = None
        if budget is not None:
            cursor.execute(
                "SELECT ISNULL(SUM(dollar_amount), 0) FROM dbo.simulator_holdings WHERE portfolio_id=?",
                pid_
            )
            new_total = float(cursor.fetchone()[0])
            bank_balance = round(budget - new_total, 2)
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"ticker": ticker, "new_dollar_amount": new_dollar_amount,
                    "sold_amount": sold_amount, "budget": budget, "bank_balance": bank_balance})


@app.route("/portfolio_simulator/portfolios/<int:pid_>/analyze")
def sim_analyze(pid_):
    """AJAX: score all holdings in a simulator portfolio and return full metrics."""
    import warnings, math
    import numpy as np
    warnings.filterwarnings("ignore")
    import yfinance as yf

    pid = get_profile_id()
    benchmark = request.args.get("benchmark", "SPY")
    period = request.args.get("period", "1y")

    from datetime import date as date_type
    period_map = {
        "3mo": dict(period="3mo"), "6mo": dict(period="6mo"),
        "1y": dict(period="1y"), "2y": dict(period="2y"), "5y": dict(period="5y"),
        "ytd": dict(period="ytd"), "max": dict(period="max"),
    }
    if period.isdigit() and len(period) == 4:
        yr = int(period)
        today = date_type.today()
        start = f"{yr}-01-01"
        end = today.strftime("%Y-%m-%d") if yr == today.year else f"{yr}-12-31"
        yf_kwargs = dict(start=start, end=end)
    else:
        yf_kwargs = period_map.get(period, dict(period="1y"))

    conn = get_connection()
    try:
        meta = pd.read_sql(
            "SELECT id, name, budget FROM dbo.simulator_portfolios WHERE id=? AND profile_id=?",
            conn, params=[pid_, pid]
        )
        if meta.empty:
            conn.close()
            return jsonify({"error": "Portfolio not found."}), 404
        holdings_df = pd.read_sql(
            "SELECT ticker, dollar_amount FROM dbo.simulator_holdings WHERE portfolio_id=? ORDER BY added_at",
            conn, params=[pid_]
        )
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()

    if holdings_df.empty:
        return jsonify({"error": "No holdings in this portfolio."}), 400

    budget_val = meta["budget"].iloc[0]
    budget = float(budget_val) if budget_val is not None and not pd.isna(budget_val) else None

    tickers = holdings_df["ticker"].tolist()
    dollar_map = {r["ticker"]: float(r["dollar_amount"]) for _, r in holdings_df.iterrows()}
    total_dollars = sum(dollar_map.values())
    bank_balance = round(budget - total_dollars, 2) if budget is not None else None

    all_fetch = list(set(tickers + [benchmark]))

    try:
        raw_adj = yf.download(" ".join(all_fetch), **yf_kwargs,
                              progress=False, auto_adjust=True)
        raw_unadj = yf.download(" ".join(all_fetch), **yf_kwargs,
                                progress=False, auto_adjust=False, actions=True)
        if raw_adj.empty:
            return jsonify({"error": "No price data returned."}), 500
    except Exception as e:
        return jsonify({"error": f"yfinance error: {str(e)}"}), 500

    def _get_col(raw, col_name, ticker):
        if isinstance(raw.columns, pd.MultiIndex):
            if col_name in raw.columns.get_level_values(0) and ticker in raw.columns.get_level_values(1):
                return raw[col_name][ticker].dropna()
        else:
            if col_name in raw.columns:
                return raw[col_name].dropna()
        return pd.Series(dtype=float)

    bench_close = _get_col(raw_adj, "Close", benchmark)
    bench_ret = bench_close.pct_change().dropna() if len(bench_close) > 1 else None

    def safe(v):
        if v is None:
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return round(float(v), 4)

    holdings_out = []
    available_tickers = []
    returns_dict = {}

    for t in tickers:
        tc = _get_col(raw_adj, "Close", t)
        if len(tc) < 30:
            holdings_out.append({
                "ticker": t, "dollar_amount": dollar_map[t],
                "shares": None, "current_price": None, "weight_pct": None,
                "score": None, "grade": "--",
                "sharpe": None, "sortino": None, "calmar": None, "omega": None,
                "up_capture": None, "down_capture": None,
                "annual_ret": None, "annual_vol": None, "max_drawdown": None,
                "week52_high": None, "week52_low": None,
                "annual_yield_pct": None, "annual_income": None, "monthly_income": None,
                "error": "Insufficient price data"
            })
            continue

        current_price = float(tc.iloc[-1])
        shares = dollar_map[t] / current_price if current_price > 0 else None
        weight_pct = round(dollar_map[t] / total_dollars * 100, 2) if total_dollars > 0 else 0

        tr = tc.pct_change().dropna()
        up_cap, down_cap = _capture_ratios(tr, bench_ret) if bench_ret is not None else (None, None)
        annual_ret = round(float(tr.mean() * 252) * 100, 2) if len(tr) > 0 else None
        annual_vol = round(float(tr.std() * np.sqrt(252)) * 100, 2) if len(tr) > 0 else None
        mdd = _max_drawdown(tc)

        high_s = _get_col(raw_unadj, "High", t)
        low_s = _get_col(raw_unadj, "Low", t)
        w52_high = round(float(high_s.max()), 2) if len(high_s) > 0 else None
        w52_low = round(float(low_s.min()), 2) if len(low_s) > 0 else None

        div_s = _get_col(raw_unadj, "Dividends", t)
        annual_income = 0.0
        annual_yield_pct = None
        if len(div_s) > 0:
            total_divs = float(div_s[div_s > 0].sum()) if (div_s > 0).any() else 0.0
            n_days = max((tc.index[-1] - tc.index[0]).days, 1)
            annual_income = round(total_divs / n_days * 365.25 * (shares or 0), 2)
            if current_price > 0 and total_divs > 0:
                ann_yield = total_divs / n_days * 365.25
                annual_yield_pct = round(ann_yield / current_price * 100, 2)
                annual_yield_pct = min(annual_yield_pct, 50.0)
        monthly_income = round(annual_income / 12, 2)

        sharpe_v = safe(_sharpe(tc))
        sortino_v = safe(_sortino(tc))
        calmar_v = safe(_calmar(tc))
        omega_v = safe(_omega(tr))

        def _sm(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val >= exc: return 100.0
            if val >= good: return 80 + 20 * (val - good) / (exc - good)
            if val >= fair: return 60 + 20 * (val - fair) / (good - fair)
            if val >= poor: return 40 + 20 * (val - poor) / (fair - poor)
            return max(0.0, 40 * val / poor) if poor != 0 else 0.0

        def _sl(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val <= exc: return 100.0
            if val <= good: return 80 + 20 * (good - val) / (good - exc)
            if val <= fair: return 60 + 20 * (fair - val) / (fair - good)
            if val <= poor: return 40 + 20 * (poor - val) / (poor - fair)
            return max(0.0, 20.0)

        mdd_pct = safe(mdd) * 100 if safe(mdd) is not None else None
        sub_scores = {
            "sharpe":       _sm(sharpe_v,  (1.5, 1.0, 0.5, 0.0)),
            "sortino":      _sm(sortino_v, (2.0, 1.5, 1.0, 0.5)),
            "calmar":       _sm(calmar_v,  (1.5, 1.0, 0.5, 0.2)),
            "omega":        _sm(omega_v,   (2.0, 1.5, 1.2, 1.0)),
            "max_drawdown": _sl(abs(mdd_pct) if mdd_pct is not None else None, (10, 20, 30, 40)),
            "down_capture": _sl(safe(down_cap), (80, 90, 100, 120)),
        }
        gw = {"sharpe": 30, "sortino": 20, "calmar": 15,
              "omega": 15, "max_drawdown": 15, "down_capture": 5}
        tw = ts_acc = 0.0
        for k, w in gw.items():
            sc_val = sub_scores.get(k)
            if sc_val is not None:
                tw += w
                ts_acc += sc_val * w
        t_score = round(ts_acc / tw, 1) if tw > 0 else 0.0

        available_tickers.append(t)
        returns_dict[t] = tr

        holdings_out.append({
            "ticker": t,
            "dollar_amount": dollar_map[t],
            "shares": round(shares, 4) if shares is not None else None,
            "current_price": round(current_price, 2),
            "weight_pct": weight_pct,
            "score": t_score,
            "grade": _letter_grade(t_score),
            "sharpe": sharpe_v,
            "sortino": sortino_v,
            "calmar": calmar_v,
            "omega": omega_v,
            "up_capture": safe(up_cap),
            "down_capture": safe(down_cap),
            "annual_ret": annual_ret,
            "annual_vol": annual_vol,
            "max_drawdown": safe(mdd),
            "week52_high": w52_high,
            "week52_low": w52_low,
            "annual_yield_pct": annual_yield_pct,
            "annual_income": annual_income,
            "monthly_income": monthly_income,
        })

    port_metrics = {}
    result_corr = None
    result_dd = None

    if len(available_tickers) >= 1:
        returns_df = pd.DataFrame({t: returns_dict[t] for t in available_tickers}).dropna()
        weights_arr = np.array([dollar_map[t] for t in available_tickers])
        w_sum = weights_arr.sum()
        if w_sum > 0:
            weights_arr = weights_arr / w_sum

        port_daily = returns_df.dot(weights_arr) if len(available_tickers) > 1 else returns_df[available_tickers[0]]
        port_cum = (1 + port_daily).cumprod()
        port_mdd = safe(_max_drawdown(port_cum))

        port_metrics = {
            "sharpe": safe(_sharpe(port_cum)),
            "sortino": safe(_sortino(port_cum)),
            "calmar": safe(_calmar(port_cum)),
            "omega": safe(_omega(port_daily)),
            "max_drawdown": port_mdd,
        }

        if bench_ret is not None:
            aligned = pd.concat([port_daily, bench_ret], axis=1).dropna()
            if len(aligned) > 30:
                aligned.columns = ["port", "bench"]
                up_c, dn_c = _capture_ratios(aligned["port"], aligned["bench"])
                port_metrics["up_capture"] = safe(up_c)
                port_metrics["down_capture"] = safe(dn_c)

        wt_sorted = sorted(weights_arr, reverse=True)
        port_metrics["top_weight"] = round(float(wt_sorted[0]) * 100, 2) if len(wt_sorted) else 0
        hhi = float(sum(w ** 2 for w in weights_arr))
        port_metrics["effective_n"] = round(1 / hhi, 1) if hhi > 0 else 0
        port_metrics["n_holdings"] = len(available_tickers)
        port_metrics["total_value"] = round(total_dollars, 2)
        port_metrics["budget"] = budget
        port_metrics["bank_balance"] = bank_balance
        port_metrics["monthly_income"] = round(sum(h["monthly_income"] for h in holdings_out if h.get("monthly_income")), 2)
        port_metrics["annual_income"] = round(sum(h["annual_income"] for h in holdings_out if h.get("annual_income")), 2)

        def _sm(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val >= exc: return 100.0
            if val >= good: return 80.0 + 20.0 * (val - good) / (exc - good)
            if val >= fair: return 60.0 + 20.0 * (val - fair) / (good - fair)
            if val >= poor: return 40.0 + 20.0 * (val - poor) / (fair - poor)
            return max(0.0, 40.0 * val / poor) if poor != 0 else 0.0

        def _sl(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val <= exc: return 100.0
            if val <= good: return 80.0 + 20.0 * (good - val) / (good - exc)
            if val <= fair: return 60.0 + 20.0 * (fair - val) / (fair - good)
            if val <= poor: return 40.0 + 20.0 * (poor - val) / (poor - fair)
            return max(0.0, 20.0)

        mdd_pct = abs(float(port_mdd) * 100) if port_mdd is not None else None
        sub_scores = {
            "sharpe":          _sm(port_metrics.get("sharpe"),        (1.5, 1.0, 0.5, 0.0)),
            "sortino":         _sm(port_metrics.get("sortino"),       (2.0, 1.5, 1.0, 0.5)),
            "calmar":          _sm(port_metrics.get("calmar"),        (1.5, 1.0, 0.5, 0.2)),
            "omega":           _sm(port_metrics.get("omega"),         (2.0, 1.5, 1.2, 1.0)),
            "max_drawdown":    _sl(mdd_pct,                           (10,  20,  30,  40)),
            "down_capture":    _sl(port_metrics.get("down_capture"),  (80,  90,  100, 120)),
            "diversification": _sm(port_metrics.get("effective_n"),   (20,  12,  6,   3)),
        }
        grade_weights = {
            "sharpe": 25, "sortino": 15, "calmar": 10, "omega": 10,
            "max_drawdown": 20, "down_capture": 10, "diversification": 10,
        }
        label_map = {
            "sharpe": "Sharpe Ratio", "sortino": "Sortino Ratio",
            "calmar": "Calmar Ratio", "omega": "Omega Ratio",
            "max_drawdown": "Max Drawdown", "down_capture": "Downside Capture",
            "diversification": "Diversification",
        }
        total_w, total_sc = 0.0, 0.0
        breakdown = []
        for key, w in grade_weights.items():
            sc = sub_scores.get(key)
            if sc is not None:
                total_w += w
                total_sc += sc * w
                breakdown.append({
                    "category": label_map[key],
                    "score": round(sc, 1),
                    "weight": w,
                    "grade": _letter_grade(sc),
                    "key": key,
                })
        overall_score = round(total_sc / total_w, 1) if total_w > 0 else 0
        port_metrics["grade"] = {
            "overall": _letter_grade(overall_score),
            "score": overall_score,
            "breakdown": breakdown,
        }

        if len(available_tickers) >= 2:
            corr = returns_df.corr()
            result_corr = {
                "labels": available_tickers,
                "matrix": [[round(float(corr.loc[a, b]), 3) for b in available_tickers]
                           for a in available_tickers],
            }
            roll_max = port_cum.cummax()
            dd_s = (port_cum / roll_max - 1)
            step = max(1, len(dd_s) // 200)
            dd_sampled = dd_s.iloc[::step]
            result_dd = {
                "dates": [d.strftime("%Y-%m-%d") for d in dd_sampled.index],
                "values": [round(float(v) * 100, 2) for v in dd_sampled.values],
            }

    result = {
        "portfolio_id": pid_,
        "portfolio_name": meta["name"].iloc[0],
        "portfolio_metrics": port_metrics,
        "holdings": holdings_out,
    }
    if result_corr:
        result["correlation"] = result_corr
    if result_dd:
        result["drawdown_series"] = result_dd

    return jsonify(result)


@app.route("/portfolio_simulator/compare", methods=["POST"])
def sim_compare():
    """AJAX: compare multiple simulator portfolios — returns metrics for charts."""
    import warnings, math
    import numpy as np
    warnings.filterwarnings("ignore")
    import yfinance as yf

    pid = get_profile_id()
    data = request.get_json(force=True)
    portfolio_ids = data.get("portfolio_ids", [])
    period = data.get("period", "1y")
    benchmark = data.get("benchmark", "SPY")

    if not portfolio_ids:
        return jsonify({"error": "No portfolio IDs provided."}), 400

    period_map = {
        "3mo": dict(period="3mo"), "6mo": dict(period="6mo"),
        "1y": dict(period="1y"), "2y": dict(period="2y"), "5y": dict(period="5y"),
        "ytd": dict(period="ytd"), "max": dict(period="max"),
    }
    yf_kwargs = period_map.get(period, dict(period="1y"))

    conn = get_connection()
    results = []

    for port_id in portfolio_ids:
        try:
            meta = pd.read_sql(
                "SELECT id, name FROM dbo.simulator_portfolios WHERE id=? AND profile_id=?",
                conn, params=[port_id, pid]
            )
            if meta.empty:
                continue
            holdings_df = pd.read_sql(
                "SELECT ticker, dollar_amount FROM dbo.simulator_holdings WHERE portfolio_id=?",
                conn, params=[port_id]
            )
        except Exception:
            continue

        if holdings_df.empty:
            continue

        tickers = holdings_df["ticker"].tolist()
        dollar_map = {r["ticker"]: float(r["dollar_amount"]) for _, r in holdings_df.iterrows()}

        all_fetch = list(set(tickers + [benchmark]))
        try:
            raw_adj = yf.download(" ".join(all_fetch), **yf_kwargs,
                                  progress=False, auto_adjust=True)
            raw_unadj = yf.download(" ".join(all_fetch), **yf_kwargs,
                                    progress=False, auto_adjust=False, actions=True)
            if raw_adj.empty:
                continue
        except Exception:
            continue

        def _gc(raw, col_name, ticker):
            if isinstance(raw.columns, pd.MultiIndex):
                if col_name in raw.columns.get_level_values(0) and ticker in raw.columns.get_level_values(1):
                    return raw[col_name][ticker].dropna()
            else:
                if col_name in raw.columns:
                    return raw[col_name].dropna()
            return pd.Series(dtype=float)

        def safe(v):
            if v is None: return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
            return round(float(v), 4)

        bench_close = _gc(raw_adj, "Close", benchmark)
        bench_ret = bench_close.pct_change().dropna() if len(bench_close) > 1 else None

        available = []
        returns_dict_c = {}
        monthly_income = 0.0

        for t in tickers:
            tc = _gc(raw_adj, "Close", t)
            if len(tc) < 30:
                continue
            tr = tc.pct_change().dropna()
            available.append(t)
            returns_dict_c[t] = tr

            current_price = float(tc.iloc[-1])
            shares = dollar_map[t] / current_price if current_price > 0 else 0
            div_s = _gc(raw_unadj, "Dividends", t)
            if len(div_s) > 0 and (div_s > 0).any():
                total_divs = float(div_s[div_s > 0].sum())
                n_days = max((tc.index[-1] - tc.index[0]).days, 1)
                monthly_income += total_divs / n_days * 365.25 * shares / 12

        if not available:
            continue

        returns_df = pd.DataFrame({t: returns_dict_c[t] for t in available}).dropna()
        weights_arr = np.array([dollar_map[t] for t in available])
        w_sum = weights_arr.sum()
        if w_sum > 0:
            weights_arr = weights_arr / w_sum

        port_daily = returns_df.dot(weights_arr) if len(available) > 1 else returns_df[available[0]]
        port_cum = (1 + port_daily).cumprod()
        port_mdd_raw = _max_drawdown(port_cum)
        mdd_pct = abs(float(port_mdd_raw) * 100) if port_mdd_raw is not None else None

        hhi = float(sum(w ** 2 for w in weights_arr))
        eff_n = round(1 / hhi, 1) if hhi > 0 else 0

        pm = {
            "sharpe": safe(_sharpe(port_cum)),
            "sortino": safe(_sortino(port_cum)),
            "calmar": safe(_calmar(port_cum)),
            "omega": safe(_omega(port_daily)),
            "max_drawdown": safe(port_mdd_raw),
            "effective_n": eff_n,
        }
        if bench_ret is not None:
            aligned = pd.concat([port_daily, bench_ret], axis=1).dropna()
            if len(aligned) > 30:
                aligned.columns = ["port", "bench"]
                _, dn_c = _capture_ratios(aligned["port"], aligned["bench"])
                pm["down_capture"] = safe(dn_c)

        def _sm2(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val >= exc: return 100.0
            if val >= good: return 80.0 + 20.0 * (val - good) / (exc - good)
            if val >= fair: return 60.0 + 20.0 * (val - fair) / (good - fair)
            if val >= poor: return 40.0 + 20.0 * (val - poor) / (fair - poor)
            return max(0.0, 40.0 * val / poor) if poor != 0 else 0.0

        def _sl2(val, thr):
            if val is None: return None
            exc, good, fair, poor = thr
            if val <= exc: return 100.0
            if val <= good: return 80.0 + 20.0 * (good - val) / (good - exc)
            if val <= fair: return 60.0 + 20.0 * (fair - val) / (fair - good)
            if val <= poor: return 40.0 + 20.0 * (poor - val) / (poor - fair)
            return max(0.0, 20.0)

        sub_scores = {
            "sharpe":          _sm2(pm.get("sharpe"),        (1.5, 1.0, 0.5, 0.0)),
            "sortino":         _sm2(pm.get("sortino"),       (2.0, 1.5, 1.0, 0.5)),
            "calmar":          _sm2(pm.get("calmar"),        (1.5, 1.0, 0.5, 0.2)),
            "omega":           _sm2(pm.get("omega"),         (2.0, 1.5, 1.2, 1.0)),
            "max_drawdown":    _sl2(mdd_pct,                 (10,  20,  30,  40)),
            "down_capture":    _sl2(pm.get("down_capture"),  (80,  90,  100, 120)),
            "diversification": _sm2(eff_n,                   (20,  12,  6,   3)),
        }
        gw = {"sharpe": 25, "sortino": 15, "calmar": 10, "omega": 10,
              "max_drawdown": 20, "down_capture": 10, "diversification": 10}
        total_w2, total_sc2 = 0.0, 0.0
        for k, w in gw.items():
            sc = sub_scores.get(k)
            if sc is not None:
                total_w2 += w
                total_sc2 += sc * w
        overall_score = round(total_sc2 / total_w2, 1) if total_w2 > 0 else 0

        results.append({
            "id": int(port_id),
            "name": str(meta["name"].iloc[0]),
            "score": overall_score,
            "grade": _letter_grade(overall_score),
            "monthly_income": round(monthly_income, 2),
            "sharpe": pm.get("sharpe"),
            "sortino": pm.get("sortino"),
            "calmar": pm.get("calmar"),
            "omega": pm.get("omega"),
            "max_drawdown": pm.get("max_drawdown"),
            "effective_n": eff_n,
            "breakdown_scores": {k: round(v, 1) if v is not None else None
                                 for k, v in sub_scores.items()},
        })

    conn.close()
    return jsonify({"portfolios": results})


@app.route("/portfolio_simulator/account_holdings")
def sim_account_holdings():
    """AJAX: return tickers held in the active profile for the import panel."""
    pid = get_profile_id()
    conn = get_connection()
    df = pd.read_sql("""
        SELECT ticker, current_value, current_price, quantity
        FROM dbo.all_account_info
        WHERE profile_id = ?
          AND purchase_value IS NOT NULL AND purchase_value > 0
        ORDER BY ticker
    """, conn, params=[pid])
    conn.close()
    # Replace NaN with None so jsonify serialises correctly
    records = [{k: (None if (isinstance(v, float) and v != v) else v)
                for k, v in row.items()}
               for row in df.to_dict('records')]
    return jsonify(records)


# ── Profile management routes ─────────────────────────────────────────────────

@app.route("/profiles")
def profiles_page():
    """List all profiles, showing which is active."""
    conn = get_connection()
    ensure_tables_exist(conn)
    try:
        all_profiles_df = pd.read_sql(
            "SELECT id, name, created_at FROM dbo.profiles ORDER BY id", conn
        )
        profiles = all_profiles_df.to_dict('records')
    except Exception:
        profiles = []
    finally:
        conn.close()
    return render_template("profiles.html", profiles=profiles)


@app.route("/profiles/create", methods=["POST"])
def profiles_create():
    name = request.form.get("name", "").strip()
    if not name:
        flash("Profile name cannot be empty.", "error")
        return redirect(url_for("profiles_page"))
    conn = get_connection()
    ensure_tables_exist(conn)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO dbo.profiles (name) VALUES (?)", name)
        conn.commit()
        # Get the new profile's id
        cursor.execute("SELECT MAX(id) FROM dbo.profiles")
        new_id = cursor.fetchone()[0]
        session['profile_id'] = new_id
        flash(f"Profile '{name}' created. You are now viewing it.", "success")
    except Exception as e:
        flash(f"Failed to create profile: {e}", "error")
        conn.close()
        return redirect(url_for("profiles_page"))
    conn.close()
    return redirect(url_for("portfolio_setup"))


@app.route("/profiles/switch/<int:pid>", methods=["POST"])
def profiles_switch(pid):
    session['profile_id'] = pid
    return redirect(request.referrer or url_for('index'))


@app.route("/profiles/delete/<int:pid>", methods=["POST"])
def profiles_delete(pid):
    if pid == 1:
        flash("Cannot delete the Owner profile.", "error")
        return redirect(url_for("profiles_page"))
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Delete all data for this profile from profiled tables
        for table in [
            "all_account_info", "income_tracking",
            "weekly_payouts", "weekly_payout_tickers",
            "monthly_payouts", "monthly_payout_tickers",
        ]:
            cursor.execute(f"DELETE FROM dbo.{table} WHERE profile_id = ?", pid)
        cursor.execute("DELETE FROM dbo.profiles WHERE id = ?", pid)
        conn.commit()
        # Reset session to owner if deleted profile was active
        if int(session.get('profile_id', 1)) == pid:
            session['profile_id'] = 1
        flash("Profile deleted.", "success")
    except Exception as e:
        flash(f"Failed to delete profile: {e}", "error")
    finally:
        conn.close()
    return redirect(url_for("profiles_page"))


# ── Portfolio setup routes ────────────────────────────────────────────────────

@app.route("/portfolio/refresh", methods=["POST"])
def portfolio_refresh():
    """Re-enrich the current non-Owner profile's holdings from yfinance."""
    pid = get_profile_id()
    if pid == 1:
        flash("Use the Import Data button to refresh the Owner profile.", "error")
        return redirect(url_for("index"))
    conn = get_connection()
    try:
        df = pd.read_sql(
            "SELECT ticker, quantity, price_paid, reinvest "
            "FROM dbo.all_account_info WHERE profile_id = ?",
            conn, params=[pid]
        )
    except Exception as e:
        conn.close()
        flash(f"Could not read holdings: {e}", "error")
        return redirect(url_for("index"))
    finally:
        conn.close()

    if df.empty:
        flash("No holdings found. Add your portfolio first.", "error")
        return redirect(url_for("portfolio_setup"))

    try:
        row_count, msg = import_from_upload(df, pid)
        flash(f"Refreshed {row_count} holdings with latest prices and dividend data.", "success")
    except Exception as e:
        flash(f"Refresh failed: {e}", "error")
    return redirect(url_for("index"))


@app.route("/portfolio/setup")
def portfolio_setup():
    """Upload CSV/XLSX or manually enter holdings for the current profile."""
    pid = get_profile_id()
    conn = get_connection()
    try:
        holdings_df = pd.read_sql(
            "SELECT ticker, quantity, price_paid, reinvest "
            "FROM dbo.all_account_info WHERE profile_id = ? ORDER BY ticker",
            conn, params=[pid]
        )
        if not holdings_df.empty:
            holdings_df = holdings_df.where(pd.notnull(holdings_df), None)
            existing_holdings = holdings_df.to_dict('records')
        else:
            existing_holdings = []
    except Exception:
        existing_holdings = []
    finally:
        conn.close()
    return render_template("portfolio_setup.html", existing_holdings=existing_holdings)


@app.route("/portfolio/template")
def portfolio_template():
    """Return a CSV template file for download."""
    csv_content = (
        "Ticker,Shares,Price Paid,Div/Share,Div Frequency,Ex-Div Date,DRIP\n"
        "JEPI,100,54.50,0.43,M,2025-01-22,Y\n"
        "SCHD,50,28.10,0.2786,Q,2025-01-17,N\n"
        "O,25,55.00,0.2635,M,2025-01-30,N\n"
    )
    return Response(
        csv_content,
        mimetype='text/csv',
        headers={"Content-Disposition": "attachment;filename=portfolio_template.csv"},
    )


@app.route("/portfolio/upload", methods=["POST"])
def portfolio_upload():
    """Handle CSV/XLSX file upload, enrich via yfinance, write to all_account_info."""
    pid = get_profile_id()
    f = request.files.get("file")
    if not f or not f.filename:
        flash("No file selected.", "error")
        return redirect(url_for("portfolio_setup"))

    filename = f.filename.lower()
    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(f)
        elif filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(f)
        else:
            flash("Unsupported file type. Please upload a .csv or .xlsx file.", "error")
            return redirect(url_for("portfolio_setup"))

        row_count, msg = import_from_upload(df, pid)
        flash(msg, "success")
    except Exception as e:
        flash(f"Upload failed: {e}", "error")
        return redirect(url_for("portfolio_setup"))

    return redirect(url_for("index"))


@app.route("/portfolio/manual_save", methods=["POST"])
def portfolio_manual_save():
    """Accept JSON array of {ticker, shares, price_paid} and write to DB."""
    pid = get_profile_id()
    try:
        rows = request.get_json()
        if not rows:
            return jsonify({"error": "No data provided."}), 400

        df = pd.DataFrame(rows)
        # Rename to expected canonical names
        df = df.rename(columns={
            'shares':     'quantity',
            'price_paid': 'price_paid',
        })

        row_count, msg = import_from_upload(df, pid)
        return jsonify({"success": True, "message": msg, "count": row_count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
