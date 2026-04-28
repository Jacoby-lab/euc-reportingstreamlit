import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import date, timedelta

# ── Page config ───────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EUC Weekly Report",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="metric-container"] {
        background: #0d1117;
        border: 1px solid #21262d;
        border-radius: 10px;
        padding: 14px 18px;
    }
    .stTabs [data-baseweb="tab"] { font-weight: 500; }
    .member-card {
        border: 1px solid #21262d;
        border-radius: 10px;
        padding: 14px 16px;
        margin-bottom: 12px;
        background: #0d1117;
    }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────
CATEGORIES = ["KTLO", "Initiative", "Tech Debt", "Svc Req", "Incident"]

CAT_COLORS = {
    "KTLO":       "#F59E0B",
    "Initiative": "#3B82F6",
    "Tech Debt":  "#8B5CF6",
    "Svc Req":    "#10B981",
    "Incident":   "#EF4444",
}

CAT_LABELS = {
    "KTLO":       "🔧 KTLO — Keep The Lights On",
    "Initiative": "🚀 Initiative",
    "Tech Debt":  "🏗️ Tech Debt",
    "Svc Req":    "🎫 Service Request",
    "Incident":   "🚨 Incident",
}

REGION_COLORS = {"US": "#3B82F6", "MX": "#F59E0B"}


# ── Helpers ───────────────────────────────────────────────────────────────
def ph(s: str) -> float:
    """Parse 'Xh Ym' string → decimal hours."""
    if not s or s.strip() in ("—", ""):
        return 0.0
    h = m = 0
    if "h" in s:
        parts = s.split("h", 1)
        h = int(parts[0].strip() or 0)
        rest = parts[1].strip()
        if "m" in rest:
            m = int(rest.replace("m", "").strip() or 0)
    elif "m" in s:
        m = int(s.replace("m", "").strip() or 0)
    return h + m / 60.0


def fh(d: float) -> str:
    """Decimal hours → 'Xh Ym' display string."""
    t = round(d * 60)
    return f"{t // 60}h {t % 60:02d}m"


# ── Team registry — maps Jira display names → region ─────────────────────
TEAM_REGIONS = {
    # US
    "Nick Shelton":              "US",
    "Jake Snodgrass":            "US",
    "Matthew Davis":             "US",
    "Khai Nguyen":               "US",
    "Justin Pham":               "US",
    "Nicholas Bowling":          "US",
    "Wes Hurd":                  "US",
    "Kenneth Calvert":           "US",
    "Jaylon Martin":             "US",
    "Hector Cossyleon":          "US",
    # MX
    "Eduardo Rangel Ruiz":       "MX",
    "Alonso Renteria Olvera":    "MX",
    "Santiago Morales":          "MX",
    "Antonio Lopez":             "MX",
    "Luis Tejeda Sosa":          "MX",
    "Esaú Gallardo":             "MX",
    "Esau Gallardo":             "MX",  # accent-free variant in Jira
    "Roberto Gaitan Zamudio":    "MX",
    "Gabriela Martinez Atriano": "MX",
    "Edgar Aquino Lopez":        "MX",
    "Joshua Ramos Dailey":       "MX",
    "Mildred Moron Guerrero":    "MX",
    # NL — rolled into US
    "Julian Hoeksema":           "US",
    "Armand Theunis":            "US",
    "Wessel Geest":              "US",
}

TEAM_NAMES = set(TEAM_REGIONS.keys())

# ── Jira label → category mapping ────────────────────────────────────────
LABEL_TO_CAT = {
    "ktlo":            "KTLO",
    "initiative":      "Initiative",
    "tech_debt":       "Tech Debt",
    "tech debt":       "Tech Debt",
    "service_request": "Svc Req",
    "service request": "Svc Req",
    "incident":        "Incident",
}


# ── Jira data fetch ───────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_worklogs(date_start: str, date_end: str) -> pd.DataFrame:
    """Pull EUC, TechConnect, and Identity worklogs for the given date range from Jira."""
    try:
        base_url = st.secrets["jira"]["base_url"].rstrip("/")
        email    = st.secrets["jira"]["email"]
        token    = st.secrets["jira"]["api_token"]
        # Comma-separated Jira project keys.
        # TC covers "End User Computing" and "End User Computing Mexico" in TechConnect.
        # ID covers the Identity project space.
        projects = [p.strip() for p in
                    st.secrets["jira"].get("projects", "EUC,TC,ID").split(",")]
    except KeyError as e:
        st.error(f"Missing Jira secret: {e}. Check Settings → Secrets.")
        return pd.DataFrame()

    auth    = (email, token)
    headers = {"Accept": "application/json"}
    proj_str = ", ".join(f'"{p}"' for p in projects)
    jql = (
        f'project in ({proj_str}) '
        f'AND worklogDate >= "{date_start}" '
        f'AND worklogDate <= "{date_end}"'
    )

    # ── Paginate through all matching issues (cursor-based) ──
    issues, next_page_token = [], None
    while True:
        params = {
            "jql":        jql,
            "maxResults": 100,
            "fields":     "summary,labels,worklog,project",
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        resp = requests.get(
            f"{base_url}/rest/api/3/search/jql",
            auth=auth, headers=headers,
            params=params,
            timeout=30,
        )
        if not resp.ok:
            st.error(
                f"Jira API error {resp.status_code}: {resp.reason}\n\n"
                f"URL tried: `{base_url}/rest/api/3/search/jql`\n\n"
                f"Response: {resp.text[:500]}"
            )
            return pd.DataFrame()
        data = resp.json()
        issues.extend(data.get("issues", []))
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    # ── Extract individual worklog rows ──
    rows = []
    for issue in issues:
        fields      = issue["fields"]
        raw_labels  = [l.lower() for l in fields.get("labels", [])]
        project_key = fields.get("project", {}).get("key", "")
        if project_key in ("EUC", "ID"):
            source = "EUC"  # ID (Identity) rolls up into EUC bucket
        else:
            source = "TechConnect"  # TC, End User Computing, End User Computing Mexico

        # First matching label wins; EUC/ID unlabeled → KTLO, TC unlabeled → Svc Req
        category = next(
            (LABEL_TO_CAT[l] for l in raw_labels if l in LABEL_TO_CAT),
            "KTLO" if source == "EUC" else "Svc Req",
        )

        wl_data  = fields.get("worklog", {})
        worklogs = wl_data.get("worklogs", [])

        # Fetch remaining worklogs if Jira only returned the first 20
        if wl_data.get("total", 0) > len(worklogs):
            wl_resp = requests.get(
                f"{base_url}/rest/api/3/issue/{issue['key']}/worklog",
                auth=auth, headers=headers, timeout=30,
            )
            if wl_resp.ok:
                worklogs = wl_resp.json().get("worklogs", [])

        for wl in worklogs:
            log_date = wl["started"][:10]
            if not (date_start <= log_date <= date_end):
                continue
            author = wl["author"]["displayName"]
            if author not in TEAM_NAMES:
                continue
            rows.append({
                "Name":     author,
                "Region":   TEAM_REGIONS.get(author, "Unknown"),
                "source":   source,
                "category": category,
                "hours":    wl["timeSpentSeconds"] / 3600,
                "date":     log_date,
                "issue":    issue["key"],
            })

    empty_cols = ["Name", "Region", "source", "category", "hours", "date", "issue"]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=empty_cols)


def build_summary_df(raw: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw worklog rows into the per-person summary the app uses."""
    src_cols = ["EUC", "TechConnect"]
    all_cols  = ["Name", "Region", "Total"] + src_cols + CATEGORIES
    if raw.empty:
        return pd.DataFrame(columns=all_cols)

    totals = raw.groupby(["Name", "Region"])["hours"].sum().reset_index()
    totals.columns = ["Name", "Region", "Total"]

    src_piv = raw.pivot_table(
        index=["Name", "Region"], columns="source",
        values="hours", aggfunc="sum", fill_value=0,
    ).reset_index()
    for col in src_cols:
        if col not in src_piv.columns:
            src_piv[col] = 0.0

    cat_piv = raw.pivot_table(
        index=["Name", "Region"], columns="category",
        values="hours", aggfunc="sum", fill_value=0,
    ).reset_index()
    for cat in CATEGORIES:
        if cat not in cat_piv.columns:
            cat_piv[cat] = 0.0

    result = (
        totals
        .merge(src_piv[["Name", "Region"] + src_cols], on=["Name", "Region"], how="left")
        .merge(cat_piv[["Name", "Region"] + CATEGORIES], on=["Name", "Region"], how="left")
        .fillna(0.0)
    )
    return result


# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 EUC Report")
    st.divider()

    _today  = date.today()
    _monday = _today - timedelta(days=_today.weekday())

    range_type = st.selectbox(
        "📅 Date Range",
        ["Weekly", "Bi-Weekly", "Year to Date", "Monthly", "Quarterly", "Yearly", "Custom"],
    )

    if range_type == "Weekly":
        picked     = st.date_input("Week (pick any day)", value=_monday)
        date_start = picked - timedelta(days=picked.weekday())
        date_end   = date_start + timedelta(days=6)
        period_code  = f"W{date_start.isocalendar()[1]:02d}"

    elif range_type == "Bi-Weekly":
        picked     = st.date_input("Start week (pick any day)", value=_monday)
        date_start = picked - timedelta(days=picked.weekday())
        date_end   = date_start + timedelta(days=13)
        period_code  = f"W{date_start.isocalendar()[1]:02d}–W{date_end.isocalendar()[1]:02d}"

    elif range_type == "Year to Date":
        date_start  = date(_today.year, 1, 1)
        date_end    = _today
        period_code = f"{_today.year}-YTD"

    elif range_type == "Monthly":
        c1, c2     = st.columns(2)
        sel_year   = int(c1.number_input("Year",  value=_today.year,  min_value=2020, max_value=2035, step=1))
        sel_month  = int(c2.number_input("Month", value=_today.month, min_value=1,    max_value=12,   step=1))
        date_start = date(sel_year, sel_month, 1)
        _nm        = date_start.replace(day=28) + timedelta(days=4)
        date_end   = _nm - timedelta(days=_nm.day)
        period_code  = date_start.strftime("%Y-%m")

    elif range_type == "Quarterly":
        c1, c2   = st.columns(2)
        sel_year = int(c1.number_input("Year", value=_today.year, min_value=2020, max_value=2035, step=1))
        sel_q    = c2.selectbox("Quarter", ["Q1", "Q2", "Q3", "Q4"])
        _qmap    = {"Q1": (1,3,31), "Q2": (4,6,30), "Q3": (7,9,30), "Q4": (10,12,31)}
        _sm, _em, _ed = _qmap[sel_q]
        date_start   = date(sel_year, _sm, 1)
        date_end     = date(sel_year, _em, _ed)
        period_code  = f"{sel_year}-{sel_q}"

    elif range_type == "Yearly":
        sel_year   = int(st.number_input("Year", value=_today.year, min_value=2020, max_value=2035, step=1))
        date_start = date(sel_year, 1, 1)
        date_end   = date(sel_year, 12, 31)
        period_code  = str(sel_year)

    else:  # Custom
        _range = st.date_input(
            "Date range",
            value=[_monday - timedelta(weeks=1), _monday + timedelta(days=4)],
        )
        date_start = _range[0] if isinstance(_range, (list, tuple)) else _range
        date_end   = _range[1] if isinstance(_range, (list, tuple)) and len(_range) > 1 else date_start
        period_code  = "Custom"

    # Build human-readable period label
    if date_start.month == date_end.month and date_start.year == date_end.year:
        period_label = f"{date_start.strftime('%b %-d')}–{date_end.strftime('%-d, %Y')}"
    elif date_start.year == date_end.year:
        period_label = f"{date_start.strftime('%b %-d')}–{date_end.strftime('%b %-d, %Y')}"
    else:
        period_label = f"{date_start.strftime('%b %-d, %Y')}–{date_end.strftime('%b %-d, %Y')}"

    if range_type == "Year to Date":
        period_label = f"Year to Date {_today.year} (Jan 1 – {_today.strftime('%b %-d')})"
    elif range_type == "Monthly":
        period_label = date_start.strftime("%B %Y")
    elif range_type == "Quarterly":
        period_label = f"{sel_q} {sel_year}"
    elif range_type == "Yearly":
        period_label = str(sel_year)

    date_start_s = date_start.strftime("%Y-%m-%d")
    date_end_s   = date_end.strftime("%Y-%m-%d")
    st.caption(f"{period_label} · {period_code}")

    st.divider()

    search_query = st.text_input(
        "🔍 Search by name",
        placeholder="e.g. Nick, Martinez...",
    )
    st.divider()

    st.markdown("**Filters**")
    region_filter = st.multiselect(
        "Region",
        options=["US", "MX"],
        default=["US", "MX"],
    )
    source_filter = st.radio(
        "Ticket Source",
        options=["All", "EUC", "TechConnect"],
        help=(
            "EUC → KTLO, Initiative, Tech Debt (includes Identity project)\n"
            "TechConnect → Svc Req, Incident"
        ),
    )
    st.divider()
    st.caption("Jira EUC · TechConnect · refreshes hourly")


# ── Load data ─────────────────────────────────────────────────────────────
with st.spinner(f"Loading Jira data for {period_label}…"):
    _raw = fetch_worklogs(date_start_s, date_end_s)
    df   = build_summary_df(_raw)

_active  = int((df["Total"] > 0).sum()) if not df.empty else 0
_tot_all = fh(df["Total"].sum()) if not df.empty else "0h 00m"


# ── Apply filters ─────────────────────────────────────────────────────────
mask = df["Region"].isin(region_filter) if not df.empty else pd.Series([], dtype=bool)
if search_query.strip():
    mask &= df["Name"].str.contains(search_query.strip(), case=False, na=False)
fdf = df[mask].copy() if not df.empty else df.copy()

if source_filter == "EUC":
    fdf["_display_total"] = fdf["EUC"]
    active_cats = ["KTLO", "Initiative", "Tech Debt"]
elif source_filter == "TechConnect":
    fdf["_display_total"] = fdf["TechConnect"]
    active_cats = ["Svc Req", "Incident"]
else:  # All
    fdf["_display_total"] = fdf["Total"]
    active_cats = CATEGORIES

# ── Re-scope category columns to match source filter ─────────────────────
# Without this, category columns include all sources even when one is selected,
# causing totals and category sums to disagree.
if source_filter != "All" and not _raw.empty:
    _raw_src = _raw[_raw["source"] == source_filter]
    _src_cat = (
        _raw_src[_raw_src["Name"].isin(fdf["Name"])]
        .groupby(["Name", "category"])["hours"]
        .sum()
        .unstack(fill_value=0.0)
    )
    for cat in CATEGORIES:
        fdf[cat] = fdf["Name"].map(
            _src_cat[cat] if cat in _src_cat.columns else pd.Series(dtype=float)
        ).fillna(0.0)


# ── Page header ───────────────────────────────────────────────────────────
st.markdown("# EUC Team — Report")
st.markdown(f"**{period_label} · {period_code}**")

if df.empty:
    st.warning(f"No worklogs found for {period_label}. The team may not have logged time yet, or check your Jira project key in secrets.")
elif search_query.strip():
    st.info(
        f"Showing results for **\"{search_query.strip()}\"** · "
        f"{len(fdf)} member(s) · {', '.join(region_filter) if region_filter else 'No regions'} · {source_filter}"
    )
elif len(fdf) < len(df):
    st.caption(
        f"{len(fdf)} member(s) shown · {', '.join(region_filter)} · {source_filter}"
    )


# ── Tabs ──────────────────────────────────────────────────────────────────
tab_dash, tab1, tab2, tab3, tab4 = st.tabs(
    ["🏢 Dashboard", "📊 Overview", "👤 Individual", "🏷️ By Label", "📋 Full Table"]
)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 0 · DASHBOARD  (uses full unfiltered dataset — sidebar filters ignored)
# ═══════════════════════════════════════════════════════════════════════════
with tab_dash:
    # Always use the full dataset for the executive dashboard
    us_df  = df[df["Region"] == "US"]
    mx_df  = df[df["Region"] == "MX"]

    us_total  = us_df["Total"].sum()
    mx_total  = mx_df["Total"].sum()
    all_total = us_total + mx_total or 1

    st.markdown(f"""
<div style="margin-bottom:6px;">
  <span style="font-size:1.5rem;font-weight:800;letter-spacing:-0.5px">EUC Team — Executive Dashboard</span><br>
  <span style="color:#6b7280;font-size:0.9rem">{period_label} &nbsp;·&nbsp; {period_code} &nbsp;·&nbsp; Full team · Sidebar filters not applied</span>
</div>
""", unsafe_allow_html=True)

    st.divider()

    # ── Region summary row ──────────────────────────────────────────────
    st.markdown("##### Team Hours by Region")
    r1, r2, r3 = st.columns(3)
    with r1:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e3a5f,#1e3a8a);border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#93c5fd;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">US Team</div>
  <div style="font-size:2rem;font-weight:800;color:#fff">{fh(us_total)}</div>
  <div style="color:#93c5fd;font-size:0.82rem;margin-top:4px">{int((us_df["Total"] > 0).sum())} active members</div>
  <div style="color:#60a5fa;font-size:0.82rem">{us_total / all_total * 100:.0f}% of team total</div>
</div>""", unsafe_allow_html=True)
    with r2:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#4a1942,#7c3aed);border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#c4b5fd;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">MX Team</div>
  <div style="font-size:2rem;font-weight:800;color:#fff">{fh(mx_total)}</div>
  <div style="color:#c4b5fd;font-size:0.82rem;margin-top:4px">{int((mx_df["Total"] > 0).sum())} active members</div>
  <div style="color:#a78bfa;font-size:0.82rem">{mx_total / all_total * 100:.0f}% of team total</div>
</div>""", unsafe_allow_html=True)
    with r3:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1c1c2e,#0f172a);border:1px solid #334155;border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#94a3b8;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">⏱ Grand Total</div>
  <div style="font-size:2rem;font-weight:800;color:#f1f5f9">{fh(all_total)}</div>
  <div style="color:#94a3b8;font-size:0.82rem;margin-top:4px">{int((df["Total"] > 0).sum())} active · {len(set(df["Region"].unique()) & set(region_filter))} regions</div>
  <div style="color:#64748b;font-size:0.82rem">{period_code}</div>
</div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Category summary row ────────────────────────────────────────────
    st.markdown("##### Work Distribution by Category")

    all_cat_sums = {c: df[c].sum() for c in CATEGORIES}
    grand_cat = sum(all_cat_sums.values()) or 1

    _DASH_CAT_BG = {
        "KTLO":       ("linear-gradient(135deg,#451a03,#92400e)", "#fbbf24", "#fef3c7"),
        "Initiative": ("linear-gradient(135deg,#0c1445,#1e3a8a)", "#60a5fa", "#dbeafe"),
        "Tech Debt":  ("linear-gradient(135deg,#2e1065,#5b21b6)", "#c084fc", "#f3e8ff"),
        "Svc Req":    ("linear-gradient(135deg,#022c22,#065f46)", "#34d399", "#d1fae5"),
        "Incident":   ("linear-gradient(135deg,#450a0a,#991b1b)", "#f87171", "#fee2e2"),
    }
    cat_cols = st.columns(5)
    for cat, col in zip(CATEGORIES, cat_cols):
        bg, accent, light = _DASH_CAT_BG[cat]
        h = all_cat_sums[cat]
        pct = h / grand_cat * 100
        with col:
            st.markdown(f"""
<div style="background:{bg};border-radius:12px;padding:16px 18px;text-align:center;">
  <div style="color:{light};font-size:0.75rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">{cat}</div>
  <div style="font-size:1.55rem;font-weight:800;color:#fff">{fh(h)}</div>
  <div style="margin:8px auto 0;width:100%;background:rgba(255,255,255,0.15);border-radius:999px;height:5px;">
    <div style="width:{pct:.0f}%;background:{accent};border-radius:999px;height:5px;"></div>
  </div>
  <div style="color:{accent};font-size:0.88rem;font-weight:700;margin-top:6px">{pct:.0f}%</div>
</div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.divider()

    # ── US + MX individual charts side-by-side ──────────────────────────
    st.markdown("##### Individual Breakdown by Category")
    st.caption("Bars normalized to 100% — hover for actual hours · sorted by total hours")

    col_us, col_mx = st.columns(2)

    def make_normalized_bar(team_df, title, color_key="Region"):
        sort_names = team_df.sort_values("Total", ascending=True)["Name"].tolist()
        melt = team_df.melt(
            id_vars=["Name"], value_vars=CATEGORIES,
            var_name="Category", value_name="Hours",
        )
        melt["Hours_fmt"] = melt["Hours"].apply(fh)
        # Compute % of each person's total
        person_totals = team_df.set_index("Name")["Total"]
        melt["pct"] = melt.apply(
            lambda r: r["Hours"] / (person_totals.get(r["Name"], 1) or 1) * 100, axis=1
        ).round(1)

        fig = px.bar(
            melt,
            y="Name", x="pct",
            color="Category",
            color_discrete_map=CAT_COLORS,
            orientation="h",
            category_orders={"Name": sort_names, "Category": CATEGORIES},
            title=title,
            custom_data=["Hours_fmt", "Hours", "pct"],
        )
        fig.update_traces(
            hovertemplate=(
                "<b>%{y}</b><br>"
                "%{fullData.name}: %{customdata[0]} (%{customdata[2]:.1f}%)"
                "<extra></extra>"
            )
        )
        fig.update_layout(
            xaxis=dict(
                title="% of period",
                ticksuffix="%",
                range=[0, 100],
                showgrid=True,
                gridcolor="rgba(255,255,255,0.06)",
            ),
            yaxis=dict(title="", tickfont=dict(size=11)),
            legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="left", x=0, font=dict(size=11)),
            margin=dict(t=10, b=20, l=0, r=10),
            height=max(340, len(team_df) * 42),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        fig.update_xaxes(showline=False)
        fig.update_yaxes(showgrid=False)
        return fig

    with col_us:
        st.markdown("##### US Team — Time by Category")
        st.plotly_chart(
            make_normalized_bar(us_df, ""),
            width='stretch',
        )

    with col_mx:
        st.markdown("##### MX Team — Time by Category")
        st.plotly_chart(
            make_normalized_bar(mx_df, ""),
            width='stretch',
        )

    st.divider()

    # ── Total team treemap ───────────────────────────────────────────────
    st.markdown("##### Total Team — Hours by Category & Person")
    st.caption("Box size = hours logged · click a category to drill into its members")

    tree_melt = df[df["Total"] > 0].melt(
        id_vars=["Name", "Region"],
        value_vars=CATEGORIES,
        var_name="Category",
        value_name="Hours",
    )
    tree_melt = tree_melt[tree_melt["Hours"] > 0].copy()
    tree_melt["Hours_fmt"] = tree_melt["Hours"].apply(fh)
    # First name + last initial keeps labels short but unique
    def _short_name(n):
        parts = n.split()
        return f"{parts[0]} {parts[-1][0]}." if len(parts) > 1 else n
    tree_melt["label"] = tree_melt["Name"].apply(_short_name)

    fig_tree = px.treemap(
        tree_melt,
        path=["Category", "label"],
        values="Hours",
        color="Category",
        color_discrete_map=CAT_COLORS,
        custom_data=["Hours_fmt", "Name", "Region"],
    )
    fig_tree.update_traces(
        texttemplate="<b>%{label}</b><br>%{customdata[0]}",
        hovertemplate=(
            "<b>%{customdata[1]}</b> (%{customdata[2]})<br>"
            "%{parent}: %{customdata[0]}"
            "<extra></extra>"
        ),
        textfont=dict(size=13),
        marker=dict(line=dict(width=2, color="rgba(0,0,0,0.3)")),
    )
    fig_tree.update_layout(
        height=480,
        margin=dict(t=10, b=0, l=0, r=0),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_tree, width='stretch')

    # ── Footer note ──────────────────────────────────────────────────────
    _zero_members = [r["Name"] for _, r in df.iterrows() if r["Total"] == 0] if not df.empty else []
    _zero_note = " · " + ", ".join(_zero_members) + " logged 0h" if _zero_members else ""
    st.markdown(f"""
<div style="color:#4b5563;font-size:0.78rem;margin-top:8px;border-top:1px solid #1f2937;padding-top:10px;">
  Source: Jira EUC + TechConnect · {period_label} · {period_code} ·
  KTLO = Keep The Lights On · TC tickets default to Svc Req or Incident{_zero_note}
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 · OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════
with tab1:
    if fdf.empty:
        st.info("No members match the current filters.")
    else:
        total_h = fdf["_display_total"].sum()
        cat_sums = {c: fdf[c].sum() for c in active_cats}
        grand = sum(cat_sums.values()) or 1

        # ── KPI row 1 ──
        m0, m1, m2 = st.columns(3)
        with m0:
            st.metric("Total Hours", fh(total_h))
        with m1:
            st.metric("Active Members", int((fdf["Total"] > 0).sum()))
        with m2:
            st.metric("Regions", " · ".join(sorted(fdf["Region"].unique())))

        st.markdown("")

        # ── KPI row 2: per-category ──
        cat_cols_row = st.columns(len(active_cats))
        for cat, col in zip(active_cats, cat_cols_row):
            pct = cat_sums[cat] / grand * 100
            with col:
                st.metric(cat, fh(cat_sums[cat]), f"{pct:.0f}%")

        st.divider()

        # ── Charts row ──
        left, right = st.columns([1, 2])

        with left:
            pie_df = pd.DataFrame({
                "Category": active_cats,
                "Hours": [cat_sums[c] for c in active_cats],
                "Hours_fmt": [fh(cat_sums[c]) for c in active_cats],
            })
            fig_pie = px.pie(
                pie_df,
                names="Category", values="Hours",
                color="Category", color_discrete_map=CAT_COLORS,
                hole=0.42,
                title="Distribution by Category",
                custom_data=["Hours_fmt"],
            )
            fig_pie.update_traces(
                textposition="auto",
                textinfo="percent+label",
                hovertemplate="<b>%{label}</b><br>%{customdata[0]}<br>%{percent}<extra></extra>",
            )
            fig_pie.update_layout(
                showlegend=False,
                height=380,
                margin=dict(t=40, b=0, l=0, r=0),
            )
            st.plotly_chart(fig_pie, width='stretch')

        with right:
            sort_order = fdf.sort_values("Total", ascending=False)["Name"].tolist()
            stack_melt = fdf.melt(
                id_vars=["Name", "Region"],
                value_vars=active_cats,
                var_name="Category", value_name="Hours",
            )
            stack_melt["Hours_fmt"] = stack_melt["Hours"].apply(fh)

            fig_stack = px.bar(
                stack_melt,
                x="Name", y="Hours",
                color="Category",
                color_discrete_map=CAT_COLORS,
                category_orders={"Name": sort_order, "Category": active_cats},
                title="",
                labels={"Hours": "Hours (decimal)", "Name": ""},
                custom_data=["Hours_fmt"],
            )
            fig_stack.update_traces(
                hovertemplate="<b>%{x}</b><br>%{fullData.name}: %{customdata[0]}<extra></extra>"
            )
            fig_stack.update_layout(
                xaxis_tickangle=-30,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                height=380,
                margin=dict(t=40, b=80, l=0, r=0),
            )
            st.markdown("**Hours by Person — Stacked by Category**")
            st.plotly_chart(fig_stack, width='stretch')

        # ── US vs MX comparison ──
        if len(region_filter) > 1 and not search_query.strip():
            st.divider()
            st.markdown("#### US vs MX — Category Comparison")

            reg_agg = fdf.groupby("Region")[active_cats].sum().reset_index()
            reg_melt = reg_agg.melt(id_vars="Region", var_name="Category", value_name="Hours")
            reg_melt["Hours_fmt"] = reg_melt["Hours"].apply(fh)

            fig_reg = px.bar(
                reg_melt,
                x="Category", y="Hours",
                color="Region",
                barmode="group",
                color_discrete_map=REGION_COLORS,
                custom_data=["Region", "Hours_fmt"],
            )
            fig_reg.update_traces(
                hovertemplate="<b>%{customdata[0]}</b> · %{x}<br>%{customdata[1]}<extra></extra>"
            )
            fig_reg.update_layout(
                height=320,
                margin=dict(t=10, b=20, l=0, r=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_reg, width='stretch')


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 · INDIVIDUAL
# ═══════════════════════════════════════════════════════════════════════════
with tab2:
    if fdf.empty:
        st.info("No members match the current filters.")

    elif len(fdf) == 1:
        # ── Single-person focus ──
        row = fdf.iloc[0]
        st.markdown(f"## {row['Name']}")
        st.markdown(f"`{row['Region']}` · {period_label}")

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total Hours", fh(row["Total"]))
        with m2:
            st.metric("EUC (incl. Identity)", fh(row["EUC"]))
        with m3:
            st.metric("TechConnect", fh(row["TechConnect"]))

        st.divider()

        cat_vals = [row[c] for c in active_cats]
        cat_df = pd.DataFrame({
            "Category": active_cats,
            "Hours": cat_vals,
        })
        cat_df["Hours_fmt"] = cat_df["Hours"].apply(fh)
        cat_df["pct"] = (cat_df["Hours"] / (sum(cat_vals) or 1) * 100).round(1)

        p1, p2 = st.columns(2)

        with p1:
            fig_p = px.pie(
                cat_df, names="Category", values="Hours",
                color="Category", color_discrete_map=CAT_COLORS,
                hole=0.45,
                custom_data=["Hours_fmt", "pct"],
            )
            fig_p.update_traces(
                textinfo="percent+label",
                hovertemplate="<b>%{label}</b><br>%{customdata[0]} (%{customdata[1]:.1f}%)<extra></extra>",
            )
            fig_p.update_layout(showlegend=False, height=360)
            st.plotly_chart(fig_p, width='stretch')

        with p2:
            cat_sorted = cat_df.sort_values("Hours")
            fig_b = px.bar(
                cat_sorted,
                x="Hours", y="Category",
                color="Category", color_discrete_map=CAT_COLORS,
                orientation="h",
                text="Hours_fmt",
                custom_data=["pct"],
            )
            fig_b.update_traces(
                textposition="outside",
                cliponaxis=False,
                hovertemplate="%{y}: %{text} (%{customdata[0]:.1f}%)<extra></extra>",
            )
            fig_b.update_layout(
                showlegend=False,
                height=360,
                xaxis_title="Hours (decimal)",
                margin=dict(t=20, r=80),
            )
            st.plotly_chart(fig_b, width='stretch')

    else:
        # ── Multi-person view ──
        st.markdown(f"**{len(fdf)} people** — sorted by total hours")

        sort_order_asc = fdf.sort_values("Total", ascending=True)["Name"].tolist()
        ind_melt = fdf.melt(
            id_vars=["Name", "Region"],
            value_vars=active_cats,
            var_name="Category", value_name="Hours",
        )
        ind_melt["Hours_fmt"] = ind_melt["Hours"].apply(fh)

        fig_ind = px.bar(
            ind_melt,
            y="Name", x="Hours",
            color="Category",
            color_discrete_map=CAT_COLORS,
            orientation="h",
            category_orders={"Name": sort_order_asc, "Category": active_cats},
            title="",
            height=max(420, len(fdf) * 44),
            custom_data=["Hours_fmt"],
        )
        fig_ind.update_traces(
            hovertemplate="<b>%{y}</b><br>%{fullData.name}: %{customdata[0]}<extra></extra>"
        )
        fig_ind.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(t=40, b=10, l=0, r=0),
            xaxis_title="Hours (decimal)",
        )
        st.markdown("**Hours by Individual — Stacked by Category**")
        st.plotly_chart(fig_ind, width='stretch')

        st.divider()
        st.markdown("#### Member Cards")

        grid_cols = st.columns(3)
        for i, (_, r) in enumerate(fdf.sort_values("Total", ascending=False).iterrows()):
            dominant = max(active_cats, key=lambda c: r[c])
            pct_dom = r[dominant] / (r["Total"] or 1) * 100
            with grid_cols[i % 3]:
                st.markdown(f"""
<div class="member-card">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
    <strong style="font-size:0.9rem">{r['Name']}</strong>
    <span style="background:#1f2937;color:#9ca3af;border-radius:4px;padding:2px 8px;font-size:0.75rem">{r['Region']}</span>
  </div>
  <div style="font-size:1.35rem;font-weight:700;margin-bottom:4px">{fh(r['Total'])}</div>
  <div style="color:#6b7280;font-size:0.8rem;margin-bottom:8px">EUC {fh(r['EUC'])} &nbsp;·&nbsp; TC {fh(r['TechConnect'])}</div>
  <div style="font-size:0.82rem;color:{CAT_COLORS.get(dominant,'#fff')}">▶ {dominant}: {fh(r[dominant])} ({pct_dom:.0f}%)</div>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 · BY LABEL
# ═══════════════════════════════════════════════════════════════════════════
with tab3:
    selected_cat = st.selectbox(
        "Work category",
        options=active_cats,
        format_func=lambda x: CAT_LABELS[x],
    )

    cat_tab = fdf[["Name", "Region", selected_cat, "Total"]].copy()
    cat_tab = cat_tab[cat_tab[selected_cat] > 0].sort_values(selected_cat, ascending=False)

    total_cat_h = cat_tab[selected_cat].sum()
    pct_of_all = total_cat_h / (fdf["Total"].sum() or 1) * 100

    km1, km2, km3 = st.columns(3)
    with km1:
        st.metric(f"Total {selected_cat}", fh(total_cat_h))
    with km2:
        st.metric("% of All Hours", f"{pct_of_all:.1f}%")
    with km3:
        st.metric("Contributors", len(cat_tab))

    st.divider()

    if cat_tab.empty:
        st.info(f"No hours logged under **{selected_cat}** for the selected filters.")
    else:
        cat_tab["pct_own"] = (
            cat_tab[selected_cat] / cat_tab["Total"].replace(0, 1) * 100
        ).round(1)
        cat_tab["Hours_fmt"] = cat_tab[selected_cat].apply(fh)
        cat_tab["bar_label"] = cat_tab.apply(
            lambda r: f"{r['Hours_fmt']} ({r['pct_own']:.0f}%)", axis=1
        )

        cl, cr = st.columns([3, 1])

        with cl:
            cat_sorted_asc = cat_tab.sort_values(selected_cat, ascending=True)
            fig_cat = px.bar(
                cat_sorted_asc,
                x=selected_cat, y="Name",
                color="Region",
                color_discrete_map=REGION_COLORS,
                orientation="h",
                text="bar_label",
                title="",
                height=max(360, len(cat_tab) * 50),
                custom_data=["pct_own", "Hours_fmt"],
            )
            fig_cat.update_traces(
                textposition="outside",
                cliponaxis=False,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + selected_cat
                    + ": %{customdata[1]}<br>%{customdata[0]:.1f}% of their week<extra></extra>"
                ),
            )
            fig_cat.update_layout(
                xaxis_title="Hours (decimal)",
                yaxis_title="",
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(t=40, r=130, b=20, l=0),
            )
            st.markdown(f"**{CAT_LABELS[selected_cat]} — Hours by Person**")
            st.plotly_chart(fig_cat, width='stretch')

        with cr:
            if len(region_filter) > 1:
                reg_split = fdf.groupby("Region")[selected_cat].sum().reset_index()
                reg_split["Hours_fmt"] = reg_split[selected_cat].apply(fh)
                fig_r = px.pie(
                    reg_split,
                    names="Region", values=selected_cat,
                    color="Region", color_discrete_map=REGION_COLORS,
                    hole=0.45,
                    title="By Region",
                    custom_data=["Hours_fmt"],
                )
                fig_r.update_traces(
                    textinfo="percent+label",
                    hovertemplate="<b>%{label}</b><br>%{customdata[0]}<extra></extra>",
                )
                fig_r.update_layout(
                    showlegend=False,
                    height=260,
                    margin=dict(t=40, b=0, l=0, r=0),
                )
                st.plotly_chart(fig_r, width='stretch')

            st.markdown("**Top contributors**")
            for _, r in cat_tab.head(5).iterrows():
                st.markdown(
                    f"**{r['Name']}** `{r['Region']}`  \n"
                    f"{fh(r[selected_cat])} · {r['pct_own']:.0f}% of their week"
                )
                st.markdown("")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 · FULL TABLE
# ═══════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown(f"**{len(fdf)} members** · sorted by total hours")

    if source_filter == "EUC":
        show_cols = ["Name", "Region", "Total", "EUC"] + active_cats
    elif source_filter == "TechConnect":
        show_cols = ["Name", "Region", "Total", "TechConnect"] + active_cats
    else:  # All
        show_cols = ["Name", "Region", "Total", "EUC", "TechConnect"] + active_cats

    # Sort numerically first, then format for display
    sorted_fdf = fdf.sort_values("Total", ascending=False)
    display_df = sorted_fdf[show_cols].copy()
    for col in show_cols:
        if col not in ("Name", "Region"):
            display_df[col] = display_df[col].apply(fh)

    st.dataframe(
        display_df,
        width='stretch',
        hide_index=True,
        height=min(700, 80 + len(display_df) * 37),
    )

    # Totals row
    if not fdf.empty:
        st.markdown("**Totals**")
        totals = {"Name": "TOTAL", "Region": "—"}
        for col in show_cols:
            if col not in ("Name", "Region"):
                totals[col] = fh(fdf[col].sum())
        st.dataframe(
            pd.DataFrame([totals])[show_cols],
            width='stretch',
            hide_index=True,
        )

    st.markdown("")
    # Download (numeric values for CSV)
    csv_data = sorted_fdf[show_cols].copy()
    st.download_button(
        label="⬇️  Download as CSV",
        data=csv_data.to_csv(index=False),
        file_name=f"EUC_{period_code.replace('–', '_')}.csv",
        mime="text/csv",
    )
