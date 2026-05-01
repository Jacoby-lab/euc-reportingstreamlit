import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from collections import OrderedDict
from datetime import date, timedelta

# ── Page config ───────────────────────────────────────────────────────────
st.set_page_config(
    page_title="I&O Report",
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

SOURCE_COLORS = {"Jira": "#3B82F6", "TC": "#10B981"}

# ── Group configuration ───────────────────────────────────────────────────
# Maps display group name → Jira project keys + TC JSM team names.
# tc_team_field secret controls the JQL field (default: "Team[Jira Software]").
# If your Jira instance uses a different field, set tc_team_field in secrets.
GROUPS = OrderedDict([
    ("End User Computing", {
        "jira_keys": ["EUC"],
        "tc_teams":  ["End User Computing", "End User Computing Mexico"],
    }),
    ("Identity", {
        "jira_keys": ["ID"],
        "tc_teams":  ["Identity Access Management"],
    }),
    ("Service Desk", {
        "jira_keys": ["IT"],
        "tc_teams":  ["Service Desk"],
    }),
    ("Collaboration", {
        "jira_keys": ["COL"],
        "tc_teams":  ["Collaboration Technology"],
    }),
    ("Systems Engineering", {
        "jira_keys": ["SYS"],
        "tc_teams":  ["System Administration"],
    }),
    ("Network Services", {
        "jira_keys": ["NET"],
        "tc_teams":  ["Network Administration"],
    }),
])

LABEL_TO_CAT = {
    "ktlo":            "KTLO",
    "initiative":      "Initiative",
    "tech_debt":       "Tech Debt",
    "tech debt":       "Tech Debt",
    "service_request": "Svc Req",
    "service request": "Svc Req",
    "incident":        "Incident",
}


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


# ── Jira fetch helpers ────────────────────────────────────────────────────
def _paginate_issues(base_url, auth, headers, jql):
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
            auth=auth, headers=headers, params=params, timeout=30,
        )
        if not resp.ok:
            st.error(
                f"Jira API error {resp.status_code}: {resp.reason}\n\n"
                f"JQL: `{jql}`\n\nResponse: {resp.text[:500]}"
            )
            return []
        data = resp.json()
        issues.extend(data.get("issues", []))
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
    return issues


def _extract_rows(issues, base_url, auth, headers, date_start, date_end, source, default_cat):
    rows = []
    for issue in issues:
        fields     = issue["fields"]
        raw_labels = [lb.lower() for lb in fields.get("labels", [])]
        category   = next(
            (LABEL_TO_CAT[lb] for lb in raw_labels if lb in LABEL_TO_CAT),
            default_cat,
        )
        wl_data  = fields.get("worklog", {})
        worklogs = wl_data.get("worklogs", [])
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
            rows.append({
                "Name":     wl["author"]["displayName"],
                "source":   source,
                "category": category,
                "hours":    wl["timeSpentSeconds"] / 3600,
                "date":     log_date,
                "issue":    issue["key"],
            })
    return rows


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_worklogs(date_start: str, date_end: str, group_name: str) -> pd.DataFrame:
    """Pull worklogs for the selected I&O group from Jira projects + TC JSM teams."""
    try:
        base_url      = st.secrets["jira"]["base_url"].rstrip("/")
        email         = st.secrets["jira"]["email"]
        token         = st.secrets["jira"]["api_token"]
        tc_team_field = st.secrets["jira"].get("tc_team_field", "Team[Jira Software]")
    except KeyError as e:
        st.error(f"Missing Jira secret: {e}. Check Settings → Secrets.")
        return pd.DataFrame()

    auth    = (email, token)
    headers = {"Accept": "application/json"}
    cfg     = GROUPS[group_name]
    rows    = []

    # Jira project tickets (KTLO/Initiative/Tech Debt by default)
    if cfg["jira_keys"]:
        proj_str = ", ".join(f'"{k}"' for k in cfg["jira_keys"])
        jql = (
            f'project in ({proj_str}) '
            f'AND worklogDate >= "{date_start}" '
            f'AND worklogDate <= "{date_end}"'
        )
        issues = _paginate_issues(base_url, auth, headers, jql)
        rows.extend(_extract_rows(issues, base_url, auth, headers, date_start, date_end, "Jira", "KTLO"))

    # TC / JSM tickets filtered by team (Svc Req/Incident by default)
    if cfg["tc_teams"]:
        teams_str = ", ".join(f'"{t}"' for t in cfg["tc_teams"])
        jql = (
            f'project = "TC" '
            f'AND "{tc_team_field}" in ({teams_str}) '
            f'AND worklogDate >= "{date_start}" '
            f'AND worklogDate <= "{date_end}"'
        )
        issues = _paginate_issues(base_url, auth, headers, jql)
        rows.extend(_extract_rows(issues, base_url, auth, headers, date_start, date_end, "TC", "Svc Req"))

    empty_cols = ["Name", "source", "category", "hours", "date", "issue"]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=empty_cols)


def build_summary_df(raw: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw worklog rows into per-person summary."""
    src_cols = ["Jira", "TC"]
    all_cols = ["Name", "Total"] + src_cols + CATEGORIES
    if raw.empty:
        return pd.DataFrame(columns=all_cols)

    totals = raw.groupby("Name")["hours"].sum().reset_index()
    totals.columns = ["Name", "Total"]

    src_piv = raw.pivot_table(
        index="Name", columns="source",
        values="hours", aggfunc="sum", fill_value=0,
    ).reset_index()
    for col in src_cols:
        if col not in src_piv.columns:
            src_piv[col] = 0.0

    cat_piv = raw.pivot_table(
        index="Name", columns="category",
        values="hours", aggfunc="sum", fill_value=0,
    ).reset_index()
    for cat in CATEGORIES:
        if cat not in cat_piv.columns:
            cat_piv[cat] = 0.0

    result = (
        totals
        .merge(src_piv[["Name"] + src_cols], on="Name", how="left")
        .merge(cat_piv[["Name"] + CATEGORIES], on="Name", how="left")
        .fillna(0.0)
    )
    return result


# ── Sidebar Part 1: Filters + Date Range ─────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 I&O Report")
    st.divider()

    st.markdown("**Filters**")
    selected_group = st.selectbox(
        "Group",
        options=list(GROUPS.keys()),
        help="Select the I&O group to view",
    )
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


# ── Load data ─────────────────────────────────────────────────────────────
with st.spinner(f"Loading Jira data for {selected_group} · {period_label}…"):
    _raw = fetch_worklogs(date_start_s, date_end_s, selected_group)
    df   = build_summary_df(_raw)

_active  = int((df["Total"] > 0).sum()) if not df.empty else 0
_tot_all = fh(df["Total"].sum()) if not df.empty else "0h 00m"


# ── Sidebar Part 2: Search by name (populated from fetched data) ──────────
with st.sidebar:
    name_options   = sorted(_raw["Name"].unique().tolist()) if not _raw.empty else []
    selected_names = st.multiselect(
        "🔍 Search by name",
        options=name_options,
        placeholder="Select team members…",
    )
    st.divider()
    st.caption(f"I&O · {selected_group} · refreshes hourly")


# ── Apply filters ─────────────────────────────────────────────────────────
fdf = df[df["Name"].isin(selected_names)].copy() if selected_names and not df.empty else df.copy()
fdf["_display_total"] = fdf["Total"]
active_cats = CATEGORIES

_raw_filtered = _raw[_raw["Name"].isin(selected_names)] if selected_names and not _raw.empty else _raw


# ── Page header ───────────────────────────────────────────────────────────
st.markdown(f"# I&O — {selected_group} Report")
st.markdown(f"**{period_label} · {period_code}**")

if df.empty:
    st.warning(
        f"No worklogs found for {selected_group} · {period_label}. "
        "Team may not have logged time yet, or check Jira project keys in secrets."
    )
elif selected_names:
    st.info(f"Showing **{', '.join(selected_names)}** · {len(fdf)} member(s)")
elif len(fdf) < len(df):
    st.caption(f"{len(fdf)} member(s) shown")


# ── Tabs ──────────────────────────────────────────────────────────────────
tab_dash, tab1, tab2, tab3, tab4 = st.tabs(
    ["🏢 Dashboard", "📊 Overview", "👤 Individual", "🏷️ By Label", "📋 Full Table"]
)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 0 · DASHBOARD  (always full group — sidebar name filter not applied)
# ═══════════════════════════════════════════════════════════════════════════
with tab_dash:
    jira_total = df["Jira"].sum() if not df.empty else 0
    tc_total   = df["TC"].sum()   if not df.empty else 0
    all_total  = jira_total + tc_total or 1

    st.markdown(f"""
<div style="margin-bottom:6px;">
  <span style="font-size:1.5rem;font-weight:800;letter-spacing:-0.5px">I&O — {selected_group} Executive Dashboard</span><br>
  <span style="color:#6b7280;font-size:0.9rem">{period_label} &nbsp;·&nbsp; {period_code} &nbsp;·&nbsp; Full group · Sidebar filters not applied</span>
</div>
""", unsafe_allow_html=True)

    st.divider()

    # ── Source summary row ──────────────────────────────────────────────
    st.markdown("##### Team Hours by Source")
    r1, r2, r3 = st.columns(3)
    with r1:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e3a5f,#1e3a8a);border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#93c5fd;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Jira Projects</div>
  <div style="font-size:2rem;font-weight:800;color:#fff">{fh(jira_total)}</div>
  <div style="color:#93c5fd;font-size:0.82rem;margin-top:4px">{int((df["Jira"] > 0).sum()) if not df.empty else 0} contributors</div>
  <div style="color:#60a5fa;font-size:0.82rem">{jira_total / all_total * 100:.0f}% of total</div>
</div>""", unsafe_allow_html=True)
    with r2:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#022c22,#065f46);border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#6ee7b7;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">TechConnect (TC)</div>
  <div style="font-size:2rem;font-weight:800;color:#fff">{fh(tc_total)}</div>
  <div style="color:#6ee7b7;font-size:0.82rem;margin-top:4px">{int((df["TC"] > 0).sum()) if not df.empty else 0} contributors</div>
  <div style="color:#34d399;font-size:0.82rem">{tc_total / all_total * 100:.0f}% of total</div>
</div>""", unsafe_allow_html=True)
    with r3:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1c1c2e,#0f172a);border:1px solid #334155;border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#94a3b8;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">⏱ Grand Total</div>
  <div style="font-size:2rem;font-weight:800;color:#f1f5f9">{fh(all_total)}</div>
  <div style="color:#94a3b8;font-size:0.82rem;margin-top:4px">{_active} active members</div>
  <div style="color:#64748b;font-size:0.82rem">{period_code}</div>
</div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Category summary row ────────────────────────────────────────────
    st.markdown("##### Work Distribution by Category")
    all_cat_sums = {c: df[c].sum() for c in CATEGORIES} if not df.empty else {c: 0 for c in CATEGORIES}
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
        h   = all_cat_sums[cat]
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

    # ── Individual breakdown ────────────────────────────────────────────
    st.markdown("##### Individual Breakdown by Category")
    st.caption("Bars normalized to 100% — hover for actual hours · sorted by total hours")

    if not df.empty:
        sort_names = df.sort_values("Total", ascending=True)["Name"].tolist()
        melt = df.melt(
            id_vars=["Name"], value_vars=CATEGORIES,
            var_name="Category", value_name="Hours",
        )
        melt["Hours_fmt"] = melt["Hours"].apply(fh)
        person_totals = df.set_index("Name")["Total"]
        melt["pct"] = melt.apply(
            lambda r: r["Hours"] / (person_totals.get(r["Name"], 1) or 1) * 100, axis=1
        ).round(1)

        fig_ind = px.bar(
            melt,
            y="Name", x="pct",
            color="Category",
            color_discrete_map=CAT_COLORS,
            orientation="h",
            category_orders={"Name": sort_names, "Category": CATEGORIES},
            custom_data=["Hours_fmt", "pct"],
        )
        fig_ind.update_traces(
            hovertemplate=(
                "<b>%{y}</b><br>"
                "%{fullData.name}: %{customdata[0]} (%{customdata[1]:.1f}%)"
                "<extra></extra>"
            )
        )
        fig_ind.update_layout(
            xaxis=dict(
                title="% of period",
                ticksuffix="%",
                range=[0, 100],
                showgrid=True,
                gridcolor="rgba(255,255,255,0.06)",
            ),
            yaxis=dict(title="", tickfont=dict(size=11)),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=11)),
            margin=dict(t=10, b=20, l=0, r=10),
            height=max(340, len(df) * 42),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        fig_ind.update_xaxes(showline=False)
        fig_ind.update_yaxes(showgrid=False)
        st.plotly_chart(fig_ind, width='stretch')

    st.divider()

    # ── Treemap ─────────────────────────────────────────────────────────
    st.markdown("##### Total Team — Hours by Category & Person")
    st.caption("Box size = hours logged · click a category to drill into its members")

    if not df.empty:
        tree_melt = df[df["Total"] > 0].melt(
            id_vars=["Name"],
            value_vars=CATEGORIES,
            var_name="Category",
            value_name="Hours",
        )
        tree_melt = tree_melt[tree_melt["Hours"] > 0].copy()
        tree_melt["Hours_fmt"] = tree_melt["Hours"].apply(fh)

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
            custom_data=["Hours_fmt", "Name"],
        )
        fig_tree.update_traces(
            texttemplate="<b>%{label}</b><br>%{customdata[0]}",
            hovertemplate=(
                "<b>%{customdata[1]}</b><br>"
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

    _zero_members = [r["Name"] for _, r in df.iterrows() if r["Total"] == 0] if not df.empty else []
    _zero_note = " · " + ", ".join(_zero_members) + " logged 0h" if _zero_members else ""
    _jira_keys  = " + ".join(GROUPS[selected_group]["jira_keys"])
    _tc_teams   = ", ".join(GROUPS[selected_group]["tc_teams"])
    st.markdown(f"""
<div style="color:#4b5563;font-size:0.78rem;margin-top:8px;border-top:1px solid #1f2937;padding-top:10px;">
  Source: Jira ({_jira_keys}) + TechConnect ({_tc_teams}) · {period_label} · {period_code}{_zero_note}
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 · OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════
with tab1:
    if fdf.empty:
        st.info("No members match the current filters.")
    else:
        total_h  = fdf["_display_total"].sum()
        cat_sums = {c: fdf[c].sum() for c in active_cats}
        grand    = sum(cat_sums.values()) or 1

        m0, m1, m2, m3 = st.columns(4)
        with m0:
            st.metric("Total Hours", fh(total_h))
        with m1:
            st.metric("Active Members", int((fdf["Total"] > 0).sum()))
        with m2:
            st.metric("Jira", fh(fdf["Jira"].sum()))
        with m3:
            st.metric("TC", fh(fdf["TC"].sum()))

        st.markdown("")

        cat_cols_row = st.columns(len(active_cats))
        for cat, col in zip(active_cats, cat_cols_row):
            pct = cat_sums[cat] / grand * 100
            with col:
                st.metric(cat, fh(cat_sums[cat]), f"{pct:.0f}%")

        st.divider()

        left, right = st.columns([1, 2])

        with left:
            pie_df = pd.DataFrame({
                "Category": active_cats,
                "Hours":    [cat_sums[c] for c in active_cats],
                "Hours_fmt":[fh(cat_sums[c]) for c in active_cats],
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
                id_vars=["Name"],
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


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 · INDIVIDUAL
# ═══════════════════════════════════════════════════════════════════════════
with tab2:
    if fdf.empty:
        st.info("No members match the current filters.")

    elif len(fdf) == 1:
        row = fdf.iloc[0]
        st.markdown(f"## {row['Name']}")
        st.markdown(f"`{selected_group}` · {period_label}")

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total Hours", fh(row["Total"]))
        with m2:
            st.metric("Jira", fh(row["Jira"]))
        with m3:
            st.metric("TechConnect", fh(row["TC"]))

        st.divider()

        cat_vals = [row[c] for c in active_cats]
        cat_df   = pd.DataFrame({"Category": active_cats, "Hours": cat_vals})
        cat_df["Hours_fmt"] = cat_df["Hours"].apply(fh)
        cat_df["pct"]       = (cat_df["Hours"] / (sum(cat_vals) or 1) * 100).round(1)

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
        st.markdown(f"**{len(fdf)} people** — sorted by total hours")

        sort_order_asc = fdf.sort_values("Total", ascending=True)["Name"].tolist()
        ind_melt = fdf.melt(
            id_vars=["Name"],
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
            pct_dom  = r[dominant] / (r["Total"] or 1) * 100
            with grid_cols[i % 3]:
                st.markdown(f"""
<div class="member-card">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
    <strong style="font-size:0.9rem">{r['Name']}</strong>
    <span style="background:#1f2937;color:#9ca3af;border-radius:4px;padding:2px 8px;font-size:0.75rem">{selected_group}</span>
  </div>
  <div style="font-size:1.35rem;font-weight:700;margin-bottom:4px">{fh(r['Total'])}</div>
  <div style="color:#6b7280;font-size:0.8rem;margin-bottom:8px">Jira {fh(r['Jira'])} &nbsp;·&nbsp; TC {fh(r['TC'])}</div>
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

    cat_tab = fdf[["Name", selected_cat, "Total"]].copy()
    cat_tab = cat_tab[cat_tab[selected_cat] > 0].sort_values(selected_cat, ascending=False)

    total_cat_h = cat_tab[selected_cat].sum()
    pct_of_all  = total_cat_h / (fdf["Total"].sum() or 1) * 100

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
            cat_sorted_asc = cat_tab.sort_values(selected_cat, ascending=True).copy()

            # Color bars by dominant source (Jira vs TC) for this person+category
            if not _raw_filtered.empty:
                src_map = (
                    _raw_filtered[_raw_filtered["category"] == selected_cat]
                    .groupby("Name")["source"]
                    .agg(lambda x: "Jira" if (x == "Jira").sum() >= (x == "TC").sum() else "TC")
                )
                cat_sorted_asc["Source"] = cat_sorted_asc["Name"].map(src_map).fillna("Jira")
            else:
                cat_sorted_asc["Source"] = "Jira"

            fig_cat = px.bar(
                cat_sorted_asc,
                x=selected_cat, y="Name",
                color="Source",
                color_discrete_map=SOURCE_COLORS,
                orientation="h",
                text="bar_label",
                height=max(360, len(cat_tab) * 50),
                custom_data=["pct_own", "Hours_fmt"],
            )
            fig_cat.update_traces(
                textposition="outside",
                cliponaxis=False,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + selected_cat
                    + ": %{customdata[1]}<br>%{customdata[0]:.1f}% of their period<extra></extra>"
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
            # Source breakdown pie (Jira vs TC)
            if not _raw_filtered.empty:
                src_cat_data = (
                    _raw_filtered[_raw_filtered["category"] == selected_cat]
                    .groupby("source")["hours"].sum()
                    .reset_index()
                )
                if not src_cat_data.empty:
                    src_cat_data.columns = ["Source", "Hours"]
                    src_cat_data["Hours_fmt"] = src_cat_data["Hours"].apply(fh)
                    fig_src = px.pie(
                        src_cat_data,
                        names="Source", values="Hours",
                        color="Source", color_discrete_map=SOURCE_COLORS,
                        hole=0.45,
                        title="By Source",
                        custom_data=["Hours_fmt"],
                    )
                    fig_src.update_traces(
                        textinfo="percent+label",
                        hovertemplate="<b>%{label}</b><br>%{customdata[0]}<extra></extra>",
                    )
                    fig_src.update_layout(
                        showlegend=False,
                        height=260,
                        margin=dict(t=40, b=0, l=0, r=0),
                    )
                    st.plotly_chart(fig_src, width='stretch')

            st.markdown("**Top contributors**")
            for _, r in cat_tab.head(5).iterrows():
                st.markdown(
                    f"**{r['Name']}**  \n"
                    f"{fh(r[selected_cat])} · {r['pct_own']:.0f}% of their period"
                )
                st.markdown("")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 · FULL TABLE
# ═══════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown(f"**{len(fdf)} members** · sorted by total hours")

    show_cols = ["Name", "Total", "Jira", "TC"] + active_cats

    sorted_fdf = fdf.sort_values("Total", ascending=False)
    display_df = sorted_fdf[show_cols].copy()
    for col in show_cols:
        if col != "Name":
            display_df[col] = display_df[col].apply(fh)

    st.dataframe(
        display_df,
        width='stretch',
        hide_index=True,
        height=min(700, 80 + len(display_df) * 37),
    )

    if not fdf.empty:
        st.markdown("**Totals**")
        totals = {"Name": "TOTAL"}
        for col in show_cols:
            if col != "Name":
                totals[col] = fh(fdf[col].sum())
        st.dataframe(
            pd.DataFrame([totals])[show_cols],
            width='stretch',
            hide_index=True,
        )

    st.markdown("")
    csv_data = sorted_fdf[show_cols].copy()
    st.download_button(
        label="⬇️  Download as CSV",
        data=csv_data.to_csv(index=False),
        file_name=f"IO_{selected_group.replace(' ', '_')}_{period_code.replace('–', '_')}.csv",
        mime="text/csv",
    )
