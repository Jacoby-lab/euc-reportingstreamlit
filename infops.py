import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from collections import OrderedDict
from datetime import date, timedelta, datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Feature flags ────────────────────────────────────────────────────────
ENABLE_PERIOD_COMPARISON = True   # set False to disable prev-period delta arrows

# ── Timezone config ───────────────────────────────────────────────────────
# UTC offset for your local timezone (used for expected-hours target line).
# CDT (summer) = -5, CST (winter) = -6. Adjust if the target line is off by a day.
LOCAL_UTC_OFFSET = -5

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
        color: #e6edf3;
    }
    @media (prefers-color-scheme: light) {
        .member-card {
            background: #f6f8fa;
            border: 1px solid #d0d7de;
            color: #1f2328;
        }
        .member-card .card-sub  { color: #57606a !important; }
        .member-card .card-badge { background: #e8ecf0 !important; color: #57606a !important; }
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
# tc_team_field secret controls the JQL field (default: "Assigned Group").
# "members": optional allowlist — only worklogs from these authors are counted.
#   Leave empty/omit to include all authors found in those projects.
GROUPS = OrderedDict([
    ("End User Computing", {
        # jira_keys: core EUC projects (KTLO/Initiative/Tech Debt default category)
        "jira_keys": ["EUC", "ID"],
        # tc_teams: not used for EUC — cross_projects replaces it for broader coverage
        "tc_teams":  [],
        # cross_projects: all other projects EUC members may log time in (TC source)
        "cross_projects": ["TC", "IT", "COL", "SYS", "NET", "ITPP", "ITDS", "IOAI", "SDI"],
        "sprint_prefix": "EUC",
        "members": {
            "Nick Shelton", "Jake Snodgrass", "Matthew Davis", "Khai Nguyen",
            "Justin Pham", "Nicholas Bowling", "Wes Hurd", "Kenneth Calvert",
            "Jaylon Martin", "Hector Cossyleon",
            "Eduardo Rangel Ruiz", "Alonso Renteria Olvera", "Santiago Morales",
            "Antonio Lopez", "Luis Tejeda Sosa", "Esaú Gallardo", "Esau Gallardo",
            "Roberto Gaitan Zamudio", "Gabriela Martinez Atriano",
            "Edgar Aquino Lopez", "Joshua Ramos Dailey", "Mildred Moron Guerrero",
            "Julian Hoeksema", "Armand Theunis", "Wessel Geest",
        },
    }),
    ("Identity", {
        "jira_keys":      ["ID"],
        "tc_teams":       [],
        "cross_projects": ["EUC", "TC", "IT", "COL", "SYS", "NET", "ITPP", "ITDS", "IOAI", "SDI"],
        "sprint_prefix":  "ID",
        "members": {
            "Alexis Lopez Lopez",
            "Scott Tarnell",
        },
    }),
    ("Service Desk", {
        "jira_keys":      ["IT"],
        "tc_teams":       [],
        "cross_projects": ["EUC", "ID", "TC", "COL", "SYS", "NET", "ITPP", "ITDS", "IOAI", "SDI"],
        "sprint_prefix":  "IT",
        "members": {
            "George Loyola", "Jef Davis", "Nazir Latefe", "Noel Abraham",
            "Paul Forte", "Taylor Johnson", "Tirth Patel", "Xavier Abaunza",
        },
    }),
    ("Collaboration", {
        "jira_keys":      ["COL"],
        "tc_teams":       [],
        "cross_projects": ["EUC", "ID", "TC", "IT", "SYS", "NET", "ITPP", "ITDS", "IOAI", "SDI"],
        "sprint_prefix":  "COL",
        "members": {
            "Kevin Maiberger",
            "Scott Tarnell",
            "Tim Mayville",
        },
    }),
    ("Systems Engineering", {
        "jira_keys":      ["SYS"],
        "tc_teams":       [],
        "cross_projects": ["EUC", "ID", "TC", "IT", "COL", "NET", "ITPP", "ITDS", "IOAI", "SDI"],
        "sprint_prefix":  "SYS",
        "members": {
            "Allen Neely", "Amar Rana", "Ambers Ferrara", "Areshkumar Venkatesan",
            "Bianca Fialho", "Bijoy Babu", "Chris Dugas", "David Stratman",
            "Gelila Wallace", "Gopinath Gopalam", "Jim Gardella", "Min Li",
            "Satish Kothuru", "Shivendra Bajpai", "Tim Mayville", "Vishwas Gunjal",
        },
    }),
    ("Network Services", {
        "jira_keys":      ["NET"],
        "tc_teams":       [],
        "cross_projects": ["EUC", "ID", "TC", "IT", "COL", "SYS", "ITPP", "ITDS", "IOAI", "SDI"],
        "sprint_prefix":  "NET",
        "members": {
            "Brian Graham", "David Fuentes", "Kailash Mohnani",
            "Mukesh Kumar", "Shaurya Katiyar",
        },
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


def _extract_rows(issues, base_url, auth, headers, date_start, date_end, members, jira_keys):
    """Extract worklog rows; source derived from project key vs jira_keys set."""
    rows = []
    for issue in issues:
        fields      = issue["fields"]
        project_key = fields["project"]["key"]
        source      = "Jira" if project_key in jira_keys else "TC"
        default_cat = "KTLO" if source == "Jira" else "Svc Req"
        raw_labels  = [lb.lower() for lb in fields.get("labels", [])]
        category    = next(
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
            author = wl["author"]["displayName"]
            if members and author not in members:
                continue
            rows.append({
                "Name":     author,
                "source":   source,
                "category": category,
                "hours":    wl["timeSpentSeconds"] / 3600,
                "date":     log_date,
                "issue":    issue["key"],
            })
    return rows


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_worklogs(date_start: str, date_end: str, group_name: str) -> pd.DataFrame:
    """Pull worklogs for the selected I&O group using worklogAuthor across all projects."""
    try:
        base_url = st.secrets["jira"]["base_url"].rstrip("/")
        email    = st.secrets["jira"]["email"]
        token    = st.secrets["jira"]["api_token"]
    except KeyError as e:
        st.error(f"Missing Jira secret: {e}. Check Settings → Secrets.")
        return pd.DataFrame()

    auth    = (email, token)
    headers = {"Accept": "application/json"}
    cfg     = GROUPS[group_name]
    members = cfg.get("members") or None

    empty_cols = ["Name", "source", "category", "hours", "date", "issue"]
    if not members:
        st.warning(f"No members configured for {group_name}.")
        return pd.DataFrame(columns=empty_cols)

    jira_keys   = set(cfg.get("jira_keys", []))
    authors_str = ", ".join(f'"{m}"' for m in members)
    # worklogDate is unreliable for older ranges (Jira Cloud index gaps).
    # updated >= date_start catches active tickets but misses retroactive worklogs on
    # closed/old tickets (e.g. JSM tickets closed before date_start).
    # OR combination catches both cases; _extract_rows filters exact date range.
    jql = (
        f'worklogAuthor in ({authors_str}) '
        f'AND (worklogDate >= "{date_start}" OR updated >= "{date_start}")'
    )
    issues = _paginate_issues(base_url, auth, headers, jql)
    rows   = _extract_rows(issues, base_url, auth, headers, date_start, date_end, members, jira_keys)

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=empty_cols)


# ── Sprint fetch helpers ──────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def fetch_sprints_for_group(group_name: str) -> list:
    """Return [{id, name, state, startDate, endDate}] newest-first for group's boards."""
    try:
        base_url = st.secrets["jira"]["base_url"].rstrip("/")
        email    = st.secrets["jira"]["email"]
        token    = st.secrets["jira"]["api_token"]
    except KeyError:
        return []
    auth    = (email, token)
    headers = {"Accept": "application/json"}
    jira_keys = GROUPS[group_name].get("jira_keys", [])

    board_ids = set()
    for key in jira_keys:
        r = requests.get(
            f"{base_url}/rest/agile/1.0/board",
            auth=auth, headers=headers,
            params={"projectKeyOrId": key, "maxResults": 50}, timeout=15,
        )
        if r.ok:
            for b in r.json().get("values", []):
                board_ids.add(b["id"])

    sprints, seen = [], set()
    for board_id in board_ids:
        start = 0
        while True:
            r = requests.get(
                f"{base_url}/rest/agile/1.0/board/{board_id}/sprint",
                auth=auth, headers=headers,
                params={"state": "active,closed,future", "startAt": start, "maxResults": 50},
                timeout=15,
            )
            if not r.ok:
                break
            data = r.json()
            for s in data.get("values", []):
                if s["id"] not in seen:
                    seen.add(s["id"])
                    sprints.append(s)
            if data.get("isLast", True):
                break
            start += 50

    sprints.sort(key=lambda s: s.get("startDate", ""), reverse=True)
    return sprints


@st.cache_data(ttl=300, show_spinner=False)
def fetch_sprint_issues(sprint_ids: tuple, group_name: str) -> pd.DataFrame:
    """
    Return DataFrame with per-(issue, member) rows:
    sprint, issue_key, summary, assignee, estimate_h, logged_h, member, status
    estimate_h is set only on the row where member == assignee.
    """
    try:
        base_url = st.secrets["jira"]["base_url"].rstrip("/")
        email    = st.secrets["jira"]["email"]
        token    = st.secrets["jira"]["api_token"]
    except KeyError:
        return pd.DataFrame()
    auth    = (email, token)
    headers = {"Accept": "application/json"}
    members = set(GROUPS[group_name].get("members", []))
    cols    = ["sprint", "issue_key", "summary", "assignee", "estimate_h", "logged_h", "member", "status"]

    rows = []
    for sprint_id in sprint_ids:
        sr = requests.get(
            f"{base_url}/rest/agile/1.0/sprint/{sprint_id}",
            auth=auth, headers=headers, timeout=15,
        )
        sprint_name = sr.json().get("name", str(sprint_id)) if sr.ok else str(sprint_id)

        start = 0
        while True:
            r = requests.get(
                f"{base_url}/rest/agile/1.0/sprint/{sprint_id}/issue",
                auth=auth, headers=headers,
                params={
                    "startAt": start, "maxResults": 100,
                    "fields": "summary,assignee,timeoriginalestimate,worklog,status,issuetype",
                }, timeout=30,
            )
            if not r.ok:
                break
            data   = r.json()
            issues = data.get("issues", [])

            for issue in issues:
                f         = issue["fields"]
                ikey      = issue["key"]
                assignee  = (f.get("assignee") or {}).get("displayName", "Unassigned")
                est_h     = (f.get("timeoriginalestimate") or 0) / 3600
                status    = (f.get("status") or {}).get("name", "")
                summary   = f.get("summary", "")

                wl_data  = f.get("worklog", {})
                worklogs = wl_data.get("worklogs", [])
                if wl_data.get("total", 0) > len(worklogs):
                    wr = requests.get(
                        f"{base_url}/rest/api/3/issue/{ikey}/worklog",
                        auth=auth, headers=headers, timeout=30,
                    )
                    if wr.ok:
                        worklogs = wr.json().get("worklogs", [])

                member_logged: dict = {}
                for wl in worklogs:
                    author = wl["author"]["displayName"]
                    if members and author not in members:
                        continue
                    member_logged[author] = member_logged.get(author, 0) + wl["timeSpentSeconds"] / 3600

                all_on_issue = set(member_logged.keys())
                if assignee in members:
                    all_on_issue.add(assignee)

                for member in all_on_issue:
                    rows.append({
                        "sprint":     sprint_name,
                        "issue_key":  ikey,
                        "summary":    summary,
                        "assignee":   assignee,
                        "estimate_h": est_h if member == assignee else 0,
                        "logged_h":   member_logged.get(member, 0),
                        "member":     member,
                        "status":     status,
                    })

            start += len(issues)
            if start >= data.get("total", 0) or not issues:
                break

    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_ytd_goal_data(group_name: str) -> pd.DataFrame:
    """
    Fetch Jan 1 – today worklogs for the Goal Tracker.

    Strategy: one JQL query PER MEMBER run in parallel (ThreadPoolExecutor).
      - Each query covers ALL projects (no project filter) → catches everything
      - Each result set < ~500 issues per person → stays under 5000 limit
      - 25 parallel workers ≈ 3-5s wall time vs ~60s sequential
      - Deduplicates rows by (issue, date, Name, hours) across results
    """
    try:
        base_url = st.secrets["jira"]["base_url"].rstrip("/")
        email    = st.secrets["jira"]["email"]
        token    = st.secrets["jira"]["api_token"]
    except KeyError:
        return pd.DataFrame()

    auth         = (email, token)
    headers      = {"Accept": "application/json"}
    cfg          = GROUPS[group_name]
    members      = list(cfg.get("members") or [])
    if not members:
        return pd.DataFrame()

    jira_keys    = set(cfg.get("jira_keys", []))
    _today       = (datetime.now(timezone.utc) + timedelta(hours=LOCAL_UTC_OFFSET)).date()
    date_start_s = "2026-01-01"
    date_end_s   = _today.strftime("%Y-%m-%d")
    empty_cols   = ["Name", "source", "category", "hours", "date", "issue"]

    def _fetch_member(member: str) -> list:
        """Fetch all 2026 worklogs for a single member across all projects."""
        jql = (
            f'worklogAuthor = "{member}" '
            f'AND (worklogDate >= "{date_start_s}" OR updated >= "{date_start_s}")'
        )
        # Thread-safe: no st.* calls — return empty list on error
        try:
            issues, npt = [], None
            while True:
                params = {
                    "jql": jql, "maxResults": 100,
                    "fields": "summary,labels,worklog,project",
                }
                if npt:
                    params["nextPageToken"] = npt
                resp = requests.get(
                    f"{base_url}/rest/api/3/search/jql",
                    auth=auth, headers=headers, params=params, timeout=30,
                )
                if not resp.ok:
                    break
                data = resp.json()
                issues.extend(data.get("issues", []))
                npt = data.get("nextPageToken")
                if not npt:
                    break
            return _extract_rows(
                issues, base_url, auth, headers,
                date_start_s, date_end_s, {member}, jira_keys,
            )
        except Exception:
            return []

    all_rows = []
    seen: set = set()

    with ThreadPoolExecutor(max_workers=min(len(members), 10)) as executor:
        futures = {executor.submit(_fetch_member, m): m for m in members}
        for fut in as_completed(futures):
            for row in fut.result():
                key = (row["issue"], row["date"], row["Name"], round(row["hours"], 4))
                if key not in seen:
                    seen.add(key)
                    all_rows.append(row)

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=empty_cols)


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

    # Previous period dates (same span length, shifted back)
    _span = (date_end - date_start).days + 1
    if range_type == "Monthly":
        _pm = date_start.month - 1 or 12
        _py = date_start.year if date_start.month > 1 else date_start.year - 1
        prev_start = date(_py, _pm, 1)
        _pnm = prev_start.replace(day=28) + timedelta(days=4)
        prev_end = _pnm - timedelta(days=_pnm.day)
    elif range_type == "Quarterly":
        prev_start = date_start - timedelta(days=91)
        prev_start = date(prev_start.year, ((prev_start.month - 1) // 3) * 3 + 1, 1)
        _pnm = prev_start.replace(month=min(prev_start.month + 2, 12), day=28) + timedelta(days=4)
        prev_end = _pnm - timedelta(days=_pnm.day)
    elif range_type == "Yearly":
        prev_start = date(date_start.year - 1, 1, 1)
        prev_end   = date(date_start.year - 1, 12, 31)
    elif range_type == "Year to Date":
        prev_start = date(date_start.year - 1, 1, 1)
        prev_end   = date(date_start.year - 1, _today.month, _today.day)
    else:
        prev_start = date_start - timedelta(days=_span)
        prev_end   = date_end   - timedelta(days=_span)

    prev_start_s = prev_start.strftime("%Y-%m-%d")
    prev_end_s   = prev_end.strftime("%Y-%m-%d")

    date_start_s = date_start.strftime("%Y-%m-%d")
    date_end_s   = date_end.strftime("%Y-%m-%d")
    st.caption(f"{period_label} · {period_code}")
    st.divider()


# ── Load data ─────────────────────────────────────────────────────────────
with st.spinner(f"Loading Jira data for {selected_group} · {period_label}…"):
    _raw = fetch_worklogs(date_start_s, date_end_s, selected_group)
    df   = build_summary_df(_raw)
    if ENABLE_PERIOD_COMPARISON:
        _raw_prev = fetch_worklogs(prev_start_s, prev_end_s, selected_group)
        _prev_df  = build_summary_df(_raw_prev)
    else:
        _prev_df  = pd.DataFrame()

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
    if st.button("🔄 Clear cache", help="Force re-fetch from Jira (use if data looks stale)"):
        fetch_worklogs.clear()
        st.rerun()
    st.caption(f"I&O · {selected_group} · refreshes hourly")


# ── Apply filters ─────────────────────────────────────────────────────────
fdf = df[df["Name"].isin(selected_names)].copy() if selected_names and not df.empty else df.copy()
fdf["_display_total"] = fdf["Total"]
active_cats = CATEGORIES

# ── Expected hours target line ─────────────────────────────────────────────
# 6h/day × weekdays elapsed (capped at date_end so future days don't count)
def _count_weekdays(start, end):
    count, d = 0, start
    while d <= end:
        if d.weekday() < 5:
            count += 1
        d += timedelta(days=1)
    return count

_local_today   = (datetime.now(timezone.utc) + timedelta(hours=LOCAL_UTC_OFFSET)).date()
_eff_end       = min(date_end, _local_today)
_expected_days = _count_weekdays(date_start, _eff_end)
expected_hours = _expected_days * 6

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
tab_dash, tab1, tab2, tab_sprint, tab_goal, tab3 = st.tabs(
    ["🏢 Dashboard", "📊 Overview", "🏷️ By Label", "🏃 Sprint", "🎯 Goal Tracker", "📋 Full Table"]
)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 0 · DASHBOARD  (always full group — sidebar name filter not applied)
# ═══════════════════════════════════════════════════════════════════════════
with tab_dash:
    jira_total  = df["Jira"].sum() if not df.empty else 0
    tc_total    = df["TC"].sum()   if not df.empty else 0
    grand_total = jira_total + tc_total
    all_total   = grand_total or 1  # denominator only — never used for display

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
  <div style="font-size:2rem;font-weight:800;color:#f1f5f9">{fh(grand_total)}</div>
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

        prev_cat_sums = {}
        if ENABLE_PERIOD_COMPARISON and not _prev_df.empty:
            _prev_filtered = _prev_df[_prev_df["Name"].isin(selected_names)].copy() if selected_names else _prev_df.copy()
            prev_cat_sums = {c: _prev_filtered[c].sum() for c in active_cats if c in _prev_filtered.columns}

        cat_cols_row = st.columns(len(active_cats))
        for cat, col in zip(active_cats, cat_cols_row):
            pct = cat_sums[cat] / grand * 100
            with col:
                if ENABLE_PERIOD_COMPARISON and cat in prev_cat_sums:
                    delta_h = cat_sums[cat] - prev_cat_sums[cat]
                    delta_str = f"+{fh(delta_h)}" if delta_h >= 0 else f"-{fh(abs(delta_h))}"
                    st.metric(cat, fh(cat_sums[cat]), delta_str)
                else:
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
            if expected_hours > 0:
                fig_stack.add_hline(
                    y=expected_hours,
                    line_dash="dash",
                    line_color="rgba(255,255,255,0.45)",
                    line_width=1.5,
                    annotation_text=f"Target {fh(expected_hours)}",
                    annotation_position="top right",
                    annotation_font_size=11,
                    annotation_font_color="rgba(255,255,255,0.6)",
                )
            st.markdown("**Hours by Person — Stacked by Category**")
            st.plotly_chart(fig_stack, width='stretch')

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
    <span class="card-badge" style="background:#1f2937;color:#9ca3af;border-radius:4px;padding:2px 8px;font-size:0.75rem">{selected_group}</span>
  </div>
  <div style="font-size:1.35rem;font-weight:700;margin-bottom:4px">{fh(r['Total'])}</div>
  <div class="card-sub" style="color:#9ca3af;font-size:0.8rem;margin-bottom:8px">Jira {fh(r['Jira'])} &nbsp;·&nbsp; TC {fh(r['TC'])}</div>
  <div style="font-size:0.82rem;color:{CAT_COLORS.get(dominant,'#fff')}">▶ {dominant}: {fh(r[dominant])} ({pct_dom:.0f}%)</div>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 · BY LABEL
# ═══════════════════════════════════════════════════════════════════════════
with tab2:
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

            # Assign a unique color per person from a qualitative palette
            palette = px.colors.qualitative.Plotly + px.colors.qualitative.D3
            names_ordered = cat_sorted_asc["Name"].tolist()
            color_map = {n: palette[i % len(palette)] for i, n in enumerate(names_ordered)}
            cat_sorted_asc["_color"] = cat_sorted_asc["Name"].map(color_map)

            # Add source breakdown to custom data for tooltip
            if not _raw_filtered.empty:
                src_hrs = (
                    _raw_filtered[_raw_filtered["category"] == selected_cat]
                    .groupby(["Name", "source"])["hours"].sum().unstack(fill_value=0)
                )
                for col in ["Jira", "TC"]:
                    if col not in src_hrs:
                        src_hrs[col] = 0
                src_hrs = src_hrs.reset_index()
                src_hrs["Jira_fmt"] = src_hrs["Jira"].apply(fh)
                src_hrs["TC_fmt"]   = src_hrs["TC"].apply(fh)
                cat_sorted_asc = cat_sorted_asc.merge(src_hrs[["Name","Jira_fmt","TC_fmt"]], on="Name", how="left")
                cat_sorted_asc["Jira_fmt"] = cat_sorted_asc["Jira_fmt"].fillna("0h 00m")
                cat_sorted_asc["TC_fmt"]   = cat_sorted_asc["TC_fmt"].fillna("0h 00m")
            else:
                cat_sorted_asc["Jira_fmt"] = "0h 00m"
                cat_sorted_asc["TC_fmt"]   = "0h 00m"

            fig_cat = px.bar(
                cat_sorted_asc,
                x=selected_cat, y="Name",
                color="Name",
                color_discrete_map=color_map,
                orientation="h",
                text="bar_label",
                height=max(360, len(cat_tab) * 50),
                custom_data=["pct_own", "Hours_fmt", "Jira_fmt", "TC_fmt"],
            )
            fig_cat.update_traces(
                textposition="outside",
                cliponaxis=False,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + selected_cat
                    + ": %{customdata[1]}<br>"
                    + "Jira: %{customdata[2]} · TC: %{customdata[3]}<br>"
                    + "%{customdata[0]:.1f}% of their period<extra></extra>"
                ),
            )
            fig_cat.update_layout(
                xaxis_title="Hours (decimal)",
                yaxis_title="",
                yaxis=dict(categoryorder="total ascending"),
                showlegend=False,
                margin=dict(t=40, r=130, b=20, l=0),
            )
            st.markdown(f"**{CAT_LABELS[selected_cat]} — Hours by Person**")
            st.plotly_chart(fig_cat, width='stretch')

        with cr:
            # % of workload chart — what share of each person's total hours is this category
            pct_df = cat_tab[["Name", "pct_own", selected_cat]].sort_values("pct_own", ascending=True).copy()
            pct_df["Hours_fmt"] = pct_df[selected_cat].apply(fh)
            fig_pct = px.bar(
                pct_df,
                x="pct_own", y="Name",
                orientation="h",
                text=pct_df["pct_own"].apply(lambda v: f"{v:.0f}%"),
                color="pct_own",
                color_continuous_scale=["#3B82F6", CAT_COLORS.get(selected_cat, "#F59E0B")],
                custom_data=["Hours_fmt"],
                title="% of Their Workload",
                height=max(360, len(cat_tab) * 50),
            )
            fig_pct.update_traces(
                textposition="outside",
                cliponaxis=False,
                hovertemplate="<b>%{y}</b><br>%{customdata[0]} · %{x:.1f}% of period<extra></extra>",
            )
            fig_pct.update_layout(
                xaxis_title="% of total hours",
                xaxis_ticksuffix="%",
                yaxis_title="",
                yaxis=dict(categoryorder="total ascending"),
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(t=40, r=60, b=20, l=0),
            )
            st.plotly_chart(fig_pct, width='stretch')


# ═══════════════════════════════════════════════════════════════════════════
# TAB 5 · GOAL TRACKER  (Apr 1 – Dec 31 2026, 1,250h annual goal)
# ═══════════════════════════════════════════════════════════════════════════
with tab_goal:
    _GOAL_START   = date(2026, 1, 1)
    _GOAL_END     = date(2026, 12, 31)
    _ANNUAL_GOAL  = 1250.0   # Warren's 2026 target
    _DAILY_TARGET = 6.0      # hours per workday

    _local_today_g  = (datetime.now(timezone.utc) + timedelta(hours=LOCAL_UTC_OFFSET)).date()
    _goal_fetch_end = min(_GOAL_END, _local_today_g)

    _total_wd    = _count_weekdays(_GOAL_START, _GOAL_END)
    _elapsed_wd  = _count_weekdays(_GOAL_START, _goal_fetch_end) if _goal_fetch_end >= _GOAL_START else 0
    _remaining_wd = max(_total_wd - _elapsed_wd, 0)
    _expected_to_date = _elapsed_wd * _DAILY_TARGET

    st.markdown("### 🎯 2026 Time Tracking Goal")
    st.markdown(
        f"Track progress toward **1,250 logged hours** by Dec 31, 2026 · "
        f"**6h/workday** · Jan 1 – Dec 31, 2026 &nbsp;·&nbsp; "
        f"**{_elapsed_wd}** workdays elapsed &nbsp;·&nbsp; **{_remaining_wd}** remaining"
    )
    st.caption(
        "Projected year-end total = current daily rate × remaining workdays + hours logged so far. "
        "On Track = within 10% of expected pace."
    )

    # Fetch goal-period data month-by-month (avoids Jira's 5000-issue-per-query limit)
    with st.spinner("Loading 2026 goal data (fetching month by month for accuracy)…"):
        _goal_raw = fetch_ytd_goal_data(selected_group)
        _goal_df  = build_summary_df(_goal_raw)

    # Build per-member rows (include all configured members, even those with 0h logged)
    _members_list  = sorted(GROUPS[selected_group].get("members", set()))
    _goal_members  = pd.DataFrame({"Name": _members_list})
    if not _goal_df.empty:
        _goal_members = _goal_members.merge(
            _goal_df[["Name", "Total"]], on="Name", how="left"
        ).fillna(0)
    else:
        _goal_members["Total"] = 0.0

    _goal_members = _goal_members.rename(columns={"Total": "logged_h"})
    _goal_members["expected_h"]  = _expected_to_date
    _goal_members["surplus_h"]   = _goal_members["logged_h"] - _expected_to_date

    if _elapsed_wd > 0:
        _goal_members["daily_rate"]  = _goal_members["logged_h"] / _elapsed_wd
        _goal_members["projected_h"] = (
            _goal_members["logged_h"] + _goal_members["daily_rate"] * _remaining_wd
        )
    else:
        _goal_members["daily_rate"]  = 0.0
        _goal_members["projected_h"] = 0.0

    # "On track" = logged >= 90% of expected-to-date
    _goal_members["on_track"] = (
        _goal_members["logged_h"] >= (_expected_to_date * 0.9)
        if _expected_to_date > 0
        else True
    )
    # Hours still needed per remaining workday to hit 1,250h
    _goal_members["h_per_day_needed"] = (
        (_ANNUAL_GOAL - _goal_members["logged_h"]) / (_remaining_wd or 1)
    ).clip(lower=0)

    _goal_members = _goal_members.sort_values("logged_h", ascending=False)

    # ── Summary metrics ─────────────────────────────────────────────────────
    _g_logged   = _goal_members["logged_h"].sum()
    _g_expected = _expected_to_date * len(_goal_members)
    _g_on_track = int(_goal_members["on_track"].sum())
    _g_at_risk  = int((~_goal_members["on_track"]).sum())
    _g_pct      = _g_logged / max(_g_expected, 1) * 100

    gm0, gm1, gm2, gm3, gm4 = st.columns(5)
    gm0.metric("Group Total Logged",    fh(_g_logged))
    gm1.metric("Expected to Date",      fh(_g_expected))
    gm2.metric("Workdays Elapsed",      _elapsed_wd)
    gm3.metric("✅ On Track",           _g_on_track)
    gm4.metric("⚠️ Behind Pace",        _g_at_risk)

    # ── At-risk banner ──────────────────────────────────────────────────────
    _at_risk_names = _goal_members[~_goal_members["on_track"]]["Name"].tolist()
    if _at_risk_names:
        st.error(
            f"⚠️ **Behind pace ({len(_at_risk_names)} member{'s' if len(_at_risk_names)>1 else ''}):** "
            + " · ".join(_at_risk_names)
        )
    elif _elapsed_wd > 0:
        st.success("✅ All members are on pace for the 1,250h annual goal.")

    st.divider()

    # ── Per-member progress cards ───────────────────────────────────────────
    st.markdown("#### Member Progress")

    for _, row in _goal_members.iterrows():
        logged    = row["logged_h"]
        surplus   = row["surplus_h"]
        projected = row["projected_h"]
        on_track  = row["on_track"]
        need_pd   = row["h_per_day_needed"]

        pct_pace  = (logged / max(_expected_to_date, 1) * 100) if _expected_to_date > 0 else 0
        bar_pct   = min(pct_pace, 100)

        if on_track:
            _card_bg    = "#0a1f14"
            _card_border = "#1a3a2a"
            _color       = "#10b981"
            _status_icon = "✅"
        elif pct_pace >= 75:
            _card_bg     = "#1c1507"
            _card_border = "#3a2e0a"
            _color       = "#f59e0b"
            _status_icon = "⚠️"
        else:
            _card_bg     = "#1f0a0a"
            _card_border = "#3a1a1a"
            _color       = "#ef4444"
            _status_icon = "🔴"

        surplus_str   = (f"+{fh(surplus)}" if surplus >= 0 else f"−{fh(abs(surplus))}")
        projected_str = fh(projected) if _elapsed_wd > 0 else "—"
        pace_label    = f"{pct_pace:.0f}% of pace" if _expected_to_date > 0 else "Goal period not started"

        st.markdown(f"""
<div style="border:1px solid {_card_border};border-radius:10px;padding:14px 18px;margin-bottom:10px;background:{_card_bg};">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
    <span style="font-size:0.95rem;font-weight:700;color:#e6edf3;">{_status_icon} {row['Name']}</span>
    <span style="font-size:0.82rem;color:#9ca3af;">Need <b style="color:{_color}">{need_pd:.1f}h/day</b> remaining to hit 1,250h</span>
  </div>
  <div style="display:flex;gap:24px;margin-bottom:10px;flex-wrap:wrap;align-items:baseline;">
    <span style="font-size:1.15rem;font-weight:800;color:#fff;">Logged: <span style="color:{_color}">{fh(logged)}</span></span>
    <span style="color:#9ca3af;font-size:0.88rem;">Expected: {fh(_expected_to_date)}</span>
    <span style="color:#9ca3af;font-size:0.88rem;">Δ <span style="color:{_color};font-weight:600">{surplus_str}</span></span>
    <span style="color:#9ca3af;font-size:0.88rem;">Projected EOY: <span style="color:{_color};font-weight:600">{projected_str}</span></span>
  </div>
  <div style="width:100%;background:rgba(255,255,255,0.08);border-radius:999px;height:9px;position:relative;">
    <div style="width:{bar_pct:.1f}%;background:{_color};border-radius:999px;height:9px;"></div>
  </div>
  <div style="display:flex;justify-content:space-between;margin-top:4px;">
    <span style="font-size:0.72rem;color:#6b7280;">{pace_label}</span>
    <span style="font-size:0.72rem;color:#6b7280;">1,250h annual goal</span>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Projected EOY chart ────────────────────────────────────────────────
    if _elapsed_wd > 0:
        st.divider()
        st.markdown("#### Projected Year-End Totals")

        _proj_df = _goal_members[["Name", "logged_h", "projected_h", "on_track"]].copy()
        _proj_df["projected_fmt"] = _proj_df["projected_h"].apply(fh)
        _proj_df["logged_fmt"]    = _proj_df["logged_h"].apply(fh)

        fig_proj = px.bar(
            _proj_df.sort_values("projected_h"),
            x="projected_h", y="Name",
            orientation="h",
            color="on_track",
            color_discrete_map={True: "#10b981", False: "#ef4444"},
            labels={"projected_h": "Projected Hours (EOY)", "Name": "", "on_track": "On Track"},
            custom_data=["projected_fmt", "logged_fmt"],
        )
        fig_proj.add_vline(
            x=_ANNUAL_GOAL,
            line_color="rgba(255,215,0,0.65)",
            line_dash="dash",
            line_width=2,
            annotation_text="1,250h Goal",
            annotation_position="top",
            annotation_font_color="rgba(255,215,0,0.8)",
            annotation_font_size=12,
        )
        fig_proj.update_traces(
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Projected EOY: %{customdata[0]}<br>"
                "Logged so far: %{customdata[1]}"
                "<extra></extra>"
            )
        )
        fig_proj.update_layout(
            height=max(360, len(_proj_df) * 44),
            margin=dict(t=30, b=20, l=0, r=20),
            legend=dict(
                title="On Track",
                orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
            ),
            xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.06)"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_proj, width='stretch')


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 · FULL TABLE
# ═══════════════════════════════════════════════════════════════════════════
with tab3:
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


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 · SPRINT
# ═══════════════════════════════════════════════════════════════════════════
with tab_sprint:
    with st.spinner("Loading sprints…"):
        all_sprints = fetch_sprints_for_group(selected_group)

    if not all_sprints:
        st.info(
            "No sprints found for this group. "
            "Ensure the Jira project has a Scrum board and the API user has access."
        )
    else:
        _prefix = GROUPS[selected_group].get("sprint_prefix", "")
        _filtered_sprints = sorted(
            [
                s for s in all_sprints
                if (not _prefix or s["name"].startswith(_prefix))
                and (
                    s.get("startDate", "")[:4] == "2026"
                    or s.get("endDate", "")[:4] == "2026"
                    or s.get("state") == "active"
                )
            ],
            key=lambda s: s.get("startDate", ""),
            reverse=True,
        )

        if not _filtered_sprints:
            st.info(f"No 2026 sprints found with prefix '{_prefix}' for this group.")
        else:
            _STATE_BADGE = {"active": "🟢", "closed": "⚫", "future": "🔵"}
            sprint_options = {
                s["id"]: (
                    f"{_STATE_BADGE.get(s.get('state',''), '')} {s['name']}"
                    + (f"  ({s['startDate'][:10]} → {s['endDate'][:10]})" if s.get("startDate") else "")
                )
                for s in _filtered_sprints
            }
            _active_ids = [s["id"] for s in _filtered_sprints if s.get("state") == "active"]
            _default    = _active_ids if _active_ids else [_filtered_sprints[0]["id"]]
            selected_sprint_ids = st.multiselect(
                "Select Sprint(s)",
                options=list(sprint_options.keys()),
                format_func=lambda x: sprint_options[x],
                default=_default,
            )

            sdf = pd.DataFrame()
            if not selected_sprint_ids:
                st.info("Select at least one sprint above.")
            else:
                with st.spinner("Loading sprint issues and worklogs…"):
                    sdf = fetch_sprint_issues(tuple(selected_sprint_ids), selected_group)
                if sdf.empty:
                    st.warning("No members from this group have logged time on the selected sprint(s).")

            if not sdf.empty:
                # ── Member-level aggregation ──────────────────────────────
                mem = (
                    sdf.groupby("member")
                    .agg(estimated=("estimate_h", "sum"), logged=("logged_h", "sum"), stories=("issue_key", "nunique"))
                    .reset_index()
                )
                mem["variance"] = mem["logged"] - mem["estimated"]
                mem = mem.sort_values("logged", ascending=False)

                tot_est      = mem["estimated"].sum()
                tot_log      = mem["logged"].sum()
                tot_var      = tot_log - tot_est
                tot_stories  = sdf["issue_key"].nunique()
                var_sign     = "+" if tot_var >= 0 else ""

                # ── Summary metrics ───────────────────────────────────────
                c0, c1, c2, c3 = st.columns(4)
                c0.metric("Sprint Stories", tot_stories)
                c1.metric("Total Estimated", fh(tot_est))
                c2.metric("Total Logged",    fh(tot_log))
                c3.metric("Variance", f"{var_sign}{fh(tot_var)}")

                st.divider()

                # ── Grouped horizontal bar: estimated vs logged ───────────
                bar_melt = mem[["member", "estimated", "logged"]].melt(
                    id_vars="member", var_name="Type", value_name="Hours"
                )
                bar_melt["Hours_fmt"] = bar_melt["Hours"].apply(fh)
                sort_names = mem["member"].tolist()

                fig_bar = px.bar(
                    bar_melt,
                    x="Hours", y="member",
                    color="Type",
                    barmode="group",
                    orientation="h",
                    category_orders={"member": sort_names[::-1], "Type": ["estimated", "logged"]},
                    color_discrete_map={"estimated": "#3b82f6", "logged": "#10b981"},
                    custom_data=["Hours_fmt"],
                    labels={"Hours": "Hours (decimal)", "member": ""},
                    title="Estimated vs Logged Hours by Member",
                )
                fig_bar.update_traces(
                    hovertemplate="<b>%{y}</b><br>%{fullData.name}: %{customdata[0]}<extra></extra>"
                )
                fig_bar.update_layout(
                    height=max(350, len(sort_names) * 50 + 140),
                    legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
                    margin=dict(t=50, b=60, l=0, r=20),
                )
                st.plotly_chart(fig_bar, width='stretch')

                # ── Bottom row: scatter (accuracy) + variance bar ─────────
                left, right = st.columns(2)

                with left:
                    _max = max(float(mem[["estimated", "logged"]].max().max()) * 1.15, 10)
                    fig_sc = px.scatter(
                        mem,
                        x="estimated", y="logged",
                        text="member",
                        size="stories",
                        size_max=30,
                        color="variance",
                        color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                        range_color=[-15, 15],
                        title="Accuracy — Estimated vs Logged",
                        labels={"estimated": "Estimated (h)", "logged": "Logged (h)", "variance": "Variance (h)"},
                        custom_data=["member", "stories"],
                    )
                    fig_sc.add_shape(
                        type="line", x0=0, y0=0, x1=_max, y1=_max,
                        line=dict(color="rgba(255,255,255,0.25)", dash="dash", width=1),
                    )
                    fig_sc.update_traces(
                        textposition="top center",
                        hovertemplate=(
                            "<b>%{customdata[0]}</b><br>"
                            "Estimated: %{x:.1f}h<br>"
                            "Logged: %{y:.1f}h<br>"
                            "Stories: %{customdata[1]}<extra></extra>"
                        ),
                    )
                    fig_sc.update_layout(
                        height=380,
                        showlegend=False,
                        coloraxis_showscale=False,
                        margin=dict(t=40, b=20, l=0, r=20),
                        xaxis=dict(range=[0, _max]),
                        yaxis=dict(range=[0, _max]),
                    )
                    st.plotly_chart(fig_sc, width='stretch')

                with right:
                    mem_var = mem.copy()
                    mem_var["var_fmt"] = mem_var["variance"].apply(
                        lambda v: f"+{fh(v)}" if v >= 0 else f"-{fh(abs(v))}"
                    )
                    fig_var = px.bar(
                        mem_var.sort_values("variance"),
                        x="variance", y="member",
                        orientation="h",
                        color="variance",
                        color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                        range_color=[-15, 15],
                        title="Variance per Member (Logged − Estimated)",
                        labels={"variance": "Variance (h)", "member": ""},
                        custom_data=["var_fmt"],
                    )
                    fig_var.add_vline(
                        x=0,
                        line_color="rgba(255,255,255,0.35)",
                        line_dash="dash",
                        line_width=1,
                    )
                    fig_var.update_traces(
                        hovertemplate="<b>%{y}</b><br>Variance: %{customdata[0]}<extra></extra>"
                    )
                    fig_var.update_layout(
                        height=380,
                        showlegend=False,
                        coloraxis_showscale=False,
                        margin=dict(t=40, b=20, l=0, r=20),
                    )
                    st.plotly_chart(fig_var, width='stretch')

                # ── Story detail expander ─────────────────────────────────
                with st.expander("📋 Story Detail", expanded=False):
                    detail = sdf[sdf["logged_h"] > 0].copy()
                    detail["Estimate"] = detail["estimate_h"].apply(fh)
                    detail["Logged"]   = detail["logged_h"].apply(fh)
                    st.dataframe(
                        detail[["sprint", "issue_key", "summary", "member", "assignee", "Estimate", "Logged", "status"]],
                        use_container_width=True,
                        column_config={
                            "sprint":     "Sprint",
                            "issue_key":  "Issue",
                            "summary":    st.column_config.TextColumn("Summary", width="large"),
                            "member":     "Member",
                            "assignee":   "Assignee",
                            "Estimate":   "Estimate",
                            "Logged":     "Logged",
                            "status":     "Status",
                        },
                        hide_index=True,
                    )
