import streamlit as st
import pandas as pd
import plotly.express as px

# ── Page config ───────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EUC Weekly Report — 2026-W17",
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


# ── Raw data ──────────────────────────────────────────────────────────────
# Columns: Name, Region, Total, EUC (Jira), TC (TechConnect),
#          KTLO, Initiative, Tech Debt, Svc Req, Incident
_RAW = [
    ("Nick Shelton",              "US", "38h 50m", "27h 30m", "11h 20m", "5h 48m",  "17h 24m", "4h 12m",  "8h 54m",  "2h 24m"),
    ("Jake Snodgrass",            "US", "32h 50m", "32h 35m", "0h 15m",  "9h 18m",  "22h 0m",  "1h 30m",  "0h 0m",   "0h 0m"),
    ("Matthew Davis",             "US", "32h 5m",  "15h 15m", "16h 50m", "9h 0m",   "0h 0m",   "6h 12m",  "15h 36m", "1h 12m"),
    ("Khai Nguyen",               "US", "31h 30m", "29h 0m",  "2h 30m",  "26h 0m",  "0h 0m",   "3h 0m",   "2h 30m",  "0h 0m"),
    ("Justin Pham",               "US", "30h 30m", "17h 0m",  "13h 30m", "6h 30m",  "0h 0m",   "10h 30m", "10h 6m",  "3h 24m"),
    ("Nicholas Bowling",          "US", "30h 30m", "7h 30m",  "23h 0m",  "5h 30m",  "0h 0m",   "2h 0m",   "16h 12m", "6h 48m"),
    ("Wes Hurd",                  "US", "29h 55m", "24h 0m",  "5h 55m",  "0h 0m",   "24h 0m",  "0h 0m",   "1h 12m",  "4h 48m"),
    ("Kenneth Calvert",           "US", "29h 45m", "26h 30m", "3h 15m",  "10h 0m",  "15h 0m",  "1h 30m",  "3h 0m",   "0h 18m"),
    ("Jaylon Martin",             "US", "12h 54m", "5h 42m",  "7h 12m",  "5h 42m",  "0h 0m",   "0h 0m",   "7h 12m",  "0h 0m"),
    ("Hector Cossyleon",          "US", "7h 30m",  "2h 0m",   "5h 30m",  "2h 0m",   "0h 0m",   "0h 0m",   "5h 0m",   "0h 30m"),
    ("Eduardo Rangel Ruiz",       "MX", "30h 6m",  "3h 54m",  "26h 12m", "3h 54m",  "0h 0m",   "0h 0m",   "20h 54m", "5h 18m"),
    ("Alonso Renteria Olvera",    "MX", "29h 0m",  "2h 0m",   "27h 0m",  "2h 0m",   "0h 0m",   "0h 0m",   "27h 0m",  "0h 0m"),
    ("Santiago Morales",          "MX", "23h 49m", "9h 18m",  "14h 31m", "7h 0m",   "0h 0m",   "2h 18m",  "13h 30m", "1h 0m"),
    ("Antonio Lopez",             "MX", "19h 15m", "11h 25m", "7h 50m",  "10h 12m", "1h 12m",  "0h 0m",   "7h 48m",  "0h 0m"),
    ("Luis Tejeda Sosa",          "MX", "18h 40m", "8h 0m",   "10h 40m", "8h 0m",   "0h 0m",   "0h 0m",   "6h 18m",  "4h 18m"),
    ("Esaú Gallardo",             "MX", "14h 29m", "8h 38m",  "5h 51m",  "8h 36m",  "0h 0m",   "0h 0m",   "5h 48m",  "0h 0m"),
    ("Roberto Gaitan Zamudio",    "MX", "11h 0m",  "0h 0m",   "11h 0m",  "0h 0m",   "0h 0m",   "0h 0m",   "11h 0m",  "0h 0m"),
    ("Gabriela Martinez Atriano", "MX", "4h 0m",   "4h 0m",   "0h 0m",   "4h 0m",   "0h 0m",   "0h 0m",   "0h 0m",   "0h 0m"),
]

_COLS = [
    "Name", "Region",
    "_total_s", "_euc_s", "_tc_s",
    "_ktlo_s", "_init_s", "_debt_s", "_svcreq_s", "_incident_s",
]

df = pd.DataFrame(_RAW, columns=_COLS)

_COL_MAP = {
    "_total_s":    "Total",
    "_euc_s":      "EUC (Jira)",
    "_tc_s":       "TC (TechConnect)",
    "_ktlo_s":     "KTLO",
    "_init_s":     "Initiative",
    "_debt_s":     "Tech Debt",
    "_svcreq_s":   "Svc Req",
    "_incident_s": "Incident",
}
for raw_col, clean_col in _COL_MAP.items():
    df[clean_col] = df[raw_col].apply(ph)
df = df[[c for c in df.columns if not c.startswith("_")]]


# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 EUC Weekly")
    st.markdown("**Apr 20–24, 2026 · W17**")
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
        options=["Both", "EUC (Jira)", "TC (TechConnect)"],
        help="EUC = Jira EUC project · TC = TechConnect helpdesk",
    )
    st.divider()
    st.caption("18 members · 426h 38m total  \nJira EUC + TechConnect")


# ── Apply filters ─────────────────────────────────────────────────────────
mask = df["Region"].isin(region_filter)
if search_query.strip():
    mask &= df["Name"].str.contains(search_query.strip(), case=False, na=False)
fdf = df[mask].copy()

if source_filter == "EUC (Jira)":
    fdf["_display_total"] = fdf["EUC (Jira)"]
elif source_filter == "TC (TechConnect)":
    fdf["_display_total"] = fdf["TC (TechConnect)"]
else:
    fdf["_display_total"] = fdf["Total"]


# ── Page header ───────────────────────────────────────────────────────────
st.markdown("# EUC Team — Weekly Report")
st.markdown("**Week of Apr 20–24, 2026 · 2026-W17**")

if search_query.strip():
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
    eu_total  = 0.0  # Julian Hoeksema: 0h logged W17
    all_total = us_total + mx_total + eu_total

    st.markdown("""
<div style="margin-bottom:6px;">
  <span style="font-size:1.5rem;font-weight:800;letter-spacing:-0.5px">EUC Team — Executive Dashboard</span><br>
  <span style="color:#6b7280;font-size:0.9rem">Week of Apr 20–24, 2026 &nbsp;·&nbsp; 2026-W17 &nbsp;·&nbsp; Full team · Sidebar filters not applied</span>
</div>
""", unsafe_allow_html=True)

    st.divider()

    # ── Region summary row ──────────────────────────────────────────────
    st.markdown("##### Team Hours by Region")
    r1, r2, r3, r4 = st.columns(4)
    with r1:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e3a5f,#1e3a8a);border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#93c5fd;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">🇺🇸 US Team</div>
  <div style="font-size:2rem;font-weight:800;color:#fff">{fh(us_total)}</div>
  <div style="color:#93c5fd;font-size:0.82rem;margin-top:4px">{int((us_df["Total"] > 0).sum())} active members</div>
  <div style="color:#60a5fa;font-size:0.82rem">{us_total / all_total * 100:.0f}% of team total</div>
</div>""", unsafe_allow_html=True)
    with r2:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#4a1942,#7c3aed);border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#c4b5fd;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">🇲🇽 MX Team</div>
  <div style="font-size:2rem;font-weight:800;color:#fff">{fh(mx_total)}</div>
  <div style="color:#c4b5fd;font-size:0.82rem;margin-top:4px">{int((mx_df["Total"] > 0).sum())} active members</div>
  <div style="color:#a78bfa;font-size:0.82rem">{mx_total / all_total * 100:.0f}% of team total</div>
</div>""", unsafe_allow_html=True)
    with r3:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1a2e1a,#374151);border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#9ca3af;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">🇳🇱 Netherlands</div>
  <div style="font-size:2rem;font-weight:800;color:#6b7280">0h 00m</div>
  <div style="color:#6b7280;font-size:0.82rem;margin-top:4px">0 active members</div>
  <div style="color:#6b7280;font-size:0.82rem">0% of team total</div>
</div>""", unsafe_allow_html=True)
    with r4:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1c1c2e,#0f172a);border:1px solid #334155;border-radius:12px;padding:20px 22px;text-align:center;">
  <div style="color:#94a3b8;font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">⏱ Grand Total</div>
  <div style="font-size:2rem;font-weight:800;color:#f1f5f9">{fh(all_total)}</div>
  <div style="color:#94a3b8;font-size:0.82rem;margin-top:4px">18 members · 2 regions</div>
  <div style="color:#64748b;font-size:0.82rem">Week 17 · Apr 20–24</div>
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
                title="% of week",
                ticksuffix="%",
                range=[0, 100],
                showgrid=True,
                gridcolor="rgba(255,255,255,0.06)",
            ),
            yaxis=dict(title="", tickfont=dict(size=11)),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0, font=dict(size=11)),
            margin=dict(t=60, b=20, l=0, r=10),
            height=max(340, len(team_df) * 42),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        fig.update_xaxes(showline=False)
        fig.update_yaxes(showgrid=False)
        return fig

    with col_us:
        st.plotly_chart(
            make_normalized_bar(us_df, "🇺🇸 US Team — Time by Category"),
            use_container_width=True,
        )

    with col_mx:
        st.plotly_chart(
            make_normalized_bar(mx_df, "🇲🇽 MX Team — Time by Category"),
            use_container_width=True,
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
        insidetextanchor="middle",
        marker_line_width=2,
        marker_line_color="rgba(0,0,0,0.3)",
    )
    fig_tree.update_layout(
        height=480,
        margin=dict(t=10, b=0, l=0, r=0),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_tree, use_container_width=True)

    # ── Footer note ──────────────────────────────────────────────────────
    st.markdown("""
<div style="color:#4b5563;font-size:0.78rem;margin-top:8px;border-top:1px solid #1f2937;padding-top:10px;">
  Source: Jira EUC project + TechConnect · W17 (Apr 20–24 2026) ·
  KTLO = Keep The Lights On · TC tickets classified as Svc Req or Incident ·
  Edgar Aquino Lopez, Joshua Ramos Dailey (MX) and Julian Hoeksema (NL) logged 0h this week.
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
        cat_sums = {c: fdf[c].sum() for c in CATEGORIES}
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
        c1, c2, c3, c4, c5 = st.columns(5)
        for cat, col in zip(CATEGORIES, [c1, c2, c3, c4, c5]):
            pct = cat_sums[cat] / grand * 100
            with col:
                st.metric(cat, fh(cat_sums[cat]), f"{pct:.0f}%")

        st.divider()

        # ── Charts row ──
        left, right = st.columns([1, 2])

        with left:
            pie_df = pd.DataFrame({
                "Category": CATEGORIES,
                "Hours": [cat_sums[c] for c in CATEGORIES],
                "Hours_fmt": [fh(cat_sums[c]) for c in CATEGORIES],
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
            st.plotly_chart(fig_pie, use_container_width=True)

        with right:
            sort_order = fdf.sort_values("Total", ascending=False)["Name"].tolist()
            stack_melt = fdf.melt(
                id_vars=["Name", "Region"],
                value_vars=CATEGORIES,
                var_name="Category", value_name="Hours",
            )
            stack_melt["Hours_fmt"] = stack_melt["Hours"].apply(fh)

            fig_stack = px.bar(
                stack_melt,
                x="Name", y="Hours",
                color="Category",
                color_discrete_map=CAT_COLORS,
                category_orders={"Name": sort_order, "Category": CATEGORIES},
                title="Hours by Person — Stacked by Category",
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
                margin=dict(t=60, b=80, l=0, r=0),
            )
            st.plotly_chart(fig_stack, use_container_width=True)

        # ── US vs MX comparison ──
        if len(region_filter) > 1 and not search_query.strip():
            st.divider()
            st.markdown("#### US vs MX — Category Comparison")

            reg_agg = fdf.groupby("Region")[CATEGORIES].sum().reset_index()
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
            st.plotly_chart(fig_reg, use_container_width=True)


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
        st.markdown(f"`{row['Region']}` · Week of Apr 20–24, 2026")

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total Hours", fh(row["Total"]))
        with m2:
            st.metric("EUC (Jira)", fh(row["EUC (Jira)"]))
        with m3:
            st.metric("TC (TechConnect)", fh(row["TC (TechConnect)"]))

        st.divider()

        cat_vals = [row[c] for c in CATEGORIES]
        cat_df = pd.DataFrame({
            "Category": CATEGORIES,
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
            st.plotly_chart(fig_p, use_container_width=True)

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
            st.plotly_chart(fig_b, use_container_width=True)

    else:
        # ── Multi-person view ──
        st.markdown(f"**{len(fdf)} people** — sorted by total hours")

        sort_order_asc = fdf.sort_values("Total", ascending=True)["Name"].tolist()
        ind_melt = fdf.melt(
            id_vars=["Name", "Region"],
            value_vars=CATEGORIES,
            var_name="Category", value_name="Hours",
        )
        ind_melt["Hours_fmt"] = ind_melt["Hours"].apply(fh)

        fig_ind = px.bar(
            ind_melt,
            y="Name", x="Hours",
            color="Category",
            color_discrete_map=CAT_COLORS,
            orientation="h",
            category_orders={"Name": sort_order_asc, "Category": CATEGORIES},
            title="Hours by Individual — Stacked by Category",
            height=max(420, len(fdf) * 44),
            custom_data=["Hours_fmt"],
        )
        fig_ind.update_traces(
            hovertemplate="<b>%{y}</b><br>%{fullData.name}: %{customdata[0]}<extra></extra>"
        )
        fig_ind.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(t=60, b=10, l=0, r=0),
            xaxis_title="Hours (decimal)",
        )
        st.plotly_chart(fig_ind, use_container_width=True)

        st.divider()
        st.markdown("#### Member Cards")

        grid_cols = st.columns(3)
        for i, (_, r) in enumerate(fdf.sort_values("Total", ascending=False).iterrows()):
            dominant = max(CATEGORIES, key=lambda c: r[c])
            pct_dom = r[dominant] / (r["Total"] or 1) * 100
            with grid_cols[i % 3]:
                st.markdown(f"""
<div class="member-card">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
    <strong style="font-size:0.9rem">{r['Name']}</strong>
    <span style="background:#1f2937;color:#9ca3af;border-radius:4px;padding:2px 8px;font-size:0.75rem">{r['Region']}</span>
  </div>
  <div style="font-size:1.35rem;font-weight:700;margin-bottom:4px">{fh(r['Total'])}</div>
  <div style="color:#6b7280;font-size:0.8rem;margin-bottom:8px">EUC {fh(r['EUC (Jira)'])} &nbsp;·&nbsp; TC {fh(r['TC (TechConnect)'])}</div>
  <div style="font-size:0.82rem;color:{CAT_COLORS.get(dominant,'#fff')}">▶ {dominant}: {fh(r[dominant])} ({pct_dom:.0f}%)</div>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 · BY LABEL
# ═══════════════════════════════════════════════════════════════════════════
with tab3:
    selected_cat = st.selectbox(
        "Work category",
        options=CATEGORIES,
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
                title=f"{CAT_LABELS[selected_cat]} — Hours by Person",
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
                margin=dict(t=60, r=130, b=20, l=0),
            )
            st.plotly_chart(fig_cat, use_container_width=True)

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
                st.plotly_chart(fig_r, use_container_width=True)

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

    if source_filter == "EUC (Jira)":
        show_cols = ["Name", "Region", "Total", "EUC (Jira)"] + CATEGORIES
    elif source_filter == "TC (TechConnect)":
        show_cols = ["Name", "Region", "Total", "TC (TechConnect)"] + CATEGORIES
    else:
        show_cols = ["Name", "Region", "Total", "EUC (Jira)", "TC (TechConnect)"] + CATEGORIES

    # Sort numerically first, then format for display
    sorted_fdf = fdf.sort_values("Total", ascending=False)
    display_df = sorted_fdf[show_cols].copy()
    for col in show_cols:
        if col not in ("Name", "Region"):
            display_df[col] = display_df[col].apply(fh)

    st.dataframe(
        display_df,
        use_container_width=True,
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
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("")
    # Download (numeric values for CSV)
    csv_data = sorted_fdf[show_cols].copy()
    st.download_button(
        label="⬇️  Download as CSV",
        data=csv_data.to_csv(index=False),
        file_name="EUC_W17_2026.csv",
        mime="text/csv",
    )
