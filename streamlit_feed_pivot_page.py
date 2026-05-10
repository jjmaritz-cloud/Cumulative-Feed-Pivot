# streamlit_feed_pivot_page.py
# Cumulative Feed Pivot Streamlit app
# Updates:
# - Removed Pivot Summary and Age Detail Export tabs from the main report menu.
# - Filters dropdown is closed by default.
# - Uploaded workbook can be saved locally as versioned data.
# - App loads the latest saved workbook on reopen so data is not lost.
# - Data version management is available from the menu.
# - Farm dropdown bar turns red/white if any flock has HW Variance <= -10.
# - Feed actual/std cells turn orange when feed variance is outside +/-10 kg/bird.

from __future__ import annotations

import base64
import html
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "saved_data_versions"
MANIFEST_PATH = DATA_DIR / "manifest.json"

DATA_SHEET_NAME = "DATA"
STANDARD_SHEET_NAME = "Standards ISA Floor"

HW_VARIANCE_RED_TRIGGER = -10.0
FEED_VARIANCE_ORANGE_LIMIT = 10.0


# -----------------------------------------------------------------------------
# System exclusions
# -----------------------------------------------------------------------------
EXCLUDED_FLOCKS = [
    {"Farm_Name": "Jordan Layer", "Flock_Name": "2708-241103"},
]




def apply_data_quality_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove impossible rows from all app views by default.

    Negative cumulative feed kg/bird creates impossible feed gaps such as
    -263 kg/bird or -677 kg/bird. Those rows are data errors, not bird insights.
    """
    if df is None or df.empty:
        return df

    clean_df = df.copy()

    if "Cumulative Feed kg/Bird_Calc" in clean_df.columns:
        actual_feed = pd.to_numeric(clean_df["Cumulative Feed kg/Bird_Calc"], errors="coerce")
        clean_df = clean_df[
            actual_feed.notna()
            & (actual_feed >= 0)
        ].copy()

    if "Cumm_Feed_std" in clean_df.columns:
        std_feed = pd.to_numeric(clean_df["Cumm_Feed_std"], errors="coerce")
        clean_df = clean_df[
            std_feed.notna()
            & (std_feed >= 0)
        ].copy()

    if "Closing_Bird_Numbers" in clean_df.columns:
        birds = pd.to_numeric(clean_df["Closing_Bird_Numbers"], errors="coerce")
        clean_df = clean_df[
            birds.notna()
            & (birds > 0)
        ].copy()

    return clean_df


def apply_system_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    """Remove flocks that should be excluded from all app views by default."""
    if df is None or df.empty:
        return df

    clean_df = df.copy()

    for item in EXCLUDED_FLOCKS:
        farm = str(item.get("Farm_Name", "")).strip()
        flock = str(item.get("Flock_Name", "")).strip()

        if "Farm_Name" in clean_df.columns and "Flock_Name" in clean_df.columns:
            clean_df = clean_df[
                ~(
                    clean_df["Farm_Name"].astype(str).str.strip().eq(farm)
                    & clean_df["Flock_Name"].astype(str).str.strip().eq(flock)
                )
            ].copy()

    return clean_df



REQUIRED_COLUMNS = [
    "State",
    "Reporting_Period",
    "Week_End_Date",
    "Farm_Name",
    "Flock_Name",
    "Flock_Status",
    "Egg_Type",
    "RoundDownAgeCalc",
    "Cumulative Feed kg/Bird_Calc",
    "Cumm_Feed_std",
    "Closing_Bird_Numbers",
    "Final",
    "HW%",
    "HW% std",
]

REQUIRED_STANDARD_COLUMNS = [
    "Age",
    "CummFeed Calc",
]


# -----------------------------------------------------------------------------
# Version storage
# -----------------------------------------------------------------------------
def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not MANIFEST_PATH.exists():
        MANIFEST_PATH.write_text(json.dumps({"versions": [], "active_version": None}, indent=2), encoding="utf-8")


def load_manifest() -> dict:
    ensure_data_dir()
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"versions": [], "active_version": None}


def save_manifest(manifest: dict) -> None:
    ensure_data_dir()
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def save_uploaded_workbook(uploaded_file, version_note: str = "") -> dict:
    ensure_data_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = Path(uploaded_file.name).name.replace(" ", "_")
    version_id = f"v_{timestamp}"
    filename = f"{version_id}_{safe_name}"
    saved_path = DATA_DIR / filename

    uploaded_file.seek(0)
    saved_path.write_bytes(uploaded_file.read())

    manifest = load_manifest()
    entry = {
        "version_id": version_id,
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "original_name": uploaded_file.name,
        "filename": filename,
        "note": version_note.strip(),
    }
    manifest.setdefault("versions", []).append(entry)
    manifest["active_version"] = version_id
    save_manifest(manifest)
    return entry


def get_version_entry(version_id: Optional[str]) -> Optional[dict]:
    if not version_id:
        return None
    manifest = load_manifest()
    for entry in manifest.get("versions", []):
        if entry.get("version_id") == version_id:
            return entry
    return None


def get_latest_version_entry() -> Optional[dict]:
    manifest = load_manifest()
    active = get_version_entry(manifest.get("active_version"))
    if active:
        return active
    versions = manifest.get("versions", [])
    return versions[-1] if versions else None


def get_version_path(entry: dict) -> Path:
    return DATA_DIR / entry["filename"]


def set_active_version(version_id: str) -> None:
    manifest = load_manifest()
    manifest["active_version"] = version_id
    save_manifest(manifest)


def delete_version(version_id: str) -> None:
    manifest = load_manifest()
    versions = manifest.get("versions", [])
    entry = next((v for v in versions if v.get("version_id") == version_id), None)
    if entry:
        path = get_version_path(entry)
        if path.exists():
            path.unlink()
    manifest["versions"] = [v for v in versions if v.get("version_id") != version_id]
    if manifest.get("active_version") == version_id:
        manifest["active_version"] = manifest["versions"][-1]["version_id"] if manifest["versions"] else None
    save_manifest(manifest)


# -----------------------------------------------------------------------------
# Data loading and pivot building
# -----------------------------------------------------------------------------
def clean_money(value):
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    txt = str(value).replace("$", "").replace(",", "").replace("(", "-").replace(")", "").strip()
    try:
        return float(txt)
    except ValueError:
        return 0.0


@st.cache_data(show_spinner="Loading workbook...")
def load_amino_workbook_from_path(path_string: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    path = Path(path_string)
    data_df = pd.read_excel(path, sheet_name=DATA_SHEET_NAME)

    missing = [c for c in REQUIRED_COLUMNS if c not in data_df.columns]
    if missing:
        st.error(f"Missing required DATA columns: {missing}")
        st.stop()

    data_df = data_df.copy()
    for col in ["Farm_Name", "Flock_Name", "State", "Flock_Status", "Egg_Type", "Reporting_Period"]:
        data_df[col] = data_df[col].astype(str).str.strip()

    data_df = data_df[
        (data_df["Farm_Name"].notna())
        & (data_df["Farm_Name"].str.len() > 0)
        & (data_df["Farm_Name"].str.lower() != "nan")
        & (data_df["Flock_Name"].notna())
        & (data_df["Flock_Name"].str.len() > 0)
        & (data_df["Flock_Name"].str.lower() != "nan")
    ]

    data_df["Week_End_Date"] = pd.to_datetime(data_df["Week_End_Date"], errors="coerce")
    data_df["RoundDownAgeCalc"] = pd.to_numeric(data_df["RoundDownAgeCalc"], errors="coerce")
    data_df["Cumulative Feed kg/Bird_Calc"] = pd.to_numeric(data_df["Cumulative Feed kg/Bird_Calc"], errors="coerce")
    data_df["Cumm_Feed_std"] = pd.to_numeric(data_df["Cumm_Feed_std"], errors="coerce")
    data_df["Closing_Bird_Numbers"] = pd.to_numeric(data_df["Closing_Bird_Numbers"], errors="coerce").fillna(0)
    data_df["Final"] = data_df["Final"].apply(clean_money)
    data_df["HW%"] = pd.to_numeric(data_df["HW%"], errors="coerce")
    data_df["HW% std"] = pd.to_numeric(data_df["HW% std"], errors="coerce")

    standard_df = pd.read_excel(path, sheet_name=STANDARD_SHEET_NAME)
    missing_standard = [c for c in REQUIRED_STANDARD_COLUMNS if c not in standard_df.columns]
    if missing_standard:
        st.error(f"Missing required standard-sheet columns: {missing_standard}")
        st.stop()

    standard_df = standard_df.copy()
    standard_df["Age"] = pd.to_numeric(standard_df["Age"], errors="coerce")
    standard_df["CummFeed Calc"] = pd.to_numeric(standard_df["CummFeed Calc"], errors="coerce")
    standard_df = standard_df.dropna(subset=["Age", "CummFeed Calc"])
    standard_df = standard_df.sort_values("CummFeed Calc").reset_index(drop=True)

    data_df = apply_system_exclusions(data_df)
    data_df = apply_data_quality_exclusions(data_df)

    return data_df, standard_df


def get_age_from_standard(max_cumm_feed_std, standard_df: pd.DataFrame):
    if pd.isna(max_cumm_feed_std) or standard_df.empty:
        return pd.NA
    diff = (standard_df["CummFeed Calc"] - float(max_cumm_feed_std)).abs()
    idx = diff.idxmin()
    return standard_df.loc[idx, "Age"]


def build_pivot(df: pd.DataFrame, standard_df: pd.DataFrame, include_age: bool = True, feed_price_per_kg: float = 0.50, egg_price_per_dozen: float = 2.50, production_days: int = 7) -> pd.DataFrame:
    feed_price_per_kg = 0.50 if feed_price_per_kg is None else float(feed_price_per_kg)
    egg_price_per_dozen = 2.50 if egg_price_per_dozen is None else float(egg_price_per_dozen)
    production_days = 7 if production_days is None else int(production_days)
    group_cols = ["Farm_Name", "Flock_Name"]
    if include_age:
        group_cols.append("RoundDownAgeCalc")

    pivot = (
        df.groupby(group_cols, dropna=False)
        .agg(
            **{
                "Max of Cumulative Feed kg/Bird_Calc": ("Cumulative Feed kg/Bird_Calc", "max"),
                "Max of Cumm_Feed_std": ("Cumm_Feed_std", "max"),
                "Max Closing Birds": ("Closing_Bird_Numbers", "max"),
                "Max of Final": ("Final", "max"),
                "Average of HW%": ("HW%", "mean"),
                "Average of HW% std": ("HW% std", "mean"),
            }
        )
        .reset_index()
    )

    pivot["Max Standard Age"] = pivot["Max of Cumm_Feed_std"].apply(lambda x: get_age_from_standard(x, standard_df))
    pivot["Feed Variance kg/Bird"] = pivot["Max of Cumulative Feed kg/Bird_Calc"] - pivot["Max of Cumm_Feed_std"]

    # Positive = under standard / feed saving. Negative = over standard / extra feed used.
    pivot["Feed kg Better / Worse vs Std"] = (
        (pivot["Max of Cumm_Feed_std"] - pivot["Max of Cumulative Feed kg/Bird_Calc"])
        * pivot["Max Closing Birds"]
    )

    # Positive = better off / feed saving because actual cumulative feed is below standard.
    # Negative = worse off / extra feed cost because actual cumulative feed is above standard.
    pivot["Feed $ Better / Worse vs Std"] = (
        (pivot["Max of Cumm_Feed_std"] - pivot["Max of Cumulative Feed kg/Bird_Calc"])
        * pivot["Max Closing Birds"]
        * float(feed_price_per_kg)
    )

    pivot["HW Variance"] = pivot["Average of HW%"] - pivot["Average of HW% std"]

    # HW production impact using closing birds.
    # This is the practical egg-production side of the feed story.
    pivot["HW Total Eggs/day"] = pivot["Max Closing Birds"] * (pivot["Average of HW%"] / 100.0)
    pivot["HW Std Total Eggs/day"] = pivot["Max Closing Birds"] * (pivot["Average of HW% std"] / 100.0)
    pivot["HW Egg Variance/day"] = pivot["HW Total Eggs/day"] - pivot["HW Std Total Eggs/day"]
    pivot["HW Dozen Variance/day"] = pivot["HW Egg Variance/day"] / 12.0
    pivot["HW $ Variance/day"] = pivot["HW Dozen Variance/day"] * float(egg_price_per_dozen)
    pivot["HW Egg Variance/period"] = pivot["HW Egg Variance/day"] * int(production_days)
    pivot["HW Dozen Variance/period"] = pivot["HW Dozen Variance/day"] * int(production_days)
    pivot["HW $ Variance/period"] = pivot["HW $ Variance/day"] * int(production_days)

    # Production impact from HW% variance.
    # Positive = more eggs/dozens than standard. Negative = fewer eggs/dozens than standard.
    pivot["Eggs Better / Worse vs Std"] = (
        (pivot["HW Variance"] / 100.0)
        * pivot["Max Closing Birds"]
        * int(production_days)
    )
    pivot["Egg dozens Better / Worse vs Std"] = pivot["Eggs Better / Worse vs Std"] / 12.0
    pivot["Egg $ Better / Worse vs Std"] = pivot["Egg dozens Better / Worse vs Std"] * float(egg_price_per_dozen)
    pivot["Net $ Impact vs Std"] = pivot["Feed $ Better / Worse vs Std"] + pivot["Egg $ Better / Worse vs Std"]

    ordered_cols = ["Farm_Name", "Flock_Name", "Max Standard Age"]
    ordered_cols += [
        "Max of Cumulative Feed kg/Bird_Calc",
        "Max of Cumm_Feed_std",
        "Feed Variance kg/Bird",
        "Max Closing Birds",
        "Feed kg Better / Worse vs Std",
        "Feed $ Better / Worse vs Std",
        "Egg dozens Better / Worse vs Std",
        "Egg $ Better / Worse vs Std",
        "Net $ Impact vs Std",
        "Max of Final",
        "Average of HW%",
        "Average of HW% std",
        "HW Variance",
        "HW Total Eggs/day",
        "HW Std Total Eggs/day",
        "HW Egg Variance/day",
        "HW Egg Variance/period",
        "HW Dozen Variance/day",
        "HW Dozen Variance/period",
        "HW $ Variance/day",
        "HW $ Variance/period",
    ]

    sort_cols = ["Farm_Name", "Flock_Name"]
    if include_age:
        sort_cols.append("RoundDownAgeCalc")

    
    pivot = pivot.sort_values(sort_cols).reset_index(drop=True)
    return pivot[ordered_cols].reset_index(drop=True)



# -----------------------------------------------------------------------------
# Styling and rendering
# -----------------------------------------------------------------------------
def multiselect_with_all(label, options, default=None, key=None):
    options = sorted([x for x in options if pd.notna(x) and str(x).strip() != ""])
    if default is None:
        default = options
    return st.multiselect(label, options=options, default=default, key=key)


def format_cell_value(col: str, value) -> str:
    if pd.isna(value):
        return ""
    if col in ["Max Standard Age", "RoundDownAgeCalc", "Max Closing Birds", "Feed kg Better / Worse vs Std", "Egg dozens Better / Worse vs Std", "HW Total Eggs/day", "HW Std Total Eggs/day", "HW Egg Variance/day", "HW Egg Variance/period", "HW Dozen Variance/day", "HW Dozen Variance/period"]:
        return f"{value:,.0f}"
    if col in ["Max of Cumulative Feed kg/Bird_Calc", "Max of Cumm_Feed_std", "Feed Variance kg/Bird"]:
        return f"{value:,.1f}"
    if col in ["Max of Final", "Feed $ Better / Worse vs Std", "Egg $ Better / Worse vs Std", "Net $ Impact vs Std", "HW $ Variance/day", "HW $ Variance/period"]:
        return f"${value:,.2f}"
    if col in ["Average of HW%", "Average of HW% std", "HW Variance"]:
        return f"{value:,.2f}"
    return str(value)


def cell_style(col: str, row: pd.Series) -> str:
    styles = []

    if col in ["Max of Final", "Feed kg Better / Worse vs Std", "Feed $ Better / Worse vs Std", "Egg dozens Better / Worse vs Std", "Egg $ Better / Worse vs Std", "Net $ Impact vs Std", "HW Variance", "HW Egg Variance/day", "HW Egg Variance/period", "HW Dozen Variance/day", "HW Dozen Variance/period", "HW $ Variance/day", "HW $ Variance/period", "Feed Variance kg/Bird"]:
        val = row.get(col, pd.NA)
        if pd.notna(val):
            styles.append("color:#0f766e;font-weight:800;" if val >= 0 else "color:#b91c1c;font-weight:800;")

    if col in ["Max of Cumulative Feed kg/Bird_Calc", "Max of Cumm_Feed_std"]:
        feed_var = row.get("Feed Variance kg/Bird", pd.NA)
        if pd.notna(feed_var) and abs(feed_var) >= FEED_VARIANCE_ORANGE_LIMIT:
            styles.append("background:#f97316;color:white;font-weight:900;")

    return "".join(styles)


def build_html_table(df: pd.DataFrame) -> str:
    columns = list(df.columns)
    parts = ['<div class="pivot-table-wrap"><table class="pivot-table"><thead><tr>']
    for col in columns:
        parts.append(f"<th>{html.escape(str(col))}</th>")
    parts.append("</tr></thead><tbody>")

    for _, row in df.iterrows():
        parts.append("<tr>")
        for col in columns:
            val = format_cell_value(col, row[col])
            style = cell_style(col, row)
            parts.append(f'<td style="{style}">{html.escape(val)}</td>')
        parts.append("</tr>")

    parts.append("</tbody></table></div>")
    return "".join(parts)


def render_table_css() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.2rem; max-width: 1700px;}
        div[data-testid="stMetricValue"] {font-size: 1.5rem;}
        .farm-details {
            border: 1px solid #d1d5db;
            border-radius: 8px;
            margin: 10px 0 14px 0;
            overflow: hidden;
            background: #ffffff;
        }
        .farm-summary {
            cursor: pointer;
            padding: 10px 14px;
            font-weight: 800;
            background: #f8fafc;
            color: #111827;
            border-bottom: 1px solid #e5e7eb;
        }
        .farm-summary-red {
            background: #b91c1c !important;
            color: #ffffff !important;
            border-bottom: 1px solid #991b1b;
        }
        .farm-body {padding: 12px 12px 16px 12px;}
        .flock-heading {
            margin: 18px 0 8px 0;
            font-weight: 900;
            color: #334155;
            font-size: 0.95rem;
        }
        .pivot-table-wrap {width: 100%; overflow-x: auto; margin-bottom: 10px;}
        .pivot-table {width: 100%; border-collapse: collapse; font-size: 0.82rem; background: white;}
        .pivot-table th {
            background: #f1f5f9;
            color: #64748b;
            text-align: left;
            padding: 8px 9px;
            border: 1px solid #e5e7eb;
            white-space: nowrap;
        }
        .pivot-table td {
            padding: 7px 9px;
            border: 1px solid #edf2f7;
            white-space: nowrap;
            text-align: right;
        }
        .pivot-table td:first-child, .pivot-table td:nth-child(2) {text-align: left;}
        .data-version-card {
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            padding: 12px 14px;
            margin: 8px 0;
            background: #ffffff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_custom_farm_drilldown(flock_summary: pd.DataFrame, age_detail: pd.DataFrame) -> None:
    farm_blocks = []

    for farm_name, farm_df in flock_summary.groupby("Farm_Name", sort=True):
        farm_variance = farm_df["Max of Final"].sum()
        has_hw_alert = (farm_df["HW Variance"].notna() & (farm_df["HW Variance"] <= HW_VARIANCE_RED_TRIGGER)).any()
        summary_class = "farm-summary farm-summary-red" if has_hw_alert else "farm-summary"

        detail_parts = [build_html_table(farm_df)]
        for flock_name in farm_df["Flock_Name"].unique():
            detail = age_detail[(age_detail["Farm_Name"] == farm_name) & (age_detail["Flock_Name"] == flock_name)].copy()
            detail_parts.append(f'<div class="flock-heading">Age detail: {html.escape(str(flock_name))}</div>')
            detail_parts.append(build_html_table(detail))

        farm_blocks.append(
            f"""
            <details class="farm-details">
                <summary class="{summary_class}">{html.escape(str(farm_name))} — ${farm_variance:,.0f}</summary>
                <div class="farm-body">{''.join(detail_parts)}</div>
            </details>
            """
        )

    st.markdown("".join(farm_blocks), unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Pages
# -----------------------------------------------------------------------------
def render_data_versions_page() -> None:
    st.header("Data versions")
    st.caption("Save uploaded workbooks here so the app can be closed and reopened without losing the data.")

    uploaded_file = st.file_uploader("Upload new Amino Eggsactly workbook", type=["xlsx"], key="version_upload")
    version_note = st.text_input("Version note", placeholder="Example: NSW active flocks report after Monday update")

    if uploaded_file is not None:
        c1, c2 = st.columns([1, 3])
        with c1:
            if st.button("Save as new version", type="primary", use_container_width=True):
                entry = save_uploaded_workbook(uploaded_file, version_note)
                st.success(f"Saved {entry['version_id']} and set it as the active version.")
                st.cache_data.clear()
                st.rerun()
        with c2:
            st.info("Saving creates a timestamped copy inside saved_data_versions next to this Streamlit file.")

    manifest = load_manifest()
    versions = manifest.get("versions", [])
    active_version = manifest.get("active_version")

    st.subheader("Saved versions")
    if not versions:
        st.warning("No saved workbook versions yet.")
        return

    version_labels = [
        f"{v['version_id']} | {v.get('saved_at', '')} | {v.get('original_name', '')}"
        for v in reversed(versions)
    ]
    id_by_label = {label: v["version_id"] for label, v in zip(version_labels, reversed(versions))}

    selected_label = st.selectbox("Choose saved version", options=version_labels)
    selected_version_id = id_by_label[selected_label]
    selected_entry = get_version_entry(selected_version_id)

    if selected_entry:
        active_text = "Active version" if selected_version_id == active_version else "Not active"
        st.markdown(
            f"""
            <div class="data-version-card">
                <b>{html.escape(selected_entry['version_id'])}</b> — {html.escape(active_text)}<br>
                Saved: {html.escape(selected_entry.get('saved_at', ''))}<br>
                File: {html.escape(selected_entry.get('original_name', ''))}<br>
                Note: {html.escape(selected_entry.get('note', ''))}
            </div>
            """,
            unsafe_allow_html=True,
        )

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Use this version", use_container_width=True):
                set_active_version(selected_version_id)
                st.cache_data.clear()
                st.success("Active workbook version updated.")
                st.rerun()
        with c2:
            version_path = get_version_path(selected_entry)
            if version_path.exists():
                st.download_button(
                    "Download version",
                    data=version_path.read_bytes(),
                    file_name=selected_entry.get("original_name", "saved_workbook.xlsx"),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
        with c3:
            if st.button("Delete version", use_container_width=True):
                delete_version(selected_version_id)
                st.cache_data.clear()
                st.warning("Version deleted.")
                st.rerun()



def calculate_period_hw_impact_from_rows(filtered_df: pd.DataFrame, egg_price_per_dozen: float) -> dict:
    """
    Calculate HW production impact across the actual filtered rows.

    This is the correct period calculation because it sums the row-level production
    variance for the selected filtered records, rather than taking one flock-level
    average and multiplying it by a fixed number of days.
    """
    if filtered_df is None or filtered_df.empty:
        return {
            "actual_eggs": 0.0,
            "standard_eggs": 0.0,
            "egg_variance": 0.0,
            "dozen_variance": 0.0,
            "dollar_variance": 0.0,
        }

    df = filtered_df.copy()

    birds = pd.to_numeric(df.get("Closing_Bird_Numbers", 0), errors="coerce").fillna(0)
    hw_actual = pd.to_numeric(df.get("HW%", 0), errors="coerce").fillna(0)
    hw_std = pd.to_numeric(df.get("HW% std", 0), errors="coerce").fillna(0)

    actual_eggs = birds * hw_actual / 100.0
    standard_eggs = birds * hw_std / 100.0
    egg_variance = actual_eggs - standard_eggs

    total_actual_eggs = float(actual_eggs.sum())
    total_standard_eggs = float(standard_eggs.sum())
    total_egg_variance = float(egg_variance.sum())
    total_dozen_variance = total_egg_variance / 12.0
    total_dollar_variance = total_dozen_variance * float(egg_price_per_dozen)

    return {
        "actual_eggs": total_actual_eggs,
        "standard_eggs": total_standard_eggs,
        "egg_variance": total_egg_variance,
        "dozen_variance": total_dozen_variance,
        "dollar_variance": total_dollar_variance,
    }


def calculate_period_feed_impact_from_flocks(flock_summary: pd.DataFrame) -> dict:
    """
    Feed impact is still calculated at selected-flock level using cumulative feed.
    This avoids summing cumulative values row-by-row, which would double count.
    """
    if flock_summary is None or flock_summary.empty:
        return {
            "feed_kg": 0.0,
            "feed_dollars": 0.0,
            "weighted_kg_per_bird": 0.0,
            "birds": 0.0,
        }

    feed_kg = (
        flock_summary["Feed kg Better / Worse vs Std"].sum()
        if "Feed kg Better / Worse vs Std" in flock_summary.columns
        else 0.0
    )
    feed_dollars = (
        flock_summary["Feed $ Better / Worse vs Std"].sum()
        if "Feed $ Better / Worse vs Std" in flock_summary.columns
        else 0.0
    )
    birds = (
        flock_summary["Max Closing Birds"].sum()
        if "Max Closing Birds" in flock_summary.columns
        else 0.0
    )

    weighted = feed_kg / birds if birds else 0.0

    return {
        "feed_kg": float(feed_kg),
        "feed_dollars": float(feed_dollars),
        "weighted_kg_per_bird": float(weighted),
        "birds": float(birds),
    }


def render_report_page() -> None:
    active_entry = get_latest_version_entry()
    if not active_entry:
        st.warning("No saved data found. Go to Data versions, upload the workbook, then save it as a new version.")
        return

    active_path = get_version_path(active_entry)
    if not active_path.exists():
        st.error("The active workbook file is missing. Go to Data versions and select or upload another version.")
        return

    st.caption(
        f"Using saved workbook: {active_entry.get('original_name', '')} | "
        f"Version: {active_entry.get('version_id', '')} | Saved: {active_entry.get('saved_at', '')}"
    )

    df, standard_df = load_amino_workbook_from_path(str(active_path))
    st.caption("Data quality guard: impossible rows are excluded by default (negative cumulative feed, negative standard feed, or non-positive closing birds).")


    c_price1, c_price2, c_price3 = st.columns(3)

    with c_price1:
        feed_price_per_kg = st.number_input(
            "Feed price used for feed $ calculation ($/kg)",
            min_value=0.0,
            value=float(locals().get("feed_price_per_kg", 0.50)),
            step=0.01,
            format="%.2f",
            help=(
                "Feed formula: (Standard cumulative kg/bird - Actual cumulative kg/bird) "
                "× closing birds × feed price/kg. Positive = feed saving. Negative = extra feed cost."
            ),
        )

    with c_price2:
        egg_price_per_dozen = st.number_input(
            "Egg value used for production loss ($/dozen)",
            min_value=0.0,
            value=2.50,
            step=0.05,
            format="%.2f",
            help="Production value formula: ((Actual HW% - Standard HW%) / 100) × closing birds × production days ÷ 12 × egg value/dozen.",
        )

    with c_price3:
        production_days = st.number_input(
            "Production days for row audit columns",
            min_value=1,
            max_value=31,
            value=7,
            step=1,
            help="Only used for the row-level audit columns. Top HW period KPIs are summed directly from the filtered worksheet rows.",
        )

    with st.expander("Filters", expanded=False):
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            selected_egg_types = multiselect_with_all("Egg Type", df["Egg_Type"].unique(), key="amino_egg_type_filter")
            selected_reporting = multiselect_with_all("Reporting Period", df["Reporting_Period"].unique(), key="amino_reporting_period_filter")

        with c2:
            selected_status = multiselect_with_all(
                "Flock Status",
                df["Flock_Status"].unique(),
                default=["Active"] if "Active" in set(df["Flock_Status"]) else None,
                key="amino_status_filter",
            )
            selected_state = multiselect_with_all(
                "State",
                df["State"].unique(),
                default=["NSW"] if "NSW" in set(df["State"]) else None,
                key="amino_state_filter",
            )

        with c3:
            valid_dates = df["Week_End_Date"].dropna()
            if valid_dates.empty:
                date_range = None
            else:
                date_range = st.date_input(
                    "Week End Date range",
                    value=(valid_dates.min().date(), valid_dates.max().date()),
                    key="amino_week_end_date_filter",
                )

        with c4:
            selected_farms = multiselect_with_all("Farm", df["Farm_Name"].unique(), key="amino_farm_filter")
            selected_flocks = multiselect_with_all("Flock", df["Flock_Name"].unique(), key="amino_flock_filter")

    filtered = df[
        df["Egg_Type"].isin(selected_egg_types)
        & df["Reporting_Period"].isin(selected_reporting)
        & df["Flock_Status"].isin(selected_status)
        & df["State"].isin(selected_state)
        & df["Farm_Name"].isin(selected_farms)
        & df["Flock_Name"].isin(selected_flocks)
    ].copy()

    if isinstance(date_range, tuple) and len(date_range) == 2 and all(date_range):
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        filtered = filtered[(filtered["Week_End_Date"] >= start) & (filtered["Week_End_Date"] <= end)]

    if filtered.empty:
        st.warning("No records match the selected filters.")
        return

    feed_price_per_kg = locals().get('feed_price_per_kg', 0.50)
    flock_summary = build_pivot(filtered, standard_df, include_age=False, feed_price_per_kg=feed_price_per_kg, egg_price_per_dozen=egg_price_per_dozen, production_days=production_days)
    age_detail = build_pivot(filtered, standard_df, include_age=True, feed_price_per_kg=feed_price_per_kg, egg_price_per_dozen=egg_price_per_dozen, production_days=production_days)

    total_variance = flock_summary["Feed $ Better / Worse vs Std"].sum()
    positive_flocks = int((flock_summary["Max of Final"] >= 0).sum())
    negative_flocks = int((flock_summary["Max of Final"] < 0).sum())
    avg_hw_var = flock_summary["HW Variance"].mean()




    total_egg_dozens_better_worse = (
        flock_summary["Egg dozens Better / Worse vs Std"].sum()
        if "Egg dozens Better / Worse vs Std" in flock_summary.columns
        else 0
    )
    total_egg_dollars_better_worse = (
        flock_summary["Egg $ Better / Worse vs Std"].sum()
        if "Egg $ Better / Worse vs Std" in flock_summary.columns
        else 0
    )

    # Final KPI calculations for the selected filters.
    # Feed impact is based on flock-level cumulative feed vs standard.
    # HW impact is based on the actual filtered worksheet rows.
    feed_impact = calculate_period_feed_impact_from_flocks(flock_summary)
    period_hw_impact = calculate_period_hw_impact_from_rows(filtered, egg_price_per_dozen)

    total_variance = feed_impact.get("feed_dollars", 0.0)
    total_feed_kg_better_worse = feed_impact.get("feed_kg", 0.0)
    total_selected_birds = feed_impact.get("birds", 0.0)
    weighted_feed_kg_per_bird_better_worse = feed_impact.get("weighted_kg_per_bird", 0.0)

    total_hw_egg_variance_period = period_hw_impact.get("egg_variance", 0.0)
    total_hw_dozen_variance_period = period_hw_impact.get("dozen_variance", 0.0)
    total_hw_dollar_variance_period = period_hw_impact.get("dollar_variance", 0.0)

    total_net_dollars_vs_std = total_variance + total_hw_dollar_variance_period

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Feed saving / cost vs std", f"${total_variance:,.0f}")
    k2.metric("HW egg variance/period", f"{total_hw_egg_variance_period:,.0f}")
    k3.metric("HW dozen variance/period", f"{total_hw_dozen_variance_period:,.0f}")
    k4.metric("HW $ variance/period", f"${total_hw_dollar_variance_period:,.0f}")
    k5.metric("Net $ impact vs std", f"${total_net_dollars_vs_std:,.0f}")

    st.info("HW period variance is now calculated from the filtered worksheet rows, not daily variance × 7. This means it reflects all selected weeks/rows in the current filters.")

    st.caption(
        f"Average HW variance: {avg_hw_var:,.2f} | "
        f"Weighted feed kg/bird better/worse: {weighted_feed_kg_per_bird_better_worse:,.2f} | "
        f"Top HW egg/dozen/$ variance is summed from all filtered worksheet rows at ${egg_price_per_dozen:,.2f}/dozen. Feed impact is based on selected flock cumulative feed vs standard."
    )

    st.subheader("Farm / flock drilldown")
    st.caption(
        "Red farm bar = at least one flock has HW Variance of -10.00 or worse. "
        "Orange feed cells = feed variance is outside +/-10 kg/bird."
    )
    render_custom_farm_drilldown(flock_summary, age_detail)



# -----------------------------------------------------------------------------
# Farm management check view
# -----------------------------------------------------------------------------
def get_active_loaded_workbook():
    active_entry = get_latest_version_entry()
    if not active_entry:
        return None, None, None, None

    active_path = get_version_path(active_entry)
    if not active_path.exists():
        return active_entry, active_path, None, None

    df, standard_df = load_amino_workbook_from_path(str(active_path))
    return active_entry, active_path, df, standard_df


def render_management_filters(df: pd.DataFrame, key_prefix: str = "mgmt"):
    with st.expander("Filters", expanded=False):
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            selected_egg_types = multiselect_with_all(
                "Egg Type",
                df["Egg_Type"].unique(),
                key=f"{key_prefix}_egg_type_filter",
            )
            selected_reporting = multiselect_with_all(
                "Reporting Period",
                df["Reporting_Period"].unique(),
                key=f"{key_prefix}_reporting_period_filter",
            )

        with c2:
            selected_status = multiselect_with_all(
                "Flock Status",
                df["Flock_Status"].unique(),
                default=["Active"] if "Active" in set(df["Flock_Status"]) else None,
                key=f"{key_prefix}_status_filter",
            )
            selected_state = multiselect_with_all(
                "State",
                df["State"].unique(),
                default=["NSW"] if "NSW" in set(df["State"]) else None,
                key=f"{key_prefix}_state_filter",
            )

        with c3:
            valid_dates = df["Week_End_Date"].dropna()
            if valid_dates.empty:
                date_range = None
            else:
                date_range = st.date_input(
                    "Week End Date range",
                    value=(valid_dates.min().date(), valid_dates.max().date()),
                    key=f"{key_prefix}_week_end_date_filter",
                )

        with c4:
            selected_farms = multiselect_with_all(
                "Farm",
                df["Farm_Name"].unique(),
                key=f"{key_prefix}_farm_filter",
            )
            selected_flocks = multiselect_with_all(
                "Flock",
                df["Flock_Name"].unique(),
                key=f"{key_prefix}_flock_filter",
            )

    filtered = df[
        df["Egg_Type"].isin(selected_egg_types)
        & df["Reporting_Period"].isin(selected_reporting)
        & df["Flock_Status"].isin(selected_status)
        & df["State"].isin(selected_state)
        & df["Farm_Name"].isin(selected_farms)
        & df["Flock_Name"].isin(selected_flocks)
    ].copy()

    if isinstance(date_range, tuple) and len(date_range) == 2 and all(date_range):
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        filtered = filtered[(filtered["Week_End_Date"] >= start) & (filtered["Week_End_Date"] <= end)]

    return filtered


def classify_management_focus(row: pd.Series) -> tuple[str, str, int]:
    feed_var = row.get("Feed Variance kg/Bird", 0.0)  # actual - standard
    hw_var = row.get("HW Variance", 0.0)              # actual - standard
    birds = row.get("Max Closing Birds", 0.0)

    if pd.isna(birds) or birds <= 0:
        return "DATA CHECK", "No closing birds. Confirm flock/bird numbers before interpreting.", 75

    if pd.isna(hw_var):
        return "DATA CHECK", "Missing HW% result or standard. Confirm production data/standard mapping.", 70

    # Critical: eating below standard and producing below standard.
    if feed_var < -1.0 and hw_var <= -10.0:
        return (
            "WALK FIRST",
            "Birds are eating under standard and producing well under standard. Feed saving may be masking lost egg income.",
            100,
        )

    if feed_var < -1.0 and hw_var < -5.0:
        return (
            "BIRD CHECK",
            "Feed is under standard and production is also behind. Check whether under-consumption is limiting output.",
            88,
        )

    if feed_var > 1.0 and hw_var < -5.0:
        return (
            "EFFICIENCY LEAK",
            "Birds are eating more than standard but still producing below standard. Check health, environment, ration and data quality.",
            92,
        )

    if abs(feed_var) <= 1.0 and hw_var < -5.0:
        return (
            "PRODUCTION CHECK",
            "Feed is close to standard, but production is behind. Look beyond feed: health, water, environment, egg counting and bird condition.",
            82,
        )

    if feed_var < -1.0 and hw_var >= -2.0:
        return (
            "VERIFY GOOD RESULT",
            "Feed is below standard but production is holding. Check bird condition and egg weight before calling it a true efficiency gain.",
            55,
        )

    if feed_var > 1.0 and hw_var >= -2.0:
        return (
            "WATCH FEED",
            "Feed is above standard while production is acceptable. Check wastage, stock readings, ration match and feeder settings.",
            58,
        )

    return (
        "MONITOR",
        "No urgent feed-production conflict detected. Keep watching trend and bird condition.",
        35,
    )


def make_ai_management_table(flock_summary: pd.DataFrame) -> pd.DataFrame:
    if flock_summary is None or flock_summary.empty:
        return pd.DataFrame()

    df = flock_summary.copy()

    focus_results = df.apply(classify_management_focus, axis=1)
    df["AI Focus"] = [x[0] for x in focus_results]
    df["AI Interpretation"] = [x[1] for x in focus_results]
    df["AI Priority Score"] = [x[2] for x in focus_results]

    # Practical ranking: combine severity with estimated net dollar risk.
    if "Net $ Impact vs Std" in df.columns:
        df["Estimated Net Risk $"] = df["Net $ Impact vs Std"].where(df["Net $ Impact vs Std"] < 0, 0).abs()
    else:
        df["Estimated Net Risk $"] = 0.0

    df["Production Gap pts"] = df["HW Variance"].fillna(0)
    df["Feed Gap kg/bird"] = df["Feed Variance kg/Bird"].fillna(0)

    service_manager_col = get_service_manager_column(df)
    if service_manager_col and service_manager_col not in df.columns:
        service_manager_col = None

    display_cols = [
        "AI Focus",
        "AI Priority Score",
    ]

    if service_manager_col:
        display_cols.append(service_manager_col)

    display_cols += [
        "Farm_Name",
        "Flock_Name",
        "Max Standard Age",
        "Max Closing Birds",
        "Feed Gap kg/bird",
        "Production Gap pts",
        "Feed $ Better / Worse vs Std",
        "HW $ Variance/period",
        "Net $ Impact vs Std",
        "AI Interpretation",
    ]

    existing_cols = [c for c in display_cols if c in df.columns]

    sort_cols = ["AI Priority Score"]
    sort_ascending = [False]

    if "Estimated Net Risk $" in df.columns:
        sort_cols.append("Estimated Net Risk $")
        sort_ascending.append(False)

    sorted_df = df.sort_values(
        by=sort_cols,
        ascending=sort_ascending,
    ).reset_index(drop=True)

    return sorted_df[existing_cols]


def focus_area_counts(ai_df: pd.DataFrame) -> dict:
    if ai_df is None or ai_df.empty or "AI Focus" not in ai_df.columns:
        return {}
    return ai_df["AI Focus"].value_counts().to_dict()


def ai_focus_area_summary(ai_df: pd.DataFrame) -> list[str]:
    counts = focus_area_counts(ai_df)
    lines = []

    if counts.get("WALK FIRST", 0):
        lines.append(
            f"Walk first: {counts['WALK FIRST']} flock(s) are eating below standard and producing badly below standard. Treat these as bird-check priorities, not feed savings."
        )
    if counts.get("EFFICIENCY LEAK", 0):
        lines.append(
            f"Efficiency leak: {counts['EFFICIENCY LEAK']} flock(s) are eating above standard but production is still behind. Check wastage, ration, water, disease pressure and environment."
        )
    if counts.get("PRODUCTION CHECK", 0):
        lines.append(
            f"Production check: {counts['PRODUCTION CHECK']} flock(s) are close to feed standard but behind on HW%. Feed may not be the main issue."
        )
    if counts.get("VERIFY GOOD RESULT", 0):
        lines.append(
            f"Verify good result: {counts['VERIFY GOOD RESULT']} flock(s) are under feed standard while production is holding. Confirm bodyweight, egg weight and bird condition before celebrating."
        )
    if counts.get("DATA CHECK", 0):
        lines.append(
            f"Data check: {counts['DATA CHECK']} flock(s) need bird numbers, HW%, standards or feed records checked before decisions are made."
        )
    if not lines:
        lines.append("No major feed-production conflict detected in the selected flocks. Keep watching trends and bird condition.")

    return lines


def render_ai_focus_cards(ai_df: pd.DataFrame, top_n: int = 5) -> None:
    if ai_df is None or ai_df.empty:
        st.info("No flocks available for management focus.")
        return

    priority = ai_df.head(top_n).copy()

    st.markdown(
        """
        <style>
        .ai-card {
            border: 1px solid #d9e2ec;
            border-radius: 14px;
            padding: 14px 16px;
            margin: 10px 0;
            background: #ffffff;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
        }
        .ai-card-critical { border-left: 8px solid #b91c1c; }
        .ai-card-warning { border-left: 8px solid #f97316; }
        .ai-card-good { border-left: 8px solid #0f766e; }
        .ai-card-monitor { border-left: 8px solid #64748b; }
        .ai-pill {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 900;
            margin-right: 8px;
            color: white;
            background: #334155;
        }
        .ai-pill-critical { background: #b91c1c; }
        .ai-pill-warning { background: #f97316; }
        .ai-pill-good { background: #0f766e; }
        .ai-small { color: #64748b; font-size: 0.86rem; }
        .ai-title { font-weight: 900; font-size: 1.02rem; color: #0f172a; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    for _, row in priority.iterrows():
        focus = str(row.get("AI Focus", "MONITOR"))
        if focus == "WALK FIRST":
            cls = "ai-card-critical"
            pill = "ai-pill-critical"
        elif focus in ["BIRD CHECK", "EFFICIENCY LEAK", "PRODUCTION CHECK", "DATA CHECK", "WATCH FEED"]:
            cls = "ai-card-warning"
            pill = "ai-pill-warning"
        elif focus == "VERIFY GOOD RESULT":
            cls = "ai-card-good"
            pill = "ai-pill-good"
        else:
            cls = "ai-card-monitor"
            pill = ""

        farm = html.escape(str(row.get("Farm_Name", "")))
        flock = html.escape(str(row.get("Flock_Name", "")))
        advisor_col = get_service_manager_column(ai_df)
        advisor_text = ""
        if advisor_col:
            advisor_val = row.get(advisor_col, "")
            if pd.notna(advisor_val) and str(advisor_val).strip():
                advisor_text = f" | Service manager: {html.escape(str(advisor_val))}"
        interpretation = html.escape(str(row.get("AI Interpretation", "")))
        feed_gap = row.get("Feed Gap kg/bird", 0.0)
        prod_gap = row.get("Production Gap pts", 0.0)
        net = row.get("Net $ Impact vs Std", 0.0)
        age = row.get("Max Standard Age", pd.NA)

        st.markdown(
            f"""
            <div class="ai-card {cls}">
                <div>
                    <span class="ai-pill {pill}">{html.escape(focus)}</span>
                    <span class="ai-title">{farm} / {flock}</span>
                </div>
                <div class="ai-small">
                    Age: {'' if pd.isna(age) else f'{age:,.0f}'} weeks |
                    Feed gap: {feed_gap:,.1f} kg/bird |
                    HW gap: {prod_gap:,.2f} pts |
                    Net impact: ${net:,.0f}{advisor_text}
                </div>
                <div style="margin-top:8px;">{interpretation}</div>
                <div class="ai-small" style="margin-top:8px;">
                    Shed questions: Are birds actively eating? Is water intake normal? Any heat, disease, ration, feeder, wastage or bird-number issue? Does bird condition match the numbers?
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def style_management_table(df: pd.DataFrame):
    format_map = {
        "AI Priority Score": "{:,.0f}",
        "Max Standard Age": "{:,.0f}",
        "Max Closing Birds": "{:,.0f}",
        "Feed Gap kg/bird": "{:,.1f}",
        "Production Gap pts": "{:,.2f}",
        "Feed $ Better / Worse vs Std": "${:,.0f}",
        "HW $ Variance/period": "${:,.0f}",
        "Net $ Impact vs Std": "${:,.0f}",
    }

    colour_cols = [
        c for c in ["Feed Gap kg/bird", "Production Gap pts", "Feed $ Better / Worse vs Std", "HW $ Variance/period", "Net $ Impact vs Std"]
        if c in df.columns
    ]

    styled = df.style.format(format_map, na_rep="")
    if colour_cols:
        styled = styled.map(
            lambda v: "color: #0f766e; font-weight: 800;"
            if pd.notna(v) and v >= 0
            else "color: #b91c1c; font-weight: 800;",
            subset=colour_cols,
        )

    if "AI Focus" in df.columns:
        styled = styled.map(
            lambda v: (
                "background-color:#b91c1c;color:#ffffff;font-weight:900;"
                if v == "WALK FIRST"
                else "background-color:#f97316;color:#ffffff;font-weight:900;"
                if v in ["BIRD CHECK", "EFFICIENCY LEAK", "PRODUCTION CHECK", "DATA CHECK", "WATCH FEED"]
                else "background-color:#0f766e;color:#ffffff;font-weight:900;"
                if v == "VERIFY GOOD RESULT"
                else "font-weight:800;"
            ),
            subset=["AI Focus"],
        )

    return styled



def get_service_manager_column(df: pd.DataFrame) -> str | None:
    """Return the best available service-manager/advisor column from the workbook."""
    candidates = [
        "TechAdvisorName",
        "Service_Manager",
        "Service Manager",
        "ServiceManager",
        "Technical Advisor",
        "Technical_Advisor",
        "Advisor",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def render_farm_management_check_view() -> None:
    active_entry, active_path, df, standard_df = get_active_loaded_workbook()
    if active_entry is None:
        st.warning("No saved data found. Go to Data versions, upload the workbook, then save it as a new version.")
        return
    if df is None or standard_df is None:
        st.error("The active workbook file is missing or could not be loaded. Go to Data versions and select or upload another version.")
        return

    st.caption(
        f"Using saved workbook: {active_entry.get('original_name', '')} | "
        f"Version: {active_entry.get('version_id', '')} | Saved: {active_entry.get('saved_at', '')}"
    )

    st.caption("Data quality guard: impossible rows are excluded by default (negative cumulative feed, negative standard feed, or non-positive closing birds).")

    st.markdown("### Farm Management Check View")
    st.markdown(
        """
        This page is designed as a practical farm management check. It highlights **which flocks may need attention first** and gives service managers a clear starting point for shed walks, follow-up questions, and farm discussions.

        By default, this view focuses on **30–60 week flocks**, where feed intake versus egg production is usually most actionable.
        """
    )

    c_price1, c_price2, c_price3 = st.columns(3)
    with c_price1:
        feed_price_per_kg = st.number_input(
            "Feed price ($/kg)",
            min_value=0.0,
            value=0.50,
            step=0.01,
            format="%.2f",
            key="mgmt_feed_price",
        )
    with c_price2:
        egg_price_per_dozen = st.number_input(
            "Egg value ($/dozen)",
            min_value=0.0,
            value=2.50,
            step=0.05,
            format="%.2f",
            key="mgmt_egg_price",
        )
    with c_price3:
        production_days = st.number_input(
            "Row audit days",
            min_value=1,
            max_value=31,
            value=7,
            step=1,
            key="mgmt_production_days",
            help="Used only for per-flock audit columns. Top HW period impact is calculated from filtered worksheet rows.",
        )

    st.markdown("#### Age focus")
    age_col1, age_col2, age_col3 = st.columns([1, 1, 2])

    with age_col1:
        min_focus_age = st.number_input(
            "Minimum flock age",
            min_value=0,
            max_value=100,
            value=30,
            step=1,
            key="mgmt_min_focus_age",
            help="Default focus starts at 30 weeks because young flocks can still be settling into production.",
        )

    with age_col2:
        max_focus_age = st.number_input(
            "Maximum flock age",
            min_value=0,
            max_value=100,
            value=60,
            step=1,
            key="mgmt_max_focus_age",
            help="Default focus ends at 60 weeks because very old flocks are less useful for action-based comparison.",
        )

    with age_col3:
        st.info(
            "Default focus is 30–60 weeks. Older flocks can still be reviewed by widening this range, "
            "but they are not the best place to judge feed saving vs production loss."
        )

    service_manager_col = get_service_manager_column(df)
    selected_service_manager = "All service managers"

    if service_manager_col:
        service_manager_options = (
            df[service_manager_col]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .sort_values()
            .unique()
            .tolist()
        )

        selected_service_manager = st.selectbox(
            "Service manager / technical advisor",
            options=["All service managers"] + service_manager_options,
            index=0,
            key="mgmt_service_manager_filter",
            help="Filters the management check view to one service manager's farms/flocks.",
        )
    else:
        st.info("No service manager / technical advisor column found in the workbook. Expected column example: TechAdvisorName.")

    filtered = render_management_filters(df, key_prefix="mgmt")

    if service_manager_col and selected_service_manager != "All service managers":
        filtered = filtered[
            filtered[service_manager_col].astype(str).str.strip() == selected_service_manager
        ].copy()

    if "RoundDownAgeCalc" in filtered.columns:
        filtered["RoundDownAgeCalc"] = pd.to_numeric(filtered["RoundDownAgeCalc"], errors="coerce")
        filtered = filtered[
            (filtered["RoundDownAgeCalc"] >= min_focus_age)
            & (filtered["RoundDownAgeCalc"] <= max_focus_age)
        ].copy()
    if filtered.empty:
        st.warning("No records match the selected filters.")
        return

    flock_summary = build_pivot(
        filtered,
        standard_df,
        include_age=False,
        feed_price_per_kg=feed_price_per_kg,
        egg_price_per_dozen=egg_price_per_dozen,
        production_days=production_days,
    )

    feed_impact = calculate_period_feed_impact_from_flocks(flock_summary)
    hw_impact = calculate_period_hw_impact_from_rows(filtered, egg_price_per_dozen)

    feed_dollars = feed_impact.get("feed_dollars", 0.0)
    hw_dollars = hw_impact.get("dollar_variance", 0.0)
    net_dollars = feed_dollars + hw_dollars
    egg_variance = hw_impact.get("egg_variance", 0.0)
    dozen_variance = hw_impact.get("dozen_variance", 0.0)

    ai_df = make_ai_management_table(flock_summary)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Feed saving / cost", f"${feed_dollars:,.0f}")
    k2.metric("Egg value gain / loss", f"${hw_dollars:,.0f}")
    k3.metric("Net position", f"${net_dollars:,.0f}")
    k4.metric("Dozens variance", f"{dozen_variance:,.0f}")

    if feed_dollars > 0 and hw_dollars < 0:
        st.warning(
            f"The selected flocks look ${feed_dollars:,.0f} better on feed, "
            f"but production is ${abs(hw_dollars):,.0f} worse. "
            f"That is the key management conversation: are we saving feed, or are birds not eating enough to produce?"
        )
    elif net_dollars < 0:
        st.error(
            f"The combined feed and egg position is ${abs(net_dollars):,.0f} worse than standard. "
            "Use the priority list below to decide which sheds to walk first."
        )
    else:
        st.success(
            f"The combined feed and egg position is ${net_dollars:,.0f} better than standard. "
            "Still verify bird condition, egg weight and bodyweight before treating feed saving as a true win."
        )

    st.caption(
        f"Service manager: {selected_service_manager} | "
        f"Age focus: {min_focus_age:,.0f}–{max_focus_age:,.0f} weeks. "
        "This management view is intentionally focused on the flocks where feed intake versus egg production is most actionable."
    )

    st.markdown("#### AI Management Focus")
    for line in ai_focus_area_summary(ai_df):
        st.markdown(f"- {line}")

    st.markdown("#### Walk these sheds first")
    render_ai_focus_cards(ai_df, top_n=6)

    st.markdown("#### Service manager checklist")
    with st.expander("What to check in the shed", expanded=True):
        st.markdown(
            """
            **1. Birds and behaviour**  
            Are birds active, evenly spread, eating confidently, and showing normal behaviour?

            **2. Feed access and wastage**  
            Check feeder height, feed level, stale feed, blocked lines, ration changes, feed spills and bin readings.

            **3. Water**  
            Check water pressure, flow rate, nipple lines, filters, leaks, and whether water intake has moved with feed intake.

            **4. Environment**  
            Check temperature, ventilation, drafts, ammonia, litter/manure, light program and any recent heat-stress events.

            **5. Bird condition**  
            Check bodyweight, uniformity, feather cover, crop fill, keel condition, mortality and culls.

            **6. Egg result sanity check**  
            Confirm egg counts, grading floor records, egg weight, rejects, floor eggs, dirty/crack trends and collection issues.

            **7. Data check before blame**  
            Confirm closing bird numbers, flock age, standards mapping, feed deliveries, feed stock readings and missing production entries.
            """
        )

    st.markdown("#### Priority table")
    st.dataframe(
        style_management_table(ai_df),
        use_container_width=True,
        hide_index=True,
        height=560,
    )

    st.caption(
        "AI focus is rule-based guidance from the selected feed, production and flock data. "
        "It is designed to start a practical shed conversation, not replace service-manager judgement."
    )



# -----------------------------------------------------------------------------
# App shell
# -----------------------------------------------------------------------------
def render_app() -> None:
    st.set_page_config(page_title="Cumulative Feed Pivot", layout="wide", initial_sidebar_state="collapsed")
    ensure_data_dir()
    render_table_css()

    st.title("Cumulative Feed Pivot")

    page = st.sidebar.radio(
        "Menu",
        options=["Farm Management Check", "Farm / flock report", "Data versions"],
        index=0,
    )

    if page == "Farm Management Check":
        render_farm_management_check_view()
    elif page == "Farm / flock report":
        render_report_page()
    else:
        render_data_versions_page()


if __name__ == "__main__":
    render_app()
