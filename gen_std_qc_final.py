"""
gen.py
======
Validates that rgd_gold_ad's standardized columns are faithful row-level
copies of rgd_udm_silver's corresponding `_std` columns.

JOIN KEY (2026-07-31, second redesign)
---------------------------------------
Per the data engineering team: rgd_gold_ad.source_udm_inc_id records
exactly which silver row (by rgd_udm_silver's own primary key, udm_inc_id)
sourced that gold row. This replaces two earlier, more fragile designs:
  - udm_unq_id as the join key: turned out to be non-unique on the SILVER
    side too (not just intended-unique gold), so a naive join fanned out.
  - ndid as the join key: not unique per row in any table except patients
    (many rows per patient in encounters/diagnosis/procedures/labs/etc.),
    so no per-row pairing was possible at all — the workaround was a
    per-ndid value-SET comparison instead of true row matching.

udm_inc_id is silver's actual PRIMARY KEY (verified: unique, confirmed via
INFORMATION_SCHEMA and COUNT(*) = COUNT(DISTINCT udm_inc_id) checks), so
gold.source_udm_inc_id = silver.udm_inc_id is a clean many-to-one join —
each gold row matches AT MOST one silver row, no fan-out, no dedup logic
needed. This means true row-level "does this exact record's value match"
comparison is possible again (more precise than the ndid value-set
approach), and count_ndid_not_matching / associated_psids are per-COLUMN
statistics this time (the distinct ndids/psids behind THAT column's
mismatched rows), not a table-level orphan count.

Scope filter: silver is restricted to the source_udm_inc_id population
gold actually references — since every query starts from gold and LEFT
JOINs to silver, this is automatic (no gold row outside gold's own
population is ever considered).

WHY PANDAS, NOT SQL COMPARISON
-------------------------------
Two prior attempts (this and yesterday's) confirmed rgd_gold_ad and
rgd_udm_silver were created with different default collations
(utf8mb4_0900_ai_ci vs utf8mb4_unicode_ci) — MySQL refuses to compare
text across them without an explicit COLLATE. Comparing values in pandas
client-side (after one plain SELECT per side) sidesteps that entirely,
and also avoids needing CREATE TEMPORARY TABLE (the DB user has read-only
access to rgd_gold_ad, no temp-table privilege there — found out the hard
way on an earlier design).

Also note: this DB is shared/under heavy concurrent load from other jobs
using the same nd_rwe credential. Connections here use AUTOCOMMIT so a
plain SELECT never leaves an open transaction (and its locks) lingering
on the server if this script is interrupted — SQLAlchemy connections
default to non-autocommit, which caused real lock-contention problems
(orphaned sessions blocking new queries) in earlier iterations of this
script.

COMPUTED SILVER-SIDE EXPRESSIONS (2026-08-03)
-----------------------------------------------
Radiology's cpt_code mapping isn't a plain column: gold.cpt_code is
sourced from COALESCE(proc_code_std, probable_cpt_code) on the silver
side. TABLE_SPECS column entries now accept either the original plain
(silver_col, gold_col) tuple, OR a dict for computed expressions:
  {"silver_expr": "<raw SQL expression>", "gold_col": "<gold_col>"}
_resolve_column() normalizes either form into (select_sql, alias,
gold_col, display_name) used throughout fetch_silver_scope() and
build_table_outputs(). Every silver SELECT (plain or computed) is
aliased to a synthetic __silver_col_N name, never the raw column name —
needed because radiology's study_name is spelled identically on both
the gold and silver side, and relying on pandas merge()'s implicit
suffixing to tell them apart turned out to silently compare gold against
itself (caught in a smoke test, see _resolve_column()'s docstring).

OUTPUT
------
./gen_output/gen_silver_gold_validation.xlsx
  - "std_column_qc" — Output A: one row per (table, column) pair —
    total_rows_in_gold, rows_matching_silver, pct_rows_matching_silver,
    rows_not_matching_silver, pct_rows_not_matching_silver,
    top_20_mismatched_values, count_ndid_not_matching (distinct ndids
    behind THIS column's mismatches), associated_psids (their psids,
    sampled).

./gen_output/unmatched_detail_<table_name>.csv (2026-08-03: one file per
table, not a single accumulating file)
  - Output B: every (table_name, column_name, ndid, psid,
    source_udm_inc_id, gold_value, silver_value) where the two layers
    disagree — the granular audit trail. Written via pyarrow.csv
    (write_detail_csv), not pandas.to_csv — meaningfully faster at the
    scale this produces (confirmed: vitals alone produced 42.9M detail
    rows, itself already past Excel's hard 1,048,576-row-per-sheet
    limit, which is why this is CSV and not a workbook sheet at all).
    Per-table files rather than one shared file: pyarrow's CSV writer
    has no append mode, and per-table files also mean a failed write on
    one table can't corrupt another table's already-saved detail — a
    real concern after today's mid-run source-table-truncation incident.
    (unmatched_detail.csv, the original single-file version covering
    procedures/labs/allergies/vitals-summary-only, is left as-is as a
    historical record.)

TARGET TABLES & EXECUTION SCOPE
--------------------------------
procedures, labs, patients, encounters, diagnosis, radiology, vitals, and
allergies are active (8 of 11). TABLE_SPECS is a dict so the remaining 3
can be added later without touching any other code — just add entries
and re-run.

HOW TO RUN
----------
python gen.py
"""
from __future__ import annotations

import os

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

load_dotenv()

# ============================================================================
# 1. CONFIG
# ============================================================================

DB_CONFIG = {
    # mysqlclient was evaluated (2026-08-03) but fails to build on this
    # machine (no Homebrew/pkg-config/libmysqlclient at all). mysql-connector-
    # python ships a prebuilt C extension wheel — no system library needed —
    # and is already installed; noticeably faster than pure-Python PyMySQL
    # for the large result sets this script pulls (confirmed: labs/vitals
    # fetches run into the tens of millions of rows).
    "drivername": "mysql+mysqlconnector",
    "username": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": int(os.environ.get("DB_PORT", 3306)),
    "database": os.environ.get("DB_NAME", ""),
}

TOP_N_MISMATCHES = 20
TOP_N_PSIDS = 20

# Dynamic table config — add more entries here as they're confirmed
# (target: 11 total; patients, encounters, diagnosis, radiology, vitals,
# allergies still to come).
TABLE_SPECS = {
    "procedures": {
        "gold_table": "rgd_gold_ad.procedures",
        "silver_table": "rgd_udm_silver.procedures",
        "columns": [
            ("proc_code_std", "proc_code"),
            ("proc_name_std", "proc_name"),
            ("proc_coding_system_std", "proc_coding_system"),
            ("proc_description_std", "proc_description"),
        ],
    },
    "labs": {
        "gold_table": "rgd_gold_ad.labs",
        "silver_table": "rgd_udm_silver.labs",
        "columns": [
            ("result_panel_std", "test_panel_name"),
            ("panel_code_std", "test_code"),
        ],
    },
    "patients": {
        "gold_table": "rgd_gold_ad.patients",
        "silver_table": "rgd_udm_silver.patients",
        "columns": [
            ("gender_hl7_std", "gender"),
            ("pat_race_std", "race"),
            ("pat_ethnicity_std", "ethnicity"),
            ("pat_marital_status_std", "marital_status"),
            ("pat_deceased_status_std", "deceased_status"),
        ],
    },
    "encounters": {
        "gold_table": "rgd_gold_ad.encounters",
        "silver_table": "rgd_udm_silver.encounters",
        "columns": [
            ("enc_type_std", "enc_type"),
            ("enc_sub_type_std", "enc_subtype"),
        ],
    },
    "diagnosis": {
        "gold_table": "rgd_gold_ad.diagnosis",
        "silver_table": "rgd_udm_silver.diagnosis",
        "columns": [
            ("diag_desc_std", "diag_desc"),
            ("diag_coding_system_std", "diag_coding_system"),
            ("primary_diagnosis_flag_std", "primary_diagnosis_flag"),
        ],
    },
    # --- placeholders for the remaining 3 tables (of 11 total) ---
    "radiology": {
        "gold_table": "rgd_gold_ad.radiology",
        "silver_table": "rgd_udm_silver.radiology",
        "columns": [
            ("study_name", "study_name"),  # no _std variant exists in silver — raw-to-raw, confirmed w/ user
            ("modality_std", "img_modality"),
            ("body_part_std", "img_body_part"),
            ("contrast_type_std", "img_contrast_type"),
            ("tracer_name_std", "img_tracer_name"),
            # Computed silver-side expression (not a plain column) — gold's
            # cpt_code is sourced from whichever of these two silver columns
            # is non-null, per the 2026-08-03 mapping update. See
            # _resolve_column() for how this dict form is handled.
            {
                "silver_expr": "COALESCE(proc_code_std, probable_cpt_code)",
                "gold_col": "cpt_code",
            },
        ],
    },
    "vitals": {
        "gold_table": "rgd_gold_ad.vitals",
        "silver_table": "rgd_udm_silver.vitals",
        "columns": [
            ("vital_name_std", "vital_name"),
            ("vital_code_std", "vital_code"),
            ("vital_coding_system_std", "vital_coding_system"),
            ("vital_result_std", "vital_result"),
            ("vital_unit_std", "vital_unit"),
        ],
    },
    "allergies": {
        "gold_table": "rgd_gold_ad.allergies",
        "silver_table": "rgd_udm_silver.allergies",
        "columns": [
            ("allergen_name_std", "allergen_name"),
            ("allergen_code_std", "allergen_code"),
            ("allergen_coding_system_std", "allergen_coding_system"),
            ("allergy_reaction_name_std", "allergy_reaction_name"),
            ("allergy_reaction_code_std", "allergy_reaction_code"),
            ("allergy_reaction_coding_system_std", "allergy_reaction_coding_system"),
        ],
    },
}

OUTPUT_DIR = "./gen_output"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "gen_silver_gold_validation.xlsx")  # "std_column_qc" sheet only
# Output B (detail) is per-table: OUTPUT_DIR/unmatched_detail_<table_name>.csv — see detail_csv_path()


def get_engine() -> Engine:
    url = URL.create(
        drivername=DB_CONFIG["drivername"],
        username=DB_CONFIG["username"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
    )
    # AUTOCOMMIT: a plain SELECT should never leave an open transaction (and
    # its locks) on the server if this script is killed/interrupted. Earlier
    # designs without this caused real lock-contention problems on this
    # shared DB (orphaned idle sessions blocking new queries indefinitely).
    return create_engine(url, isolation_level="AUTOCOMMIT")


# ============================================================================
# 2. QUERIES — one plain SELECT per side, per table (no joins/temp tables
# on the DB; the join + comparison happens client-side in pandas).
# ============================================================================

def fetch_gold(engine: Engine, gold_table: str, gold_cols_needed: list) -> pd.DataFrame:
    cols_sql = ", ".join(["ndid", "psid", "source_udm_inc_id"] + gold_cols_needed)
    query = text(f"SELECT {cols_sql} FROM {gold_table}")
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def fetch_silver_scope(engine: Engine, gold_table: str, silver_table: str, silver_select_sqls: list) -> pd.DataFrame:
    """Silver rows restricted to the source_udm_inc_id population gold
    actually references (the scope filter), in ONE query. `silver_select_sqls`
    are already-resolved SELECT fragments (plain column names or aliased
    expressions — see _resolve_column()).
    """
    cols_sql = ", ".join(["udm_inc_id"] + silver_select_sqls)
    query = text(f"""
        SELECT {cols_sql}
        FROM {silver_table}
        WHERE udm_inc_id IN (SELECT DISTINCT source_udm_inc_id FROM {gold_table})
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


# ============================================================================
# 3. ANALYSIS — client-side merge (udm_inc_id is silver's real PK, so this
# merge is guaranteed 1:1 or many:1, never a fan-out).
# ============================================================================

def _resolve_column(entry, index):
    """Normalize a TABLE_SPECS column entry into (select_sql, col_name,
    gold_col, display_name).

    Plain entries are (silver_col, gold_col) tuples. Computed entries are
    dicts: {"silver_expr": "<raw SQL expression>", "gold_col": "<gold_col>"}
    — used when the silver-side value isn't a single column (e.g.
    COALESCE(proc_code_std, probable_cpt_code) -> cpt_code for radiology).

    Every silver-side SELECT is aliased to a synthetic, index-based name
    (__silver_col_N) rather than relying on pandas' implicit merge()
    suffixing to disambiguate name collisions — necessary because at least
    one real case (radiology's study_name) uses the SAME column name on
    both the gold and silver side. Relying on merge's suffixes=("", "_silver")
    for that case silently compared gold's study_name against itself
    (merged["study_name"] resolved to gold's copy on both lookups) —
    caught via a smoke test before this ever ran against real data.
    """
    if isinstance(entry, dict):
        raw_expr = entry["silver_expr"]
        gold_col = entry["gold_col"]
    else:
        raw_expr, gold_col = entry

    alias = f"__silver_col_{index}"
    select_sql = f"{raw_expr} AS {alias}"
    display_name = f"{raw_expr} -> {gold_col}"
    return select_sql, alias, gold_col, display_name


def build_table_outputs(engine: Engine, table_name: str, spec: dict):
    """Returns (summary_rows: list[dict], mismatch_detail_df: pd.DataFrame)
    for one table.
    """
    gold_table = spec["gold_table"]
    silver_table = spec["silver_table"]
    resolved_columns = [_resolve_column(entry, i) for i, entry in enumerate(spec["columns"])]

    silver_select_sqls = sorted({select_sql for select_sql, _, _, _ in resolved_columns})
    gold_cols_needed = sorted({gold_col for _, _, gold_col, _ in resolved_columns})

    gold_df = fetch_gold(engine, gold_table, gold_cols_needed)
    silver_df = fetch_silver_scope(engine, gold_table, silver_table, silver_select_sqls)

    merged = gold_df.merge(
        silver_df, how="left", left_on="source_udm_inc_id", right_on="udm_inc_id",
        suffixes=("", "_silver"),
    )
    assert len(merged) == len(gold_df), (
        f"{table_name}: merge changed row count ({len(merged)} vs {len(gold_df)}) — "
        "udm_inc_id may not be unique in silver; investigate before trusting these stats."
    )

    total_rows_in_gold = len(merged)
    summary_rows = []
    detail_frames = []

    for _, silver_col, gold_col, display_name in resolved_columns:
        gold_values = merged[gold_col]
        silver_values = merged[silver_col]
        # NULL-safe equality: both null counts as a match.
        is_matched = (gold_values == silver_values) | (gold_values.isna() & silver_values.isna())

        rows_matching = int(is_matched.sum())
        rows_not_matching = total_rows_in_gold - rows_matching
        pct_matching = round(100.0 * rows_matching / total_rows_in_gold, 2) if total_rows_in_gold else None
        pct_not_matching = round(100.0 - pct_matching, 2) if pct_matching is not None else None

        mismatch_rows = merged.loc[~is_matched, ["ndid", "psid", "source_udm_inc_id", gold_col, silver_col]].copy()
        mismatch_rows.insert(0, "column_name", display_name)
        mismatch_rows.insert(0, "table_name", table_name)
        mismatch_rows = mismatch_rows.rename(columns={gold_col: "gold_value", silver_col: "silver_value"})
        detail_frames.append(mismatch_rows)

        count_ndid_not_matching = int(mismatch_rows["ndid"].nunique())
        associated_psids_sample = ", ".join(
            str(p) for p in sorted(mismatch_rows["psid"].dropna().unique())[:TOP_N_PSIDS]
        )

        top_mismatches = merged.loc[~is_matched, gold_col].value_counts().head(TOP_N_MISMATCHES)
        top_mismatches_str = "; ".join(
            f"{value!r} (x{count})" for value, count in top_mismatches.items()
        )

        summary_rows.append({
            "table_name": table_name,
            "column_name": display_name,
            "total_rows_in_gold": total_rows_in_gold,
            "rows_matching_silver": rows_matching,
            "pct_rows_matching_silver": pct_matching,
            "rows_not_matching_silver": rows_not_matching,
            "pct_rows_not_matching_silver": pct_not_matching,
            "top_20_mismatched_values": top_mismatches_str,
            "count_ndid_not_matching": count_ndid_not_matching,
            "associated_psids": associated_psids_sample,
        })

    mismatch_detail_df = (
        pd.concat(detail_frames, ignore_index=True) if detail_frames
        else pd.DataFrame(columns=["table_name", "column_name", "ndid", "psid", "source_udm_inc_id",
                                    "gold_value", "silver_value"])
    )
    return summary_rows, mismatch_detail_df


def write_sheets(path: str, sheets: dict):
    """Write/replace the given {sheet_name: df} entries in the workbook
    WITHOUT touching any other existing sheets.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "a" if os.path.exists(path) else "w"
    kwargs = {"if_sheet_exists": "replace"} if mode == "a" else {}
    with pd.ExcelWriter(path, engine="openpyxl", mode=mode, **kwargs) as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def detail_csv_path(table_name: str) -> str:
    return os.path.join(OUTPUT_DIR, f"unmatched_detail_{table_name}.csv")


def write_detail_csv(path: str, df: pd.DataFrame):
    """Writes mismatch-detail rows to their own CSV via pyarrow.csv —
    meaningfully faster than pandas.to_csv at this scale (confirmed: vitals
    alone produced 42.9M detail rows). No append mode: one file per table
    (pyarrow's CSV writer has no append mode, and per-table files also mean
    a failed write on one table can't corrupt another's already-saved
    detail — see module docstring).
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pa_csv.write_csv(table, path)


def run_specs(engine, specs: dict):
    """Runs build_table_outputs for the given {table_name: spec} dict,
    writing each table's detail CSV as soon as it's computed (not holding
    every table's detail in memory at once). Returns summary_df only —
    detail lives on disk at detail_csv_path(table_name) per table.
    """
    all_summary_rows = []

    for table_name, spec in specs.items():
        print(f"[{table_name}] Fetching {spec['gold_table']} and scoped {spec['silver_table']}, "
              f"checking {len(spec['columns'])} column(s) ...")
        summary_rows, detail_df = build_table_outputs(engine, table_name, spec)
        all_summary_rows.extend(summary_rows)

        path = detail_csv_path(table_name)
        write_detail_csv(path, detail_df)
        print(f"    -> {len(detail_df):,} mismatch rows written to {path}")

        for row in summary_rows:
            print(f"    {row['column_name']:45} "
                  f"matching={row['pct_rows_matching_silver']}% "
                  f"not_matching={row['pct_rows_not_matching_silver']}% "
                  f"(distinct ndids affected: {row['count_ndid_not_matching']:,})")

    return pd.DataFrame(all_summary_rows)


# ============================================================================
# 4. RUNNER
# ============================================================================

def main():
    """Full run across every TABLE_SPECS entry. To run a subset (e.g. just
    one newly-added table), call run_specs(engine, {"<name>": TABLE_SPECS["<name>"]})
    directly instead — see how the radiology-only run was done.
    """
    engine = get_engine()
    summary_df = run_specs(engine, TABLE_SPECS)
    write_sheets(OUTPUT_PATH, {"std_column_qc": summary_df})

    print(f"\nSummary workbook saved to: {OUTPUT_PATH}")
    print(f"Detail CSVs written per table at: {OUTPUT_DIR}/unmatched_detail_<table_name>.csv")


if __name__ == "__main__":
    main()
