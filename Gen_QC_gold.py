--By using gold - source_udm_inc_id  silver - udm_inc_id--------

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

OUTPUT
------
./gen_output/gen_silver_gold_validation.xlsx
  - "std_column_qc" — Output A: one row per (table, column) pair —
    total_rows_in_gold, rows_matching_silver, pct_rows_matching_silver,
    rows_not_matching_silver, pct_rows_not_matching_silver,
    top_20_mismatched_values, count_ndid_not_matching (distinct ndids
    behind THIS column's mismatches), associated_psids (their psids,
    sampled).
  - "Unmatched_Detail" — Output B: every (table_name, column_name, ndid,
    psid, source_udm_inc_id, gold_value, silver_value) where the two
    layers disagree — the granular audit trail.

TARGET TABLES & EXECUTION SCOPE
--------------------------------
Only `procedures` and `labs` are active for now (per current request).
TABLE_SPECS is a dict so the remaining 9 (patients, encounters,
diagnosis, radiology, vitals, allergies, ...) can be added later without
touching any other code — just add entries and re-run.

HOW TO RUN
----------
python gen.py
"""
from __future__ import annotations

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

load_dotenv()

# ============================================================================
# 1. CONFIG
# ============================================================================

DB_CONFIG = {
    "drivername": "mysql+pymysql",
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
    # --- placeholders for the remaining 9 tables ---
    # "patients": {
    #     "gold_table": "rgd_gold_ad.patients",
    #     "silver_table": "rgd_udm_silver.patients",
    #     "columns": [
    #         ("gender_hl7_std", "gender"),
    #         ("pat_race_std", "race"),
    #         ("pat_ethnicity_std", "ethnicity"),
    #         ("pat_marital_status_std", "marital_status"),
    #         ("pat_deceased_status_std", "deceased_status"),
    #     ],
    # },
    # "encounters": {
    #     "gold_table": "rgd_gold_ad.encounters",
    #     "silver_table": "rgd_udm_silver.encounters",
    #     "columns": [
    #         ("enc_type_std", "enc_type"),
    #         ("enc_sub_type_std", "enc_subtype"),
    #     ],
    # },
    # "diagnosis": {
    #     "gold_table": "rgd_gold_ad.diagnosis",
    #     "silver_table": "rgd_udm_silver.diagnosis",
    #     "columns": [
    #         ("diag_desc_std", "diag_desc"),
    #         ("diag_coding_system_std", "diag_coding_system"),
    #         ("primary_diagnosis_flag_std", "primary_diagnosis_flag"),
    #     ],
    # },
    # "radiology": {
    #     "gold_table": "rgd_gold_ad.radiology",
    #     "silver_table": "rgd_udm_silver.radiology",
    #     "columns": [
    #         ("study_name", "study_name"),  # no _std variant exists in silver
    #         ("modality_std", "img_modality"),
    #     ],
    # },
    # "vitals": {
    #     "gold_table": "rgd_gold_ad.vitals",
    #     "silver_table": "rgd_udm_silver.vitals",
    #     "columns": [
    #         ("vital_name_std", "vital_name"),
    #         ("vital_code_std", "vital_code"),
    #         ("vital_coding_system_std", "vital_coding_system"),
    #         ("vital_result_std", "vital_result"),
    #         ("vital_unit_std", "vital_unit"),
    #     ],
    # },
    # "allergies": {
    #     "gold_table": "rgd_gold_ad.allergies",
    #     "silver_table": "rgd_udm_silver.allergies",
    #     "columns": [
    #         ("allergen_name_std", "allergen_name"),
    #         ("allergen_code_std", "allergen_code"),
    #         ("allergen_coding_system_std", "allergen_coding_system"),
    #         ("allergy_reaction_name_std", "allergy_reaction_name"),
    #         ("allergy_reaction_code_std", "allergy_reaction_code"),
    #         ("allergy_reaction_coding_system_std", "allergy_reaction_coding_system"),
    #     ],
    # },
}

OUTPUT_DIR = "./gen_output"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "gen_silver_gold_validation.xlsx")


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


def fetch_silver_scope(engine: Engine, gold_table: str, silver_table: str, silver_cols_needed: list) -> pd.DataFrame:
    """Silver rows restricted to the source_udm_inc_id population gold
    actually references (the scope filter), in ONE query.
    """
    cols_sql = ", ".join(["udm_inc_id"] + silver_cols_needed)
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

def build_table_outputs(engine: Engine, table_name: str, spec: dict):
    """Returns (summary_rows: list[dict], mismatch_detail_df: pd.DataFrame)
    for one table.
    """
    gold_table = spec["gold_table"]
    silver_table = spec["silver_table"]
    columns = spec["columns"]

    silver_cols_needed = sorted({c for c, _ in columns})
    gold_cols_needed = sorted({c for _, c in columns})

    gold_df = fetch_gold(engine, gold_table, gold_cols_needed)
    silver_df = fetch_silver_scope(engine, gold_table, silver_table, silver_cols_needed)

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

    for silver_col, gold_col in columns:
        gold_values = merged[gold_col]
        silver_values = merged[silver_col]
        # NULL-safe equality: both null counts as a match.
        is_matched = (gold_values == silver_values) | (gold_values.isna() & silver_values.isna())

        rows_matching = int(is_matched.sum())
        rows_not_matching = total_rows_in_gold - rows_matching
        pct_matching = round(100.0 * rows_matching / total_rows_in_gold, 2) if total_rows_in_gold else None
        pct_not_matching = round(100.0 - pct_matching, 2) if pct_matching is not None else None

        mismatch_rows = merged.loc[~is_matched, ["ndid", "psid", "source_udm_inc_id", gold_col, silver_col]].copy()
        mismatch_rows.insert(0, "column_name", f"{silver_col} -> {gold_col}")
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
            "column_name": f"{silver_col} -> {gold_col}",
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


def run_specs(engine, specs: dict):
    """Runs build_table_outputs for the given {table_name: spec} dict and
    returns (summary_df, mismatch_detail_df).
    """
    all_summary_rows = []
    all_detail = []

    for table_name, spec in specs.items():
        print(f"[{table_name}] Fetching {spec['gold_table']} and scoped {spec['silver_table']}, "
              f"checking {len(spec['columns'])} column(s) ...")
        summary_rows, detail_df = build_table_outputs(engine, table_name, spec)
        all_summary_rows.extend(summary_rows)
        all_detail.append(detail_df)

        for row in summary_rows:
            print(f"    {row['column_name']:45} "
                  f"matching={row['pct_rows_matching_silver']}% "
                  f"not_matching={row['pct_rows_not_matching_silver']}% "
                  f"(distinct ndids affected: {row['count_ndid_not_matching']:,})")

    summary_df = pd.DataFrame(all_summary_rows)
    detail_df = pd.concat(all_detail, ignore_index=True) if all_detail else pd.DataFrame()
    return summary_df, detail_df


# ============================================================================
# 4. RUNNER
# ============================================================================

def main():
    engine = get_engine()
    summary_df, detail_df = run_specs(engine, TABLE_SPECS)

    write_sheets(OUTPUT_PATH, {
        "std_column_qc": summary_df,
        "Unmatched_Detail": detail_df,
    })

    print(f"\nWorkbook saved to: {OUTPUT_PATH}")
    print(f"Total mismatched rows across all tables/columns: {len(detail_df):,}")


if __name__ == "__main__":
    main()
