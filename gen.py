"""
gen.py
======
Validates that rgd_gold_ad's standardized columns are faithful copies of
rgd_udm_silver's corresponding `_std` columns, across 8 table groups
(Patients, Encounters, Diagnosis, Procedures, Labs, Radiology, Vitals,
Allergy). Column pairs come from "silver to gold mapping.pdf", verified
directly against the live schema before writing this (see fixes below).

MATCHING LOGIC
--------------
1. Primary join: gold.udm_unq_id = silver.udm_unq_id. udm_unq_id turns out
   to NOT be unique in silver either (confirmed: rgd_udm_silver.patients
   has 9,498,915 rows but only 6,231,171 distinct udm_unq_id — ~1.5x
   duplication), even though it IS unique in gold. A naive join on it
   fans out (verified: gold.patients has 125,369 rows but a naive join
   returned 207,445). So BOTH the primary and fallback candidates are
   deduplicated the same way, each via its own
   ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY updated_datetime DESC),
   taking the most recently updated silver row per key. Requires MySQL 8+
   window functions (confirmed: this DB runs 8.0.44). Ties (identical or
   NULL updated_datetime among candidates) are broken arbitrarily-but-
   deterministically by MySQL — fine for QC purposes, not guaranteed
   stable across runs if the underlying data changes.
2. Fallback join: for a gold row whose udm_unq_id has no silver match,
   fall back to silver.ndid instead (same dedup approach — ndid is also
   NOT unique in silver; e.g. medication silver had ~13x more rows than
   distinct ndids).
3. A row with no match via EITHER key is "unmatched" — a true orphan.
4. Column value equality uses MySQL's NULL-safe `<=>` operator (NULL <=>
   NULL is a match; NULL vs non-NULL is not), computed in SQL rather than
   in pandas to avoid NaN-comparison pitfalls.

Schema fixes applied vs. the original PDF (verified against live schema):
  - "Allergy" group's actual table is `allergies` (plural) in both layers.
  - Radiology's gold column is `img_modality`, not `img_modlaity` (typo).
  - Radiology has no `study_name_std` in silver — only a plain `study_name`
    on both sides, so that pair is compared raw-to-raw (confirmed with
    user).

PERFORMANCE NOTE
-----------------
rgd_gold_ad.encounters and rgd_gold_ad.vitals have no index on udm_unq_id
itself (only on udm_unq_id_hash, a different column silver doesn't have).
The join here still drives from gold — the smaller side — and probes
silver via silver's own udm_unq_id index, so this should be fine, but
these two tables are worth watching if they run slow; if so, adding
`CREATE INDEX ... ON rgd_gold_ad.<table>(udm_unq_id)` would fix it (a
schema change — confirm with the user before applying).

OUTPUT
------
./qc_output/gen_silver_gold_validation.xlsx
  - "std_column_qc" — Output A across ALL tables (written by main()/the
    full run): one row per (table, column) pair — total_rows_in_gold,
    rows_matching_silver, pct_rows_matching_silver, rows_not_matching_
    silver, pct_rows_not_matching_silver, top_20_mismatched_values,
    count_ndid_not_matching, associated_psids.
  - "Unmatched_Detail" — Output B across ALL tables: every (table_name,
    ndid, psid, udm_unq_id) that found no silver row via either key.
  - Single-table ad hoc runs (see run_specs()) write their own sheet
    (e.g. "patient_column_qc") into the same workbook via write_sheets(),
    which replaces only the named sheet(s) and leaves everything else
    (including sheets from other runs) untouched.

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

# Placeholder list — add more table groups here as they're confirmed.
# Each entry: table_group, gold_table, silver_table, columns (list of
# (silver_column, gold_column) pairs).
TABLE_SPECS = [
    {
        "table_group": "patients",
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
    {
        "table_group": "encounters",
        "gold_table": "rgd_gold_ad.encounters",
        "silver_table": "rgd_udm_silver.encounters",
        "columns": [
            ("enc_type_std", "enc_type"),
            ("enc_sub_type_std", "enc_subtype"),
        ],
    },
    {
        "table_group": "diagnosis",
        "gold_table": "rgd_gold_ad.diagnosis",
        "silver_table": "rgd_udm_silver.diagnosis",
        "columns": [
            ("diag_desc_std", "diag_desc"),
            ("diag_coding_system_std", "diag_coding_system"),
            ("primary_diagnosis_flag_std", "primary_diagnosis_flag"),
        ],
    },
    {
        "table_group": "procedures",
        "gold_table": "rgd_gold_ad.procedures",
        "silver_table": "rgd_udm_silver.procedures",
        "columns": [
            ("proc_code_std", "proc_code"),
            ("proc_name_std", "proc_name"),
            ("proc_coding_system_std", "proc_coding_system"),
            ("proc_description_std", "proc_description"),
        ],
    },
    {
        "table_group": "labs",
        "gold_table": "rgd_gold_ad.labs",
        "silver_table": "rgd_udm_silver.labs",
        "columns": [
            ("result_panel_std", "test_panel_name"),
            ("panel_code_std", "test_code"),
        ],
    },
    {
        "table_group": "radiology",
        "gold_table": "rgd_gold_ad.radiology",
        "silver_table": "rgd_udm_silver.radiology",
        "columns": [
            ("study_name", "study_name"),  # no _std variant exists — raw-to-raw, confirmed with user
            ("modality_std", "img_modality"),  # gold column corrected from PDF's "img_modlaity" typo
        ],
    },
    {
        "table_group": "vitals",
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
    {
        "table_group": "allergies",
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
]

OUTPUT_DIR = "./qc_output"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "gen_silver_gold_validation.xlsx")
TOP_N_MISMATCHES = 20
TOP_N_PSIDS = 20


def get_engine() -> Engine:
    url = URL.create(
        drivername=DB_CONFIG["drivername"],
        username=DB_CONFIG["username"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
    )
    return create_engine(url)


# ============================================================================
# 2. PER-TABLE FETCH — one query per table (not per column)
# ============================================================================

def fetch_table_join(engine: Engine, spec: dict) -> pd.DataFrame:
    """One row per gold row, with match_method + each column pair's gold
    value / resolved silver value / match flag, via the primary+fallback
    join described in the module docstring.
    """
    gold_table = spec["gold_table"]
    silver_table = spec["silver_table"]
    columns = spec["columns"]

    silver_cols_needed = sorted({silver_col for silver_col, _ in columns})
    silver_select_list = ", ".join(silver_cols_needed)

    # rgd_gold_ad and rgd_udm_silver were created with different default
    # collations (utf8mb4_0900_ai_ci vs utf8mb4_unicode_ci) — MySQL refuses
    # to compare text across them with <=> unless both sides are coerced to
    # a common collation.
    collate = "utf8mb4_unicode_ci"
    select_clauses = []
    for silver_col, gold_col in columns:
        silver_value_expr = f"COALESCE(s1.{silver_col}, s2.{silver_col})"
        select_clauses.append(
            f"g.{gold_col} AS gold__{gold_col},\n"
            f"    {silver_value_expr} AS silver__{silver_col},\n"
            f"    ((g.{gold_col} COLLATE {collate}) <=> ({silver_value_expr} COLLATE {collate})) AS match__{gold_col}"
        )
    select_sql = ",\n    ".join(select_clauses)

    query = text(f"""
        WITH silver_primary AS (
            SELECT udm_unq_id, updated_datetime, {silver_select_list},
                   ROW_NUMBER() OVER (PARTITION BY udm_unq_id ORDER BY updated_datetime DESC) AS rn
            FROM {silver_table}
            WHERE udm_unq_id IS NOT NULL
        ),
        silver_fallback AS (
            SELECT ndid, updated_datetime, {silver_select_list},
                   ROW_NUMBER() OVER (PARTITION BY ndid ORDER BY updated_datetime DESC) AS rn
            FROM {silver_table}
            WHERE ndid IS NOT NULL
        )
        SELECT
            g.ndid, g.psid, g.udm_unq_id,
            CASE
                WHEN s1.udm_unq_id IS NOT NULL THEN 'primary'
                WHEN s2.ndid IS NOT NULL THEN 'fallback'
                ELSE 'unmatched'
            END AS match_method,
            {select_sql}
        FROM {gold_table} g
        LEFT JOIN silver_primary s1 ON g.udm_unq_id = s1.udm_unq_id AND s1.rn = 1
        LEFT JOIN silver_fallback s2 ON g.ndid = s2.ndid AND s2.rn = 1 AND s1.udm_unq_id IS NULL
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


# ============================================================================
# 3. ANALYSIS
# ============================================================================

def build_table_outputs(table_group: str, columns: list, joined_df: pd.DataFrame):
    """Returns (summary_rows: list[dict], unmatched_df: pd.DataFrame) for
    one table group.
    """
    total_rows_in_gold = len(joined_df)
    unmatched_mask = joined_df["match_method"] == "unmatched"
    count_ndid_not_matching = int(unmatched_mask.sum())

    unmatched_df = joined_df.loc[unmatched_mask, ["ndid", "psid", "udm_unq_id"]].copy()
    unmatched_df.insert(0, "table_name", table_group)

    associated_psids_sample = ", ".join(
        str(p) for p in sorted(unmatched_df["psid"].dropna().unique())[:TOP_N_PSIDS]
    )

    summary_rows = []
    for silver_col, gold_col in columns:
        match_col = f"match__{gold_col}"
        rows_matching = int(joined_df[match_col].sum())
        rows_not_matching = total_rows_in_gold - rows_matching
        pct_matching = round(100.0 * rows_matching / total_rows_in_gold, 2) if total_rows_in_gold else None
        pct_not_matching = round(100.0 - pct_matching, 2) if pct_matching is not None else None

        mismatches = joined_df.loc[~joined_df[match_col].astype(bool),
                                    [f"gold__{gold_col}", f"silver__{silver_col}"]]
        top_mismatches = (
            mismatches.value_counts().head(TOP_N_MISMATCHES).reset_index(name="occurrences")
        )
        top_mismatches_str = "; ".join(
            f"gold={row[f'gold__{gold_col}']!r} vs silver={row[f'silver__{silver_col}']!r} (x{row['occurrences']})"
            for _, row in top_mismatches.iterrows()
        )

        summary_rows.append({
            "table_name": table_group,
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

    return summary_rows, unmatched_df


def write_sheets(path: str, sheets: dict):
    """Write/replace the given {sheet_name: df} entries in the workbook
    WITHOUT touching any other existing sheets — so a single-table ad hoc
    run and the full multi-table run can both write into the same file
    (at different times, possibly different processes) without clobbering
    each other's sheets.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "a" if os.path.exists(path) else "w"
    kwargs = {"if_sheet_exists": "replace"} if mode == "a" else {}
    with pd.ExcelWriter(path, engine="openpyxl", mode=mode, **kwargs) as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def run_specs(engine, specs: list):
    """Runs fetch_table_join + build_table_outputs for the given specs and
    returns (summary_df, unmatched_detail_df) — used by both main() (all
    tables) and single-table ad hoc runs.
    """
    all_summary_rows = []
    all_unmatched = []

    for spec in specs:
        table_group = spec["table_group"]
        print(f"[{table_group}] Joining {spec['gold_table']} <-> {spec['silver_table']} "
              f"(primary: udm_unq_id, fallback: ndid) ...")
        joined_df = fetch_table_join(engine, spec)
        print(f"  {len(joined_df):,} gold rows fetched")

        summary_rows, unmatched_df = build_table_outputs(table_group, spec["columns"], joined_df)
        all_summary_rows.extend(summary_rows)
        all_unmatched.append(unmatched_df)

        n_unmatched = len(unmatched_df)
        print(f"  {n_unmatched:,} rows unmatched via either key "
              f"({round(100*n_unmatched/len(joined_df), 2) if len(joined_df) else 0}%)")
        for row in summary_rows:
            print(f"    {row['column_name']:45} "
                  f"matching={row['pct_rows_matching_silver']}% "
                  f"not_matching={row['pct_rows_not_matching_silver']}%")

    summary_df = pd.DataFrame(all_summary_rows)
    unmatched_detail_df = pd.concat(all_unmatched, ignore_index=True) if all_unmatched else pd.DataFrame(
        columns=["table_name", "ndid", "psid", "udm_unq_id"]
    )
    return summary_df, unmatched_detail_df


# ============================================================================
# 4. RUNNER
# ============================================================================

def main():
    """Full run across every TABLE_SPECS entry. Writes the consolidated
    Output A to sheet 'std_column_qc' and Output B to 'Unmatched_Detail',
    without disturbing any per-table sheets (e.g. 'patient_column_qc')
    already written by a prior single-table run.
    """
    engine = get_engine()
    summary_df, unmatched_detail_df = run_specs(engine, TABLE_SPECS)

    write_sheets(OUTPUT_PATH, {
        "std_column_qc": summary_df,
        "Unmatched_Detail": unmatched_detail_df,
    })

    print(f"\nWorkbook saved to: {OUTPUT_PATH}")
    print(f"Total unmatched rows across all tables: {len(unmatched_detail_df):,}")


if __name__ == "__main__":
    main()
