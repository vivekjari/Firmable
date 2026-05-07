from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Tuple

import altair as alt
import pandas as pd
import streamlit as st


def query_df(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)


def ensure_review_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS human_review_labels (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          quality_result_id INTEGER UNIQUE NOT NULL,
          event_id TEXT,
          check_name TEXT,
          human_label TEXT NOT NULL,
          reviewer TEXT,
          notes TEXT,
          labeled_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def get_run_filters(selected_run: str) -> Tuple[str, str]:
    where_quality = "" if selected_run == "ALL" else f" AND run_id = '{selected_run}' "
    where_audit = "" if selected_run == "ALL" else f" AND run_id = '{selected_run}' "
    return where_quality, where_audit


def render_overview(conn: sqlite3.Connection, selected_run: str) -> None:
    where_run, where_audit = get_run_filters(selected_run)

    st.header("KPI Scorecards")
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    total_points = query_df(
        conn,
        f"""
        SELECT count(DISTINCT event_id) as n
        FROM llm_quality_results
        WHERE event_id IS NOT NULL {where_run}
        """,
    )
    failed_points = query_df(
        conn,
        f"""
        SELECT count(DISTINCT event_id) as n
        FROM llm_quality_results
        WHERE result='fail' {where_run}
        """,
    )
    total_calls = query_df(conn, f"SELECT count(*) as n FROM llm_call_audit WHERE 1=1 {where_audit}")
    total_cost = query_df(
        conn, f"SELECT COALESCE(sum(cost_usd), 0) as v FROM llm_call_audit WHERE 1=1 {where_audit}"
    )
    avg_latency = query_df(
        conn,
        f"""
        SELECT COALESCE(avg(latency_ms), 0) as v
        FROM llm_call_audit
        WHERE stage='quality_check' {where_audit}
        """,
    )
    total_points_n = int(total_points.iloc[0]["n"]) if not total_points.empty else 0
    failed_points_n = int(failed_points.iloc[0]["n"]) if not failed_points.empty else 0
    clean_points_n = max(total_points_n - failed_points_n, 0)
    k1.metric("Total Data Points", total_points_n)
    k2.metric("Failed Data Points", failed_points_n)
    k3.metric("Clean Data Points", clean_points_n)
    k4.metric("LLM Calls", int(total_calls.iloc[0]["n"]) if not total_calls.empty else 0)
    k5.metric("Total Cost", f"${float(total_cost.iloc[0]['v']):.4f}" if not total_cost.empty else "$0.0000")
    k6.metric("Avg LLM Latency (ms)", f"{float(avg_latency.iloc[0]['v']):.1f}" if not avg_latency.empty else "0.0")
    st.caption("Latency is API call-level, so large batches produce higher values.")

    st.header("DQ Trend Over Time")
    granularity = st.selectbox("Trend granularity", options=["hour", "day"], index=0)
    bucket_sql = "strftime('%Y-%m-%d %H:00', created_at)" if granularity == "hour" else "date(created_at)"
    trend = query_df(
        conn,
        f"""
        SELECT {bucket_sql} as bucket, check_name, result, count(*) as cnt
        FROM llm_quality_results
        WHERE created_at IS NOT NULL {where_run}
        GROUP BY bucket, check_name, result
        ORDER BY bucket ASC
        """,
    )
    if not trend.empty:
        pivoted = trend.pivot_table(
            index="bucket", columns=["check_name", "result"], values="cnt", fill_value=0
        ).sort_index()
        pivoted.columns = [f"{a}__{b}" for a, b in pivoted.columns]
        st.line_chart(pivoted)
    else:
        st.info("No quality trend data available.")

    st.header("Failures by Check (Donut)")
    donut_run = selected_run
    if donut_run == "ALL":
        latest_run_df = query_df(
            conn,
            """
            SELECT run_id
            FROM llm_quality_results
            WHERE run_id IS NOT NULL
            ORDER BY run_id DESC
            LIMIT 1
            """,
        )
        if not latest_run_df.empty:
            donut_run = str(latest_run_df.iloc[0]["run_id"])
    donut_where = "" if donut_run == "ALL" else f" AND run_id = '{donut_run}' "
    fail_by_check = query_df(
        conn,
        "SELECT check_name, count(*) as cnt FROM llm_quality_results "
        f"WHERE result='fail' {donut_where} GROUP BY check_name ORDER BY cnt DESC",
    )
    if not fail_by_check.empty:
        total_fail_checks = int(fail_by_check["cnt"].sum())
        donut = (
            alt.Chart(fail_by_check)
            .mark_arc(innerRadius=70)
            .encode(
                theta=alt.Theta(field="cnt", type="quantitative"),
                color=alt.Color(field="check_name", type="nominal"),
                tooltip=["check_name", "cnt"],
            )
        )
        center = (
            alt.Chart(pd.DataFrame({"text": [str(total_fail_checks)]}))
            .mark_text(size=24, fontWeight="bold", color="#F9FAFB")
            .encode(text="text:N")
        )
        title = (
            alt.Chart(pd.DataFrame({"t": [f"Total failed checks ({donut_run})"]}))
            .mark_text(y=8, size=12, color="#9CA3AF")
            .encode(text="t:N")
        )
        st.altair_chart((donut + center + title).properties(height=420), use_container_width=True)
    else:
        st.info("No failed rows available for donut chart.")

    st.header("Backlog Awaiting Human Review")
    backlog = query_df(
        conn,
        f"""
        SELECT q.event_id, q.run_id, count(*) as failed_checks,
               min(q.confidence) as min_confidence, max(q.created_at) as last_seen_at
        FROM llm_quality_results q
        LEFT JOIN human_review_labels h ON h.quality_result_id = q.id
        WHERE q.result='fail' AND h.quality_result_id IS NULL
          {where_run.replace('run_id', 'q.run_id')}
        GROUP BY q.event_id, q.run_id
        ORDER BY last_seen_at DESC
        LIMIT 200
        """,
    )
    backlog.index = backlog.index + 1
    st.caption(f"Backlog data points shown: {len(backlog)}")
    st.dataframe(backlog, use_container_width=True)


def load_unreviewed(conn: sqlite3.Connection, check_name: str, limit: int = 100) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT q.id as quality_result_id, q.event_id, q.run_id, q.check_name, q.model,
               q.prompt_version, q.result, q.confidence, q.reason, q.source_record_json, q.created_at
        FROM llm_quality_results q
        LEFT JOIN human_review_labels h ON h.quality_result_id = q.id
        WHERE q.result='fail' AND q.check_name = ? AND h.quality_result_id IS NULL
        ORDER BY q.created_at DESC
        LIMIT ?
        """,
        conn,
        params=(check_name, limit),
    )


def render_source_context(source_record_json: str, check_name: str) -> None:
    source = json.loads(source_record_json) if source_record_json else {}
    data = (source.get("data") or [{}])[0]
    attrs = data.get("attributes") or {}
    included = source.get("included") or []
    article = next((x for x in included if x.get("type") == "news_article"), {})
    article_attrs = article.get("attributes") or {}
    st.markdown("**Core fields**")
    st.write(
        {
            "event_id": data.get("id"),
            "category": attrs.get("category"),
            "summary": attrs.get("summary"),
            "article_sentence": attrs.get("article_sentence"),
            "found_at": attrs.get("found_at"),
        }
    )
    if check_name == "entity_resolution":
        companies = [
            {
                "id": x.get("id"),
                "company_name": (x.get("attributes") or {}).get("company_name"),
                "domain": (x.get("attributes") or {}).get("domain"),
            }
            for x in included
            if x.get("type") == "company"
        ]
        st.markdown("**Linked entities**")
        st.write(companies)
    if check_name == "source_credibility":
        st.markdown("**Source metadata**")
        st.write(
            {
                "title": article_attrs.get("title"),
                "url": article_attrs.get("url"),
                "published_at": article_attrs.get("published_at"),
                "author": article_attrs.get("author"),
            }
        )
    st.markdown("**Source title**")
    st.write(article_attrs.get("title"))
    st.markdown("**Source body (excerpt)**")
    body = article_attrs.get("body")
    st.write(body[:2500] if isinstance(body, str) else body)


def submit_label(
    conn: sqlite3.Connection,
    quality_result_id: int,
    event_id: str,
    check_name: str,
    human_label: str,
    reviewer: str,
    notes: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO human_review_labels(
          quality_result_id, event_id, check_name, human_label, reviewer, notes, labeled_at
        ) VALUES(?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (quality_result_id, event_id, check_name, human_label, reviewer, notes),
    )
    conn.commit()


def render_human_review(conn: sqlite3.Connection, reviewer: str) -> None:
    st.header("Human Review Workbench")
    tabs = st.tabs(["Semantic Accuracy", "Entity Resolution", "Source Credibility"])
    check_map = [
        ("semantic_accuracy", tabs[0]),
        ("entity_resolution", tabs[1]),
        ("source_credibility", tabs[2]),
    ]
    for check_name, tab in check_map:
        with tab:
            df = load_unreviewed(conn, check_name, limit=200)
            st.subheader(f"{check_name} - Awaiting Review ({len(df)})")
            if df.empty:
                st.info("No pending records in this section.")
                continue
            for _, row in df.head(30).iterrows():
                with st.expander(
                    f"Event {row['event_id']} | run {row['run_id']} | conf {row['confidence']}",
                    expanded=False,
                ):
                    st.markdown("**LLM verdict**")
                    st.write(
                        {
                            "result": row["result"],
                            "confidence": row["confidence"],
                            "reason": row["reason"],
                            "model": row["model"],
                            "prompt_version": row["prompt_version"],
                        }
                    )
                    render_source_context(row["source_record_json"], check_name)
                    c1, c2 = st.columns([1, 2])
                    with c1:
                        human_label = st.selectbox(
                            "Human label",
                            ["approve_fail", "override_pass", "needs_changes", "reject_record"],
                            key=f"label_{check_name}_{row['quality_result_id']}",
                        )
                    with c2:
                        notes = st.text_area(
                            "Reviewer notes",
                            key=f"notes_{check_name}_{row['quality_result_id']}",
                            height=90,
                        )
                    if st.button("Submit Review", key=f"submit_{check_name}_{row['quality_result_id']}"):
                        submit_label(
                            conn=conn,
                            quality_result_id=int(row["quality_result_id"]),
                            event_id=row["event_id"],
                            check_name=check_name,
                            human_label=human_label,
                            reviewer=reviewer,
                            notes=notes,
                        )
                        st.success("Saved. This record will drop from pending queue.")
                        st.rerun()


def render_review_evaluation(conn: sqlite3.Connection, selected_run: str) -> None:
    st.header("Human Review Evaluation Report")
    where_run = "" if selected_run == "ALL" else f" AND q.run_id = '{selected_run}' "
    summary = query_df(
        conn,
        f"""
        SELECT q.check_name, h.human_label, count(*) as cnt
        FROM human_review_labels h
        JOIN llm_quality_results q ON q.id = h.quality_result_id
        WHERE 1=1 {where_run}
        GROUP BY q.check_name, h.human_label
        ORDER BY cnt DESC
        """,
    )
    total = query_df(
        conn,
        f"""
        SELECT count(*) as n
        FROM human_review_labels h
        JOIN llm_quality_results q ON q.id = h.quality_result_id
        WHERE 1=1 {where_run}
        """,
    )
    total_n = int(total.iloc[0]["n"]) if not total.empty else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Reviewed Data Points", total_n)
    by_reviewer = query_df(
        conn,
        f"""
        SELECT COALESCE(h.reviewer, 'unknown') as reviewer, count(*) as cnt
        FROM human_review_labels h
        JOIN llm_quality_results q ON q.id = h.quality_result_id
        WHERE 1=1 {where_run}
        GROUP BY COALESCE(h.reviewer, 'unknown')
        ORDER BY cnt DESC
        """,
    )
    c2.metric("Active Reviewers", len(by_reviewer))
    overrides = query_df(
        conn,
        f"""
        SELECT count(*) as n
        FROM human_review_labels h
        JOIN llm_quality_results q ON q.id = h.quality_result_id
        WHERE h.human_label = 'override_pass' {where_run}
        """,
    )
    c3.metric("Overrides", int(overrides.iloc[0]["n"]) if not overrides.empty else 0)

    st.subheader("Reviewed Labels Distribution")
    if not summary.empty:
        summary.index = summary.index + 1
        st.dataframe(summary, use_container_width=True)
    else:
        st.info("No reviewed labels available yet.")
    st.subheader("Recent Reviewed Records")
    recent = query_df(
        conn,
        f"""
        SELECT h.labeled_at, h.reviewer, q.run_id, q.check_name, q.event_id, h.human_label, h.notes
        FROM human_review_labels h
        JOIN llm_quality_results q ON q.id = h.quality_result_id
        WHERE 1=1 {where_run}
        ORDER BY h.labeled_at DESC
        LIMIT 200
        """,
    )
    recent.index = recent.index + 1
    st.dataframe(recent, use_container_width=True)


def latest_run_id_for_version(conn: sqlite3.Connection, check_name: str, version: str) -> str | None:
    df = query_df(
        conn,
        f"""
        SELECT run_id
        FROM llm_quality_results
        WHERE check_name = '{check_name}'
          AND prompt_version = '{version}'
          AND run_id IS NOT NULL
        ORDER BY run_id DESC
        LIMIT 1
        """,
    )
    if df.empty:
        return None
    return str(df.iloc[0]["run_id"])


def load_check_run_df(
    conn: sqlite3.Connection, check_name: str, run_id: str, version: str
) -> pd.DataFrame:
    return query_df(
        conn,
        f"""
        SELECT event_id, result, confidence
        FROM llm_quality_results
        WHERE check_name = '{check_name}'
          AND run_id = '{run_id}'
          AND prompt_version = '{version}'
          AND event_id IS NOT NULL
        """,
    )


def render_prompt_version_comparison(conn: sqlite3.Connection) -> None:
    st.header("Prompt Version Comparison")
    c1, c2 = st.columns(2)
    with c1:
        base_version = st.text_input("Base prompt version", value="v1")
    with c2:
        candidate_version = st.text_input("Candidate prompt version", value="v2")

    checks = ["semantic_accuracy", "entity_resolution", "source_credibility"]
    rows = []
    examples = {}

    for check in checks:
        base_run = latest_run_id_for_version(conn, check, base_version)
        cand_run = latest_run_id_for_version(conn, check, candidate_version)
        if not base_run or not cand_run:
            rows.append(
                {
                    "check_name": check,
                    "base_run_id": base_run,
                    "candidate_run_id": cand_run,
                    "base_rows": 0,
                    "candidate_rows": 0,
                    "common_rows": 0,
                    "decision_changed_count": 0,
                    "decision_changed_rate": 0.0,
                    "avg_confidence_delta_candidate_minus_base": None,
                    "status": "missing_run",
                }
            )
            continue

        base_df = load_check_run_df(conn, check, base_run, base_version)
        cand_df = load_check_run_df(conn, check, cand_run, candidate_version)
        merged = base_df.merge(cand_df, on="event_id", suffixes=("_base", "_candidate"))
        if merged.empty:
            rows.append(
                {
                    "check_name": check,
                    "base_run_id": base_run,
                    "candidate_run_id": cand_run,
                    "base_rows": int(len(base_df)),
                    "candidate_rows": int(len(cand_df)),
                    "common_rows": 0,
                    "decision_changed_count": 0,
                    "decision_changed_rate": 0.0,
                    "avg_confidence_delta_candidate_minus_base": None,
                    "status": "no_overlap",
                }
            )
            continue

        changed = merged[merged["result_base"] != merged["result_candidate"]]
        conf_comp = merged.dropna(subset=["confidence_base", "confidence_candidate"]).copy()
        avg_delta = None
        if not conf_comp.empty:
            avg_delta = float(
                (conf_comp["confidence_candidate"] - conf_comp["confidence_base"]).mean()
            )

        rows.append(
            {
                "check_name": check,
                "base_run_id": base_run,
                "candidate_run_id": cand_run,
                "base_rows": int(len(base_df)),
                "candidate_rows": int(len(cand_df)),
                "common_rows": int(len(merged)),
                "decision_changed_count": int(len(changed)),
                "decision_changed_rate": float(len(changed) / len(merged)),
                "avg_confidence_delta_candidate_minus_base": avg_delta,
                "status": "ok",
            }
        )
        examples[check] = changed.head(10)

    summary_df = pd.DataFrame(rows)
    st.subheader("Comparison Summary")
    st.dataframe(summary_df, use_container_width=True)

    st.subheader("Changed Decision Examples")
    for check in checks:
        with st.expander(f"{check} examples", expanded=False):
            ex_df = examples.get(check)
            if ex_df is None or ex_df.empty:
                st.info("No changed decisions for this check (or missing runs).")
            else:
                ex_df = ex_df[
                    [
                        "event_id",
                        "result_base",
                        "result_candidate",
                        "confidence_base",
                        "confidence_candidate",
                    ]
                ]
                ex_df.index = ex_df.index + 1
                st.dataframe(ex_df, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="News Events QA Console", layout="wide")
    st.title("News Events QA Console")
    db_path = st.sidebar.text_input("SQLite DB Path", "quality_checks.db")
    reviewer = st.sidebar.text_input("Reviewer name", value="reviewer_1")
    if not Path(db_path).exists():
        st.error(f"DB not found: {db_path}")
        return
    conn = sqlite3.connect(db_path)
    try:
        ensure_review_table(conn)
        runs_df = query_df(
            conn,
            """
            SELECT DISTINCT run_id
            FROM llm_quality_results
            WHERE run_id IS NOT NULL
            ORDER BY run_id DESC
            """,
        )
        run_options = ["ALL"] + runs_df["run_id"].tolist() if not runs_df.empty else ["ALL"]
        selected_run = st.sidebar.selectbox("Run ID", options=run_options, index=0)
        section = st.sidebar.radio(
            "Section",
            [
                "Quality Overview",
                "Human Review",
                "Review Evaluation Report",
                "Prompt Version Comparison",
            ],
            index=0,
        )
        if section == "Quality Overview":
            render_overview(conn, selected_run)
        elif section == "Human Review":
            render_human_review(conn, reviewer)
        elif section == "Review Evaluation Report":
            render_review_evaluation(conn, selected_run)
        else:
            render_prompt_version_comparison(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
