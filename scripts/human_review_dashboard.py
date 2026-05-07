from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


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


def load_unreviewed(conn: sqlite3.Connection, check_name: str, limit: int = 100) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT q.id as quality_result_id,
               q.event_id,
               q.run_id,
               q.check_name,
               q.model,
               q.prompt_version,
               q.result,
               q.confidence,
               q.reason,
               q.llm_check_json,
               q.source_record_json,
               q.created_at
        FROM llm_quality_results q
        LEFT JOIN human_review_labels h ON h.quality_result_id = q.id
        WHERE q.result='fail'
          AND q.check_name = ?
          AND h.quality_result_id IS NULL
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


def render_check_tab(conn: sqlite3.Connection, check_name: str, reviewer: str) -> None:
    df = load_unreviewed(conn, check_name, limit=200)
    st.subheader(f"{check_name} - Awaiting Review ({len(df)})")
    if df.empty:
        st.info("No pending records in this section.")
        return

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

            label_col1, label_col2 = st.columns([1, 2])
            with label_col1:
                human_label = st.selectbox(
                    "Human label",
                    ["approve_fail", "override_pass", "needs_changes", "reject_record"],
                    key=f"label_{check_name}_{row['quality_result_id']}",
                )
            with label_col2:
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


def main() -> None:
    st.set_page_config(page_title="Human Review Workbench", layout="wide")
    st.title("Human Review Workbench")

    db_path = st.sidebar.text_input("SQLite DB Path", "quality_checks.db")
    reviewer = st.sidebar.text_input("Reviewer name", value="reviewer_1")
    if not Path(db_path).exists():
        st.error(f"DB not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    try:
        ensure_review_table(conn)
        tabs = st.tabs(["Semantic Accuracy", "Entity Resolution", "Source Credibility"])
        with tabs[0]:
            render_check_tab(conn, "semantic_accuracy", reviewer)
        with tabs[1]:
            render_check_tab(conn, "entity_resolution", reviewer)
        with tabs[2]:
            render_check_tab(conn, "source_credibility", reviewer)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
