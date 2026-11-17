#!/usr/bin/env python3
"""Read dbt artifacts and append dbt object execution rows to the NAS audit table.

Call this after a dbt command (for example, dbt build) has completed and
run_results.json plus manifest.json are available. The parent DEPLOYMENT_AUDIT
row must already exist and DEPLOYMENT_ID must be the same ID used by the
workflow's other deployment-audit steps.

By default, artifacts are read from dbt/target/. Override the locations with
DBT_RUN_RESULTS_PATH and DBT_MANIFEST_PATH if the dbt project uses another
project directory or target-path.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

OBJECT_AUDIT_TABLE = "NAS_ADMIN.SCHEMACHANGE.DEPLOYMENT_OBJECT_AUDIT"


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is missing.")
    return value


def load_json(path: Path) -> dict[str, Any]:
    """Load a dbt artifact and provide a useful message for missing/invalid files."""
    if not path.is_file():
        raise FileNotFoundError(
            f"Required dbt artifact not found: {path}. Run dbt build/run/test first."
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in dbt artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"Unexpected JSON structure in dbt artifact: {path}")
    return value


def main() -> None:
    import snowflake.connector

    # Use the same deployment ID as the parent workflow run. This lets the
    # deployment report join dbt nodes and Schemachange migrations together.
    deployment_id = required_env("DEPLOYMENT_ID")
    environment = required_env("DEPLOY_ENVIRONMENT").upper()

    # dbt's default target directory is <project>/target. The default project
    # root in this NAS repository is dbt/; both paths can be overridden.
    run_results_path = Path(
        os.getenv("DBT_RUN_RESULTS_PATH", "dbt/target/run_results.json")
    )
    manifest_path = Path(
        os.getenv("DBT_MANIFEST_PATH", "dbt/target/manifest.json")
    )
    run_results = load_json(run_results_path)
    manifest = load_json(manifest_path)

    results = run_results.get("results", [])
    if not isinstance(results, list):
        raise RuntimeError("dbt run_results.json does not contain a results array.")

    metadata = run_results.get("metadata") or manifest.get("metadata") or {}
    dbt_invocation_id = metadata.get("invocation_id")
    dbt_version = metadata.get("dbt_version")

    # run_results contains only executed nodes and identifies each node by
    # unique_id. manifest.json provides the full model/test/snapshot/seed metadata.
    manifest_nodes = manifest.get("nodes") or {}
    manifest_sources = manifest.get("sources") or {}

    records: list[dict[str, Any]] = []
    for result in results:
        unique_id = result.get("unique_id")
        if not unique_id:
            continue
        node = manifest_nodes.get(unique_id) or manifest_sources.get(unique_id) or {}
        config = node.get("config") or {}
        adapter_response = result.get("adapter_response")

        # The dbt resource_type distinguishes model, test, snapshot, seed,
        # analysis and other executed node types. Do not create rows for
        # manifest-only resources that are absent from run_results.
        resource_type = node.get("resource_type") or unique_id.split(".")[0]
        records.append(
            {
                "deployment_id": deployment_id,
                "environment": environment,
                "deployment_tool": "DBT",
                "object_type": str(resource_type).upper(),
                "object_name": node.get("name") or unique_id,
                "object_path": node.get("original_file_path") or node.get("path"),
                "object_unique_id": unique_id,
                "database_name": node.get("database"),
                "schema_name": node.get("schema"),
                "relation_name": result.get("relation_name") or node.get("alias"),
                "materialization": config.get("materialized"),
                "status": result.get("status"),
                "execution_time": result.get("execution_time"),
                "error_message": result.get("message"),
                "executed_by": os.getenv("GITHUB_ACTOR"),
                "dbt_invocation_id": dbt_invocation_id,
                "dbt_version": dbt_version,
                "adapter_response": json.dumps(adapter_response)
                if adapter_response is not None
                else None,
            }
        )

    if not records:
        print("No executed dbt nodes were found in run_results.json; no child rows inserted.")
        return

    connection = snowflake.connector.connect(
        account=required_env("SNOWFLAKE_ACCOUNT"),
        user=required_env("SNOWFLAKE_USER"),
        role=required_env("SNOWFLAKE_ROLE"),
        warehouse=required_env("SNOWFLAKE_WAREHOUSE"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=required_env("SNOWFLAKE_PRIVATE_KEY_FILE"),
        session_parameters={
            "QUERY_TAG": f"NAS_DBT_DEPLOYMENT_AUDIT|{environment}|{dbt_invocation_id or deployment_id}"
        },
    )

    # JSON adapter_response is converted to VARIANT in the SELECT projection.
    # All external values remain bound parameters, not interpolated SQL text.
    sql = f"""
        INSERT INTO {OBJECT_AUDIT_TABLE}
        (
            DEPLOYMENT_ID, ENVIRONMENT, DEPLOYMENT_TOOL, OBJECT_TYPE,
            OBJECT_NAME, OBJECT_PATH, OBJECT_UNIQUE_ID, DATABASE_NAME,
            SCHEMA_NAME, RELATION_NAME, MATERIALIZATION, STATUS,
            EXECUTION_TIME, ERROR_MESSAGE, EXECUTED_BY,
            DBT_INVOCATION_ID, DBT_VERSION, ADAPTER_RESPONSE
        )
        SELECT
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, PARSE_JSON(%s)
    """

    params = [
        (
            row["deployment_id"],
            row["environment"],
            row["deployment_tool"],
            row["object_type"],
            row["object_name"],
            row["object_path"],
            row["object_unique_id"],
            row["database_name"],
            row["schema_name"],
            row["relation_name"],
            row["materialization"],
            row["status"],
            row["execution_time"],
            row["error_message"],
            row["executed_by"],
            row["dbt_invocation_id"],
            row["dbt_version"],
            row["adapter_response"],
        )
        for row in records
    ]

    try:
        with connection.cursor() as cursor:
            # Insert one result per executed dbt node; one deployment can
            # therefore have many model, test, seed or snapshot detail rows.
            for row_params in params:
                cursor.execute(sql, row_params)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    print(
        f"Recorded {len(records)} dbt object audit row(s): "
        f"deployment_id={deployment_id}, environment={environment}, "
        f"invocation_id={dbt_invocation_id}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"::error::Unable to write dbt object audit: {exc}", file=sys.stderr)
        raise
