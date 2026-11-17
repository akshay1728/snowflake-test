#!/usr/bin/env python3
"""Write deployment-level and Schemachange object-level audit records to Snowflake."""
from __future__ import annotations

import importlib.metadata
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

# Parent record stores GitHub PR/reviewer/approval/deployment metadata.
AUDIT_TABLE = "NAS_ADMIN.SCHEMACHANGE.DEPLOYMENT_AUDIT"
# Child records store one row per deployed migration or dbt resource.
OBJECT_AUDIT_TABLE = "NAS_ADMIN.SCHEMACHANGE.DEPLOYMENT_OBJECT_AUDIT"
GITHUB_API = "https://api.github.com"
GITHUB_API_VERSION = "2026-03-10"

# The values below match the current environment configuration files in the
# NAS repository. An explicit environment variable can override the mapping.
DEFAULT_HISTORY_TABLES = {
    "DEV": "NAS_ADMIN.SCHEMACHANGE.CHANGE_HISTORY_DEV",
    "UAT": "NAS_MONITORING.SCHEMACHANGE.CHANGE_HISTORY_UAT",
    "PROD": "NAS_MONITORING.SCHEMACHANGE.CHANGE_HISTORY_PROD",
}


def required_env(name: str) -> str:
    """Read a required environment variable and fail clearly if it is absent."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is missing.")
    return value


def github_get(path: str) -> Any:
    """Call a GitHub REST API GET endpoint using the short-lived Actions token."""
    request = urllib.request.Request(
        f"{GITHUB_API}{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {required_env('GITHUB_TOKEN')}",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
            "User-Agent": "NAS-Snowflake-Deployment-Audit",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"GitHub API request failed ({exc.code}) for {path}: {body[:1000]}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"GitHub API request failed for {path}: {exc}") from exc


def list_pr_reviews(repository: str, pull_number: int) -> list[dict[str, Any]]:
    """Retrieve all PR review pages (up to 100 review records per page)."""
    reviews: list[dict[str, Any]] = []
    page = 1
    while True:
        query = urllib.parse.urlencode({"per_page": 100, "page": page})
        batch = github_get(f"/repos/{repository}/pulls/{pull_number}/reviews?{query}")
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected PR reviews response for PR #{pull_number}.")
        reviews.extend(batch)
        if len(batch) < 100:
            return reviews
        page += 1


def compact_pr(pr: dict[str, Any]) -> dict[str, Any]:
    """Keep only PR metadata needed for deployment governance and traceability."""
    user = pr.get("user") or {}
    merged_by = pr.get("merged_by") or {}
    base = pr.get("base") or {}
    head = pr.get("head") or {}
    return {
        "number": pr.get("number"),
        "title": pr.get("title"),
        "url": pr.get("html_url"),
        "author": user.get("login"),
        "merged_by": merged_by.get("login"),
        "merged_at": pr.get("merged_at"),
        "base_branch": base.get("ref"),
        "head_branch": head.get("ref"),
        "merge_commit_sha": pr.get("merge_commit_sha"),
    }


def validate_table_name(table_name: str) -> str:
    """Allow only a three-part Snowflake identifier from trusted workflow config."""
    candidate = table_name.upper()
    if not re.fullmatch(r"[A-Z0-9_$]+\.[A-Z0-9_$]+\.[A-Z0-9_$]+", candidate):
        raise ValueError(
            "SCHEMACHANGE_CHANGE_HISTORY_TABLE must be a three-part Snowflake identifier."
        )
    return candidate


def get_schemachange_history_rows(
    cursor: Any, environment: str, started_at: str | None
) -> list[tuple[Any, ...]]:
    """Read history rows written by this deployment after its start marker.

    The GitHub workflow writes DEPLOYMENT_STARTED_AT immediately before the
    real Schemachange deploy command. The query also filters INSTALLED_BY to
    CURRENT_USER(), and workflow concurrency prevents overlapping deployments
    in the same workflow/environment. Dry-runs do not set this start marker and
    therefore do not create child migration rows.
    """
    if not started_at:
        print("No DEPLOYMENT_STARTED_AT supplied; no Schemachange object rows to capture.")
        return []

    configured_table = os.getenv("SCHEMACHANGE_CHANGE_HISTORY_TABLE", "").strip()
    history_table = validate_table_name(
        configured_table or DEFAULT_HISTORY_TABLES.get(environment, "")
    )

    query = f"""
        SELECT
            VERSION,
            DESCRIPTION,
            SCRIPT,
            SCRIPT_TYPE,
            CHECKSUM,
            EXECUTION_TIME,
            STATUS,
            INSTALLED_BY,
            INSTALLED_ON
        FROM {history_table}
        WHERE INSTALLED_ON >= TO_TIMESTAMP_TZ(%s)
          AND INSTALLED_BY = CURRENT_USER()
        ORDER BY INSTALLED_ON, VERSION
    """
    cursor.execute(query, (started_at,))
    rows = cursor.fetchall()
    print(f"Found {len(rows)} Schemachange history row(s) in {history_table} for this run.")
    return rows


def insert_schemachange_object_rows(
    cursor: Any,
    deployment_id: str,
    environment: str,
    history_rows: list[tuple[Any, ...]],
) -> int:
    """Insert one child audit row for each Schemachange history entry."""
    if not history_rows:
        return 0

    sql = f"""
        INSERT INTO {OBJECT_AUDIT_TABLE}
        (
            DEPLOYMENT_ID, ENVIRONMENT, DEPLOYMENT_TOOL, OBJECT_TYPE,
            OBJECT_NAME, OBJECT_PATH, OBJECT_UNIQUE_ID, DATABASE_NAME,
            SCHEMA_NAME, RELATION_NAME, MATERIALIZATION, VERSION,
            SCRIPT_TYPE, CHECKSUM, STATUS, EXECUTION_TIME, ERROR_MESSAGE,
            EXECUTED_BY, EXECUTED_AT, DBT_INVOCATION_ID, DBT_VERSION,
            ADAPTER_RESPONSE
        )
        VALUES (
            %s, %s, 'SCHEMACHANGE', 'MIGRATION', %s, %s, NULL, NULL,
            NULL, NULL, NULL, %s, %s, %s, %s, %s, NULL, %s, %s,
            NULL, NULL, NULL
        )
    """

    params = []
    for row in history_rows:
        version, _description, script, script_type, checksum, execution_time, status, installed_by, installed_on = row
        # SCRIPT is retained as both the displayed object name and its best
        # available path; Schemachange's history table may store only the file name.
        params.append(
            (
                deployment_id,
                environment,
                script,
                script,
                version,
                script_type,
                checksum,
                status,
                execution_time,
                installed_by,
                installed_on,
            )
        )

    cursor.executemany(sql, params)
    return len(params)


def main() -> None:
    """Collect GitHub details and write parent plus migration child audit rows."""
    import snowflake.connector

    repository = required_env("GITHUB_REPOSITORY")
    run_id = required_env("GITHUB_RUN_ID")
    commit_sha = required_env("DEPLOY_COMMIT_SHA")
    deployment_id = required_env("DEPLOYMENT_ID")
    environment = required_env("DEPLOY_ENVIRONMENT").upper()
    source_branch = os.getenv("DEPLOY_SOURCE_BRANCH", "")
    mode = os.getenv("DEPLOYMENT_MODE", "EXECUTE").upper()
    status = os.getenv("DEPLOYMENT_STATUS", "UNKNOWN").upper()
    started_at = os.getenv("DEPLOYMENT_STARTED_AT", "").strip() or None
    if mode == "DRY_RUN" and status == "SUCCESS":
        status = "DRY_RUN"

    # Find all PRs GitHub associates with the exact deployed commit.
    encoded_sha = urllib.parse.quote(commit_sha, safe="")
    associated_prs = github_get(f"/repos/{repository}/commits/{encoded_sha}/pulls")
    if not isinstance(associated_prs, list):
        raise RuntimeError("Unexpected response from the commit-to-PR GitHub API.")

    # Choose a primary PR for scalar columns while preserving all PRs as JSON.
    primary_pr = next(
        (pr for pr in associated_prs if pr.get("merge_commit_sha") == commit_sha),
        None,
    )
    if primary_pr is None and associated_prs:
        primary_pr = sorted(
            associated_prs, key=lambda item: item.get("merged_at") or "", reverse=True
        )[0]

    # Use each reviewer's latest review state so an old APPROVED review is not
    # counted if the same reviewer later submitted REQUEST_CHANGES.
    approved_reviews: list[dict[str, Any]] = []
    for pr in associated_prs:
        number = pr.get("number")
        if not number:
            continue
        latest_by_reviewer: dict[str, dict[str, Any]] = {}
        for review in list_pr_reviews(repository, int(number)):
            login = (review.get("user") or {}).get("login")
            if not login:
                continue
            prior = latest_by_reviewer.get(login)
            if prior is None or (review.get("submitted_at") or "") >= (prior.get("submitted_at") or ""):
                latest_by_reviewer[login] = review
        for login, review in latest_by_reviewer.items():
            if str(review.get("state", "")).upper() == "APPROVED":
                approved_reviews.append(
                    {
                        "pull_request": int(number),
                        "reviewer": login,
                        "submitted_at": review.get("submitted_at"),
                        "body": review.get("body"),
                        "commit_id": review.get("commit_id"),
                        "url": review.get("html_url"),
                    }
                )

    # Environment protection approvals are distinct from PR code reviews.
    # GitHub's workflow-run approvals endpoint returns reviewer/state/comment;
    # it does not supply the exact timestamp of the reviewer's click.
    approvals = github_get(f"/repos/{repository}/actions/runs/{run_id}/approvals")
    if not isinstance(approvals, list):
        raise RuntimeError("Unexpected response from the workflow-run approvals API.")
    approval_retrieved_at = datetime.now(timezone.utc).isoformat()

    pr_approved_by = ", ".join(
        sorted({item["reviewer"] for item in approved_reviews if item.get("reviewer")})
    )
    environment_approved_by = ", ".join(
        sorted(
            {
                (item.get("user") or {}).get("login")
                for item in approvals
                if str(item.get("state", "")).lower() == "approved"
                and (item.get("user") or {}).get("login")
            }
        )
    )
    environment_approval_comments = "\n".join(
        item.get("comment", "").strip()
        for item in approvals
        if str(item.get("state", "")).lower() == "approved"
        and item.get("comment", "").strip()
    )

    pr_metadata = [compact_pr(pr) for pr in associated_prs]
    primary = compact_pr(primary_pr) if primary_pr else {}
    run_number = os.getenv("GITHUB_RUN_NUMBER")
    run_url = (
        f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/"
        f"{repository}/actions/runs/{run_id}"
    )
    try:
        schemachange_version = importlib.metadata.version("schemachange")
    except importlib.metadata.PackageNotFoundError:
        schemachange_version = None

    # Reuse the same deployment ID generated by the workflow. Future dbt steps
    # in this same deployment run can use this ID to add dbt object rows later.
    connection = snowflake.connector.connect(
        account=required_env("SNOWFLAKE_ACCOUNT"),
        user=required_env("SNOWFLAKE_USER"),
        role=required_env("SNOWFLAKE_ROLE"),
        warehouse=required_env("SNOWFLAKE_WAREHOUSE"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=required_env("SNOWFLAKE_PRIVATE_KEY_FILE"),
        session_parameters={"QUERY_TAG": f"NAS_DEPLOYMENT_AUDIT|{environment}|{run_id}"},
    )

    sql_parent = f"""
        INSERT INTO {AUDIT_TABLE}
        (
            DEPLOYMENT_ID, ENVIRONMENT, GITHUB_REPOSITORY, GITHUB_WORKFLOW,
            GITHUB_RUN_ID, GITHUB_RUN_NUMBER, GITHUB_RUN_URL, SOURCE_BRANCH,
            COMMIT_SHA, PR_NUMBER, PR_TITLE, PR_URL, PR_AUTHOR, PR_MERGED_BY,
            PR_MERGED_AT, PR_APPROVED_BY, PR_METADATA, PR_APPROVED_REVIEWS,
            ENVIRONMENT_APPROVED_BY, ENVIRONMENT_APPROVAL_COMMENT,
            ENVIRONMENT_APPROVALS, APPROVALS_RETRIEVED_AT, DEPLOYMENT_ACTOR,
            DEPLOYMENT_STATUS, DEPLOYMENT_MODE, SCHEMACHANGE_VERSION
        )
        SELECT
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            TO_TIMESTAMP_TZ(%s), %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s,
            PARSE_JSON(%s), TO_TIMESTAMP_TZ(%s), %s, %s, %s, %s
    """

    params_parent = (
        deployment_id,
        environment,
        repository,
        os.getenv("GITHUB_WORKFLOW"),
        run_id,
        int(run_number) if run_number else None,
        run_url,
        source_branch,
        commit_sha,
        primary.get("number"),
        primary.get("title"),
        primary.get("url"),
        primary.get("author"),
        primary.get("merged_by"),
        primary.get("merged_at"),
        pr_approved_by,
        json.dumps(pr_metadata),
        json.dumps(approved_reviews),
        environment_approved_by,
        environment_approval_comments,
        json.dumps(approvals),
        approval_retrieved_at,
        os.getenv("GITHUB_ACTOR"),
        status,
        mode,
        schemachange_version,
    )

    try:
        with connection.cursor() as cursor:
            # Read the environment's Schemachange history rows created after
            # this run's start marker, then write parent and child rows together.
            history_rows = get_schemachange_history_rows(cursor, environment, started_at)
            cursor.execute(sql_parent, params_parent)
            migration_count = insert_schemachange_object_rows(
                cursor, deployment_id, environment, history_rows
            )
        connection.commit()
    except Exception:
        # Do not leave an incomplete parent record if child-detail insertion fails.
        connection.rollback()
        raise
    finally:
        connection.close()

    print(
        "Deployment audit recorded: "
        f"deployment_id={deployment_id}, environment={environment}, status={status}, "
        f"run_id={run_id}, associated_prs={len(associated_prs)}, "
        f"approved_pr_reviews={len(approved_reviews)}, "
        f"environment_approval_records={len(approvals)}, "
        f"schemachange_migration_rows={migration_count}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Fail the Actions step visibly rather than silently losing the audit.
        print(f"::error::Unable to write Snowflake deployment audit: {exc}", file=sys.stderr)
        raise
