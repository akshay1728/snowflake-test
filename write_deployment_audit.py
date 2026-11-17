#!/usr/bin/env python3
"""Write one GitHub/Schemachange deployment-run audit record to Snowflake."""
from __future__ import annotations

import importlib.metadata
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any

AUDIT_TABLE = "NAS_ADMIN.SCHEMACHANGE.DEPLOYMENT_AUDIT"
GITHUB_API = "https://api.github.com"
GITHUB_API_VERSION = "2026-03-10"


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is missing.")
    return value


def github_get(path: str) -> Any:
    token = required_env("GITHUB_TOKEN")
    request = urllib.request.Request(
        f"{GITHUB_API}{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
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


def main() -> None:
    import snowflake.connector

    repository = required_env("GITHUB_REPOSITORY")
    run_id = required_env("GITHUB_RUN_ID")
    commit_sha = required_env("DEPLOY_COMMIT_SHA")
    environment = required_env("DEPLOY_ENVIRONMENT").upper()
    source_branch = os.getenv("DEPLOY_SOURCE_BRANCH", "")
    mode = os.getenv("DEPLOYMENT_MODE", "EXECUTE").upper()
    status = os.getenv("DEPLOYMENT_STATUS", "UNKNOWN").upper()
    if mode == "DRY_RUN" and status == "SUCCESS":
        status = "DRY_RUN"

    # Find PR(s) associated with the exact commit being deployed.
    encoded_sha = urllib.parse.quote(commit_sha, safe="")
    associated_prs = github_get(f"/repos/{repository}/commits/{encoded_sha}/pulls")
    if not isinstance(associated_prs, list):
        raise RuntimeError("Unexpected response from the commit-to-PR GitHub API.")

    # Prefer the PR whose merge commit matches this SHA; otherwise use the most
    # recently merged associated PR. Preserve all associated PRs in PR_METADATA.
    primary_pr = next(
        (pr for pr in associated_prs if pr.get("merge_commit_sha") == commit_sha),
        None,
    )
    if primary_pr is None and associated_prs:
        primary_pr = sorted(
            associated_prs, key=lambda item: item.get("merged_at") or "", reverse=True
        )[0]

    approved_reviews: list[dict[str, Any]] = []
    for pr in associated_prs:
        number = pr.get("number")
        if not number:
            continue
        # Use each reviewer's latest submitted review state, so an older
        # APPROVED review does not remain counted after a later REQUEST_CHANGES.
        latest_by_reviewer: dict[str, dict[str, Any]] = {}
        for review in list_pr_reviews(repository, int(number)):
            reviewer = review.get("user") or {}
            login = reviewer.get("login")
            if not login:
                continue
            prior = latest_by_reviewer.get(login)
            if prior is None or (review.get("submitted_at") or "") >= (prior.get("submitted_at") or ""):
                latest_by_reviewer[login] = review
        for login, review in latest_by_reviewer.items():
            if str(review.get("state", "")).upper() == "APPROVED":
                approved_reviews.append({
                    "pull_request": int(number),
                    "reviewer": login,
                    "submitted_at": review.get("submitted_at"),
                    "body": review.get("body"),
                    "commit_id": review.get("commit_id"),
                    "url": review.get("html_url"),
                })

    # Workflow-run approval history includes the reviewer, state, comment and
    # environment. This endpoint does not return the exact reviewer-click time.
    approvals = github_get(f"/repos/{repository}/actions/runs/{run_id}/approvals")
    if not isinstance(approvals, list):
        raise RuntimeError("Unexpected response from the workflow-run approvals API.")

    approval_retrieved_at = datetime.now(timezone.utc).isoformat()
    pr_approved_by = ", ".join(sorted({
        item["reviewer"] for item in approved_reviews if item.get("reviewer")
    }))
    environment_approved_by = ", ".join(sorted({
        (item.get("user") or {}).get("login")
        for item in approvals
        if str(item.get("state", "")).lower() == "approved"
        and (item.get("user") or {}).get("login")
    }))
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

    connection = snowflake.connector.connect(
        account=required_env("SNOWFLAKE_ACCOUNT"),
        user=required_env("SNOWFLAKE_USER"),
        role=required_env("SNOWFLAKE_ROLE"),
        warehouse=required_env("SNOWFLAKE_WAREHOUSE"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=required_env("SNOWFLAKE_PRIVATE_KEY_FILE"),
        session_parameters={"QUERY_TAG": f"NAS_DEPLOYMENT_AUDIT|{environment}|{run_id}"},
    )

    deployment_id = str(uuid.uuid4())

    # Use INSERT ... SELECT rather than INSERT ... VALUES. Snowflake rejects
    # PARSE_JSON() expressions in this VALUES form; SELECT permits the VARIANT
    # conversions while keeping all supplied data safely parameter-bound.
    sql = f"""
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

    params = (
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
            cursor.execute(sql, params)
        connection.commit()
    finally:
        connection.close()

    print(
        "Deployment audit recorded: "
        f"deployment_id={deployment_id}, environment={environment}, status={status}, "
        f"run_id={run_id}, associated_prs={len(associated_prs)}, "
        f"approved_pr_reviews={len(approved_reviews)}, "
        f"environment_approval_records={len(approvals)}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"::error::Unable to write Snowflake deployment audit: {exc}", file=sys.stderr)
        raise
