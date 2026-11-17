#!/usr/bin/env python3
"""
Write one GitHub Actions deployment-run audit record to Snowflake.

Purpose
-------
This script is called by the GitHub Actions workflows after a Schemachange
deployment attempt. It collects deployment and approval metadata from GitHub,
then writes a single audit row to:

    NAS_ADMIN.SCHEMACHANGE.DEPLOYMENT_AUDIT

The Schemachange CHANGE_HISTORY table remains the source of truth for
individual migration scripts. This table provides the additional GitHub
governance context: PR information, reviewers, environment approvals,
commit details and workflow execution details.

Required GitHub Actions environment variables
----------------------------------------------
GITHUB_TOKEN              Built-in GitHub Actions token. The workflow must
                          grant contents: read, pull-requests: read and
                          actions: read permissions.
GITHUB_REPOSITORY         Repository in owner/repository format.
GITHUB_RUN_ID             Unique workflow run ID.
GITHUB_RUN_NUMBER         Human-readable workflow run number (optional).
GITHUB_WORKFLOW           Workflow name.
GITHUB_ACTOR              User or actor that triggered the workflow.
GITHUB_SERVER_URL         GitHub server URL; normally https://github.com.

Deployment metadata variables
-----------------------------
DEPLOY_COMMIT_SHA         Exact commit SHA being deployed.
DEPLOY_ENVIRONMENT        DEV, UAT or PROD.
DEPLOY_SOURCE_BRANCH      Branch from which the workflow was triggered.
DEPLOYMENT_MODE           EXECUTE or DRY_RUN.
DEPLOYMENT_STATUS         Workflow job status supplied by the workflow.

Snowflake connection variables
------------------------------
SNOWFLAKE_ACCOUNT         Snowflake account identifier.
SNOWFLAKE_USER            Environment-specific deployment service user.
SNOWFLAKE_ROLE            Environment-specific deployment role.
SNOWFLAKE_WAREHOUSE       Environment-specific deployment warehouse.
SNOWFLAKE_PRIVATE_KEY_FILE
                          Path to the temporary PKCS#8 private-key file.

Security notes
--------------
- The private key is never read into this script as a string. The Snowflake
  connector reads it from the protected temporary file.
- GitHub API calls use the short-lived GITHUB_TOKEN supplied by Actions.
- SQL values are passed as connector bind parameters. Do not interpolate
  GitHub-provided strings into the SQL statement.
- AUDIT_TABLE is a fixed, fully qualified object name controlled by this code.
"""

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


# Fixed Snowflake destination. Keep this fully qualified so the connection
# does not depend on a session-level current database or schema.
AUDIT_TABLE = "NAS_ADMIN.SCHEMACHANGE.DEPLOYMENT_AUDIT"

# GitHub REST API base URL and version header used for all metadata requests.
GITHUB_API = "https://api.github.com"
GITHUB_API_VERSION = "2026-03-10"


def required_env(name: str) -> str:
    """
    Read a required environment variable and fail with a clear message if it
    is missing or empty. This avoids attempting API or Snowflake operations
    with incomplete connection or run metadata.
    """
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is missing.")
    return value


def github_get(path: str) -> Any:
    """
    Send an authenticated GET request to the GitHub REST API.

    The token is provided by GitHub Actions as GITHUB_TOKEN; it is not stored
    in the repository. HTTP and network errors are raised with the endpoint
    path and a bounded response excerpt to help diagnose permission issues.
    """
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
        # A finite timeout prevents the audit step from hanging indefinitely.
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    except urllib.error.HTTPError as exc:
        # Include the HTTP status and a short body excerpt. A 403 commonly
        # indicates missing workflow permissions; a 404 can mean the token
        # cannot see the requested repository resource.
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"GitHub API request failed ({exc.code}) for {path}: {body[:1000]}"
        ) from exc

    except urllib.error.URLError as exc:
        # Network/DNS/TLS errors are different from GitHub API permission errors.
        raise RuntimeError(f"GitHub API request failed for {path}: {exc}") from exc


def list_pr_reviews(repository: str, pull_number: int) -> list[dict[str, Any]]:
    """
    Retrieve all submitted review records for one pull request.

    The GitHub REST API paginates this endpoint. Fetch 100 records per request
    and continue until a shorter page is returned so older reviews are not
    silently omitted.
    """
    reviews: list[dict[str, Any]] = []
    page = 1

    while True:
        query = urllib.parse.urlencode({"per_page": 100, "page": page})
        batch = github_get(
            f"/repos/{repository}/pulls/{pull_number}/reviews?{query}"
        )

        if not isinstance(batch, list):
            raise RuntimeError(
                f"Unexpected PR reviews response for PR #{pull_number}."
            )

        reviews.extend(batch)

        # A page with fewer than 100 entries is the final page.
        if len(batch) < 100:
            return reviews

        page += 1


def compact_pr(pr: dict[str, Any]) -> dict[str, Any]:
    """
    Select the PR fields needed for the audit table.

    GitHub's full PR response is large. This compact representation keeps the
    audit-relevant identifiers and branch/merge information without storing
    the complete API response.
    """
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
    """Collect GitHub metadata and insert one deployment audit row in Snowflake."""
    # Import inside main so syntax checks can run without the Snowflake package
    # being installed locally. The package is installed in the GitHub runner.
    import snowflake.connector

    # ---------------------------------------------------------------------
    # 1. Read the workflow run and target-environment metadata.
    # ---------------------------------------------------------------------
    repository = required_env("GITHUB_REPOSITORY")
    run_id = required_env("GITHUB_RUN_ID")
    commit_sha = required_env("DEPLOY_COMMIT_SHA")
    environment = required_env("DEPLOY_ENVIRONMENT").upper()

    source_branch = os.getenv("DEPLOY_SOURCE_BRANCH", "")
    mode = os.getenv("DEPLOYMENT_MODE", "EXECUTE").upper()
    status = os.getenv("DEPLOYMENT_STATUS", "UNKNOWN").upper()

    # When a dry-run completes successfully, record the business status as
    # DRY_RUN rather than implying that SQL migrations were executed.
    if mode == "DRY_RUN" and status == "SUCCESS":
        status = "DRY_RUN"

    # ---------------------------------------------------------------------
    # 2. Find pull requests associated with the exact commit being deployed.
    # ---------------------------------------------------------------------
    # The commit-to-PR endpoint can return more than one PR (for example,
    # when a commit is associated with a branch PR and a later promotion PR).
    # Preserve all returned PRs in PR_METADATA for traceability.
    encoded_sha = urllib.parse.quote(commit_sha, safe="")
    associated_prs = github_get(
        f"/repos/{repository}/commits/{encoded_sha}/pulls"
    )

    if not isinstance(associated_prs, list):
        raise RuntimeError(
            "Unexpected response from the commit-to-PR GitHub API."
        )

    # Select a primary PR for the scalar PR_NUMBER/PR_TITLE/PR_URL columns.
    # First prefer an exact merge-commit SHA match. If GitHub does not return
    # that match (which can happen with squash/rebase or branch promotion),
    # fall back to the most recently merged associated PR. All PRs remain in
    # the PR_METADATA VARIANT column, so the fallback does not discard them.
    primary_pr = next(
        (pr for pr in associated_prs if pr.get("merge_commit_sha") == commit_sha),
        None,
    )

    if primary_pr is None and associated_prs:
        primary_pr = sorted(
            associated_prs,
            key=lambda item: item.get("merged_at") or "",
            reverse=True,
        )[0]

    # ---------------------------------------------------------------------
    # 3. Retrieve PR review history and identify each reviewer's latest state.
    # ---------------------------------------------------------------------
    approved_reviews: list[dict[str, Any]] = []

    for pr in associated_prs:
        number = pr.get("number")
        if not number:
            continue

        # A reviewer can submit multiple reviews on the same PR. Only use
        # their latest submitted review state; otherwise an old APPROVED
        # review could be incorrectly counted after a later REQUEST_CHANGES.
        latest_by_reviewer: dict[str, dict[str, Any]] = {}

        for review in list_pr_reviews(repository, int(number)):
            reviewer = review.get("user") or {}
            login = reviewer.get("login")

            if not login:
                continue

            prior = latest_by_reviewer.get(login)

            if (
                prior is None
                or (review.get("submitted_at") or "")
                >= (prior.get("submitted_at") or "")
            ):
                latest_by_reviewer[login] = review

        # Keep only reviewers whose latest review state is APPROVED.
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

    # ---------------------------------------------------------------------
    # 4. Retrieve GitHub Environment protection-rule approval information.
    # ---------------------------------------------------------------------
    # This is separate from PR code reviews. Environment approvals are the
    # required reviewer decisions configured under GitHub Settings ->
    # Environments (for example, UAT and PROD deployment approvals).
    #
    # The API response includes approval state, reviewer, comment and
    # environment where available. It does not provide the exact time the
    # reviewer clicked Approve, so APPROVALS_RETRIEVED_AT below is explicitly
    # the time this script retrieved the approval history.
    approvals = github_get(
        f"/repos/{repository}/actions/runs/{run_id}/approvals"
    )

    if not isinstance(approvals, list):
        raise RuntimeError(
            "Unexpected response from the workflow-run approvals API."
        )

    approval_retrieved_at = datetime.now(timezone.utc).isoformat()

    # Create readable comma-separated reviewer summaries as well as retaining
    # the full structured approval/review details in VARIANT columns.
    pr_approved_by = ", ".join(
        sorted(
            {
                item["reviewer"]
                for item in approved_reviews
                if item.get("reviewer")
            }
        )
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

    # Convert GitHub API objects into JSON strings for insertion into Snowflake
    # VARIANT columns using PARSE_JSON() in the INSERT...SELECT statement.
    pr_metadata = [compact_pr(pr) for pr in associated_prs]
    primary = compact_pr(primary_pr) if primary_pr else {}

    # ---------------------------------------------------------------------
    # 5. Prepare workflow run details and the installed Schemachange version.
    # ---------------------------------------------------------------------
    run_number = os.getenv("GITHUB_RUN_NUMBER")

    run_url = (
        f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/"
        f"{repository}/actions/runs/{run_id}"
    )

    try:
        schemachange_version = importlib.metadata.version("schemachange")
    except importlib.metadata.PackageNotFoundError:
        # This may be None if the script is run outside the deployment runner.
        schemachange_version = None

    # ---------------------------------------------------------------------
    # 6. Connect to Snowflake using the same environment-specific key-pair
    #    credentials used by the Schemachange deployment.
    # ---------------------------------------------------------------------
    # The role must have USAGE on NAS_ADMIN database/schema and INSERT on
    # NAS_ADMIN.SCHEMACHANGE.DEPLOYMENT_AUDIT.
    connection = snowflake.connector.connect(
        account=required_env("SNOWFLAKE_ACCOUNT"),
        user=required_env("SNOWFLAKE_USER"),
        role=required_env("SNOWFLAKE_ROLE"),
        warehouse=required_env("SNOWFLAKE_WAREHOUSE"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=required_env("SNOWFLAKE_PRIVATE_KEY_FILE"),
        session_parameters={
            "QUERY_TAG": f"NAS_DEPLOYMENT_AUDIT|{environment}|{run_id}"
        },
    )

    # Each workflow run receives a unique identifier. This makes it possible
    # to distinguish retries and multiple deployment attempts for one commit.
    deployment_id = str(uuid.uuid4())

    # ---------------------------------------------------------------------
    # 7. Insert one deployment audit record.
    # ---------------------------------------------------------------------
    # Use INSERT...SELECT instead of INSERT...VALUES. Snowflake does not accept
    # PARSE_JSON() expressions in the VALUES form used in the earlier version.
    # SELECT permits conversion of JSON strings into VARIANT values.
    #
    # All dynamic values remain connector bind parameters (%s). Only the
    # fixed, code-controlled table name is interpolated into the SQL text.
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

    # The order of this tuple must match the placeholder order in the SQL.
    # The three JSON strings map to PR_METADATA, PR_APPROVED_REVIEWS and
    # ENVIRONMENT_APPROVALS respectively.
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
        # Execute the parameterised insert and commit the audit row.
        with connection.cursor() as cursor:
            cursor.execute(sql, params)

        connection.commit()

    finally:
        # Always close the Snowflake connection, including on SQL errors.
        connection.close()

    # Print a concise, non-sensitive summary for the GitHub Actions log.
    print(
        "Deployment audit recorded: "
        f"deployment_id={deployment_id}, environment={environment}, "
        f"status={status}, run_id={run_id}, "
        f"associated_prs={len(associated_prs)}, "
        f"approved_pr_reviews={len(approved_reviews)}, "
        f"environment_approval_records={len(approvals)}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # GitHub Actions recognises ::error:: and highlights the message in logs.
        # Re-raise the exception so the audit step fails visibly; do not silently
        # report success if the Snowflake audit insert did not complete.
        print(
            f"::error::Unable to write Snowflake deployment audit: {exc}",
            file=sys.stderr,
        )
        raise
