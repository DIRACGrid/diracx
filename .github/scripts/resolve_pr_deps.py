#!/usr/bin/env python3
"""Resolve the cross-repository dependencies declared by a pull request.

A pull request can declare that it needs a pull request in another repository
by adding lines like these to its description:

    Depends-On: https://github.com/DIRACGrid/diracx-charts/pull/123
    Depends-On: DIRACGrid/DIRAC#8456

The description is fetched from the API rather than taken from the event
payload so that re-running a workflow picks up edits to it.

Usage:
    resolve_pr_deps.py resolve --repo OWNER/REPO --pr NUMBER
        Write the git ref to test against for each supported repository to
        $GITHUB_OUTPUT. Outputs are left unset when there is no dependency.
    resolve_pr_deps.py check-merged --repo OWNER/REPO --pr NUMBER
        Fail if any dependency has not been merged yet.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from typing import NoReturn

API_URL = "https://api.github.com"
DEPENDS_ON_RE = re.compile(
    r"^Depends-On:\s*(?:https://github\.com/)?([\w.-]+/[\w.-]+)(?:/pull/|#)(\d+)/?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
# Repositories which the CI knows how to test against, mapped to the output name
SUPPORTED_REPOS = {
    "diracgrid/diracx-charts": "charts-ref",
    "diracgrid/dirac": "dirac-ref",
}


def api_get(path: str) -> dict:
    """GET a GitHub API endpoint, failing the job on HTTP errors."""
    request = urllib.request.Request(  # noqa: S310
        f"{API_URL}/{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    if token := os.environ.get("GH_TOKEN"):
        request.add_header("Authorization", f"Bearer {token}")
    try:
        # API_URL is always https so there is no risk of opening other schemes
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            return json.load(response)
    except urllib.error.HTTPError as e:
        fail(f"Failed to get {API_URL}/{path}: {e}")


def get_pull_request(repo: str, number: int) -> dict:
    """Fetch a pull request, waiting for GitHub to compute its mergeability.

    Requesting the pull request is also what makes GitHub refresh the
    refs/pull/N/merge ref, so it must happen before that ref is fetched.
    """
    for _ in range(10):
        pr = api_get(f"repos/{repo}/pulls/{number}")
        if pr["state"] != "open" or pr["mergeable"] is not None:
            return pr
        time.sleep(3)
    fail(f"GitHub did not compute the mergeability of {repo}#{number} in time")


def find_dependencies(repo: str, number: int) -> list[tuple[str, int]]:
    """Return the (repo, number) of each Depends-On line in a pull request's description."""
    body = api_get(f"repos/{repo}/pulls/{number}")["body"] or ""
    return [
        (dep_repo, int(dep_number))
        for dep_repo, dep_number in DEPENDS_ON_RE.findall(body)
    ]


def fail(message: str) -> NoReturn:
    """Report an error annotation and exit."""
    print(f"::error::{message}")
    sys.exit(1)


def write_file(env_var: str, lines: list[str]):
    """Append lines to the file named by a GitHub Actions environment variable, if set."""
    if path := os.environ.get(env_var):
        with open(path, "a") as fh:
            fh.writelines(f"{line}\n" for line in lines)


def resolve(dependencies: list[tuple[str, int]]):
    """Write the git ref to test against for each supported dependency."""
    outputs: dict[str, str] = {}
    summary = []
    for repo, number in dependencies:
        key = SUPPORTED_REPOS.get(repo.lower())
        if key is None:
            print(f"::warning::CI cannot test against {repo}, ignoring {repo}#{number}")
            continue
        if key in outputs:
            fail(f"Only one Depends-On is supported for {repo}")

        pr = get_pull_request(repo, number)
        if pr["merged"]:
            ref = pr["base"]["ref"]
            state = f"merged, testing against `{ref}`"
        elif pr["state"] == "closed":
            fail(f"{repo}#{number} was closed without being merged")
        elif not pr["mergeable"]:
            fail(f"{repo}#{number} has conflicts with {pr['base']['ref']}")
        else:
            ref = f"refs/pull/{number}/merge"
            state = f"open, testing against its merge into `{pr['base']['ref']}`"

        print(f"{repo}#{number}: {state}")
        outputs[key] = ref
        summary.append(f"- [{repo}#{number}]({pr['html_url']}): {state}")

    write_file("GITHUB_OUTPUT", [f"{key}={ref}" for key, ref in outputs.items()])
    if summary:
        write_file(
            "GITHUB_STEP_SUMMARY", ["### Pull request dependencies", "", *summary]
        )


def check_merged(dependencies: list[tuple[str, int]]):
    """Fail unless every dependency has been merged."""
    unmerged = []
    for repo, number in dependencies:
        pr = api_get(f"repos/{repo}/pulls/{number}")
        if pr["merged"]:
            print(f"{repo}#{number} is merged")
        elif pr["state"] == "closed":
            unmerged.append(
                f"{repo}#{number} was closed without being merged, remove the Depends-On line"
            )
        else:
            unmerged.append(f"{repo}#{number} must be merged first")
    if unmerged:
        fail("; ".join(unmerged))


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("action", choices=["resolve", "check-merged"])
    parser.add_argument("--repo", required=True)
    parser.add_argument(
        "--pr",
        default="",
        help="Pull request number, empty when not running for a pull request",
    )
    args = parser.parse_args()

    if not args.pr:
        print("Not a pull request, nothing to resolve")
        return

    dependencies = find_dependencies(args.repo, int(args.pr))
    if not dependencies:
        print("No Depends-On lines found")
    elif args.action == "resolve":
        resolve(dependencies)
    else:
        check_merged(dependencies)


if __name__ == "__main__":
    main()
