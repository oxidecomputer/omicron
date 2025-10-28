#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# We use Trunk (https://trunk.io) to track flaky tests in our suite. Trunk uses a custom CLI to
# upload JUnit XML files, but unfortunately we cannot run it in Buildomat, as it requires a secret.
#
# To work around that, we use this script to grab the JUnit XML files from all finished Buildomat
# jobs (as part of a GitHub Check Suite) and upload it. The script requires the GitHub Check Suite
# ID of the Buildomat job, and takes care of retrieving the rest of the metadata from the API.
#
# While the script is meant to be executed in a GitHub Actions workflow triggering after the
# Buildomat job (thanks to the check_suite event), it is possible to run the script locally (with a
# valid GITHUB_TOKEN, TRUNK_ORG_SLUG and TRUNK_TOKEN).

set -euo pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <check_suite_id>" >&2
    exit 1
fi
check_suite_id="$1"

log_step() {
    echo >&2
    echo "===> $*" >&2
}

tmp="$(mktemp -d)"
trap "rm -rf ${tmp}" EXIT
cd "${tmp}"

mkdir api/
mkdir git/

gh api "repos/${GITHUB_REPOSITORY}/check-suites/${check_suite_id}" > api/check_suite
branch="$(jq -r .head_branch api/check_suite)"
commit="$(jq -r .head_sha api/check_suite)"
github_app="$(jq -r .app.slug api/check_suite)"

gh api "repos/${GITHUB_REPOSITORY}/check-suites/${check_suite_id}/check-runs" > api/check_suite_runs

gh api "repos/${GITHUB_REPOSITORY}/commits/${commit}" > api/commit
author_email="$(jq -r .commit.author.email api/commit)"
author_name="$(jq -r .commit.author.name api/commit)"
commit_message="$(jq -r .commit.message api/commit)"

# It is possible for a check run to return more than one PR. This is because you cannot actually
# attach PRs to a check run, the API just returns all PRs with the matching head ref/sha. In the end
# we don't care about which one we pick too much, since this is only reported in the Trunk UI. The
# load bearing thing is whether *at least one* PR is present.
#
# This will evaluate to the raw string `null` if no PR is present, instead of an URL.
pr_url="$(jq -r .pull_requests[0].url api/check_suite)"

# Determine whether this comes from a PR or not. It's load bearing that we detect things correctly,
# since Trunk treats PRs vs main branch commits differently when analyzing flaky tests.
if [[ "${pr_url}" != "null" ]]; then
    gh api "${pr_url}" > api/pr

    is_pr=true
    pr_number="$(jq -r .number  api/pr)"
    pr_title="$(jq -r .title  api/pr)"
elif [[ "${branch}" == "null" ]]; then
    # Unfortunately, the GitHub API doesn't seem to properly detect PRs from forks outside the
    # organization. In those cases, the response doesn't include the PR information (failing the
    # `if` above) nor the branch name (resulting in a branch name of `null`), and we can assume this
    # is a PR from an outside fork.
    branch="__unknown_pr__"

    is_pr=true
    pr_number="99999999"
    pr_title="Could not determine the PR"
else
    is_pr=false
fi

# Trunk detects which commit the JUnit XML is related to by looking at the HEAD of the current git
# repository. We don't care about the contents of it though, so do a shallow partial clone.
log_step "cloning commit ${commit} of ${GITHUB_REPOSITORY}..."
git clone --filter=tree:0 --depth=1 "https://github.com/${GITHUB_REPOSITORY}" git/
git -C git/ fetch --depth=1 origin "${commit}"
git -C git/ checkout "${commit}"

log_step "downloading the trunk CLI..."
curl -fLO --retry 3 https://trunk.io/releases/trunk
sed -i "s%^#!/bin/bash$%#!/usr/bin/env bash%" trunk  # NixOS fix
chmod +x trunk

for check_run_id in $(jq -r .check_runs[].id api/check_suite_runs); do
    log_step "retrieving details of check run ${check_run_id}..."

    gh api "repos/${GITHUB_REPOSITORY}/check-runs/${check_run_id}" > api/check_run
    job_url="$(jq -r .details_url api/check_run)"
    job_name="$(jq -r .name api/check_run)"

    # We need to somehow download the JUnit XML from the job artifacts, and there are no good
    # solutions available in Buildomat to do so:
    #
    # - Buildomat supports publishing artifacts at pre-defined URLs with its [[publish]] config
    #   block, and that's what this script originally used. The problem is, if jobs run multiple
    #   times for the same commit (for example in case of a retry) Buildomat will only publish the
    #   artifact of the first job, not for any followup jobs.
    #
    # - Buildomat has a JSON API that does expose the job outputs, but it's behind authentication
    #   and this workflow cannot access it. It would also require everyone tweaking the script to
    #   obtain credentials before running the script locally.
    #
    # - Buildomat provides an unauthenticated HTML page with all the job details, including download
    #   links for the job outpouts. It requires scraping the HTML to figure out the correct link.
    #
    # Out of the three options, the second would be ideal, but cannot implemented now. The first
    # option was implemented previously, but to detect flaky tests Trunk needs the correct JUnit
    # files when a job is retried. This only leaves us with the third option, which while not ideal
    # seems to work fine for now. Sorry.
    log_step "detecting the presence of a JUnit report in job ${job_name}"
    found_link=""
    # curl flags:
    # -f: exit 1 if the response is a 4xx or a 5xx
    # -L: follow redirects
    #
    # grep flags:
    # -h: don't output file names
    # -o: only print what matches, not the whole line
    # -P: use perl regexes (needed for \K)
    #
    # Regex details:
    # - We first match `href="`, to only get links as part of an <a> tag.
    # - The \K then instructs grep to only print matching text *after* the \K.
    # - We finally include everything that looks like a link until a `"` is found.
    for link in $(curl -fL --retry 3 "${job_url}" | grep -hoP 'href="\Khttps://[^"]+'); do
        case "${link}" in
            # Look for Buildomat links for an artefact called "junit.xml".
            https://buildomat.eng.oxide.computer/wg/0/artefact/*/junit.xml)
                if [[ "${found_link}" == "${link}" ]]; then
                    echo "note: found the same JUnit link multiple times, only using the first" >&2
                elif [[ "${found_link}" == "" ]]; then
                    echo "success: found ${link}" >&2
                    rm -f git/junit.xml
                    curl -fL -o git/junit.xml --retry 3 "${link}"
                    found_link="${link}"
                else
                    # It could be possible to support multiple JUnit files, but we don't need that
                    # so far. If you encounter this error feel free to add support for it.
                    echo "error: found more than one JUnit reports:" >&2
                    echo "- ${found_link}" >&2
                    echo "- ${link}" >&2
                    exit 1
                fi
                ;;
            *)
                echo "note: ignoring non-JUnit URL ${link}" >&2
                ;;
        esac
    done
    if [[ "${found_link}" == "" ]]; then
        echo "skipping job ${job_name}, as it doesn't contain any JUnit report"
        continue
    fi

    # Configure the environment to override Trunk's CI detection (with CUSTOM=true) and to provide all
    # the relevant information about the CI run. Otherwise Trunk will either pick up nothing (when
    # running the script locally) or it will pick up the details of the GHA workflow running this.
    vars=()
    vars+=("CUSTOM=true")
    vars+=("JOB_URL=${job_url}")
    vars+=("JOB_NAME=${job_name}")
    vars+=("AUTHOR_EMAIL=${author_email}")
    vars+=("AUTHOR_NAME=${author_name}")
    vars+=("COMMIT_BRANCH=${branch}")
    vars+=("COMMIT_MESSAGE=${commit_message}")
    if [[ "${is_pr}" == true ]]; then
        vars+=("PR_NUMBER=${pr_number}")
        vars+=("PR_TITLE=${pr_title}")
    fi

    echo >&2
    echo "these environmnent variables will be set:" >&2
    for var in "${vars[@]}"; do
        echo "${var}" >&2
    done

    # Uploading to Trunk has to happen inside of the git repository at the current commit.
    if [[ -z "${DRY_RUN+x}" ]]; then
        log_step "uploading the JUnit XML report of check run ${check_run_id} to Trunk..."
        cd git/
        env "${vars[@]}" \
            ../trunk flakytests upload \
            --junit-paths junit.xml \
            --variant "${variant}" \
            --org-url-slug "${TRUNK_ORG_SLUG}" \
            --token "${TRUNK_TOKEN}"
        cd ..
    else
        log_step "skipped upload to Trunk, we are in a dry run"
    fi
done
