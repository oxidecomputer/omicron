#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# We use Trunk (https://trunk.io) to track flaky tests in our suite. Trunk uses a custom CLI to
# upload JUnit XML files, but unfortunately we cannot run it in Buildomat, as it requires a secret.
#
# To work around that, we use this script to grab the JUnit XML file from a finished Buildomat job
# and upload it. The script requires the GitHub Check Run ID of the Buildomat job, and takes care of
# retrieving the rest of the metadata from the GitHub API.
#
# While the script is meant to be executed in a GitHub Actions workflow triggering after the
# Buildomat job (thanks to the check_run event), it is possible to run the script locally (with a
# valid GITHUB_TOKEN, TRUNK_ORG_SLUG and TRUNK_TOKEN).

set -euo pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <check_run_id>" >&2
    exit 1
fi
check_run_id="$1"

tmp="$(mktemp -d)"
trap "rm -rf ${tmp}" EXIT
cd "${tmp}"

mkdir api/

gh api "repos/${GITHUB_REPOSITORY}/check-runs/${check_run_id}" > api/check_run
check_suite_id="$(jq -r .check_suite.id api/check_run)"
commit="$(jq -r .head_sha api/check_run)"
github_app="$(jq -r .app.slug api/check_run)"
job_url="$(jq -r .details_url api/check_run)"

# It is possible for a check run to return more than one PR. This is because you cannot actually
# attach PRs to a check run, the API just returns all PRs with the matching head ref/sha. In the end
# we don't care about which one we pick too much, since this is only reported in the Trunk UI. The
# load bearing thing is whether *at least one* PR is present.
#
# This will evaluate to the raw string `null` if no PR is present, instead of an URL.
pr_url="$(jq -r .pull_requests[0].url api/check_run)"

gh api "repos/${GITHUB_REPOSITORY}/commits/${commit}" > api/commit
author_email="$(jq -r .commit.author.email api/commit)"
author_name="$(jq -r .commit.author.name api/commit)"
commit_message="$(jq -r .commit.message api/commit)"

gh api "repos/${GITHUB_REPOSITORY}/check-suites/${check_suite_id}" > api/check_suite
branch="$(jq -r .head_branch api/check_suite)"

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

# Figure out where the JUnit XML report lives and the Trunk variant based on the job name. If new
# jobs are added or renamed, you will need to change this too.
#
# The Trunk variant allows to differentiate flaky tests by platform, and should be the OS name.
job_name="$(jq -r .name api/check_run)"
case "${github_app} / ${job_name}" in
    "buildomat / build-and-test (helios)")
        artifact_series="junit-helios"
        artifact_name="junit.xml"
        variant="helios"
        ;;
    "buildomat / build-and-test (ubuntu-22.04)")
        artifact_series="junit-linux"
        artifact_name="junit.xml"
        variant="ubuntu"
        ;;
    *)
        echo "unsupported job name, skipping upload: ${job_name}"
        exit 0
        ;;
esac

# Configure the environment to override Trunk's CI detection (with CUSTOM=true) and to provide all
# the relevant information about the CI run. Otherwise Trunk will either pick up nothing (when
# running the script locally) or it will pick up the details of the GHA workflow running this.
function set_var {
    echo "setting $1 to $2"
    export "$1=$2"
}
set_var CUSTOM true
set_var JOB_URL "${job_url}"
set_var JOB_NAME "${job_name}"
set_var AUTHOR_EMAIL "${author_email}"
set_var AUTHOR_NAME "${author_name}"
set_var COMMIT_BRANCH "${branch}"
set_var COMMIT_MESSAGE "${commit_message}"
if [[ "${is_pr}" == true ]]; then
    set_var PR_NUMBER "${pr_number}"
    set_var PR_TITLE "${pr_title}"
fi

mkdir upload
cd upload

# Trunk detects which commit the JUnit XML is related to by looking at the HEAD of the current git
# repository. We don't care about the contents of it though, so do a shallow partial clone.
echo >&2
echo "===> cloning commit ${commit} of ${GITHUB_REPOSITORY}..." >&2
git clone --filter=tree:0 --depth=1 "https://github.com/${GITHUB_REPOSITORY}" .
git fetch --depth=1 origin "${commit}"
git checkout "${commit}"

echo >&2
echo "===> downloading the trunk CLI..." >&2
curl -fLO --retry 3 https://trunk.io/releases/trunk
sed -i "s%^#!/bin/bash$%#!/usr/bin/env bash%" trunk  # NixOS fix
chmod +x trunk

# The URL is configured through a [[publish]] block in the Buildomat job configuration.
echo >&2
echo "===> downloading the JUnit XML report..." >&2
junit_url="https://buildomat.eng.oxide.computer/public/file/${GITHUB_REPOSITORY}/${artifact_series}/${commit}/${artifact_name}"
curl -fL -o junit.xml --retry 3 "${junit_url}"

echo >&2
echo "===> uploading the JUnit XML report to trunk..." >&2
./trunk flakytests upload \
    --junit-paths junit.xml \
    --variant "${variant}" \
    --org-url-slug "${TRUNK_ORG_SLUG}" \
    --token "${TRUNK_TOKEN}"
