#!/usr/bin/env bash
set -euo pipefail

source tools/opte_version_override
if [[ "x$OPTE_COMMIT" != "x" ]]; then
    exit 0
fi

# Grab all the oxidecomputer/opte dependencies' revisions
readarray -t opte_deps_revs < <(toml get Cargo.toml workspace.dependencies | jq -r 'to_entries | .[] | select(.value.git? | contains("oxidecomputer/opte")?) | .value.rev')
OPTE_REV="${opte_deps_revs[0]}"

# Make sure all the git dependencies are pinned to the same revision
for rev in "${opte_deps_revs[@]}"; do
    if [ "$rev" != "$OPTE_REV" ]; then
        echo "opte dependencies are pinned to different revisions"
        toml get Cargo.toml workspace.dependencies | jq -r 'to_entries | .[] | select(.value.git? | contains("oxidecomputer/opte")?) | .key + ": " + .value.rev'
        exit 1
    fi
done

# Grab the API version for this revision
API_VER=$(curl -s https://raw.githubusercontent.com/oxidecomputer/opte/"$OPTE_REV"/crates/opte-api/src/lib.rs | sed -n 's/pub const API_VERSION: u64 = \([0-9]*\);/\1/p')

# Grab the patch version which is based on the number of commits.
# Essentially `git rev-list --count $OPTE_REV` but without cloning the repo.
# We use the GitHub API endpoint for listing commits starting at `$OPTE_REV`.
# That request returns a "Link" header that looks something like this:
#
# link: <https://api.github.com/repositories/394728713/commits?per_page=1&sha=$OPTE_REV&page=2>; rel="next", <https://api.github.com/repositories/394728713/commits?per_page=1&sha=$OPTE_REV&page=162>; rel="last"
#
# Since the API is paginated we can ask for just one commit per page and
# use the total number of pages to get the total number of commits.
# Thus the query parameter `page` in the "last" link (e.g. `&page=162`)
# gives us the rev count we want.
#
# We should be resilient against a transient GitHub issue here. If GitHub gives
# any answer, we'll carry on trying to get a page number out, but we really
# shouldn't if we get a non-200 error. We could `curl -f` to fail for a 4xx or
# above status, but then the observed output when this fails would be an opaque
# "exited with status 22" or something. Help ourselves out and keep the response
# head, printing that if something goes sideways instead.
COMMIT_INFO_HEAD="$(curl -I -s "https://api.github.com/repos/oxidecomputer/opte/commits?per_page=1&sha=$OPTE_REV")"
REV_COUNT=$(echo "$COMMIT_INFO_HEAD" | sed -n '/^[Ll]ink:/ s/.*"next".*page=\([0-9]*\).*"last".*/\1/p')

if [ -z "$REV_COUNT" ]; then
    # We didn't get a OPTE rev out. The rev *should* exist, so it should not be
    # a 404. If this is a 5xx, it might be a transient error from GitHub. In
    # either case, errors are infrequent enough lets just dump the response head
    # and hope someone can do something contextually appropriate. And hope this
    # doesn't become regular enough anyone wants to be smarter.
    echo "Could not get rev count from GitHub response. Response headers:"
    echo "$COMMIT_INFO_HEAD"
    exit 1
fi

# Combine the API version and the revision count to get the full version
OPTE_VER="0.$API_VER.$REV_COUNT"

# Check that the version matches the one in `tools/opte_version`
OPTE_VER_OMICRON=$(cat tools/opte_version)
if [ "$OPTE_VER" != "$OPTE_VER_OMICRON" ]; then
    echo "OPTE version mismatch:"
    echo "Cargo.toml: $OPTE_REV ($OPTE_VER)"
    echo "tools/opte_version: $OPTE_VER_OMICRON"
    exit 1
fi

# Also check that the buildomat deploy job is using the same version
BUILDOMAT_DEPLOY_TARGET=$(cat .github/buildomat/jobs/deploy.sh | sed -n 's/#:[ ]*target[ ]*=[ ]*"\(.*\)"/\1/p')
if [ "lab-2.0-opte-0.$API_VER" != "$BUILDOMAT_DEPLOY_TARGET" ]; then
    echo "OPTE version mismatch:"
    echo "Cargo.toml: $OPTE_REV ($OPTE_VER)"
    echo "buildomat deploy job: $BUILDOMAT_DEPLOY_TARGET (expected lab-opte-0.$API_VER)"
    exit 1
fi
