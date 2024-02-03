#!/usr/bin/env bash

set -o pipefail
set -o errexit

# Configure the git cli to perform commits as Reflector Bot
function set_reflector_bot {
  local BOT_ID=$1

  # Setup commits to be made by reflector bot
  git config --local user.name "reflector[bot]"
  git config --local user.email "$BOT_ID+reflector[bot]@users.noreply.github.com"
}

# Attempt to merge a target branch in to an integration branch, ignoring conflicts on the the
# specified checkout paths. Merge will fail if there are additional conflicts left over after those
# paths are ignored.
function merge {
  local TARGET_BRANCH="$1"
  local INTEGRATION_BRANCH="$2"
  local BOT_ID="$3"
  local CHECKOUT_PATHS=$4

  set_reflector_bot "$BOT_ID"

  # Merge the target branch into the integration branch and detect conflicts
  local MERGE_STATUS=0

  # Checkout the integration branch or create if if it does not exist
  git checkout "$INTEGRATION_BRANCH" 2>/dev/null || git checkout -b "$INTEGRATION_BRANCH"
  git merge "$TARGET_BRANCH" || MERGE_STATUS=$?

  # If there was a merge conflict attempt to reset the generated files and commit them back
  if [ $MERGE_STATUS -eq 1 ]
  then
    echo "Found conflicts. Attempt to use changes from $TARGET_BRANCH"

    # For each of the requests paths, use the version on the target branch
    for path in "${CHECKOUT_PATHS[@]}"
    do
      git checkout "$TARGET_BRANCH" -- "$path"
    done

    git commit -m "Merge branch '$TARGET_BRANCH' into $INTEGRATION_BRANCH and reset generated code"
  fi

  # Determine if there are any outstanding conflicts, if so they will need to be manually fixed.
  local STATUS
  STATUS=$(git status --porcelain=v1 2>/dev/null | wc -l)

  if [ "$STATUS" -eq 0 ]
  then
    exit 0
  else
    echo 'Found additional conflicts from merge attempt that need to be manually resolved'
    git status
    exit 1
  fi
}

# Generate a commit from all of the outstanding changes and report back on `OUTPUT` the paths that
# have changed. The order of the values in `OUTPUT` corresponds to order of paths in `DIFF_PATHS`.
function commit {
  local TARGET_BRANCH="$1"
  local INTEGRATION_BRANCH="$2"
  local BOT_ID="$3"
  local -n DIFF_PATHS=$4
  local -n OUTPUT=$5

  set_reflector_bot "$BOT_ID"

  # Commit all changes back to the integration branch
  git add .
  git commit -m "Update with latest version" || echo "Nothing to commit"
  git push origin "$INTEGRATION_BRANCH"

  # Check if the API version has been updated
  for path in "${DIFF_PATHS[@]}"
  do
    CHANGED=0
    git diff "$TARGET_BRANCH...$INTEGRATION_BRANCH" --quiet "$path" || CHANGED=$?
    OUTPUT+=("$CHANGED")
  done
}

# Create a new PR from the integration branch to the target branch if one is needed. Otherwise
# either update or close the branch. These behaviors will trigger in the following cases:
# 1. Create PR if: integration is ahead of main in commits and no open PR exists
# 2. Update PR if: integration is ahead of main in commits and an open PR exists
# 3. Close PR if: integration is equal to main
function update_pr {
  local TARGET_BRANCH="$1"
  local INTEGRATION_BRANCH="$2"
  local TITLE="$3"
  local BODY_FILE="$4"

  # Compare the integration branch with the target branch
  local TARGET_TO_INTEGRATION
  TARGET_TO_INTEGRATION="$(git rev-list --count "$TARGET_BRANCH".."$INTEGRATION_BRANCH")"
  local INTEGRATION_TO_TARGET
  INTEGRATION_TO_TARGET="$(git rev-list --count "$INTEGRATION_BRANCH".."$TARGET_BRANCH")"

  # Check for an existing pull request from the integration branch to the target branch
  eval "$(gh pr view "$INTEGRATION_BRANCH" --repo "$GITHUB_REPOSITORY" --json url,number,state | jq -r 'to_entries[] | "\(.key | ascii_upcase)=\(.value)"')"
  local HASPR=0
  if [ "$NUMBER" != "" ] && [ "$BASEREFNAME" == "$TARGET_BRANCH" ]; then HASPR=1; fi

  if [ "$TARGET_TO_INTEGRATION" -eq 0 ] && [ "$INTEGRATION_TO_TARGET" -eq 0 ]
  then
    echo "$TARGET_BRANCH is up to date with $INTEGRATION_BRANCH. No pull request needed"

    if [ $HASPR -eq 0 ] && [ "$NUMBER" != "" ]
    then
      echo "Closing existing PR"
      gh pr close "$NUMBER"
    fi
  elif [ "$TARGET_TO_INTEGRATION" -gt 0 ]
  then
    echo "$TARGET_BRANCH is behind $INTEGRATION_BRANCH ($TARGET_TO_INTEGRATION)"

    if [ -z "$NUMBER" ] || [ "$STATE" != "OPEN" ]
    then
      gh pr create -B "$TARGET_BRANCH" -H "$INTEGRATION_BRANCH" --title "$TITLE" --body-file "$BODY_FILE"
    else
      echo "PR already exists: ($NUMBER) $URL . Updating..."
      gh pr edit "$NUMBER" --title "$TITLE" --body-file "$BODY_FILE"
    fi
  else
    echo "$INTEGRATION_BRANCH is behind $TARGET_BRANCH ($INTEGRATION_TO_TARGET). This is likely an error"
    exit 1
  fi
}
