---
name: flake-review
description: Review code for patterns known to cause flaky tests. Use when the user asks to review tests or a change for flakiness, or when reviewing a diff that adds or modifies tests.
---

# Flake review

Review code for patterns known to cause flaky tests in Omicron.

## Instructions

### Step 1: Read the pattern catalog

Read `docs/flake-patterns.adoc` in full. It describes each known flake pattern: what it is, why it is bad, how to fix it, and example commits. Do not work from memory; the catalog is the source of truth and may have gained patterns since your last memory or training data.

### Step 2: Determine the code under review

If the user pointed at specific files, tests, or a PR, review those. Otherwise, prompt the user to ascertain the scope of the diff. This can be:

- **Uncommitted changes**: Changes not yet committed.
  - git: `git diff` (unstaged) or `git diff --cached` (staged)
  - jj: `jj diff --git`

- **This commit only** (stacked diff workflow): Changes are in the current commit only.
  - git: `git diff HEAD^`
  - jj: `jj diff --git --from @--`

- **This branch** (feature branch workflow): Changes span the entire branch.
  - git: `git diff $(git merge-base HEAD main)`
  - jj: `jj diff --git --from 'fork_point(trunk() | @)'`

Focus on test code and on test-support code (test utilities, simulated components, test configs), but remember that some flakes are genuine bugs in the system under test, so also consider production code that the changed tests exercise.

### Step 3: Check against each pattern

For each pattern in the catalog, check whether the code under review exhibits it. Remember that you can examine the example commits by running `git show <commit-hash>` or `jj show --git <commit-hash>`.

### Step 4: Report findings

For each finding, report:

* The pattern name from the catalog.
* The location (`file:line`).
* Why the code is susceptible, in terms of the specific race or timing assumption.
* A suggested fix, preferring the helpers the catalog recommends (for example, `wait_for_condition`, `wait_for_watch_channel_condition`, `now_db_precision()`).

If a finding looks like a bug in the system under test rather than the test itself, say so explicitly.
