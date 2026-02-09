# `cargo xtask api-check` — Unified API Change Validation

## Overview

The `api-check` command provides a unified workflow for validating API changes in Omicron. It chains multiple validation steps in a single process, sharing build state to minimize cargo overhead.

**Problem it solves:** Making a small API change traditionally requires running 5-6 separate cargo commands, each with ~10 minutes of overhead. This workflow takes what would be 60+ minutes of tool execution and reduces it significantly by:
- Sharing build state between steps
- Using consistent feature flags to avoid rebuilds
- Eliminating redundant `nextest list | grep` steps

## Basic Usage

```bash
# Full validation (default)
cargo xtask api-check

# Quick mode: just check + openapi + recheck (fastest inner loop)
cargo xtask api-check --quick

# Skip specific steps
cargo xtask api-check --skip clippy
cargo xtask api-check --skip test,clippy

# Run only specific steps
cargo xtask api-check --only check,openapi

# Customize test filter (default is "ls-apis")
cargo xtask api-check --test-filter "openapi"

# Dry run: see what would be executed
cargo xtask api-check --dry-run

# Continue through failures instead of stopping at first failure
cargo xtask api-check --keep-going
```

## Steps Executed

| Step | Equivalent Command | Purpose |
|------|-------------------|---------|
| 1. `check` | `cargo check --workspace --all-targets` | Validate code compiles |
| 2. `openapi` | `cargo xtask openapi generate` | Regenerate OpenAPI specs |
| 3. `recheck` | `cargo check --workspace --all-targets` | Validate after codegen |
| 4. `test` | `cargo nextest run <filter>` | Run relevant API tests |
| 5. `clippy` | `cargo clippy --workspace --all-targets` | Lint pass |

## Example Output

```
━━━ api-check: step 1/5 — check ━━━
... cargo output ...
✓ check passed (43.2s)

━━━ api-check: step 2/5 — openapi ━━━
... cargo output ...
✓ openapi passed (12.1s)

━━━ api-check: step 3/5 — recheck ━━━
... cargo output ...
✓ recheck passed (8.3s)

━━━ api-check: step 4/5 — test ━━━
... nextest output ...
✓ test passed (28.7s)

━━━ api-check: step 5/5 — clippy ━━━
... clippy output ...
✓ clippy passed (31.4s)

━━━ api-check: summary ━━━
  ✓ check (43.2s)
  ✓ openapi (12.1s)
  ✓ recheck (8.3s)
  ✓ test (28.7s)
  ✓ clippy (31.4s)

━━━ api-check: all steps passed (123.7s) ━━━
```

## Recommended Workflows

### Inner Loop (fastest)
When iteratively developing an API change:
```bash
cargo xtask api-check --quick
```
This runs: check → openapi generate → recheck (skips tests and clippy)

### Pre-commit Validation
Before committing:
```bash
cargo xtask api-check
```
This runs all steps to ensure everything passes.

### After OpenAPI Changes
If you only changed OpenAPI specs:
```bash
cargo xtask api-check --only openapi,recheck
```

### Debugging Test Failures
To focus on just the tests:
```bash
cargo xtask api-check --only test --test-filter "<your-pattern>"
```

## Feature Flag Consistency

All steps use consistent flags (`--workspace --all-targets`) to ensure cargo doesn't rebuild artifacts between steps. This is the key optimization that makes the unified workflow faster than running commands separately.

## Future Enhancements

Potential improvements for future versions:
- Parallel execution of independent steps (test + clippy)
- Affected-crate detection to only check/test changed crates
- Watch mode for automatic re-runs on file changes
- Skip recheck step when openapi specs are unchanged
- CI integration with summary comments on PRs
