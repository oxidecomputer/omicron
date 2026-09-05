# Omicron

See @README.adoc for build and test commands, repository layout, and how to run
the stack.

## Notes for coding agents

- Run `cargo xtask setup-agents` once per checkout, before other work. It creates
  the per-directory `CLAUDE.md`/`AGENTS.md` symlinks pointing at each directory's
  README, so directory-specific guidance is available to coding agents as they
  work in the tree. The command is idempotent and its output is gitignored, so
  it's safe to re-run.
- When working in a directory, read its `README` for area-specific conventions
  and gotchas. Some agents load these automatically as you open files; others
  should read them as needed. List them all with `git ls-files '*README*'`.
- Read `schema/crdb/README.adoc` before changing the database schema.
