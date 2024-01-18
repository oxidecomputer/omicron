---
name: Test flake from buildomat
about: Report a test failure from a CI run either on "main" or on a PR where you don't
  think the PR changes caused the failure
title: 'test failed in CI: NAME_OF_TEST'
labels: Test Flake
assignees: ''

---

<!--
    This template is for cases where you've got a test that failed in CI for a pull request and
    you believe it's not related to changes in your branch.
-->

This test failed on a CI run on **"main" (or pull request XXX)**:

    Link here to the GitHub page showing the test failure.
    If it's from a PR, this might look like:
    https://github.com/oxidecomputer/omicron/pull/4588/checks?check_run_id=19198066410
    It could also be a link to a failure on "main", which would look like:
    https://github.com/oxidecomputer/omicron/runs/20589829185
    This is useful because it shows which commit failed and all the surrounding context.

Log showing the specific test failure:


    Link here to the specific line of output from the buildomat log showing the failure:
    https://buildomat.eng.oxide.computer/wg/0/details/01HGH32FQYKZJNX9J62HNABKPA/31C5jyox8tyHUIuDDevKkXlDZCyNw143z4nOq8wLl3xtjKzT/01HGH32V3P0HH6B56S46AJAT63#S4455
    This is useful because it shows all the details about the test failure.

Excerpt from the log showing the failure:

```
Paste here an excerpt from the log.
This is redundant with the log above but helps people searching for the error message
or test name.  It also works if the link above becomes unavailable.
Here's an example:

------

failures:
    integration_tests::updates::test_update_races

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 4 filtered out; finished in 4.84s


--- STDERR:              wicketd::mod integration_tests::updates::test_update_races ---
log file: /var/tmp/omicron_tmp/mod-ae2eb84a30e4213e-test_artifact_upload_while_updating.14133.0.log
note: configured to log to "/var/tmp/omicron_tmp/mod-ae2eb84a30e4213e-test_artifact_upload_while_updating.14133.0.log"
hint: Generated a random key:
hint:
hint:   ed25519:826a8f799d4cc767158c990a60f721215bfd71f8f94fa88ba1960037bd6e5554
hint:
hint: To modify this repository, you will need this key. Use the -k/--key
hint: command line flag or the TUFACEOUS_KEY environment variable:
hint:
hint:   export TUFACEOUS_KEY=ed25519:826a8f799d4cc767158c990a60f721215bfd71f8f94fa88ba1960037bd6e5554
hint:
hint: To prevent this default behavior, use --no-generate-key.
thread 'integration_tests::updates::test_update_races' panicked at wicketd/tests/integration_tests/updates.rs:482:41:
at least one event
stack backtrace:
...
```
