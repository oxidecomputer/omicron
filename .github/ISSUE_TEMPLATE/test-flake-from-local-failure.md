---
name: Test flake from local failure
about: Report a test failure that happened locally (not CI) that you believe is not
  related to local changes
title: 'test failure: TEST_NAME'
labels: Test Flake
assignees: ''

---

On branch **BRANCH** commit **COMMIT**, I saw this test failure:

```
Include the trimmed, relevant output from `cargo nextest`.  Here's an example:

-------
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

**NOTE: Consider attaching any log files produced by the test.**
