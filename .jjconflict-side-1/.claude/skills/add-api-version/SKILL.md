---
name: add-api-version
description: Add a new version to a Dropshot HTTP API. Use when the user wants to make versioned changes to a Dropshot-managed HTTP API, such as adding, making changes to, or removing an API endpoint.
---

# Add API version

Add a new version to a Dropshot API in this repository.

## Instructions

Follow these steps in order. Do not skip ahead.

### Step 1: Fetch the guide

Fetch the guide from the dropshot-api-manager repository:

```bash
curl -fsSL https://raw.githubusercontent.com/oxidecomputer/dropshot-api-manager/refs/heads/main/guides/new-version.md
```

**Do not summarize the guide.** Read and follow it exactly as written. Remember to fetch RFD 619 for context.

### Step 2: Ascertain the scope of the request

If not already provided, ask the user:

* Which API they would like to change.
* What changes to make.

### Step 3: Make the change

Follow the guide carefully and systematically to add a new version to the provided API which makes the given changes.

Follow the guide step by step; do not skip any steps.
