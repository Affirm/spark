# PySpark — Affirm Version History

This file documents every **productive** Affirm release for PySpark wheels built at Affirm.

## What is an AFFIRM_VERSION?

`AFFIRM_VERSION` is the Affirm-specific suffix appended to the wheel version string
(`2815!<SPARK_VERSION>+affirm.<AFFIRM_VERSION>`). Each productive release corresponds to
a single commit in the [Affirm/spark](https://github.com/Affirm/spark) repository that
contains all the Affirm-specific changes to apply on top of the upstream Spark tag.

## Which versions must be documented here?

| Type | Format | Document here? |
|------|--------|---------------|
| Productive | `1`, `2`, `3`, … (integer only) | **Yes — required** |
| Development | `dev1`, `dev2`, … | No — exempt |

The build script (`build-pyspark-wheels.sh`) enforces this for productive versions:

1. A section `## <AFFIRM_VERSION>` must exist in this file.
2. That section must include a `Commit SHA: <sha>` line. The SHA must exactly match
   the `--apply-commit` argument passed to the build script.

Both checks run before any compilation begins; the script exits with an error if either fails.

## Version sections

Add a new `## <N>` section for each productive release. Each section **must** include:

- `**Commit SHA:** <full-git-sha>` — the SHA from Affirm/spark containing all Affirm changes
  for this release. This is validated by the build script against `--apply-commit`.
- A short description of what changed.
- Optionally: the Spark version(s) this release targets.

Example section:

```
## 0

**Spark version:** 4.0.0
**Commit SHA:** a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2

**Changes:**
- Updated setup.py packaging metadata.
- Adjusted entrypoint.sh for Kubernetes executor compatibility.
```

---

<!-- Add new productive releases below, newest first. -->
