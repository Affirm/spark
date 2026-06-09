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

## 3

**Spark version:** 4.0.0
**Commit SHA:** ae0b681291fa868be80d84aa417eb6326ebcb2a5

**Changes:**
- Bundled the Snowflake Spark connector `net.snowflake:spark-snowflake_${scala.binary.version}:3.1.3` and the matching JDBC driver `net.snowflake:snowflake-jdbc:3.8.0` into the assembly, so `pyspark/jars/` ships the `net.snowflake.spark.snowflake.DefaultSource` datasource and `net.snowflake.client.jdbc.SnowflakeDriver` without needing `spark.jars.packages` at submit time.
- Both Snowflake deps are declared with **`<scope>runtime</scope>`**. `snowflake-jdbc` *requires* the explicit scope: the root pom's `<dependencyManagement>` manages `net.snowflake:snowflake-jdbc` at `${snowflake.jdbc.version}` (3.22.0) with `<scope>test</scope>` (for the upstream `SnowflakeDialect` tests in `sql/core`), so omitting the scope would inherit `test` and exclude the jar from the wheel. `spark-snowflake` has no managed entry (defaults to `compile`) and is set to `runtime` for symmetry/intent.
- Pinned `snowflake-jdbc` to `3.8.0` (overriding the managed 3.22.0) to match the version validated against the Snowflake account, and excluded the transitive `snowflake-jdbc` pulled in by `spark-snowflake` so the pinned 3.8.0 driver is the only JDBC jar in the wheel.

## 2

**Spark version:** 4.0.0
**Commit SHA:** 9ed48fd56fff8b729be7b0921ca600f09ca55e37

**Changes:**
- Bumped AWS SDK v2 to `2.33.0` (from `2.25.53`); `url-connection-client` now tracks `${aws.java.sdk.v2.version}` instead of a hardcoded `2.29.52`.
- Build the `hadoop-cloud` module by default (added to the root `<modules>`) and moved `spark-hadoop-cloud` + the `jetty-util` redeclare into the assembly's main `<dependencies>`, so the S3A **magic committer** classes (`org.apache.spark.internal.io.cloud.PathOutputCommitProtocol` / `BindingParquetOutputCommitter`) always ship in `pyspark/jars/` without needing `-Phadoop-cloud`. Both `hadoop-cloud` profiles (root + assembly) kept as no-ops for back-compat.
- Enforce a single AWS SDK v2 version across the wheel: import the AWS SDK v2 BOM and add an explicit `software.amazon.awssdk:bundle` pin in `dependencyManagement`; exclude the stale transitive `bundle` (2.24.6) from `hadoop-aws` (root pom **and** `hadoop-cloud/pom.xml`), `iceberg-spark-runtime-4.0`, `openlineage-spark`, `kafka-clients`, and `spark-hadoop-cloud`.
- Declare one clean `software.amazon.awssdk:bundle:2.33.0` uber-jar (with `*:*` exclusion) in the assembly as the sole AWS SDK jar — it provides the S3 Transfer Manager classes `hadoop-aws` 3.4.1 needs (absent from `iceberg-aws-bundle`).
- Added `dev/check-aws-sdk-jars.sh` and wired it into `setup.py`: the wheel build now **fails** if any AWS SDK v2 jar — standalone `software.amazon.awssdk:*` or the version embedded inside `iceberg-aws-bundle` — drifts from a single consistent version.
- Documented the `iceberg.version` ↔ `aws.java.sdk.v2.version` lockstep invariant (iceberg 1.10.x → AWS SDK v2 2.33.0).

## 1

**Spark version:** 4.0.0
**Commit SHA:** 8cccafb5f8367c2f4c360684f3a1f5b9142550e6

**Changes:**
- Bumped Java/Scala target from 17 to 21 (`pom.xml`, scalac `-release` flag).
- Disabled the upstream Kafka connector modules (`connector/kafka-0-10*`); Affirm does not run Spark-on-Kafka.
- Bundled OpenLineage (`io.openlineage:openlineage-spark` 1.38.0) and `kafka-clients` (3.9.0) into the assembly so all Spark apps get lineage tracking out of the box.
- Added AWS SDK v2 `url-connection-client` (2.29.52) to support IRSA / `WebIdentityTokenFileCredentialsProvider` on Hadoop 3.4 (workaround until Hadoop 3.5 / HADOOP-19535).
- Added Iceberg runtime for Spark 4.0 / Scala 2.13 and `iceberg-aws-bundle` (1.10.1), plus an explicit `hadoop-aws` dependency pinned to `${hadoop.version}`.
- Added MySQL JDBC driver (`com.mysql:mysql-connector-j` 9.2.0, runtime scope).
- Packaged the Kubernetes Spark Dockerfiles into the wheel as `pyspark.k8s` (`MANIFEST.in` graft, `setup.py` symlink/copy/package wiring).
- `entrypoint.sh`: export `SPARK_VERSION=4.0.0` and switch tini path from `/usr/bin/tini` to `/tini` to match the Affirm base image.
- Added `venv/` to `python/.gitignore`.
