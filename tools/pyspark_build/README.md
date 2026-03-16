# PySpark Wheel Builder

Automates the PySpark wheel build process (clone → apply Affirm commit → compile → package → optional upload) that was previously done manually on Thor. The Docker image is built and run **on a Thor** to avoid Mac M1/ARM issues and to use the Affirm network for Artifactory uploads. Thor also injects GitHub credentials into the environment, so the container has access to private GitHub repositories (e.g. Affirm/spark) without any additional auth setup.

## Flow

All Affirm modifications to Spark and PySpark must be captured in a single, unmerged commit in the [Affirm/spark](https://github.com/Affirm/spark) repo. The build script fetches that commit and applies its diff on top of the upstream Spark release tag.

The end-to-end workflow is:

1. **Author changes** — Create a commit in [Affirm/spark](https://github.com/Affirm/spark) containing all Affirm-specific changes to Spark/PySpark. This commit does **not** need to be merged to master.
2. **Register the release** — Add a new section to `build-tools/pyspark/VERSIONS.md` in this repo (ATT), including the `Commit SHA` of the Affirm/spark commit. Open a PR and merge to master.
3. **Deploy** — In Buildkite (or on a Thor), trigger the build pipeline and provide:
   - `spark-version` (e.g. `4.0.0`)
   - `affirm-version` (e.g. `1`)
   - `commit-sha` (the full SHA of the Affirm/spark commit, same one recorded in VERSIONS.md)
4. **Publish** — The script builds the three PySpark wheels and pushes them to Artifactory.

> The build script validates both fields for productive releases: the `affirm-version` section must exist in `VERSIONS.md` **and** the `Commit SHA` recorded there must match the `--apply-commit` argument. Mismatches cause an early exit before compilation.

## Files

| File | Purpose |
|------|--------|
| `Dockerfile` | Build environment (affirm-base, Java 17 by default via `JAVA_VERSION` build arg, git, wheel, twine, setuptools); copies `VERSIONS.md` into the image |
| `build-pyspark-wheels.sh` | Orchestrates clone, commit apply, Maven build, wheel packaging, and optional Artifactory upload |
| `VERSIONS.md` | Changelog for all productive Affirm releases; each entry must include the Affirm/spark commit SHA |

## VERSIONS.md — documenting productive releases

`VERSIONS.md` tracks every **productive** Affirm release and is validated at build time.

| AFFIRM_VERSION type | Example | Must be in VERSIONS.md? |
|---------------------|---------|------------------------|
| Productive | `1`, `2`, `3` (integer only) | Yes — required |
| Development | `dev1`, `dev2` | No — exempt |

For each productive release, add a `## <N>` section containing:

- `**Commit SHA:** <full-sha>` — the Affirm/spark commit applied during the build (validated by the script).
- A short description of what changed.

> The build script exits with an error if a productive `AFFIRM_VERSION` is used and either the section is missing or the recorded SHA does not match `--apply-commit`.

## ✅ Prerequisites

- Thor instance (see [Thor Quickstart Guide](https://www.notion.so/Thor-Quickstart-Guide-16d40e54ae3881ea9abaecfcc11177ab?pvs=21))
- Docker on the Thor
- Environment: `GIT_USER_EMAIL` and `GIT_USER_NAME` (used for git `user.email` / `user.name` inside the container)
- Optional: `GITHUB_TOKEN` — a GitHub PAT for authenticated repo access (see [Environment variables](#environment-variables) below)
- For upload: Artifactory credentials (`ARTIFACTORY_USER`, `ARTIFACTORY_TOKEN`)

The script supports **Python 3.9.x or 3.12.x** (as provided by the affirm-base image). It checks the version at startup and exits with an error if neither is detected.

## Build the image (on Thor)

```bash
affirm.dev connect   # or SSH into your Thor

cd ~/all-the-things/build-tools/pyspark

# Default build (Python 3.9, Java 17 — matches batch-infra base image)
docker build -t pyspark-builder:latest .

# Python 3.12 build — swap the base image via --build-arg
docker build \
  --build-arg BASE_IMAGE=998571911837.dkr.ecr.us-east-1.amazonaws.com/affirm-base:0.92.7-py3.12.8 \
  -t pyspark-builder:latest-py3.12 .

# Custom Java version (e.g. Java 21)
docker build \
  --build-arg JAVA_VERSION=21 \
  -t pyspark-builder:latest-java21 .
```

## Workflow: upstream tag + commit

Clone Affirm/spark, checkout the upstream tag (e.g. `v4.0.0`), apply the Affirm commit, then build and package wheels.

```bash
docker run --rm \
  -v /tmp/pyspark-dist:/build/dist \
  -e GIT_USER_EMAIL="your.email@affirm.com" \
  -e GIT_USER_NAME="yourname" \
  -e GITHUB_TOKEN="$GITHUB_TOKEN" \
  pyspark-builder:latest \
  --spark-version 4.0.0 \
  --affirm-version dev1 \
  --apply-commit a1b2c3d4e5f6...
```

What happens:

- Clones **Affirm/spark** and fetches upstream tag `v${SPARK_VERSION}` (e.g. `v4.0.0`).
- Creates branch `affirm-${SPARK_VERSION}-${AFFIRM_VERSION}` from that tag.
- Fetches the commit SHA from Affirm/spark and applies its diff with `git apply`.
- For productive `AFFIRM_VERSION`s: validates that a section for the version exists in `VERSIONS.md` **and** that the recorded `Commit SHA` matches `--apply-commit` before proceeding.
- Sets the PySpark version string in `version.py`, then runs Maven (`-DskipTests -Pkubernetes clean package -T 4`) and builds the three wheels. Does **not** upload unless `--upload` is passed.

## Dry-run build (no upload)

```bash
docker run --rm \
  -v /tmp/pyspark-dist:/build/dist \
  -e GIT_USER_EMAIL="your.email@affirm.com" \
  -e GIT_USER_NAME="yourname" \
  -e GITHUB_TOKEN="$GITHUB_TOKEN" \
  pyspark-builder:latest \
  --spark-version 4.0.0 \
  --affirm-version dev1 \
  --apply-commit a1b2c3d4e5f6...
```

This will clone Affirm/spark, apply the commit, compile with Maven (~20–40 min), and write three wheels to `/tmp/pyspark-dist/` on the host.

## Upload to Artifactory (opt-in)

Only when you are ready to publish:

```bash
docker run --rm \
  -v /tmp/pyspark-dist:/build/dist \
  -e GIT_USER_EMAIL="your.email@affirm.com" \
  -e GIT_USER_NAME="yourname" \
  -e GITHUB_TOKEN="$GITHUB_TOKEN" \
  -e ARTIFACTORY_USER="your.email@affirm.com" \
  -e ARTIFACTORY_TOKEN="your_jfrog_token" \
  pyspark-builder:latest \
  --spark-version 4.0.0 \
  --affirm-version 1 \
  --apply-commit a1b2c3d4e5f6... \
  --upload
```

Without `--upload`, the script never uploads, so repeated test runs do not push artifacts.

## Buildkite pipeline

The production deploy pipeline should collect three inputs from the operator and pass them into the script:

| Pipeline input | Script argument |
|----------------|----------------|
| Spark version | `--spark-version` |
| Affirm version | `--affirm-version` |
| Commit SHA | `--apply-commit` |

When publishing, the pipeline should also set `--upload` and inject `ARTIFACTORY_USER` / `ARTIFACTORY_TOKEN` from secrets.

## Docker build args

| Build arg | Default | Description |
|-----------|---------|-------------|
| `BASE_IMAGE` | `…/affirm-base:0.92.7-py3.9.17` | Base Docker image; swap to a `py3.12` tag for Python 3.12 builds |
| `JAVA_VERSION` | `17` | JDK major version installed in the image (e.g. `17`, `21`); must be `>=17` for Spark 4.x |

## Script options

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--spark-version` | Yes | — | Upstream Spark version (e.g. `4.0.0`); used for version string and upstream tag |
| `--affirm-version` | Yes | — | Affirm suffix (e.g. `dev1`, `1`) → `2815!{spark-version}+affirm.{affirm-version}` |
| `--apply-commit` | Yes | — | Full SHA of the Affirm/spark commit to apply; must match `Commit SHA` in VERSIONS.md for productive releases |
| `--output-dir` | No | `/build/dist` | Directory for built `.whl` files |
| `--upload` | No | `false` | Upload wheels to Artifactory (requires `ARTIFACTORY_USER` and `ARTIFACTORY_TOKEN`) |

### Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GIT_USER_EMAIL` | Yes | — | Used for `git config user.email` inside the container |
| `GIT_USER_NAME` | Yes | — | Used for `git config user.name` inside the container |
| `GITHUB_TOKEN` | No | — | GitHub PAT for authenticated repo access. When set, the script configures `GIT_ASKPASS` so the token never appears in URLs, git config, or process listings |
| `REPO_URL` | No | `https://affirmprod.jfrog.io/…/pypi-local` | Artifactory PyPI repository URL used by `twine upload`. Override to target a different repository |
| `ARTIFACTORY_USER` | Only with `--upload` | — | Affirm email for Artifactory authentication |
| `ARTIFACTORY_TOKEN` | Only with `--upload` | — | JFrog API token for Artifactory authentication |

## 🚦 Exit codes

| Code | Meaning |
|------|--------|
| ✅ 0 | Success |
| ❌ 1 | General error (clone, fetch, checkout, apply-commit/overlay, missing VERSIONS.md section, or unsupported Python version); scroll up to the last `ERROR:` line |
| ❌ 2 | Maven compilation failed |
| ❌ 3 | Wheel packaging failed |
| ❌ 4 | Upload failed (missing credentials or Artifactory error) |

## 🎡 Expected wheels

After a successful run you get three wheels under `--output-dir`:

- `pyspark-2815!4.0.0+affirm.1-py2.py3-none-any.whl`
- `pyspark_client-2815!4.0.0+affirm.1-py2.py3-none-any.whl`
- `pyspark_connect-2815!4.0.0+affirm.1-py2.py3-none-any.whl`

## 🔗 References

- [Spark 4.0.0 guide](https://www.notion.so/Spark-4-0-0-guide-30440e54ae38806790f3ee84f2a9f853?pvs=21) — manual process this automates
- [Runbook: how to upgrade PySpark at Affirm](https://www.notion.so/Runbook-how-to-upgrade-PySpark-at-Affirm-19340e54ae388015bb68e94a8389d526?pvs=21) — version naming
- [Thor Quickstart Guide](https://www.notion.so/Thor-Quickstart-Guide-16d40e54ae3881ea9abaecfcc11177ab?pvs=21)
- [jvm-build-tools/bin/upload-prebuilt-spark.sh](https://github.com/Affirm/all-the-things/blob/master/jvm-build-tools/bin/upload-prebuilt-spark.sh) — JAR upload sibling
