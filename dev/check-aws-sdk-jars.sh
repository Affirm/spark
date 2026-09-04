#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# check-aws-sdk-jars.sh
#
# Verifies that every AWS SDK v2 JAR shipped in a Spark/PySpark `jars/` directory is
# at a single, consistent version — both standalone software.amazon.awssdk:* JARs AND
# the AWS SDK classes embedded inside iceberg-aws-bundle.
#
# Background: see PYSPARK_PY312_ISSUE.md / BATCH-4042. Shipping multiple AWS SDK v2
# versions on the same classpath causes per-class resolution to load different
# submodules from different versions; mismatched API surfaces produce
# NoSuchMethodError at runtime.
#
# Usage:
#   dev/check-aws-sdk-jars.sh <jars-dir> [expected-aws-sdk-version]
#
# If <expected-aws-sdk-version> is omitted, it is read from the <aws.java.sdk.v2.version>
# property in the root pom.xml.
#
# IMPORTANT — audit scope and timing
#
# This script audits a `jars-dir` at the moment it's invoked. When it's wired into
# setup.py (python/packaging/classic/setup.py), it runs *during* `bdist_wheel`'s
# module-level code — which is BEFORE pip writes the final .whl AND before any
# post-build step that the surrounding build pipeline may run.
#
# Empirically, Affirm's pyspark-wheel-build pipeline has a post-setup.py JAR
# injection step that drops additional JARs (notably iceberg-aws-bundle and
# bundle-2.24.6.jar from hadoop-aws's transitive graph) directly into
# pyspark/jars/ inside the assembled wheel. The setup.py-time audit cannot see
# these later injections.
#
# A symptom of this is the audit reporting:
#   "note: no iceberg-aws-bundle-*.jar found in deps/jars — skipping embedded-version check"
# If you see that message, the audit ran against an incomplete deps/jars/ and
# its OK verdict does NOT cover what eventually ships in the wheel.
#
# For full coverage, ALSO run this script against the unpacked final wheel as a
# separate build step:
#   unzip /tmp/pyspark-dist/pyspark-*.whl -d /tmp/wheel-audit
#   dev/check-aws-sdk-jars.sh /tmp/wheel-audit/pyspark/jars

set -euo pipefail

usage() {
  echo "usage: $0 <jars-dir> [expected-aws-sdk-version]" >&2
  echo "" >&2
  echo "  <jars-dir>                 directory containing Spark JARs (e.g. assembly/target/scala-2.13/jars" >&2
  echo "                             or the unpacked pyspark/jars/)" >&2
  echo "  [expected-aws-sdk-version] AWS SDK v2 version every JAR must agree on" >&2
  echo "                             (default: read from <aws.java.sdk.v2.version> in pom.xml)" >&2
  exit 2
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
fi

JARS_DIR="$1"
if [[ ! -d "$JARS_DIR" ]]; then
  echo "error: <jars-dir> does not exist or is not a directory: $JARS_DIR" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
SPARK_ROOT="$(cd "$SCRIPT_DIR/.."; pwd)"

if [[ $# -eq 2 ]]; then
  EXPECTED_VERSION="$2"
else
  POM="$SPARK_ROOT/pom.xml"
  if [[ ! -f "$POM" ]]; then
    echo "error: cannot find $POM to read <aws.java.sdk.v2.version> from" >&2
    exit 2
  fi
  EXPECTED_VERSION="$(grep -oE '<aws\.java\.sdk\.v2\.version>[^<]+</aws\.java\.sdk\.v2\.version>' "$POM" \
    | head -1 | sed -E 's|</?aws\.java\.sdk\.v2\.version>||g')"
  if [[ -z "$EXPECTED_VERSION" ]]; then
    echo "error: could not extract <aws.java.sdk.v2.version> from $POM" >&2
    exit 2
  fi
fi

echo "checking AWS SDK v2 JAR consistency in $JARS_DIR (expected version: $EXPECTED_VERSION)"

FAILED=0

# ---------------------------------------------------------------------------
# Check 1: standalone software.amazon.awssdk:* JARs must all be at the
# expected version.
#
# AWS SDK v2's modular artifacts publish JARs named "<artifact>-<version>.jar"
# (e.g. bundle-2.33.0.jar, utils-2.33.0.jar, regions-2.33.0.jar). We don't
# know up front which subset will land in the build, so we enumerate every
# .jar in the directory and check whichever ones correspond to AWS SDK v2
# artifacts. Identification is by JAR-internal metadata (pom.properties),
# not by filename, so renames don't fool us.
# ---------------------------------------------------------------------------

# Most modular AWS SDK v2 artifacts (utils, regions, auth, annotations, ...) publish
# META-INF/maven/software.amazon.awssdk/<artifact>/pom.properties, and reading the
# version= line from those is the canonical way to identify the SDK version.
#
# The full uber-jar `software.amazon.awssdk:bundle` is an exception: it's built with
# maven-shade-plugin and the META-INF/maven entries for the bundled sub-modules are
# filtered out during packaging. So a standalone `bundle-X.X.X.jar` has no
# pom.properties at the path we look for above. For that case we fall back to
# inspecting the filename and verifying it actually contains AWS SDK class paths.
while IFS= read -r -d '' jar; do
  base="$(basename "$jar")"

  # Skip iceberg-aws-bundle here; it's handled below as a special case.
  if [[ "$base" == iceberg-aws-bundle-*.jar ]]; then
    continue
  fi

  # Primary detection: pom.properties inside the JAR.
  # Most non-AWS-SDK JARs have no matching entry; unzip prints "caution: filename
  # not matched" which we silence.
  versions=$(unzip -p "$jar" 'META-INF/maven/software.amazon.awssdk/*/pom.properties' 2>/dev/null \
    | grep -E '^version=' | sort -u || true)

  # Fallback detection for the AWS SDK `bundle` uber-jar — its META-INF/maven
  # entries are stripped by the shade plugin, so the primary detection above
  # silently misses it. Match by filename and verify by class-path content.
  if [[ -z "$versions" && "$base" =~ ^bundle-([0-9][0-9.A-Za-z+-]*)\.jar$ ]]; then
    filename_version="${BASH_REMATCH[1]}"
    if unzip -l "$jar" 2>/dev/null | grep -q 'software/amazon/awssdk/'; then
      versions="version=$filename_version"
    fi
  fi

  if [[ -z "$versions" ]]; then
    # Not an AWS SDK v2 JAR. Skip.
    continue
  fi

  while IFS= read -r v; do
    actual="${v#version=}"
    if [[ "$actual" != "$EXPECTED_VERSION" ]]; then
      echo "FAIL: $base publishes AWS SDK v2 at $actual (expected $EXPECTED_VERSION)" >&2
      FAILED=1
    fi
  done <<< "$versions"
done < <(find "$JARS_DIR" -maxdepth 1 -name '*.jar' -print0)

# ---------------------------------------------------------------------------
# Check 2: iceberg-aws-bundle embeds AWS SDK v2 classes from the version of
# AWS SDK that the Iceberg release was built against. That embedded version
# must match EXPECTED_VERSION, otherwise we ship two distinct AWS SDK
# versions and risk classpath cross-version drift.
# ---------------------------------------------------------------------------

# Heuristic check: if the wheel ships iceberg-aws-bundle (which every Affirm
# pyspark wheel does), the audit running against a jars-dir that lacks it is
# almost certainly being invoked too early — before the post-setup.py JAR
# injection step has happened. In that case the OK verdict from this script is
# not meaningful for the final wheel.
#
# This warning is informational, not fail-on. If you've confirmed your build
# pipeline doesn't post-inject JARs, you can ignore it. Otherwise, re-run the
# audit against the unpacked final wheel (see the IMPORTANT block at the top
# of this script).
iceberg_bundle="$(find "$JARS_DIR" -maxdepth 1 -name 'iceberg-aws-bundle-*.jar' -print -quit)"
if [[ -n "$iceberg_bundle" ]]; then
  # iceberg-aws-bundle is a shaded fat-jar that copies AWS SDK's META-INF/maven
  # tree verbatim, so the same pom.properties trick works.
  embedded_versions=$(unzip -p "$iceberg_bundle" 'META-INF/maven/software.amazon.awssdk/*/pom.properties' 2>/dev/null \
    | grep -E '^version=' | sort -u || true)

  if [[ -z "$embedded_versions" ]]; then
    echo "FAIL: $(basename "$iceberg_bundle") does not expose an embedded AWS SDK v2 version" >&2
    echo "  (no META-INF/maven/software.amazon.awssdk/*/pom.properties found inside the JAR)" >&2
    FAILED=1
  else
    while IFS= read -r v; do
      actual="${v#version=}"
      if [[ "$actual" != "$EXPECTED_VERSION" ]]; then
        echo "FAIL: $(basename "$iceberg_bundle") embeds AWS SDK v2 at $actual (expected $EXPECTED_VERSION)" >&2
        echo "  -> bump <iceberg.version> to a release whose embedded AWS SDK matches $EXPECTED_VERSION," >&2
        echo "     OR bump <aws.java.sdk.v2.version> to match what iceberg-aws-bundle embeds" >&2
        echo "  see the INVARIANT comment above <iceberg.version> in pom.xml" >&2
        FAILED=1
      fi
    done <<< "$embedded_versions"
  fi
else
  echo "note: no iceberg-aws-bundle-*.jar found in $JARS_DIR — skipping embedded-version check"
fi

if [[ "$FAILED" -ne 0 ]]; then
  echo "" >&2
  echo "AWS SDK v2 JAR consistency check FAILED. See PYSPARK_PY312_ISSUE.md for context." >&2
  exit 1
fi

echo "OK: every AWS SDK v2 JAR in $JARS_DIR (standalone + embedded in iceberg-aws-bundle) is at $EXPECTED_VERSION"
