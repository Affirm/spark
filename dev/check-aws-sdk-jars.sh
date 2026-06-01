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

# AWS SDK v2 every JAR has META-INF/maven/software.amazon.awssdk/<artifact>/pom.properties
# Reading the version= line in those files is the canonical way to identify the SDK version.
while IFS= read -r -d '' jar; do
  # Skip iceberg-aws-bundle here; it's handled below as a special case.
  if [[ "$(basename "$jar")" == iceberg-aws-bundle-*.jar ]]; then
    continue
  fi

  # Extract every AWS SDK v2 pom.properties version line. Most non-AWS-SDK JARs
  # have no matching entry and unzip will print "caution: filename not matched"
  # which we silence.
  versions=$(unzip -p "$jar" 'META-INF/maven/software.amazon.awssdk/*/pom.properties' 2>/dev/null \
    | grep -E '^version=' | sort -u || true)

  if [[ -z "$versions" ]]; then
    # Not an AWS SDK v2 JAR. Skip.
    continue
  fi

  while IFS= read -r v; do
    actual="${v#version=}"
    if [[ "$actual" != "$EXPECTED_VERSION" ]]; then
      echo "FAIL: $(basename "$jar") publishes AWS SDK v2 at $actual (expected $EXPECTED_VERSION)" >&2
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
