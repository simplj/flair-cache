#!/bin/sh
# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

# Apache Maven Wrapper — downloads Maven 3.9.6 on first use, then delegates.

set -e

SCRIPT_DIR="`cd "$(dirname "$0")" && pwd`"
WRAPPER_PROPERTIES="$SCRIPT_DIR/.mvn/wrapper/maven-wrapper.properties"

if [ ! -f "$WRAPPER_PROPERTIES" ]; then
  echo "Error: $WRAPPER_PROPERTIES not found." >&2
  exit 1
fi

DISTRIBUTION_URL=`grep '^distributionUrl=' "$WRAPPER_PROPERTIES" | sed 's/distributionUrl=//' | tr -d '\r'`
ZIP_NAME="`basename \"$DISTRIBUTION_URL\"`"
# zip is apache-maven-X.Y.Z-bin.zip; the top-level dir inside is apache-maven-X.Y.Z
DIST_DIR_NAME="${ZIP_NAME%-bin.zip}"

DISTS_BASE="${MAVEN_USER_HOME:-$HOME/.m2}/wrapper/dists"
MAVEN_HOME="$DISTS_BASE/$DIST_DIR_NAME"

if [ ! -x "$MAVEN_HOME/bin/mvn" ]; then
  mkdir -p "$DISTS_BASE"
  TMP_ZIP="$DISTS_BASE/$ZIP_NAME"
  echo "Downloading $DISTRIBUTION_URL ..."
  if command -v curl > /dev/null 2>&1; then
    curl -fsSL "$DISTRIBUTION_URL" -o "$TMP_ZIP"
  elif command -v wget > /dev/null 2>&1; then
    wget -q -O "$TMP_ZIP" "$DISTRIBUTION_URL"
  else
    echo "Error: curl or wget is required to download Maven." >&2
    exit 1
  fi
  unzip -q "$TMP_ZIP" -d "$DISTS_BASE"
  rm -f "$TMP_ZIP"
  echo "Maven installed to $MAVEN_HOME"
fi

if [ -z "$JAVA_HOME" ]; then
  if command -v java > /dev/null 2>&1; then
    : # java is on PATH; mvn will find it
  else
    echo "Error: JAVA_HOME is not set and java is not on the PATH." >&2
    exit 1
  fi
fi

exec "$MAVEN_HOME/bin/mvn" "$@"
