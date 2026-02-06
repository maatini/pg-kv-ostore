#!/bin/sh
# Maven Wrapper script - downloads and runs Maven
# Licensed to the Apache Software Foundation (ASF)

set -e

# Resolve symlinks
PRG="$0"
while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done

PRGDIR=$(dirname "$PRG")
BASEDIR=$(cd "$PRGDIR" && pwd)

# Maven configuration
MAVEN_PROJECTBASEDIR="${MAVEN_BASEDIR:-$BASEDIR}"
MAVEN_OPTS="${MAVEN_OPTS:--Xms256m -Xmx512m}"

# Download and use Maven Wrapper
if [ -r "$BASEDIR/.mvn/wrapper/maven-wrapper.properties" ]; then
    distributionUrl=$(grep 'distributionUrl' "$BASEDIR/.mvn/wrapper/maven-wrapper.properties" | cut -d'=' -f2-)
fi

if [ -z "$distributionUrl" ]; then
    distributionUrl="https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip"
fi

MAVEN_HOME="${HOME}/.m2/wrapper/dists/apache-maven-3.9.6"

if [ ! -d "$MAVEN_HOME" ]; then
    echo "Downloading Maven..."
    mkdir -p "$MAVEN_HOME"
    curl -fsSL "$distributionUrl" -o /tmp/maven.zip
    unzip -q /tmp/maven.zip -d "${HOME}/.m2/wrapper/dists/"
    rm /tmp/maven.zip
fi

exec "$MAVEN_HOME/bin/mvn" "$@"
