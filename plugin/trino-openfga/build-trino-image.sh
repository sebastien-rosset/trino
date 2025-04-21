#!/bin/bash

set -ex

# Get the directory of the trino-openfga plugin
PLUGIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Navigate to the Trino root directory (parent of plugin dir)
TRINO_ROOT="$( cd "${PLUGIN_DIR}/../.." &> /dev/null && pwd )"
cd "${TRINO_ROOT}"

# Build the trino-openfga plugin
echo "Building trino-openfga plugin from ${TRINO_ROOT}..."
./mvnw clean install -pl plugin/trino-openfga -am -DskipTests

# Go to the docker directory
cd core/docker

# Build the Trino docker image with the local changes
echo "Building Trino docker image with local changes..."
./build.sh

# Get the built image tag (assuming the image is tagged with the version from pom.xml)
IMAGE_TAG=$(grep -oP '<version>\K[^<]+' ../../pom.xml)
echo "Built Trino image tag: trino:${IMAGE_TAG}"

# Tag the image for our docker-compose
docker tag "trino:${IMAGE_TAG}" trino-openfga-test:latest
echo "Tagged image as trino-openfga-test:latest for docker-compose"

echo "Done!"