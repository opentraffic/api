#!/usr/bin/env bash
set -e

echo "Logging into dockerhub..."
docker login -e "${DOCKER_EMAIL}" -u "${DOCKER_USER}" -p "${DOCKER_PASS}"

echo "Tagging and pushing latest build..."
docker tag api:latest opentraffic/api:latest
docker push opentraffic/api:latest

echo "Tagging and pushing ${CIRCLE_SHA1}..."
docker tag api:latest opentraffic/api:${CIRCLE_SHA1}
docker push opentraffic/api:${CIRCLE_SHA1}

echo "Done!"
