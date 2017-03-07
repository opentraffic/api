#!/usr/bin/env bash
set -e

# env
#
datastore_port=8004

postgres_user="opentraffic"
postgres_password="changeme"
postgres_db="opentraffic"

# start the container
echo "Starting the postgres container..."
docker run \
  -d \
  --name datastore-postgres \
  -e "POSTGRES_USER=${postgres_user}" \
  -e "POSTGRES_PASSWORD=${postgres_password}" \
  -e "POSTGRES_DB=${postgres_db}" \
  postgres:9.6.1

echo "Starting the datastore container..."
docker run \
  -d \
  -p ${datastore_port}:${datastore_port} \
  --name datastore \
  --link datastore-postgres:postgres \
  -e "POSTGRES_USER=${postgres_user}" \
  -e "POSTGRES_PASSWORD=${postgres_password}" \
  -e "POSTGRES_DB=${postgres_db}" \
  -e 'POSTGRES_HOST=postgres' \
  datastore:latest

echo "Container is running, sleeping to allow creation of database..."
sleep 10

# see if the port is open for now
echo "Curl'ing the service"
curl \
  --fail \
  --silent \
  --max-time 15 \
  --retry 3 \
  --retry-delay 5 \
  localhost:${datastore_port}

echo "Done!"
