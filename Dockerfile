FROM ubuntu:16.04
MAINTAINER Grant Heffernan <grant@mapzen.com>

# env
ENV DEBIAN_FRONTEND noninteractive

ENV API_BIND_ADDR ${STORE_BIND_ADDR:-"0.0.0.0"}
ENV API_LISTEN_PORT ${STORE_LISTEN_PORT:-"8004"}

ENV POSTGRES_USER ${POSTGRES_USER:-"opentraffic"}
ENV POSTGRES_PASSWORD ${POSTGRES_PASSWORD:-"changeme"}
ENV POSTGRES_DB ${POSTGRES_DB:-"opentraffic"}
ENV POSTGRES_HOST ${POSTGRES_HOST:-"postgres"}
ENV POSTGRES_PORT ${POSTGRES_PORT:-"5432"}

# install dependencies
RUN apt-get update && apt-get install -y \
      python \
      python-psycopg2 \
      python-rtree \
      python-shapely \
      python-bitstring
      
# install code
ADD ./py /api

# cleanup
RUN apt-get clean && \
      rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

EXPOSE ${API_LISTEN_PORT}

# start the datastore service
CMD python -u /api/query.py ${API_BIND_ADDR}:${API_LISTEN_PORT}
