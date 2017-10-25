# Open Traffic Query API

Open Traffic API was a part of OTv2. It was a temporary replacement for OTv1's Data Pool and the back-end API portions of the [Traffic Engine Application](https://github.com/opentraffic/traffic-engine-app). This code was deprecated and replaced with the OTv2 [Datastore](https://github.com/opentraffic/datastore/), which runs batch jobs against files, rather than using a relational database or a "powered" web service as an API.

#### Docker build

    sudo docker build -t opentraffic/api:latest .

#### Docker Compose

    DATAPATH=. sudo -E docker-compose up

