Kafka Client
=====

Temporary publishing instructions:

Manually bump version then from sbt console:
`+publish`

The "+" is important -- it publishes the 2.12 version as well as the 2.13 version.


### Producer
[Mdoc docs.](./client/Producer.md)

### Consumer
[Mdoc docs.](./client/Consumer.md)

### Development

To run unit tests:

```bash
sbt test
```

To run integration tests, you need to ensure the Kafka/ZK docker compose configuration is running. Either run with:

```bash
# start containers
docker-compose up -d

# run integration tests once containers are up
sbt it:test
```

Alternatively run using the checked bash script, which will start up the required docker containers, run the integration tests, and then shut everything down again once complete.

```bash
./run-it-tests
```
