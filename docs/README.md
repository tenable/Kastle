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

To run integration tests:

```bash
sbt it:test
```
