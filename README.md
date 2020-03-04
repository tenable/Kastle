<p align="center">
<img src="https://raw.githubusercontent.com/tenable/Kastle/master/site/src/main/docs/img/KASTLE.png" alt="Kastle" width="200" height="200">
</p>

### Kastle - Kafka Client

[![CircleCI][circle-ci-image]][circle-ci-url]
[![Maven Central][maven-image]][maven-url]
[![Scala Steward badge][scala-steward-image]][scala-steward-url]

[circle-ci-image]: https://circleci.com/gh/tenable/Kastle.svg?style=svg
[circle-ci-url]: https://circleci.com/gh/tenable/Kastle
[maven-image]: https://maven-badges.herokuapp.com/maven-central/com.tenable/kastle_2.13/badge.svg
[maven-url]: https://maven-badges.herokuapp.com/maven-central/com.tenable/kastle_2.13
[scala-steward-image]: https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=
[scala-steward-url]: https://scala-steward.org

A purely functional, effectful, resource-safe, kafka library for Scala

# Usage

The packages are published on Maven Central.

See the badge at the top of the README for the latest version number.

```scala
libraryDependencies += "com.tenable" %% "kastle" % "<version>"
```

## Getting started

- [Consumer examples](site/src/main/docs/consumer.md)
- [Producer examples](site/src/main/docs/producer.md)

## Contributing

The kastle project welcomes contributions from anybody wishing to participate. All code or documentation that is provided must be licensed with the same license that kastle is licensed with (Apache 2.0, see LICENSE.txt).

People are expected to follow the [Scala Code of Conduct](./CODE_OF_CONDUCT.md) when discussing kastle on GitHub, Gitter, or other venues.

Feel free to open an issue if you notice a bug, have an idea for a feature, or have a question about the code. Pull requests are also gladly accepted. For more information, check out the [contributor guide](./CONTRIBUTING.md).

### Producer
[Mdoc docs.](./client/Producer.md)

### Consumer
[Mdoc docs.](./client/Consumer.md)

### Development

**Unit tests:**
```bash
sbt test
```

**Integration tests**
Integration tests will run against an in-memory kafka.

```bash
sbt it:test
```

## License

All code in this repository is licensed under the Apache License, Version 2.0. See [LICENCE.md](./LICENSE.md).

Generated Mdoc documentation:

[Raw (Always updated)](./docs/README.md)

[Generated Mdoc (Might be outdated)](./kafka-lib-docs/README.md)
