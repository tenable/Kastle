<img src="https://raw.githubusercontent.com/tenable/Kastle/master/docs/KASTLE.png" alt="Kastle" width="200" height="200">

### Kastle - Kafka Client


[![CircleCI](https://circleci.com/gh/tenable/Kastle.svg?style=svg)](https://circleci.com/gh/tenable/Kastle)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.tenable/kastle_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.tenable/kastle_2.13)

A purely functional, effectful, resource-safe, kafka library for Scala

# Usage

The packages are published on Maven Central.

The latest version is 1.1.x. See the badge at the top of the README for the exact version number.

```scala
libraryDependencies += "com.tenable" %% "kastle" % "1.1.x"
```

## Getting started

- [Configuration](site/config.md)
- [Consumers](site/consumers.md)
- [Producers](site/producers.md)

**Example Consumer**

To get started to consume or produce to a kafka topic you will first have to (Configure a Consumer or Producer)[https://github.io/tenable/kastle/config.html].

Further detail can be found in the [Documentation](https://github.io/tenable/kastle/index.html).

## Contributing

The kastle project welcomes contributions from anybody wishing to participate. All code or documentation that is provided must be licensed with the same license that kastle is licensed with (Apache 2.0, see LICENSE.txt).

People are expected to follow the [Scala Code of Conduct](./CODE_OF_CONDUCT.md) when discussing kastle on GitHub, Gitter, or other venues.

Feel free to open an issue if you notice a bug, have an idea for a feature, or have a question about the code. Pull requests are also gladly accepted. For more information, check out the [contributor guide](./CONTRIBUTING.md).

## License

All code in this repository is licensed under the Apache License, Version 2.0. See [LICENCE.md](./LICENSE.md).

Generated Mdoc documentation:

[Raw (Always updated)](./docs/README.md)

[Generated Mdoc (Might be outdated)](./kafka-lib-docs/README.md)
