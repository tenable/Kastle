import Dependencies._
import sbt.Resolver

lazy val commonTestDependencies = Seq(
  scalaTest,
  mockitoCore
) ++ logbackRelated

lazy val tenableRepo = ""

inThisBuild(
  List(
    organization := "com.tenable.consec.library",
    scalaVersion := "2.12.10",
    crossScalaVersions := Seq("2.12.10", "2.13.1")
  )
)

lazy val root = (project in file("."))
  .settings(
    publishTo := None,
    publishArtifact := false,
    publish := {},
    publishLocal := {}
  )
  .aggregate(kafkaClient)

lazy val kafkaClient = (project in file("kafka-library"))
  .overrideConfigs(IntegrationSettings.config)
  .settings(IntegrationSettings.configSettings)
  .settings(
    name := "Kafka Client",
    addCompilerPlugin(silencerPlugin),
    addCompilerPlugin(kindProjector),
    libraryDependencies ++= Seq(
      silencerPlugin,
      slf4jApi,
      catsCore,
      catsFree,
      catsEffect,
      simulacrum,
      avro,
      typesafeConfig,
      jacksonDatabind
    )
      ++ kafkaRelated
      ++ commonTestDependencies.map(_ % Test)
      ++ commonTestDependencies.map(_ % IntegrationTest)
  )

lazy val doNotPublishArtifact = Seq(
  publishArtifact := false,
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Compile, packageSrc) := false,
  publishArtifact in (Compile, packageBin) := false
)

lazy val docs = project
  .in(file("kafka-lib-docs"))
  .settings(
    mdocOut := file("kafka-lib-docs"),
    publishTo := None,
    publishArtifact := false,
    publish := {},
    publishLocal := {}
  )
  .dependsOn(kafkaClient)
  .enablePlugins(MdocPlugin)

lazy val site = project
  .in(file("site"))
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(MdocPlugin)
  .settings(doNotPublishArtifact)
  .settings {
    import microsites._
    Seq(
      micrositeName := "Kastle",
      micrositeDescription := "Purely functional, effectful, resource-safe, kafka library for Scala",
      micrositeAuthor := "Tenable",
      micrositeGithubOwner := "Tenable",
      micrositeGithubRepo := "lib-appsec-kafka",
      micrositeFooterText := None,
      micrositeHighlightTheme := "atom-one-light",
      micrositeCompilingDocsTool := WithMdoc,
      fork in mdoc := true, // ?????
      // sourceDirectory in Compile := baseDirectory.value / "src",
      // sourceDirectory in Test := baseDirectory.value / "test",
      mdocIn := (sourceDirectory in Compile).value / "docs",
      micrositeExtraMdFiles := Map(
        file("README.md") -> ExtraMdFileConfig(
          "index.md",
          "home",
          Map("title" -> "Home", "section" -> "home", "position" -> "0")
        )
      )
    )
  }
  .dependsOn(kafkaClient)
