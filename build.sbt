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

// lazy val docs = project
//   .in(file("kafka-lib-docs"))
//   .settings(
//     mdocOut := file("kafka-lib-docs"),
//     publishTo := None,
//     publishArtifact := false,
//     publish := {},
//     publishLocal := {}
//   )
//   .dependsOn(kafkaClient)
//   .enablePlugins(MdocPlugin)
