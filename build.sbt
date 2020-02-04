import Dependencies._
import sbt.Resolver

lazy val commonTestDependencies = Seq(
  scalaTest,
  mockitoCore
) ++ logbackRelated

lazy val tenableRepo = "" //ToDo

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
      ++ commonTestDependencies.map(_ % IntegrationTest),
    resolvers ++= Seq(
      "Tenable Nexus (tenable-cloud)" at tenableRepo + "/content/repositories/tenable-cloud"
    ),
    externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, false)
  )
  .settings(
    version := (version in ThisBuild).value,
    publishMavenStyle := true,
    publishArtifact in Test := false,
    credentials += Credentials(Path.userHome / ".sbt" / "snapshot.creds"),
    credentials += Credentials(Path.userHome / ".sbt" / "release.creds"),
    publishTo := Some(
      "releases" at tenableRepo + "/content/repositories/tenable-cloud-" + sys.env
        .getOrElse("SBTRELEASE", "release")
    )
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
