import Dependencies._

lazy val commonTestDependencies = Seq(
  scalaTest,
  mockitoCore
) ++ logbackRelated

inThisBuild(
  List(
    scalaVersion := "2.12.10",
    crossScalaVersions := Seq("2.12.10", "2.13.1"),
    organization := "com.tenable",
    organizationName := "Tenable",
    organizationHomepage := Some(url("https://www.tenable.com/")),
    scmInfo := Some(ScmInfo(
      url("https://github.com/tenable/kastle"),
      "scm:git@github.com:tenable/Kastle.git")),
    description := "A purely functional, effectful, resource-safe, kafka library for Scala.",
    licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage := Some(url("https://tenable.github.io/Kastle")),
    developers := List(
      Developer(
        id = "agbell",
        name = "Adam Bell",
        email = "",
        url = url("https://adamgordonbell.com/"))
    )
  )
)

lazy val root = (project in file("."))
  .settings(doNotPublishArtifact)
  .aggregate(kastle)

lazy val kastle = (project in file("kafka-library"))
  .overrideConfigs(IntegrationSettings.config)
  .settings(IntegrationSettings.configSettings)
  .settings(publishSettings)
  .settings(
    name := "kastle",
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
      ++ commonTestDependencies.map(_  % Test)
      ++ commonTestDependencies.map(_  % IntegrationTest)
      ++ Seq("io.github.embeddedkafka" %% "embedded-kafka" % "2.4.0" % IntegrationTest)
  )

lazy val publishSettings = Seq(
  publishTo := sonatypePublishToBundle.value,
  publishMavenStyle := true,
  useGpgPinentry := true
)

lazy val doNotPublishArtifact = Seq(
  skip in sonatypeBundleRelease := true,
  skip in publish := true,
  publishArtifact := false,
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Compile, packageSrc) := false,
  publishArtifact in (Compile, packageBin) := false
)

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
      micrositeHomepage := "https://tenable.github.io/Kastle",
      micrositeBaseUrl := "Kastle",
      micrositeOrganizationHomepage := "https://www.tenable.com",
      micrositeTwitter := "@TenableSecurity",
      micrositeAuthor := "Tenable",
      micrositeGithubOwner := "tenable",
      micrositeGithubRepo := "Kastle",
      micrositeFooterText := Some("Copyright Tenable, Inc 2020"),
      micrositeHighlightTheme := "atom-one-light",
      micrositeCompilingDocsTool := WithMdoc,
      fork in mdoc := true, // ?????
      // sourceDirectory in Compile := baseDirectory.value / "src",
      // sourceDirectory in Test := baseDirectory.value / "test",
      mdocIn := (sourceDirectory in Compile).value / "docs",
      micrositeExtraMdFilesOutput := resourceManaged.value / "main" / "jekyll",
      micrositeExtraMdFiles := Map(
        file("README.md") -> ExtraMdFileConfig(
          "index.md",
          "home",
          Map("title" -> "Home", "section" -> "home", "position" -> "0")
        )
      )
    )
  }
  .dependsOn(kastle)
