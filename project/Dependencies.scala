import sbt._

object Dependencies {
  lazy val catsVersion = "2.0.0"

  lazy val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.30" exclude("org.slf4j", "slf4j-log4j12")

  // Javax excluded and added specifially to avoid https://github.com/sbt/sbt/issues/3618
  lazy val javax = "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar")
  // included explicitly to let us deal with vulns
  lazy val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.10"
  lazy val kafka = "org.apache.kafka" % "kafka-clients" % "2.4.0" exclude("org.slf4j", "slf4j-log4j12")
  lazy val kafkaRelated = Seq(kafka, javax)

  lazy val avro = "org.apache.avro" % "avro" % "1.9.1"

  lazy val catsCore = "org.typelevel" %% "cats-core" % catsVersion
  lazy val catsFree = "org.typelevel" %% "cats-free" % catsVersion
  
  lazy val catsEffect = "org.typelevel" %% "cats-effect" % "2.0.0"

  lazy val typesafeConfig = "com.typesafe" % "config" % "1.4.0"

  lazy val simulacrum = "com.github.mpilquist" %% "simulacrum" % "0.19.0"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.1.0"
  lazy val mockitoCore = "org.mockito" % "mockito-core" % "3.1.0"

  private val logbackVersion = "1.2.3"
  lazy val logbackClass = "ch.qos.logback" % "logback-core" % logbackVersion
  lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % logbackVersion
  lazy val logbackRelated = Seq(logbackClass, logbackClassic)

  lazy val silencerVersion = "1.6.0"
  lazy val silencerPlugin = "com.github.ghik" % "silencer-plugin" % silencerVersion cross CrossVersion.full
  lazy val kindProjector = "org.typelevel"  % "kind-projector" % "0.11.0" cross CrossVersion.full
}
