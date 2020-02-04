
import sbt.Keys._
import sbt.internal.util.Attributed
import sbt._

//copied blindly from registry for the most part
object IntegrationSettings {

  lazy val testAll = TaskKey[Unit]("test-all")

  lazy val config : Configuration = IntegrationTest

  private [this] lazy val extraSettings =
    inConfig(config)(Defaults.itSettings ++ Defaults.testSettings) ++ Seq(
      javaOptions in config ++= Seq()  ++ (javaOptions in Test).value,
      fork in config := true,
      scalaSource in config := baseDirectory.value / "src" / "it" / "scala",
      concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
      parallelExecution in config := false
    )

  lazy val configSettings = extraSettings
}
