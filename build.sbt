import ReleaseTransformations._

scalaVersion in ThisBuild := "2.12.6"
releaseCrossBuild := true
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  setReleaseVersion,
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeReleaseAll"),
  commitReleaseVersion,
  tagRelease,
  setNextVersion,
  commitNextVersion
)

val fs2Version = "0.10.4"
val circeVersion = "0.10.0-M1"

val core =
  pro("core")
    .settings(
      libraryDependencies ++= List(
        "org.typelevel" %% "cats-free" % "1.1.0",
        "co.fs2" %% "fs2-io" % fs2Version,
        "co.fs2" %% "fs2-scodec" % fs2Version,
        "org.scodec" %% "scodec-stream" % "1.1.0",
        "org.scodec" %% "scodec-cats" % "0.8.0",
        "io.circe" %% "circe-parser" % circeVersion,
        "io.circe" %% "circe-generic" % circeVersion,
        "io.chrisdavenport" %% "log4cats-slf4j" % "0.1.0",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
      )
    )

val root =
  basicProject(project.in(file(".")))
    .aggregate(core)
    .settings(noPublish)
