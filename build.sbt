val core =
  docker(pro("core"))
    .settings(
      libraryDependencies ++= List(
        "org.typelevel" %% "cats-free" % "2.0.0",
        "co.fs2" %% "fs2-io" % fs2Version,
        "org.scodec" %% "scodec-stream" % "2.0.0",
        "org.scodec" %% "scodec-cats" % "1.0.0",
        "io.circe" %% "circe-parser" % circeVersion,
        "io.circe" %% "circe-generic" % circeVersion,
        "io.chrisdavenport" %% "log4cats-slf4j" % "1.0.0",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
      )
    )

val root =
  basicProject(project.in(file(".")))
    .aggregate(core)
    .settings(noPublish)

val fs2Version = "2.0.1"
val circeVersion = "0.12.2"

Global / onChangedBuildSource := ReloadOnSourceChanges
ThisBuild / scalaVersion := "2.13.1"
ThisBuild / dockerComposeFilePath := ((ThisBuild / baseDirectory).value / "config" / "docker-compose.yml").toString
ThisBuild / dockerComposeIgnore := false

import ReleaseTransformations._

publishTo in ThisBuild := Some(Resolver.file("file", new File("artifacts")))
releaseIgnoreUntrackedFiles := true
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)

def integration(project: Project): Project =
  project
    .configs(IntegrationTest)
    .settings(
      libraryDependencies ++= testDeps(IntegrationTest),
      Defaults.itSettings,
    )

def docker(project: Project): Project =
  integration(project)
    .enablePlugins(DockerCompose)
    .settings(
      dockerComposeIgnore := false,
      dockerComposeFilePath := (ThisBuild / dockerComposeFilePath).value,
      dockerComposeUpCommandOptions ~= (_.withOption("-d").withOption("rabbit")),
      (IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(runDocker).evaluated,
      (IntegrationTest / test) := (IntegrationTest / test).dependsOn(runDocker).value,
    )
