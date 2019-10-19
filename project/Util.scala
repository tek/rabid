import sbt._
import com.github.ehsanyou.sbt.docker.compose.DockerCompose
import sbt.Keys._

import DockerCompose.autoImport.dockerCompose

object Util
extends AutoPlugin
{
  object autoImport
  {
    val github = "https://github.com/tek"
    val projectName = "rabid"
    val repo = s"$github/$projectName"

    def noPublish: List[Setting[_]] = List(skip in publish := true)

    def basicProject(p: Project): Project =
      p.settings(
        organization := "io.tryp",
        fork := true,
        scalacOptions ++= List(
          "-deprecation",
          "-unchecked",
          "-feature",
          "-language:higherKinds",
          "-language:existentials",
          "-Ywarn-value-discard",
          "-Ywarn-unused:imports",
          "-Ywarn-unused:implicits",
          "-Ywarn-unused:params",
          "-Ywarn-unused:patvars",
        )
      )

    def testDeps(config: Configuration): List[ModuleID] =
      List(
        "io.tryp" %% "kallikrein-sbt" % "0.2.0" % config,
        "io.tryp" %% "xpct-klk" % "0.2.3" % config,
      )

    def pro(subName: String): Project =
      basicProject(Project(subName, file(subName)))
      .settings(
        name := s"$projectName-$subName",
        libraryDependencies ++= testDeps(Test),
        testFrameworks += new TestFramework("klk.KlkFramework"),
        addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
        addCompilerPlugin("org.typelevel" % "kind-projector" % "0.11.0" cross CrossVersion.full),
        publishMavenStyle := true,
        publishTo := Some(
          if (isSnapshot.value) Opts.resolver.sonatypeSnapshots
          else Resolver.url("sonatype staging", url("https://oss.sonatype.org/service/local/staging/deploy/maven2"))
        ),
        licenses := List("BOML" -> url("https://blueoakcouncil.org/license/1.0.0")),
        homepage := Some(url(repo)),
        scmInfo := Some(ScmInfo(url(repo), s"scm:git@github.com:tek/$projectName")),
        developers := List(Developer(id="tek", name="Torsten Schmits", email="torstenschmits@gmail.com",
          url=url(github))),
      )

    def runDocker: Def.Initialize[Task[Unit]] =
      Def.taskDyn {
        sys.env.get("NO_DOCKER") match {
          case Some(_) =>
            Def.task(())
          case None =>
            dockerCompose.toTask(" up").result.map {
              case Inc(a) =>
                streams.value.log.warn("docker-compose up failed")
              case Value(_) =>
                ()
            }
        }
      }
  }
}
