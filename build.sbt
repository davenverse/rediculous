import com.typesafe.tools.mima.core._

ThisBuild / tlBaseVersion := "0.6" // your current series x.y

ThisBuild / organization := "io.chrisdavenport"
ThisBuild / organizationName := "Christopher Davenport"
ThisBuild / licenses := Seq(License.MIT)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("christopherdavenport", "Christopher Davenport")
)

ThisBuild / tlCiReleaseBranches := Seq("main")

// true by default, set to false to publish to s01.oss.sonatype.org
ThisBuild / tlSonatypeUseLegacyHost := true

ThisBuild / githubWorkflowBuildPreamble ++= nativeBrewInstallWorkflowSteps.value


val catsV = "2.11.0"
val catsEffectV = "3.5.3"
val fs2V = "3.9.4"


val munitCatsEffectV = "2.0.0-M4"

ThisBuild / crossScalaVersions := Seq("2.12.18","2.13.12", "3.3.1")
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / versionScheme := Some("early-semver")

// Projects
lazy val `rediculous` = tlCrossRootProject
  .aggregate(core, examples)

lazy val core = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  // .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(
    name := "rediculous",
    testFrameworks += new TestFramework("munit.Framework"),

    libraryDependencies ++= Seq(
      "org.typelevel"               %%% "cats-core"                  % catsV,

      "org.typelevel"               %%% "cats-effect"                % catsEffectV,

      "co.fs2"                      %%% "fs2-core"                   % fs2V,
      "co.fs2"                      %%% "fs2-io"                     % fs2V,
      "co.fs2"                      %%% "fs2-scodec"                 % fs2V,

      "org.typelevel"               %%% "keypool"                    % "0.4.9",
      

      "io.chrisdavenport"           %%% "cats-scalacheck"            % "0.3.2" % Test,
      "org.typelevel"               %%% "munit-cats-effect"          % munitCatsEffectV         % Test,
      "org.scalameta"               %%% "munit-scalacheck"            % "1.0.0-M10" % Test,
    ),
    libraryDependencies += "org.scodec" %%% "scodec-core" % (if (scalaVersion.value.startsWith("2.")) "1.11.10" else "2.2.2"),
  ).jsSettings(
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)}
  ).jvmSettings(
    libraryDependencies += "com.github.jnr" % "jnr-unixsocket" % "0.38.20" % Test,
  )
  .platformsSettings(JVMPlatform, JSPlatform)(
    libraryDependencies ++= Seq(
      "io.chrisdavenport"           %%% "whale-tail-manager"         % "0.0.10" % Test,
    )
  )
  .nativeEnablePlugins(ScalaNativeBrewedConfigPlugin)
  .platformsSettings(NativePlatform)(
    libraryDependencies ++= Seq(
      "com.armanbilge" %%% "epollcat" % "0.1.6" % Test
    ),
    Test / nativeBrewFormulas ++= Set("s2n"),
    Test / envVars ++= Map("S2N_DONT_MLOCK" -> "1")
  )

lazy val examples = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("examples"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core)
  .settings(
    name := "rediculous-examples",
    run / fork := true,
  ).jsSettings(
    libraryDependencies ++= Seq(
      "io.github.cquiroz" %%% "scala-java-time" % "2.4.0"
    ),
    Compile / mainClass := Some("BasicExample"),
    scalaJSUseMainModuleInitializer := true,
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)},
    scalaJSStage := FullOptStage,
  )
lazy val examplesJVM = examples.jvm
lazy val examplesJS = examples.js

lazy val site = project.in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .settings(tlSiteIsTypelevelProject := Some(TypelevelProject.Affiliate))
  .dependsOn(core.jvm)
