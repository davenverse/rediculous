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

ThisBuild / githubWorkflowBuildPreamble ++= nativeBrewInstallWorkflowSteps.value


val catsV = "2.13.0"
val catsEffectV = "3.7.0"
val fs2V = "3.13.0"


val munitCatsEffectV = "2.2.0"

ThisBuild / crossScalaVersions := Seq("2.12.21","2.13.18", "3.3.7")
ThisBuild / scalaVersion := "2.13.18"
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

      "org.typelevel"               %%% "keypool"                    % "0.4.11",
      

      "org.typelevel"               %%% "munit-cats-effect"          % munitCatsEffectV         % Test,
      "org.scalameta"               %%% "munit-scalacheck"            % "1.3.0" % Test,
    ),
    libraryDependencies += "org.scodec" %%% "scodec-core" % (if (scalaVersion.value.startsWith("2.")) "1.11.11" else "2.3.3"),
  ).jsSettings(
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)}
  ).jvmSettings(
    libraryDependencies += "com.github.jnr" % "jnr-unixsocket" % "0.38.22" % Test,
  )
  .platformsSettings(JVMPlatform, JSPlatform)(
    libraryDependencies ++= Seq(
      "io.chrisdavenport"           %%% "whale-tail-manager"         % "0.0.12" % Test,
    )
  )
  .nativeEnablePlugins(ScalaNativeBrewedConfigPlugin)
  .platformsSettings(NativePlatform)(
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
      "io.github.cquiroz" %%% "scala-java-time" % "2.6.0"
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
