ThisBuild / tlBaseVersion := "0.2" // your current series x.y

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


val catsV = "2.7.0"
val catsEffectV = "3.3.3"
val fs2V = "3.2.3"

val munitCatsEffectV = "1.0.7"

ThisBuild / crossScalaVersions := Seq("2.12.15","2.13.8", "3.1.2")
ThisBuild / versionScheme := Some("early-semver")

// Projects
lazy val `rediculous` = tlCrossRootProject
  .aggregate(core.jvm, core.js, examples.jvm, examples.js)

lazy val core = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(
    name := "rediculous",
    mimaPreviousArtifacts := Set(), // Bincompat breaking till next release
    testFrameworks += new TestFramework("munit.Framework"),

    libraryDependencies ++= Seq(
      "org.typelevel"               %%% "cats-core"                  % catsV,

      "org.typelevel"               %%% "cats-effect"                % catsEffectV,

      "co.fs2"                      %%% "fs2-core"                   % fs2V,
      "co.fs2"                      %%% "fs2-io"                     % fs2V,
      "co.fs2"                      %%% "fs2-scodec"                 % fs2V,

      "org.typelevel"               %%% "keypool"                    % "0.4.6",
      

      "io.chrisdavenport"           %%% "cats-scalacheck"            % "0.3.1" % Test,
      "org.typelevel"               %%% "munit-cats-effect-3"        % munitCatsEffectV         % Test,
      "io.chrisdavenport"           %%% "whale-tail-manager"         % "0.0.8" % Test,
      "org.scalameta"               %%% "munit-scalacheck"            % "0.7.27" % Test,
    ),
    libraryDependencies += "org.scodec" %%% "scodec-core" % (if (scalaVersion.value.startsWith("2.")) "1.11.9" else "2.1.0")
  ).jsSettings(
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)}
  ).jvmSettings(
    libraryDependencies += "com.github.jnr" % "jnr-unixsocket" % "0.38.15" % Test,
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
    scalaJSUseMainModuleInitializer := true,
  ).jsSettings(
    libraryDependencies ++= Seq(
      "io.github.cquiroz" %%% "scala-java-time" % "2.3.0"
    ),
    Compile / mainClass := Some("BasicExample"),
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)},
    scalaJSStage := FullOptStage,
  )
lazy val examplesJVM = examples.jvm
lazy val examplesJS = examples.js

lazy val site = project.in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(core.jvm)
