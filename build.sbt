import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

val catsV = "2.6.1"
val catsEffectV = "3.2.0"
// val fs2V = "3.0.6"
val fs2V = "3.1.0"

val munitCatsEffectV = "1.0.7"

ThisBuild / crossScalaVersions := Seq("2.12.14","2.13.6", "3.1.0")
ThisBuild / scalaVersion := "2.13.6"

// Projects
lazy val `rediculous` = project.in(file("."))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core.jvm, core.js, examples.jvm, examples.js)

lazy val core = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(yPartial)
  .settings(
    name := "rediculous",
    mimaPreviousArtifacts := Set(), // Bincompat breaking till next release
    testFrameworks += new TestFramework("munit.Framework"),

    libraryDependencies ++= Seq(
      "org.typelevel"               %%% "cats-core"                  % catsV,

      "org.typelevel"               %%% "cats-effect"                % catsEffectV,

      "co.fs2"                      %%% "fs2-core"                   % fs2V,
      "co.fs2"                      %%% "fs2-io"                     % fs2V,

      "org.typelevel"               %%% "keypool"                    % "0.4.6",

      "org.typelevel"               %%% "munit-cats-effect-3"        % munitCatsEffectV         % Test,
      "io.chrisdavenport"           %%% "whale-tail-manager"         % "0.0.7" % Test,
      "org.scalameta"               %%% "munit-scalacheck"            % "0.7.27" % Test,
    )
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
  .settings(yPartial)
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
  .enablePlugins(DavenverseMicrositePlugin)
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .settings{
    import microsites._
    Seq(
      micrositeDescription := "Pure FP Redis Client",
    )
  }

lazy val yPartial = 
  Seq(
    scalacOptions ++= {
      if (scalaVersion.value.startsWith("2.12")) Seq("-Ypartial-unification")
      else Seq()
    }
  )