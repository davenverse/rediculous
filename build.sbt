import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

val catsV = "2.6.1"
val catsEffectV = "3.2.0"
val fs2V = "3.0.6"

val munitCatsEffectV = "1.0.5"

ThisBuild / crossScalaVersions := Seq("2.12.16", "2.13.5")
ThisBuild / scalaVersion := crossScalaVersions.value.last


// Projects
lazy val `rediculous` = project.in(file("."))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, examples)

lazy val core = project.in(file("core"))
  .settings(
    name := "rediculous",
    testFrameworks += new TestFramework("munit.Framework"),

    libraryDependencies ++= Seq(
      "org.typelevel"               %% "cats-core"                  % catsV,

      "org.typelevel"               %% "cats-effect"                % catsEffectV,

      "co.fs2"                      %% "fs2-core"                   % fs2V,
      "co.fs2"                      %% "fs2-io"                     % fs2V,

      "org.typelevel"               %% "keypool"                    % "0.4.6",

      "org.typelevel"               %% "munit-cats-effect-3"        % munitCatsEffectV         % Test,
      "org.scalameta"               %% "munit-scalacheck"            % "0.7.27" % Test
    )
  )

lazy val examples = project.in(file("examples"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core)
  .settings(
    name := "rediculous-examples",
    fork in run := true
  )

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