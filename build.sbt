import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

val catsV = "2.1.1"
val catsEffectV = "2.1.4"
val shapelessV = "2.3.3"
val fs2V = "2.4.2"
val http4sV = "0.21.6"
val circeV = "0.13.0"
val doobieV = "0.9.0"
val log4catsV = "1.1.1"
val specs2V = "4.10.1"

val kindProjectorV = "0.11.0"
val betterMonadicForV = "0.3.1"

// Projects
lazy val `rediculous` = project.in(file("."))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, examples)

lazy val core = project.in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "rediculous"
  )

lazy val examples = project.in(file("examples"))
  .settings(commonSettings)
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core)
  .settings(
    name := "rediculous-examples",
    fork in run := true
  )

lazy val site = project.in(file("site"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(MdocPlugin)
  .enablePlugins(NoPublishPlugin)
  .settings(commonSettings)
  .dependsOn(core)
  .settings{
    import microsites._
    Seq(
      micrositeName := "rediculous",
      micrositeDescription := "A basic project",
      micrositeAuthor := "Christopher Davenport",
      micrositeGithubOwner := "ChristopherDavenport",
      micrositeGithubRepo := "rediculous",
      micrositeBaseUrl := "/rediculous",
      micrositeDocumentationUrl := "https://www.javadoc.io/doc/io.chrisdavenport/rediculous_2.12",
      micrositeGitterChannelUrl := "ChristopherDavenport/libraries", // Feel Free to Set To Something Else
      micrositeFooterText := None,
      micrositeHighlightTheme := "atom-one-light",
      micrositePalette := Map(
        "brand-primary" -> "#3e5b95",
        "brand-secondary" -> "#294066",
        "brand-tertiary" -> "#2d5799",
        "gray-dark" -> "#49494B",
        "gray" -> "#7B7B7E",
        "gray-light" -> "#E5E5E6",
        "gray-lighter" -> "#F4F3F4",
        "white-color" -> "#FFFFFF"
      ),
      micrositeCompilingDocsTool := WithMdoc,
      scalacOptions in Tut --= Seq(
        "-Xfatal-warnings",
        "-Ywarn-unused-import",
        "-Ywarn-numeric-widen",
        "-Ywarn-dead-code",
        "-Ywarn-unused:imports",
        "-Xlint:-missing-interpolator,_"
      ),
      micrositePushSiteWith := GitHub4s,
      micrositeGithubToken := sys.env.get("GITHUB_TOKEN"),
      micrositeExtraMdFiles := Map(
          file("CODE_OF_CONDUCT.md")  -> ExtraMdFileConfig("code-of-conduct.md",   "page", Map("title" -> "code of conduct",   "section" -> "code of conduct",   "position" -> "100")),
          file("LICENSE")             -> ExtraMdFileConfig("license.md",   "page", Map("title" -> "license",   "section" -> "license",   "position" -> "101"))
      )
    )
  }

// General Settings
lazy val commonSettings = Seq(
  scalaVersion := "2.13.3",
  crossScalaVersions := Seq(scalaVersion.value, "2.12.12"),

  addCompilerPlugin("org.typelevel" %% "kind-projector" % kindProjectorV cross CrossVersion.full),
  addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % betterMonadicForV),

  libraryDependencies ++= Seq(
    "org.typelevel"               %% "cats-core"                  % catsV,

    "org.typelevel"               %% "cats-effect"                % catsEffectV,

    "co.fs2"                      %% "fs2-core"                   % fs2V,
    "co.fs2"                      %% "fs2-io"                     % fs2V,

    "io.chrisdavenport" %% "keypool" % "0.2.0",

    "io.chrisdavenport"           %% "log4cats-core"              % log4catsV,
    "io.chrisdavenport"           %% "log4cats-slf4j"             % log4catsV     % Test,
    "io.chrisdavenport"           %% "log4cats-testing"           % log4catsV     % Test,

    "org.specs2"                  %% "specs2-core"                % specs2V       % Test,
    "org.specs2"                  %% "specs2-scalacheck"          % specs2V       % Test
  )
)

// General Settings
inThisBuild(List(
  organization := "io.chrisdavenport",
  developers := List(
    Developer("ChristopherDavenport", "Christopher Davenport", "chris@christopherdavenport.tech", url("https://github.com/ChristopherDavenport"))
  ),

  homepage := Some(url("https://github.com/ChristopherDavenport/rediculous")),
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),

  pomIncludeRepository := { _ => false},
  scalacOptions in (Compile, doc) ++= Seq(
      "-groups",
      "-sourcepath", (baseDirectory in LocalRootProject).value.getAbsolutePath,
      "-doc-source-url", "https://github.com/ChristopherDavenport/rediculous/blob/v" + version.value + "€{FILE_PATH}.scala"
  )
))
