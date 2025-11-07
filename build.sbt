ThisBuild / organization := "org.cscie88c"
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / versionScheme := Some("semver-spec")

val circeVersion = "0.13.0"
val pureconfigVersion = "0.15.0"
val catsVersion = "2.2.0"

lazy val scalaTest = Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test
)

lazy val commonDependencies = Seq(
  // cats FP libary
  "org.typelevel" %% "cats-core" % catsVersion,

  // support for JSON formats
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-literal" % circeVersion,

  // support for typesafe configuration
  "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,

  // logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  // parallel collections
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
)

// common settings
lazy val commonSettings = Seq(
  scalaVersion := "2.13.16",
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-language:postfixOps",
    "-language:higherKinds", // HKT required for Monads and other HKT types
    "-Wunused", // for scalafix
    "-Wunused:imports", // for scalafix
    "-Yrangepos"
  ),
  Compile / run / fork := true, // cleaner to run programs in a JVM different from sbt
  Compile / discoveredMainClasses := Seq(), // ignore discovered main classes
  // needed to run Spark with Java 17
  run / javaOptions ++= Seq(
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
  ),
  libraryDependencies ++= commonDependencies,
  libraryDependencies ++= scalaTest
)

lazy val dockerSettings = Seq(
  dockerBaseImage := "docker.io/library/eclipse-temurin:17-jre",
  Docker / version := "latest"
)

lazy val core = project
  .in(file("core"))
  .settings(
    commonSettings
    // Core library settings
  )

lazy val cli = project
  .in(file("cli"))
  .settings(commonSettings, dockerSettings)
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)

lazy val spark = project
  .in(file("spark"))
  .settings(
    commonSettings
    // Spark module settings)
  )
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)

lazy val beam = project
  .in(file("beam"))
  .settings(
    commonSettings
    // Beam module settings
  )
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)

lazy val root = (project in file("."))
  .aggregate(core, cli, spark, beam)
  .settings(
    commonSettings,
    name := "multi-module-project",
    description := "A multi-module project for Scala and Big data frameworks like Spark, Beam, and Kafka"
  )

// Custom task to zip files for homework submission
lazy val zipHomework = taskKey[Unit]("zip files for homework submission")

zipHomework := {
  val bd = baseDirectory.value
  val targetFile = s"${bd.getAbsolutePath}/scalaHomework.zip"
  val ignoredPaths =
    ".*(\\.idea|target|\\.DS_Store|\\.bloop|\\.metals|\\.vsc|\\.git|\\.devcontainer|\\.vscode|apps|setup|data)/*".r.pattern
  val fileFilter = new FileFilter {
    override def accept(f: File) =
      !ignoredPaths.matcher(f.getAbsolutePath).lookingAt
  }
  println("zipping homework files to scalaHomework.zip ...")
  IO.delete(new File(targetFile))
  IO.zip(
    Path.selectSubpaths(new File(bd.getAbsolutePath), fileFilter),
    new File(targetFile),
    None
  )
}
