name := "beam"

val scioVersion = "0.14.1"
val beamVersion = "2.57.0"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % Test,
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.1"
  // "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.1"
)
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2"

Compile / mainClass := Some("beam.BeamJob")

assembly / mainClass := Some("beam.BeamJob")
assembly / assemblyJarName := "BeamJob.jar"
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
// see shading feature at https://github.com/sbt/sbt-assembly#shading
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapeless.@1").inAll
)
