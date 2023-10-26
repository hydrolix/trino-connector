ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17")

lazy val root = (project in file("."))
  .settings(
    name := "hydrolix-trino-connector"
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("ch", "qos", "logback", _*)                => MergeStrategy.first
  case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
  case "application.conf"                                  => MergeStrategy.concat
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

/*
ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("com.fasterxml.**" -> "shadejackson.@1").inAll
)
*/

libraryDependencies := Seq(
  "io.hydrolix" %% "hydrolix-connectors-core" % "0.3.2-SNAPSHOT",
  "io.trino" % "trino-spi" % "430" % Provided,
  "jakarta.annotation" % "jakarta.annotation-api" % "2.1.1",
  "io.airlift" % "configuration" % "237",
  "io.airlift" % "bootstrap" % "237",
  "com.google.inject" % "guice" % "7.0.0",

  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "io.trino" % "trino-spi" % "430" % Test,
)
