organization := "interretis"
name := "spark-cert"
version := "1.0"

scalaVersion := "2.11.7"
scalacOptions ++= Seq("-deprecation", "-explaintypes", "-feature", "-unchecked", "-optimise", "-target:jvm-1.8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"   % "1.6.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql"    % "1.6.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-graphx" % "1.6.0" withSources() withJavadoc(),
  "interretis" %% "spark-testing" % "1.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test" withSources() withJavadoc()
)

scalastyleConfig := file("project/scalastyle_config.xml")

parallelExecution in Test := false
