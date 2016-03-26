organization := "interretis"
name := "spark-cert"
version := "1.0"

scalaVersion := "2.11.7"
scalacOptions ++= Seq("-deprecation", "-explaintypes", "-feature", "-unchecked", "-optimise", "-target:jvm-1.8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % "1.6.0" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql"       % "1.6.0" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-graphx"    % "1.6.0" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming" % "1.6.0" % "compile" withSources() withJavadoc(),
  "interretis" %% "spark-testing" % "1.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test" withSources() withJavadoc(),
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

scalastyleConfig := file("project/scalastyle_config.xml")

parallelExecution in Test := false

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}
