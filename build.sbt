name := "Graphxtest"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "GraphFrames" at "https://dl.bintray.com/spark-packages/maven/"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion % Provided,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion % Provided,
  "org.apache.spark" % "spark-graphx_2.11" % sparkVersion % Provided,
)

libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

scalacOptions ++= Seq("-no-specialization")
