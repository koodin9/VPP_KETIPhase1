name := "KETI_Phase1"

version := "0.1"

scalaVersion := "2.11.8"

import sbt._
import Keys._
import sbt.Keys._
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

val dependencies = Seq("org.mongodb.spark" %% "mongo-spark-connector" % "2.2.3",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-avro" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.3",
  "org.apache.spark" %% "spark-streaming-flume" % "2.4.3",
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "io.spray" %% "spray-json" % "1.3.5")

lazy val root = (project in file(".")).settings(libraryDependencies ++= dependencies)