import Dependencies._

ThisBuild / scalaVersion := "2.12.10"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.jijo"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-scala-exercises",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.1",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30",
    libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"


  )
