import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.jijo"

lazy val root = (project in file("."))
  .settings(
    name := "sparkMLNLP",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkCore,
    libraryDependencies += sparkML ,
    libraryDependencies += sparkSQL,
    libraryDependencies += "com.databricks" %% "spark-xml" % "0.8.0",
    libraryDependencies += "com.github.benfradet" %% "struct-type-encoder" % "0.5.0",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.1",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1"



  )
