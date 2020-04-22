import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.4"
  lazy val sparkML = "org.apache.spark" %% "spark-mllib" % "2.4.4"
  lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.4.4"

}
