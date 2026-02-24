ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Smart-grid-streaming-analytics"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.7",
  "org.apache.spark" %% "spark-sql" % "3.5.7",
  "org.apache.spark" %% "spark-mllib"% "3.5.7",
  "org.apache.spark" %% "spark-streaming"% "3.5.7",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.7",
  // Delta Lake per la gestione dei dati in streaming
  "io.delta" %% "delta-spark" % "3.1.0",
  // Parsing avanzato dei payload JSON
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  // Gestione pulita dei Log
  "ch.qos.logback" % "logback-classic" % "1.4.14"
)

