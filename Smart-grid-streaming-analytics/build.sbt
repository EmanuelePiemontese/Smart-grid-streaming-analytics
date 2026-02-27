ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Smart-grid-streaming-analytics"
  )

// Dipendenze necessarie per Spark, Delta Lake, e altre librerie
libraryDependencies ++= Seq(
  // Core Spark
  "org.apache.spark" %% "spark-core" % "3.5.7",
  "org.apache.spark" %% "spark-sql" % "3.5.7",
  "org.apache.spark" %% "spark-mllib" % "3.5.7",
  "org.apache.spark" %% "spark-streaming" % "3.5.7",

  // KAFKA: Il connettore per Spark (Consumer) e il client nativo (Producer)
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.7",
  "org.apache.kafka" % "kafka-clients" % "3.7.0", // per il Producer Kafka

  // DELTA LAKE: Gestione Silver Layer
  "io.delta" %% "delta-spark" % "3.1.0",

  // JSON: Parsing per i messaggi nel topic
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",

  // LOGGING
  "ch.qos.logback" % "logback-classic" % "1.4.14"
)

