# London Energy Pulse: Piattaforma di Streaming Analytics per Smart City

**London Energy Pulse** è una piattaforma di **Streaming Analytics** e **Anomaly Detection** progettata per il monitoraggio in tempo reale delle Smart Grids urbane. Utilizzando il dataset *LCL (London DataStore)*, il sistema trasforma letture energetiche statiche in flussi di dati intelligenti, integrando variabili meteorologiche per rilevare anomalie di consumo.

## 🚀 Panoramica del Progetto

L'obiettivo è superare i limiti delle analisi batch tradizionali, implementando una pipeline reattiva capace di:

* **Ingestione Real-time**: simulazione di migliaia di Smart Meter tramite Apache Kafka.
* **Data Enrichment**: join dinamico tra flussi di consumo e dati meteo.
* **Stateful Analysis**: monitoraggio del profilo storico dell'utente per il rilevamento di guasti o picchi anomali.
* **Data Lakehouse**: archiviazione sicura e transazionale tramite Medallion Architecture su Delta Lake.

## 🏗️ Architettura del Sistema

Il progetto adotta un modello **decoupled (disaccoppiato)** per garantire scalabilità e resilienza:

1. **Layer di Ingestione (Kafka)**: funge da buffer intelligente gestendo la *backpressure*. Topic: `energy-readings`, `weather-data`, `system-alerts`.
2. **Motore di Calcolo (Spark Structured Streaming & Scala)**: esegue operazioni critiche come *Watermarking* (gestione ritardi), *Stream-Stream Join* e *Stateful Processing*.
3. **Layer di Persistenza (Delta Lake)**: organizzazione dei dati in livelli:
   * **Bronze**: dati grezzi (Raw).
   * **Silver**: dati puliti e arricchiti.
   * **Gold**: aggregazioni business-ready per dashboard (Grafana/Tableau).

![Architettura di Streaming Analytics](Architettura.png)

## 🛠️ Tech Stack & Dipendenze

Il sistema è sviluppato in **Scala** per garantire performance native e *Type Safety*.

* **Linguaggio**: Scala 2.12.18
* **Build Tool**: SBT 1.9.8
* **Processing**: Apache Spark 3.5.7
* **Broker**: Apache Kafka (Installazione nativa)
* **Storage**: Delta Lake 3.1.0

### Configurazione `build.sbt`

```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.7",
  "org.apache.spark" %% "spark-sql" % "3.5.7",
  "org.apache.spark" %% "spark-streaming" % "3.5.7",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.7",
  "io.delta" %% "delta-spark" % "3.1.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
)

```

## 📋 Requisiti
Per eseguire il progetto localmente è necessario:

1. **Java JDK 11** configurato nel `JAVA_HOME`.
2. **Apache Kafka & Zookeeper** avviati localmente.
3. **IntelliJ IDEA** con il plugin Scala installato.