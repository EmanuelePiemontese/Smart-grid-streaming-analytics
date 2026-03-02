package Consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}


object EnergyStreamingConsumer {

  // Classe per rappresentare una lettura del contatore, con i campi necessari per l'analisi (LCLid, timestamp, consumo e temperatura)
  case class MeterReading(LCLid: String, timestamp: java.sql.Timestamp, consumption: Double, temperature: Double)

  // Classe per mantenere lo stato di ogni contatore, con le informazioni necessarie per il calcolo della media mobile e della varianza (mean, m2, readingCount) e l'ultimo timestamp visto (lastTimestamp) per gestire i timeout
  case class MeterState(
                         meterId: String,
                         mean: Double,        // Media mobile
                         m2: Double,          // Somma dei quadrati delle differenze (per la varianza)
                         readingCount: Long,  // Numero di letture viste
                         lastTimestamp: Long
                       )

  // Classe per rappresentare un alert di anomalia, con i campi che descrivono l'anomalia rilevata (LCLid, timestamp, valore attuale, media attesa, deviazione, temperatura e messaggio di alert)
  case class AnomalyAlert(
                           LCLid: String,
                           timestamp: java.sql.Timestamp,
                           currentValue: Double,
                           expectedAvg: Double,
                           deviation: String,
                           temperature: Double,
                           message: String
                         )

  // Funzione di rilevamento delle anomalie
  def detectAnomalies(
                       meterId: String,
                       inputs: Iterator[MeterReading],
                       state: GroupState[MeterState]): Iterator[AnomalyAlert] = {

    /*
    LOGICA DI ANOMALY DETECTION CON CONTESTO CLIMATICO E GESTIONE DELLO STATO
    1. Gestione Timeout: se il contatore è inattivo per più di 2 ore, rimuoviamo lo stato per evitare di mantenere dati obsoleti.
    2. Logica Context-Aware: analizziamo i consumi in relazione alla temperatura. Se la temperatura è estremamente bassa (<6°C) o
    estremamente alta (>30°C), alziamo la soglia di rilevamento delle anomalie (Z-Score) a 15, altrimenti la manteniamo a 8.
    Questo ci permette di essere più tolleranti con i picchi di consumo che possono essere giustificati da condizioni climaticatiche
    estreme (riscaldamento in inverno o condizionamento in estate).
    3. Calcolo Statistico: utilizzo l'algoritmo di Welford per mantenere una media mobile e una varianza in streaming,
    aggiornando lo stato ad ogni nuova lettura. Dopo una fase di calibrazione iniziale di 600 letture, calcoliamo lo Z-Score per
    identificare anomalie significative, considerando anche una soglia minima di consumo (ad esempio >6 kWh/hh) per evitare
    falsi positivi su consumi molto bassi.
    4. Aggiornamento Stato: ad ogni lettura, aggiorniamo lo stato del contatore con i nuovi valori di media, varianza e conteggio,
    e impostiamo un timeout di 2 ore per gestire l'inattività. Se il contatore non riceve nuove letture per più di 2 ore, lo stato
    viene rimosso automaticamente, evitando di mantenere dati obsoleti e migliorando l'efficienza della memoria.

    Input: Iterator di letture del contatore (MeterReading) per un singolo LCLid, e lo stato corrente del contatore (MeterState).
    Output: Iterator di AnomalyAlert per ogni lettura che supera la soglia di anomalia, con informazioni dettagliate sull'anomalia
    rilevata (valore attuale, media attesa, deviazione, temperatura e un messaggio descrittivo).
    */

    // 1. Gestione Timeout
    if (state.hasTimedOut) {
      state.remove()
      return Iterator.empty
    }

    // Inizzializzo la lista degli alert che verranno generati per questo gruppo di letture
    var alerts = List[AnomalyAlert]()
    // Recupero lo stato corrente del contatore, se esiste, altrimenti inizializzo con valori di default (media 0, varianza 0, conteggio 0)
    var currentState = state.getOption.getOrElse(MeterState(meterId, 0.0, 0.0, 0, 0L))

    // Processiamo le letture in ordine cronologico (importante per la logica di timeout e per mantenere una sequenza temporale corretta)
    // Ordiniamo le letture per timestamp e poi le analizziamo una ad una, aggiornando lo stato e generando alert se necessario
    inputs.toSeq.
      sortBy(_.timestamp.getTime).
      foreach { reading =>
        val x = reading.consumption     // Valore di consumo attuale da analizzare
        val temp = reading.temperature  // Temperatura associata alla lettura, utilizzata per la logica context-aware
        val timestamp = reading.timestamp.getTime // Timestamp della lettura, usato per aggiornare lo stato e gestire i timeout

        // 2. Logica Context-Aware (Calibrazione soglia Z-Score in base al contesto climatico)
        // Definiamo se il contesto climatico giustifica un aumento dei consumi
        val isExtremeCold = temp < 6.0   // Freddo intenso (==> riscaldamento)
        val isExtremeHeat = temp > 30.0  // Caldo intenso (==> condizionamento)

        // Soglia Z-Score dinamica:
        // Se il clima è estremo, alziamo la soglia a 15 (più tolleranza).
        // Se il clima è mite, restiamo a 8 (meno tolleranza, più sensibilità).
        val dynamicZThreshold = if (isExtremeCold || isExtremeHeat) 15.0 else 8.0

        // 3. Calcolo statistico e rilevamento anomalia (Algoritmo di Welford)
        // Fase di calibrazione iniziale di 600 letture per stabilire una baseline affidabile prima di iniziare a rilevare anomalie
        if (currentState.readingCount > 600) {
          // Calcolo della varianza e dello Z-Score per identificare anomalie significative
          val variance =
            if (currentState.readingCount > 1)
              currentState.m2 / (currentState.readingCount - 1)
            else 0.0
          val stdDev = math.sqrt(variance)
          // Aggiunta di una piccola costante (0.1) al denominatore per evitare divisioni per zero e rendere il sistema più robusto a varianze molto basse
          val zScore = math.abs(x - currentState.mean) / (stdDev + 0.1)

          // Rilevamento di anomalie
          // Consideriamo un'anomalia solo se lo Z-Score supera la soglia dinamica e il consumo è superiore a 6 kWh/hh
          if (zScore > dynamicZThreshold && x > 6.0) {
            // Contesto climatico
            val weatherContext = if (isExtremeCold) "FREDDO" else if (isExtremeHeat) "CALDO" else "MITE"

            // Generazione dell'alert con tutte le informazioni rilevanti (LCLid, timestamp, valore attuale, media attesa,
            // deviazione, temperatura e un messaggio descrittivo)
            alerts = alerts :+ AnomalyAlert(
              meterId,
              reading.timestamp,
              x,
              currentState.mean,
              f"Z-Score: $zScore%.2f",
              temp,
              f"ALERT CRITICO: Picco anomalo con clima $weatherContext ($zScore%.1f sigma)"
            )
          }
        }

        // 4. Aggiornamento dello stato del contatore con i nuovi valori di media, varianza e conteggio,
        // utilizzando l'algoritmo di Welford per il calcolo in streaming
        val newCount = currentState.readingCount + 1
        val delta = x - currentState.mean
        val newMean = currentState.mean + (delta / newCount)
        val delta2 = x - newMean
        val newM2 = currentState.m2 + (delta * delta2)

        // Aggiorniamo lo stato del contatore con i nuovi valori calcolati e l'ultimo timestamp visto (per gestire i timeout)
        currentState = MeterState(meterId, newMean, newM2, newCount, reading.timestamp.getTime)
      }

      // Salvataggio stato e impostazione timeout
      state.update(currentState)
      state.setTimeoutDuration("2 hours")

      alerts.toIterator
    }

  def main(args: Array[String]): Unit = {

    // !!! ATTENZIONE: modificare 'basePath' con il vostro percorso locale alla cartella 'data' !!!
    val basePath = "/Users/emanuelepiemontese/data"

    // Inizializzazione di SparkSession con supporto per Delta Lake
    val spark = SparkSession.builder()
      .appName("LondonEnergy-Silver-Enrichment")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Import impliciti per funzioni di colonna e dataset
    import spark.implicits._

    // --- 1. CARICAMENTO DATASET STATICI (per l'Arricchimento) ---
    // Metadati Households (Broadcast Join) (Dataset B) ---
    val houseStatic = spark.read.option("header", "true")
      .csv(s"$basePath/informations_households.csv")
      .dropDuplicates("LCLid") // Misura di sicurezza per Integrità Referenziale
      .filter(!col("Acorn_grouped").contains("ACORN-")) // Rimozione di categorie non informative
      .select(
        col("LCLid").as("meter_id"),
        col("Acorn_grouped").as("acorn_group") // Selezione solo delle colonne necessarie per l'arricchimento (riduzione del dataset per il broadcast join)
      )

    // Verifica del dataset statico dei metadati
    println(s"\n=== Esempio di record del dataset metadati (households) ===")
    houseStatic.show(5)

    // Meteo Orario (Broadcast Join) (Dataset C) ---
    val weatherStatic = spark.read.option("header", "true")
      .csv(s"$basePath/weather_hourly_darksky.csv")
      .withColumn("weather_hour", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
      .select(
        col("weather_hour"),                          // Colonna per il join con i consumi (troncata all'ora)
        col("temperature").cast(DoubleType),          // Temperatura in gradi Celsius
        col("apparentTemperature").cast(DoubleType),  // Temperatura percepita (wind chill o heat index)
        col("humidity").cast(DoubleType),             // Umidità relativa (0-1)
        col("precipType")                             // Per sapere se piove o nevica
      )

    // Verifica del dataset statico del meteo
    println(s"\n=== Esempio di record del dataset meteo (weather) ===")
    weatherStatic.show(5)

    // --- 2. LETTURA STREAM DA KAFKA (Bronze Layer) ---
    val energySchema = new StructType()
      .add("LCLid", StringType)
      .add("tstp", StringType)
      .add("energy", StringType)

    // Configurazione della lettura in streaming da Kafka, con opzioni per gestire grandi volumi di dati e garantire la resilienza del sistema
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // Indirizzo del cluster Kafka (modificare se necessario)
      .option("subscribe", "energy-readings")              // Nome del topic Kafka da cui leggere i dati (deve corrispondere al topic su cui il producer sta inviando i dati)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 500000) // Limite di 500k record per trigger per evitare sovraccarichi
      .load()

    // Verifica dello schema del flusso in ingresso da Kafka
    println(s"\n=== Schema del flusso in ingresso da Kafka ===")
    kafkaStream.printSchema()


    // --- 3. TRASFORMAZIONI E PULIZIA (Logica Preprocessor) ---
    val energyParsed = kafkaStream
      // Il messaggio Kafka è in formato binario, quindi lo convertiamo in stringa JSON e poi applichiamo lo schema per estrarre i campi
      .selectExpr("CAST(value AS STRING) as json_payload")
      // Applichiamo lo schema JSON per estrarre i campi LCLid, tstp ed energy
      .select(from_json($"json_payload", energySchema).as("data"))
      .select("data.*")
      // Pulizia: rimozione di righe con energy vuoto o "Null"
      .filter($"energy" =!= "" && !$"energy".contains("Null"))
      // Parsing del timestamp e cast del consumo a double, creazione colonna per il join meteo (troncata all'ora)
      .withColumn("timestamp", to_timestamp($"tstp", "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
      // Creazione colonna per il join meteo (troncata all'ora) per permettere un join efficiente con il dataset meteo (che è orario)
      .withColumn("consumption", $"energy".cast(DoubleType))
      .withColumn("join_hour", date_trunc("hour", $"timestamp"))
      // Rimozione delle colonne originali non più necessarie dopo il parsing e la pulizia
      .drop("tstp", "energy")

    // --- 4. JOIN & ENRICHMENT (Stream-Static) ---
    // Questa trasformazione prepara i dati per il Silver Layer
    val enrichedStream = energyParsed
      // Join con i metadati delle case (Broadcast Join) per arricchire i consumi con il gruppo Acorn
      .join(broadcast(houseStatic), $"LCLid" === $"meter_id", "inner")
      // Join con i dati meteo (Broadcast Join) per arricchire i consumi con le condizioni atmosferiche dell'ora corrispondente
      .join(broadcast(weatherStatic), $"join_hour" === $"weather_hour", "left")
      .drop("meter_id", "weather_hour", "join_hour")


    // --- 5. LOGICA DI ANALISI (Watermarking & Windowing) ---
    // Questa trasformazione prepara i dati per il Gold Layer (KPI)
    val goldMetrics = enrichedStream
      .withWatermark("timestamp", "10 minutes") // Gestione Late Data con un ritardo massimo di 10 minuti
      .groupBy(
        window($"timestamp", "1 hour", "30 minutes"), // Finestre scorrevoli di 1 ora ogni 30 minuti
        $"acorn_group",
        $"precipType"
      )
      .agg(
        avg("consumption").as("avg_consumption"),            // Consumo medio per finestra, gruppo Acorn e tipo di precipitazione
        sum("consumption").as("total_load"),                 // Carico totale (somma dei consumi) per finestra, gruppo Acorn e tipo di precipitazione
        approx_count_distinct($"LCLid").as("active_meters"), // Numero di contatori attivi (distinti) per finestra, gruppo Acorn e tipo di precipitazione
        avg("temperature").as("avg_temp")
      )

    // --- 6. DEBUG A VIDEO (Multi-Display) ---
    /* Stampa a video sia i record arricchiti (Silver) che i KPI aggregati (Gold) per monitorare l'andamento dello streaming in tempo reale.
    // A. Visualizzazione Record Arricchiti (Silver)
    val debugSilverQuery = enrichedStream.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("numRows", 5) // Mostriamo solo 5 righe per non intasare la console
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // B. Visualizzazione KPI Aggregati (Gold)
    val debugGoldQuery = goldMetrics.writeStream
      .format("console")
      .outputMode("update") // Fondamentale: mostra solo i KPI che cambiano
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    */

    // --- 7. SCRITTURA MULTI-TARGET (Sinks) ---
    // A. Salvataggio Silver Layer (Dati atomici arricchiti)
    val silverQuery = enrichedStream.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"$basePath/checkpoints/silver_layer")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start(s"$basePath/silver_layer")

    // B. Salvataggio Gold Layer (KPI Aggregati)
    val goldQuery = goldMetrics.writeStream
      .format("delta")
      .outputMode("complete") // Necessario per le aggregazioni
      .option("checkpointLocation", s"$basePath/checkpoints/gold_layer")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start(s"$basePath/gold_layer")

    // --- 8. INTELLIGENCE & STATEFUL ANALYSIS ---
    // Trasformazione del DataFrame in Dataset tipizzato (necessario per mapGroupsWithState)
    val meterDataset = enrichedStream
      .filter(col("temperature").isNotNull)
      .withWatermark("timestamp", "10 minutes")
      .select(
        col("LCLid"),
        col("timestamp"),
        col("consumption"),
        col("temperature").cast(DoubleType).as("temperature")
      ).as[MeterReading]

    // Applicazione della logica di Anomaly Detection per singolo contatore
    val alertStream = meterDataset
      .groupByKey(_.LCLid)
      .flatMapGroupsWithState(
        OutputMode.Append(),
        GroupStateTimeout.ProcessingTimeTimeout()
      )(detectAnomalies)

    // --- 9. ALERTING SYSTEM (Sinks) ---
    // A. Debug degli Alert a video (Real-time monitoring)
    /* Per monitorare in tempo reale gli alert generati dalla logica di anomaly detection, stampiamo a video i record di alert non appena vengono creati. Questo ci permette di verificare immediatamente se la logica di rilevamento sta funzionando correttamente e di osservare le anomalie
    val debugAlerts = alertStream.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
     */

    // Salvataggio storico Alert su Delta Lake
    val alertQuery = alertStream.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"$basePath/checkpoints/alerts")
      .start(s"$basePath/alerts_history")

    println(s"Streaming attivo. Scrittura in corso su Silver (atomico), Gold (KPI) e rilevazione delle anomalie...")

    // Attende per continuare l'esecuzione fino a quando arrivano nuovi dati
    spark.streams.awaitAnyTermination()
  }
}