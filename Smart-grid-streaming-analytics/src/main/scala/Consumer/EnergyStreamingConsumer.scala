package Consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object EnergyStreamingConsumer {

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

    // --- 1. CARICAMENTO DATASET STATICI (Per l'Arricchimento) ---

    // Metadati Households (Broadcast Join) (Dataset B) ---
    val houseStatic = spark.read.option("header", "true")
      .csv(s"$basePath/informations_households.csv")
      .dropDuplicates("LCLid") // Misura di sicurezza per Integrità Referenziale
      .filter(!col("Acorn_grouped").contains("ACORN-")) // Rimozione di categorie non informative
      .select(
        col("LCLid").as("meter_id"), // Rinomina per chiarezza
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

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "energy-readings")
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
    val enrichedStream = energyParsed
      .join(broadcast(houseStatic), $"LCLid" === $"meter_id", "inner")
      .join(broadcast(weatherStatic), $"join_hour" === $"weather_hour", "left")
      .drop("meter_id", "weather_hour", "join_hour")

    // --- 5. DEBUG A VIDEO (Temporaneo) ---
    val debugQuery = enrichedStream.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // --- 6. SCRITTURA SU DELTA LAKE (Silver Layer) ---
    val silverPath = s"$basePath/silver_layer"
    val checkpointPath = s"$basePath/checkpoints/silver_layer"

    val query = enrichedStream.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(silverPath)

    println(s"Streaming attivo. I dati arricchiti vengono salvati in: $silverPath")
    query.awaitTermination()
  }
}