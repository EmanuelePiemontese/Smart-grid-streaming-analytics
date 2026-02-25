import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataPreprocessor {

  def main(args: Array[String]): Unit = {

    // !!! ATTENZIONE: modificare 'basePath' con il vostro percorso locale alla cartella 'data' !!!
    val basePath = "/inserire/il/vostro/percorso/alla/cartella/data"

    val spark = SparkSession.builder()
      .appName("LondonEnergy-Preprocessing")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") // Abilita le estensioni Delta Lake
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") // Configura il catalogo per Delta Lake
      .config("spark.sql.adaptive.enabled", "true") // Ottimizzazione per gestire grandi volumi di piccoli file CSV
      .getOrCreate()

    // Impostazione del livello di log per evitare output eccessivo
    spark.sparkContext.setLogLevel("WARN")

    // --- 1. CARICAMENTO DATASET ---
    // Dataset Consumi (Full - usando wildcard)
    val energyRaw = spark.read.option("header", "true") // option permette di leggere la prima riga come intestazione
      .csv(s"$basePath/halfhourly_dataset/*.csv")
      .select("LCLid", "tstp", "energy(kWh/hh)") // Column Pruning immediato: selezione solo delle colonne necessarie

    // Dataset Metadati (Households)
    val houseRaw = spark.read.option("header", "true")
      .csv(s"$basePath/informations_households.csv")

    // Dataset Meteo (Orario)
    val weatherRaw = spark.read.option("header", "true")
      .csv(s"$basePath/weather_hourly_darksky.csv")



    // --- 2. PRE-PROCESSING CONSUMI (Dataset A) ---
    val energyCleaned = energyRaw
      // Filtering: rimozione "Null" e righe vuote (0,003% del dataset)
      .filter(
        col("energy(kWh/hh)").isNotNull &&
          !col("energy(kWh/hh)").contains("Null") &&
          col("energy(kWh/hh)") =!= ""
      )
      // Normalizzazione temporale (Parsing 27 caratteri) e cast numerico
      .withColumn("timestamp", to_timestamp(col("tstp"), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
      .withColumn("consumption", col("energy(kWh/hh)").cast(DoubleType))
      // Creazione colonna per il join meteo (troncata all'ora) per permettere un join efficiente con il dataset meteo (che è orario)
      .withColumn("join_hour", date_trunc("hour", col("timestamp")))
      .drop("tstp", "energy(kWh/hh)")

    // Verifica del risultato del pre-processing
    println(s"\n=== Esempio di record dopo pre-processing consumi ===")
    energyCleaned.show(5)

    // --- 3. PRE-PROCESSING METADATI (Dataset B) ---
    val houseCleaned = houseRaw
      .dropDuplicates("LCLid") // Misura di sicurezza per Integrità Referenziale
      .filter(!col("Acorn_grouped").contains("ACORN-")) // Rimozione di categorie non informative
      .select(
        col("LCLid").as("meter_id"), // Rinomina per chiarezza
        col("Acorn_grouped").as("acorn_group") // Selezione solo delle colonne necessarie per l'arricchimento (riduzione del dataset per il broadcast join)
      )

    // Verifica del risultato del pre-processing
    println(s"\n=== Esempio di record dopo pre-processing metadati ===")
    houseCleaned.show(5)

    // --- 4. PRE-PROCESSING METEO (Dataset C) ---
    val weatherCleaned = weatherRaw
      .withColumn("weather_hour", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
      .select(
        col("weather_hour"),                          // Colonna per il join con i consumi (troncata all'ora)
        col("temperature").cast(DoubleType),          // Temperatura in gradi Celsius
        col("apparentTemperature").cast(DoubleType),  // Temperatura percepita (wind chill o heat index)
        col("humidity").cast(DoubleType),             // Umidità relativa (0-1)
        col("precipType")                             // Per sapere se piove o nevica
      )

    // Verifica del risultato del pre-processing
    println(s"\n=== Esempio di record dopo pre-processing meteo ===")
    weatherCleaned.show(5)

    // --- 5. JOIN & ENRICHMENT (Broadcast Strategy) ---
    // Join Consumi + Metadati (Broadcast Hash Join su metadati)
    val step1DF = energyCleaned.join(broadcast(houseCleaned),
        energyCleaned("LCLid") === houseCleaned("meter_id"), "inner")
      .drop("meter_id")

    // Join Risultato + Meteo (Allineamento Orario)
    val finalEnrichedDF = step1DF.join(broadcast(weatherCleaned),
        step1DF("join_hour") === weatherCleaned("weather_hour"), "left")
      .drop("join_hour", "weather_hour")

    // --- 6. OUTPUT (Salvataggio in formato colonnare) ---
    println(s"Esempio di record arricchito e normalizzato:")
    finalEnrichedDF.show(5)

    // --- 7. SALVATAGGIO NEL SILVER LAYER (FORMATO DELTA) ---

    val silverPath = s"$basePath/silver"

    finalEnrichedDF.write
      .format("delta")
      .mode("overwrite") // Sovrascrive se esiste già (utile in fase di sviluppo)
      .option("overwriteSchema", "true")
      .save(silverPath)

    println(s"Preprocessing completato. Dati Silver salvati in: $silverPath")

    spark.stop()
  }
}