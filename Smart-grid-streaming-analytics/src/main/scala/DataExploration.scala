import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object DataExploration {
  def main(args: Array[String]): Unit = {

    // !!! ATTENZIONE: modificare 'basePath' con il vostro percorso locale alla cartella 'data' !!!
    val basePath = "/inserire/il/vostro/percorso/alla/cartella/data"

    // Inizializzazione di SparkSession
    val spark = SparkSession.builder()
      .appName("LondonEnergy-EDA")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true") // Ottimizzazione per gestire grandi volumi di piccoli file CSV
      .getOrCreate()

    // Impostazione del livello di log per evitare output eccessivo
    spark.sparkContext.setLogLevel("WARN")

    println("\n=== Avvio Analisi su INTERO Dataset ===")

    // 1. Caricamento di TUTTI i blocchi (circa 167M di righe)
    // Usiamo il carattere jolly '*' per includere tutti i file .csv nella cartella
    val rawEnergyDF = spark.read
      .option("header", "true") // option permette di leggere la prima riga come intestazione
      .csv(s"$basePath/halfhourly_dataset/*.csv")

    // Nota: Spark non carica i dati in memoria finché non chiamiamo un'azione (come count o show)
    val totalRows = rawEnergyDF.count()
    println(s"Totale record nel dataset completo: $totalRows")

    println("\n=== Schema Dataset Raw (Verifica su tutti i file) ===")
    rawEnergyDF.printSchema()

    // 2. Verifica Valori Nulli/Vuoti/Sospetti
    // Sui dati grezzi, i nulli possono essere stringhe "Null" o campi vuoti
    println("\n=== Qualità dei Dati (Null o stringhe 'Null') ===")
    rawEnergyDF.select(
      count(when(col("energy(kWh/hh)").isNull ||
        col("energy(kWh/hh)") === "" ||
        col("energy(kWh/hh)").contains("Null"), 1)).alias("Missing_Energy"),
      count(when(col("tstp").isNull, 1)).alias("Missing_Timestamp"),
      count(when(col("LCLid").isNull, 1)).alias("Missing_LCLid")
    ).show()

    // 3. Distribuzione Consumi (Top 10 valori grezzi)
    println("\n=== Top 10 Valori Consumo Grezzi ===")
    rawEnergyDF.groupBy("energy(kWh/hh)")
      .count()
      .orderBy(desc("count"))
      .show(10)

    // 4. Analisi Temporale Completa
    println("\n=== Estensione Temporale del Dataset (Min/Max) ===")
    rawEnergyDF.select(
      min("tstp").alias("Data Inizio"),
      max("tstp").alias("Data Fine")
    ).show()

    // 5. Verifica Formati Timestamp su scala globale
    println("\n=== Consistenza Formato Timestamp (Lunghezza stringa) ===")
    rawEnergyDF.select(length(col("tstp")).alias("len_tstp"))
      .groupBy("len_tstp")
      .count()
      .orderBy(desc("count"))
      .show()

    // 6. Analisi Metadati Acorn (Dataset statico piccolo)
    val rawHouseholdDF = spark.read.option("header", "true")
      .csv(s"$basePath/informations_households.csv")

    println("\n=== Distribuzione Gruppi Acorn (Intera Popolazione) ===")
    rawHouseholdDF.groupBy("Acorn_grouped")
      .count()
      .orderBy(desc("count"))
      .show()

    // 7. Test di Integrità: Case senza metadati
    // Verifichiamo se esistono LCLid nel dataset energetico non censiti nei metadati
    println("\n=== Verifica Referenziale: Case senza metadati ===")
    val orphanHouses = rawEnergyDF.select("LCLid").distinct()
      .join(rawHouseholdDF, Seq("LCLid"), "left_anti")
      .count()
    println(s"Numero di LCLid orfani (senza corrispondenza in metadati): $orphanHouses")

    spark.stop()
  }
}