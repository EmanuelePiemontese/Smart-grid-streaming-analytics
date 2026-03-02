package RealTime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object RealTimeAlertManager {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTime-Alert-Handler")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // CORREZIONE: Silenzia i log di Spark per vedere solo i tuoi output
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val basePath = "/Users/emanuelepiemontese/data"

    // --- 1. LEGGERE IL FLUSSO DEGLI ALERT ---
    val liveAlerts = spark.readStream
      .format("delta")
      .load(s"$basePath/alerts_history")

    // --- 2. LOGICA DI REAZIONE ---
    val criticalAlerts = liveAlerts
      // Se vuoi vedere TUTTI gli alert che arrivano nella history, commenta il filtro sotto
      // .filter($"currentValue" > 10.0)
      .select(
        $"LCLid",
        $"timestamp",
        $"message",
        $"temperature",
        current_timestamp().as("processed_at")
      )

    // --- 3. OUTPUT CON CONTROLLO BATCH VUOTI ---
    val notificationQuery = criticalAlerts.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        // Controlliamo se il batch contiene dati
        if (!batchDF.isEmpty) {
          println(s"--- NUOVI ALERT RILEVATI (Batch: $batchId) ---")
          batchDF.show(false)
        }
        // Se è vuoto, non facciamo nulla (niente stampe in console)
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    notificationQuery.awaitTermination()
  }
}