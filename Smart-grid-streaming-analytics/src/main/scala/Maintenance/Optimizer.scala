package Maintenance

import org.apache.spark.sql.SparkSession
import io.delta.tables._

object Optimizer {

  def main(args: Array[String]): Unit = {

    // Configurazione SparkSession con supporto Delta Lake
    val spark = SparkSession.builder()
      .appName("Delta-Maintenance-Job")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // !!! ATTENZIONE: modificare 'basePath' con il vostro percorso locale alla cartella 'data' !!!
    val basePath = "/inserire/il/vostro/percorso/alla/cartella/data"

    // Elenco delle tabelle Delta da ottimizzare e pulire
    val tables = Seq("silver_layer", "gold_layer", "alerts_history")

    println("=== AVVIO MANUTENZIONE PROGRAMMATA ===")

    // Itera su ogni tabella, eseguendo ottimizzazione e pulizia
    tables.foreach { tableName =>
      val path = s"$basePath/$tableName"
      println(s"Ottimizzazione tabella: $tableName...")

      // Carica la tabella Delta
      val deltaTable = DeltaTable.forPath(spark, path)

      // Compattazione file
      deltaTable.optimize().executeCompaction()

      // Pulizia versioni vecchie (Retention 7 giorni)
      // Nota: in locale/test puoi forzare una retention più bassa se serve spazio
      deltaTable.vacuum(168.0)
    }

    println("=== MANUTENZIONE COMPLETATA CON SUCCESSO ===")
    spark.stop()
  }
}