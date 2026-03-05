package Analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BusinessInsightsEngine {

  def main(args: Array[String]): Unit = {

    // Configurazione SparkSession con supporto Delta Lake
    val spark = SparkSession.builder()
      .appName("London-Energy-Analytics")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // !!! ATTENZIONE: modificare 'basePath' con il vostro percorso locale alla cartella 'data' !!!
    val basePath = "/inserire/il/vostro/percorso/alla/cartella/data"

    // Caricamento Layer
    val silverDF = spark.read.format("delta").load(s"$basePath/silver_layer")
    val goldDF = spark.read.format("delta").load(s"$basePath/gold_layer")
    val alertsDF = spark.read.format("delta").load(s"$basePath/alerts_history")

    // =========================================================================
    // 1. SILVER LAYER: ANALISI DEL COMPORTAMENTO ATOMICO (SCIENTIFICA)
    // =========================================================================
    println("\n" + "#"*20 + " [1] SILVER LAYER ANALYSIS " + "#"*20)

    // A. Analisi di Elasticità Termica (Thermal Sensitivity)
    println("\n1.A - Sensibilità Climatica (kWh per ogni grado di variazione):")
    silverDF.groupBy("acorn_group")
      .agg(
        corr("consumption", "temperature").as("pearson_corr"),
        (stddev("consumption") / stddev("temperature")).as("thermal_slope")
      ).orderBy("pearson_corr").show()

    // B. Analisi della Volatilità Comportamentale (Coefficient of Variation)
    println("\n1.B - Imprevedibilità del Consumo (CoV):")
    silverDF.groupBy("acorn_group")
      .agg((stddev("consumption") / avg("consumption")).as("volatility_index"))
      .orderBy(desc("volatility_index")).show()

    // C. Analisi delle Abitudini (Weekend vs Weekday Shift)
    println("\n1.C - Differenza di Consumo Weekend vs Feriale (%):")
    silverDF.withColumn("is_weekend", when(date_format($"timestamp", "E").isin("Sat", "Sun"), 1).otherwise(0))
      .groupBy("acorn_group").pivot("is_weekend").avg("consumption")
      .withColumn("weekend_shift_pct", (($"1" - $"0") / $"0") * 100).show()



    // =========================================================================
    // 2. GOLD LAYER: ANALISI DELLA RETE URBANA (STRATEGICA)
    // =========================================================================
    println("\n" + "#"*20 + " [2] GOLD LAYER ANALYSIS " + "#"*20)

    // A. Analisi dello Stress della Rete (Peak-to-Average Ratio)
    println("\n2.A - Indice Stress di Rete (Peak-to-Average Ratio):")
    goldDF.withColumn("hour", hour($"window.start"))
      .groupBy("hour").agg((max("total_load") / avg("total_load")).as("PAR_index"))
      .orderBy(desc("PAR_index")).show(5)

    // B. Efficienza della Distribuzione (Load per Active Meter)
    println("\n2.B - Carico Medio per Contatore Attivo (Uso della Rete):")
    goldDF.withColumn("hour", hour($"window.start"))
      .groupBy("acorn_group", "hour")
      .agg((sum("total_load") / sum("active_meters")).as("kwh_per_meter"))
      .orderBy(desc("kwh_per_meter")).show(5)

    // C. Analisi delle Anomalie Aggregate (Gold Outliers)
    println("\n2.C - Finestre Temporali con Consumo Anomalo (> 2 stddev):")
    val goldStats = goldDF.select(avg("total_load").as("avg_l"), stddev("total_load").as("std_l")).first()
    goldDF.filter($"total_load" > (goldStats.getDouble(0) + 2 * goldStats.getDouble(1)))
      .select("window.start", "window.end", "total_load", "avg_temp").show(5)



    // =========================================================================
    // 3. ALERT LAYER: ANALISI DIAGNOSTICA (OPERATIVA)
    // =========================================================================
    println("\n" + "#"*20 + " [3] ALERT LAYER ANALYSIS " + "#"*20)

    // A. Analisi della Severità (Anomaly Magnitude)
    println("\n3.A - Severità degli Alert (Valore Attuale / Media Attesa):")
    alertsDF.withColumn("severity", $"currentValue" / $"expectedAvg")
      .groupBy("LCLid").agg(avg("severity").as("avg_severity"), count("*").as("alert_count"))
      .orderBy(desc("avg_severity")).show(5)

    // B. Correlazione Climatica degli Alert (Weather-Driven Anomalies)
    println("\n3.B - Distribuzione Alert in base alla Temperatura:")
    alertsDF.withColumn("temp_bin", round($"temperature" / 5) * 5)
      .groupBy("temp_bin").count().orderBy("temp_bin").show()

    // C. Root Cause Social Analysis (Chi genera più alert critici?)
    println("\n3.C - Classifica Anomalie per Gruppo Acorn (Unione con Silver):")
    alertsDF.as("a").join(silverDF.as("s"), "LCLid")
      .groupBy("acorn_group").count().orderBy(desc("count")).show()



    spark.stop()
  }
}