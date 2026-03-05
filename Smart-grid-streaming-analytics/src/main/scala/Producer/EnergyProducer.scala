package Producer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.io.File
import java.util.Properties
import scala.io.Source

// Case class per strutturare il messaggio JSON
// La case class rappresenta la struttura dei dati che vogliamo inviare a Kafka, con i campi LCLid, tstp ed energy
case class EnergyRecord(
                         LCLid: String,
                         tstp: String,
                         energy: String
                       )

object EnergyProducer {

  def main(args: Array[String]): Unit = {

    // !!! ATTENZIONE: modificare 'basePath' con il vostro percorso locale alla cartella 'data/halfhourly_dataset' !!!
    val basePath = "/inserire/il/vostro/percorso/alla/cartella/data/halfhourly_dataset"

    // Nome del topic Kafka su cui inviare i dati (assicurarsi che il topic esista già nel cluster Kafka)
    val topicName = "energy-readings"

    // Configurazione del bootstrap server Kafka (modificare se il server Kafka è su un host o porta diversa)
    val bootstrapServers = "localhost:9092"

    // 1. Configurazione Producer
    // Creazione delle proprietà per il KafkaProducer
    val props = new Properties()
    // Configurazione del bootstrap server Kafka: specifica l'indirizzo del cluster Kafka a cui il producer si connetterà per inviare i messaggi
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    // Configurazione dei serializer per chiave e valore (usiamo StringSerializer per entrambi): la chiave sarà l'LCLid (identificativo univoco), quindi usiamo StringSerializer per la chiave
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // Il valore sarà una stringa JSON, quindi usiamo StringSerializer anche per il valore: il messaggio JSON conterrà i campi LCLid, tstp ed energy, quindi lo serializziamo come stringa
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // Creazione del KafkaProducer con le proprietà configurate
    val producer = new KafkaProducer[String, String](props)

    // Mapper per convertire la case class in JSON string
    val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)

    println(s"Inizio invio dati al topic: $topicName")

    try {
      // 2. Lettura dei file CSV e creazione dei messaggi JSON
      // Elenco dei file CSV (block_N.csv), filtrando solo quelli che terminano con ".csv" e ordinandoli per nome
      val files = new File(basePath).listFiles().filter(_.getName.endsWith(".csv")).sorted

      for (file <- files) {
        // Stampa del nome di ogni file che viene elaborato, per monitorare l'avanzamento dell'invio dei dati
        println(s"Elaborazione file: ${file.getName}")
        // Lettura del file CSV riga per riga, saltando la prima riga (intestazione) e creando un messaggio JSON per ogni record
        val source = Source.fromFile(file)
        val lines = source.getLines().drop(1) // Salta l'intestazione CSV

        // Per ogni riga del file, split dei campi, creazione di un'istanza di EnergyRecord e serializzazione in JSON
        lines.foreach { line =>
          val cols = line.split(",").map(_.trim)
          if (cols.length == 3) {
            val record = EnergyRecord(cols(0), cols(1), cols(2))
            val jsonMessage = jsonMapper.writeValueAsString(record)

            // 3. Invio a Kafka
            val kafkaRecord = new ProducerRecord[String, String](topicName, record.LCLid, jsonMessage)
            producer.send(kafkaRecord)

            // Opzionale: piccolo delay per simulare il tempo reale (es. 1ms ogni riga)
            // Thread.sleep(1)
          }
        }
        source.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
      println("Invio completato e Producer chiuso.")
    }
  }
}