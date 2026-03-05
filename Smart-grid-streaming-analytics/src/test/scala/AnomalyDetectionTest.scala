import org.apache.spark.sql.streaming.GroupState
import org.mockito.ArgumentMatchers.argThat
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import java.sql.Timestamp
import java.time.Instant

class AnomalyDetectionTest extends AnyFunSuite with Matchers with MockitoSugar {

  import Consumer.EnergyStreamingConsumer._

  test("Rilevamento Anomalia: Z-Score elevato con clima mite deve generare Alert") {
    val meterId = "MAC000123"

    // 1. Predisposizione dello Stato (Baseline consolidata)
    // Simuliamo che il contatore abbia già 1000 letture con media 1.0 e varianza bassa
    val previousState = MeterState(
      meterId = meterId,
      mean = 1.0,
      m2 = 10.0, // Varianza molto bassa
      readingCount = 1000,
      lastTimestamp = Instant.now().toEpochMilli
    )

    // Mock del GroupState di Spark
    val mockState = mock[GroupState[MeterState]]
    when(mockState.getOption).thenReturn(Some(previousState))
    when(mockState.hasTimedOut).thenReturn(false)

    // 2. Input anomalo: Consumo 12.0 kWh (molto alto rispetto a media 1.0) con temp 20°C (Mite)
    val anomalousReading = MeterReading(
      LCLid = meterId,
      timestamp = Timestamp.from(Instant.now()),
      consumption = 12.0,
      temperature = 20.0
    )

    // 3. Esecuzione della funzione
    val alerts = detectAnomalies(meterId, Iterator(anomalousReading), mockState).toList

    // 4. Verifiche (Assertions)
    alerts should not be empty
    alerts.head.message should include("ALERT CRITICO")
    alerts.head.currentValue shouldBe 12.0

    // Verifichiamo che lo stato sia stato aggiornato correttamente
    verify(mockState).update(argThat((s: MeterState) => s.readingCount == 1001))
  }

  test("Fase di Calibrazione: Non deve generare Alert se readingCount < 600") {
    val meterId = "MAC000456"
    val lowCountState = MeterState(meterId, 1.0, 1.0, 100, Instant.now().toEpochMilli)

    val mockState = mock[GroupState[MeterState]]
    when(mockState.getOption).thenReturn(Some(lowCountState))
    when(mockState.hasTimedOut).thenReturn(false)

    val hugeReading = MeterReading(meterId, Timestamp.from(Instant.now()), 50.0, 20.0)

    val alerts = detectAnomalies(meterId, Iterator(hugeReading), mockState).toList

    // Nonostante il consumo sia folle (50.0), siamo in calibrazione (< 600)
    alerts should be (empty)
  }
}