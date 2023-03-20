import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import impl.CandleStickManagerServiceImpl
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

private val logger = LoggerFactory.getLogger("MainKt")

fun main() {
  logger.info("starting up")

  val server = Server()
  val instrumentStream = InstrumentStream()
  val quoteStream = QuoteStream()
  val instrumentsCache = ConcurrentHashMap<String, Instrument>()
  val quotesCache = ConcurrentHashMap<String,MutableList<Candlestick>>()
  val candlestickManager = CandleStickManagerServiceImpl(instrumentsCache,quotesCache)


  instrumentStream.connect { event ->
    logger.info("Instrument: {}", event)
    candlestickManager.handleInstrumentEvent(event.data,event.type)
  }

  quoteStream.connect { event ->
    logger.info("Quote: {}", event)
    candlestickManager.handleQuoteEvent(event.data)
  }

  server.service = candlestickManager
  server.start()
}

val jackson: ObjectMapper =
  jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
