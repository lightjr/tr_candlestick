import impl.CandleStickManagerServiceImpl
import io.mockk.every
import io.mockk.mockkStatic
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class CandleStickManagerServiceImplTest {
    @Test
    fun `when there is instrument event with type of DELETE expect empty result`() {
        var mockIsinAndQuoteEvents = mockIsinAndQuoteEvents(3)
        val isinToDelete = mockIsinAndQuoteEvents.first

        val instrumentToDelete = Instrument(isinToDelete, isinToDelete)
        var instrumentCache = ConcurrentHashMap(mutableMapOf(Pair(isinToDelete, instrumentToDelete)))
        val candleStickManager = CandleStickManagerServiceImpl(instrumentCache, ConcurrentHashMap())

        mockIsinAndQuoteEvents.second.forEach{ it -> candleStickManager.handleQuoteEvent(it.data) }

        assertEquals(1, candleStickManager.getCandlesticks(isinToDelete).size)

        candleStickManager.handleInstrumentEvent(instrumentToDelete, InstrumentEvent.Type.DELETE)

        assertEquals(0, candleStickManager.getCandlesticks(isinToDelete).size)
    }


    @Test
    fun `when instrument cache does not contain isin expect empty result`() {
        var mockIsinAndQuoteEvents = mockIsinAndQuoteEvents(3)
        val isin = mockIsinAndQuoteEvents.first

        var instrumentCache = ConcurrentHashMap(mutableMapOf<String, Instrument>())
        val candleStickManager = CandleStickManagerServiceImpl(instrumentCache, ConcurrentHashMap())

        mockIsinAndQuoteEvents.second.forEach{ it -> candleStickManager.handleQuoteEvent(it.data)}

        assertEquals(0, candleStickManager.getCandlesticks(isin).size)
    }

    @Test
    fun `when 31 minutes pass expect earliest element result size is 30 and earliest candlestick is not in list`() {
        var mockIsinAndQuoteEvents = mockIsinAndQuoteEvents(1520)
        val isin = mockIsinAndQuoteEvents.first
        val list = mockIsinAndQuoteEvents.second

        // set up instrument cache
        val instrumentCache = ConcurrentHashMap(mutableMapOf(Pair(isin, Instrument(isin, isin))))

        val candleStickManager = CandleStickManagerServiceImpl(instrumentCache, ConcurrentHashMap())

        // start from time
        var startTime = Instant.parse("2022-03-05T15:30:45.123Z")
        val delta = Duration.ofSeconds(10)

        mockkStatic("java.time.Instant")

        val quotesWithInstants = list.map {
            InstantWithQuoteEvent(startTime, it).also { startTime = startTime.plus(delta) }
        }.toList()

        // send every 10 seconds
        quotesWithInstants.forEach {
            every { Instant.now() } returns it.instant
            candleStickManager.handleQuoteEvent(it.event.data)
        }

        val candlesticks = candleStickManager.getCandlesticks(isin)
        assertEquals(30, candlesticks.size)
        assertNull(candlesticks.find { it.openTimestamp == startTime })
    }

    @Test
    fun `when add elements in 2 minutes expect there will be 2 elements (happy path unique isin)`() {
        val codeAndQuotesEvent = mockIsinAndQuoteEvents(10)
        val isin = codeAndQuotesEvent.first

        val instrumentCache = ConcurrentHashMap(mutableMapOf(Pair(isin, Instrument(isin, isin))))
        val candleStickManager = CandleStickManagerServiceImpl(instrumentCache, ConcurrentHashMap())


        var startTime = Instant.parse("2022-03-05T15:30:45.123Z")
        val delta = Duration.ofSeconds(10)

        mockkStatic("java.time.Instant")

        val quotesWithInstants = codeAndQuotesEvent.second.map {
            InstantWithQuoteEvent(startTime, it).also { startTime = startTime.plus(delta) }
        }.toList()

        // send every 10 seconds
        quotesWithInstants.forEach {
            every { Instant.now() } returns it.instant
            candleStickManager.handleQuoteEvent(it.event.data)
        }

        assertEquals(2, candleStickManager.getCandlesticks(isin).size)
    }
}

/**
 * Generate pair of ISIN and list of quote events
 */
private fun mockIsinAndQuoteEvents(amount: Int): Pair<String, List<QuoteEvent>> {
    // generate a isin
    val isIn = "T" + amount.toString() + "R"
    return Pair(isIn, (1..amount).map {
        QuoteEvent(
            Quote(
                isIn,
                // generate different prices
                (if (it % 20 == 0) it - 1 else it + 2).toDouble()
            )
        )
    }.toList())
}

data class InstantWithQuoteEvent(
    val instant: Instant,
    val event: QuoteEvent
)