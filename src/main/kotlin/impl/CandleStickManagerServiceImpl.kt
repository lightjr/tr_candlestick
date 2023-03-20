package impl

import Instrument
import Candlestick
import CandlestickManager
import InstrumentEvent
import Quote
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

private const val MAX_CANDLE_SIZE = 30;

class CandleStickManagerServiceImpl(
    private val instrumentCache: ConcurrentHashMap<String, Instrument>,
    private val quoteCache: ConcurrentHashMap<String, MutableList<Candlestick>>
) : CandlestickManager {
    override fun getCandlesticks(isin: String): List<Candlestick> {
        return quoteCache.getOrDefault(isin, emptyList())
    }

    override fun handleInstrumentEvent(instrument: Instrument, eventType: InstrumentEvent.Type) {
        if(eventType == InstrumentEvent.Type.ADD){
            println("Add instrument with isin ${instrument.isin}")
            instrumentCache[instrument.isin] = instrument
        }else if(eventType == InstrumentEvent.Type.DELETE){
            instrumentCache.remove(instrument.isin)
            quoteCache.remove(instrument.isin)
        }
    }

    override fun handleQuoteEvent(quote: Quote) {
        println("Check instrument with isin : ${quote.isin}")
        if(instrumentCache.containsKey(quote.isin)){
            val candles: MutableList<Candlestick> = quoteCache.getOrDefault(quote.isin, ArrayList())

            val quoteTimestamp = Instant.now()
            println("Now $quote.timestamp.")
            //get last quote
            var previousQuote = candles.findLast {
                Duration.between(it.openTimestamp,quoteTimestamp) < Duration.ofMinutes(1)
            }

            if(previousQuote != null){
                previousQuote.lowPrice = minOf(quote.price,previousQuote.lowPrice)
                previousQuote.highPrice = maxOf(quote.price,previousQuote.highPrice)
                previousQuote.closeTimestamp = quoteTimestamp
                previousQuote.closingPrice = quote.price
                candles[candles.lastIndex] = previousQuote
            }else{
                println("No candlestick within a minute. Create new one")
                 val newCandleStick = Candlestick(
                    openTimestamp = quoteTimestamp,
                    closeTimestamp = quoteTimestamp,
                    openPrice = quote.price,
                    highPrice = quote.price,
                    lowPrice = quote.price,
                    closingPrice = quote.price
                )
                candles.add(newCandleStick)
            }

            if (candles.size > MAX_CANDLE_SIZE) {
                candles.minByOrNull { it.openTimestamp }
                    ?.also {
                        candles.remove(it)
                    }
            }

            quoteCache[quote.isin] = candles
        }else{
            println("no instrument found with this [${quote.isin}]")
        }
    }
}


