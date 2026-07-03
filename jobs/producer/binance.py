from jobs.producer.events import TradeEvent


def parse_binance_trade_message(
    raw_message: dict[str, object],
    ingested_at_ms: int,
) -> TradeEvent:
    return TradeEvent(
        exchange="binance",
        symbol=raw_message["s"],
        trade_id=str(raw_message["t"]),
        price=raw_message["p"],
        quantity=raw_message["q"],
        event_time_ms=raw_message["T"],
        ingested_at_ms=ingested_at_ms,
    )
