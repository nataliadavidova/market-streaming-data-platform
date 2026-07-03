from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints


NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
PositiveDecimal = Annotated[Decimal, Field(gt=0)]
PositiveInt = Annotated[int, Field(gt=0)]


class TradeEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: NonEmptyString
    symbol: NonEmptyString
    trade_id: NonEmptyString
    price: PositiveDecimal
    quantity: PositiveDecimal
    event_time_ms: PositiveInt
    ingested_at_ms: PositiveInt

    def to_json_message(self) -> str:
        return self.model_dump_json()
