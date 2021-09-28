from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass()
class OpenseaEvent:
    class EventType(Enum):
        SALE = "sale"

    # The time of the event
    time: datetime
    # The name of the collection the event refers to (such as an art collection, e.g. Bored Ape Yacht Club)
    collection: str
    # The recorded price
    price_wei: int
    # ID of the event, used for deduplication
    id: int
    # The type of the event, e.g. sale, bid, etc
    type: EventType



