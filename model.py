from dataclasses import dataclass
from datetime import datetime, timezone
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
    type: str


# '2021-09-28T14:07:54.855700'
def event_from_dict(d) -> OpenseaEvent:
    event = OpenseaEvent(
        time=datetime.strptime(
            d["created_date"], "%Y-%m-%dT%H:%M:%S.%f"
        ),
        collection=d["collection_slug"],
        price_wei=int(d["total_price"]),
        id=int(d["id"]),
        type=d["event_type"]
    )
    event.time = event.time.replace(tzinfo=timezone.utc)
    return event
