"""Shared types for query observations and process messages.

Typed dictionaries describe the stable JSON/CSV measurement schema.
Enums name experiment states without changing their serialized string values.
Flexible metadata uses JsonObject; database result cells keep native types.
"""

import dataclasses
import enum
from typing import Any, NotRequired, Protocol, Self, TypeAlias, TypedDict

JsonObject: TypeAlias = dict[str, Any]
ResultRows: TypeAlias = list[tuple[Any, ...]]


class BackendKind(enum.StrEnum):
    """Supported execution engines."""

    DUCKDB = "duckdb"
    SIRIUS = "sirius"


class Condition(enum.StrEnum):
    """Conditions measured once per experiment repetition."""

    ISOLATED_0 = "isolated_0"
    ISOLATED_1 = "isolated_1"
    CONCURRENT = "concurrent"

    def slots(self) -> list[int]:
        """Returns the submission identities participating in this condition."""
        if self == Condition.CONCURRENT:
            return [0, 1]
        return [0] if self == Condition.ISOLATED_0 else [1]


class SampleStatus(enum.StrEnum):
    """Outcome of a query observation."""

    OK = "ok"
    ERROR = "error"
    VALIDATION_ERROR = "validation_error"


class RunStatus(enum.StrEnum):
    """Supervisor lifecycle and terminal outcomes."""

    RUNNING = "running"
    COMPLETE = "complete"
    ERROR = "error"
    TIMEOUT = "timeout"
    INTERRUPTED = "interrupted"


class EventKind(enum.StrEnum):
    """Messages sent from the engine process to its supervisor."""

    PHASE = "phase"
    METADATA = "metadata"
    BATCH = "batch"
    DONE = "done"
    ERROR = "error"


class Query(TypedDict):
    """A fixed SQL instance assigned to a distinct submission slot."""

    slot: int
    label: str
    sql: str
    sha256: str


class Sample(TypedDict):
    """One query observation; failed observations may lack timing fields."""

    slot: int
    label: str
    status: SampleStatus
    thread_id: int
    error: NotRequired[str]
    arrival_ns: NotRequired[int]
    release_ns: NotRequired[int]
    start_ns: NotRequired[int]
    finish_ns: NotRequired[int]
    completion_s: NotRequired[float]
    execute_fetch_s: NotRequired[float]
    rows: NotRequired[int]


class Batch(TypedDict):
    """All observations from one isolated or concurrent trial."""

    condition: Condition
    repetition: int
    warmup: bool
    records: list[Sample]


class PairedLatency(TypedDict):
    """Explicit per-query inputs and result of a slowdown calculation."""

    repetition: int
    slot: int
    label: str
    isolated_latency_s: float
    concurrent_completion_s: float
    slowdown: float


class Distribution(TypedDict):
    """Descriptive statistics for a nonempty series."""

    count: int
    median: float
    mean: float
    min: float
    max: float
    stdev: float


class QueryConnection(Protocol):
    """Small database interface used by the timed worker and test doubles."""

    def execute(self, query: str) -> Self:
        """Executes SQL on this connection."""
        ...

    def fetchall(self) -> ResultRows:
        """Materializes the entire query result."""
        ...

    def close(self) -> None:
        """Closes this connection without closing sibling connections."""
        ...


class ConnectionFactory(Protocol):
    """Creates independent connections to a shared database instance."""

    def connection(self) -> QueryConnection:
        """Creates and configures a connection for the calling worker."""
        ...


@dataclasses.dataclass(frozen=True)
class Message:
    """Picklable envelope for communication with the supervisor."""

    kind: EventKind
    payload: JsonObject = dataclasses.field(default_factory=dict)
