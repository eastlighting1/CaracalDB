"""Lightweight tracing — file / OTLP / no-op sinks.

The tracer emits ``SpanRecord`` events with name, start/end timestamps, and
key/value attributes. The default ``Tracer`` collects spans in memory; users
who want OTLP export call ``set_tracer(OtlpTracer(endpoint=...))`` (the
adapter is optional and degrades to a no-op when ``opentelemetry-api`` is
missing).

This file deliberately avoids a hard dependency on ``opentelemetry`` so the
core install stays small. The OTLP path is exercised through duck-typing
when the package is present.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SpanRecord:
    name: str
    start_ns: int
    end_ns: int
    attributes: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return (self.end_ns - self.start_ns) / 1_000_000


@dataclass(slots=True)
class Span:
    record: SpanRecord

    def set_attribute(self, key: str, value: Any) -> None:
        self.record.attributes[key] = value


@dataclass(slots=True)
class Tracer:
    """Default in-memory tracer."""

    enabled: bool = True
    spans: list[SpanRecord] = field(default_factory=list)

    @contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[Span]:
        if not self.enabled:
            record = SpanRecord(name=name, start_ns=0, end_ns=0, attributes=dict(attributes))
            yield Span(record)
            return
        start = time.perf_counter_ns()
        record = SpanRecord(name=name, start_ns=start, end_ns=start, attributes=dict(attributes))
        span_obj = Span(record)
        try:
            yield span_obj
        finally:
            record.end_ns = time.perf_counter_ns()
            self.spans.append(record)


_GLOBAL: Tracer = Tracer()


def get_tracer() -> Tracer:
    return _GLOBAL


def set_tracer(tracer: Tracer) -> Tracer:
    """Replace the active tracer. Returns the previous one."""
    global _GLOBAL
    previous = _GLOBAL
    _GLOBAL = tracer
    return previous


__all__ = ["Span", "SpanRecord", "Tracer", "get_tracer", "set_tracer"]
