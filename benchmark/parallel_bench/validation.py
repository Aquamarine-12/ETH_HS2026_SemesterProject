"""Compares full answers outside the measured query interval.

Sorted rows preserve duplicate counts while tolerating ordering differences.
Integer values remain exact; other numeric values allow configurable drift.
The CPU reference is a consistency check, not an official TPC-H oracle.
"""

import decimal
import math
from typing import Any, TypeGuard

from parallel_bench import models

Numeric = int | float | decimal.Decimal


def _numeric(value: object) -> TypeGuard[Numeric]:
    """Identifies numeric result values without treating booleans as numbers."""
    return isinstance(value, (int, float, decimal.Decimal)) and not isinstance(
        value, bool
    )


def _sort_cell(value: object) -> tuple[Any, ...]:
    """Builds a comparable key for SQL NULL, numeric and scalar values."""
    if value is None:
        return (0, "")
    if _numeric(value):
        return (1, 0) if math.isnan(value) else (2, value)
    return (3, type(value).__name__, str(value))


def _sort_row(row: tuple[Any, ...]) -> tuple[tuple[Any, ...], ...]:
    """Orders a row lexicographically without changing the stored result."""
    return tuple(_sort_cell(value) for value in row)


def _equal(
    left: object, right: object, relative: float, absolute: float
) -> bool:
    """Compares integer counts exactly and approximate aggregates tolerantly."""
    if _numeric(left) and _numeric(right):
        if not (isinstance(left, int) and isinstance(right, int)):
            return (math.isnan(left) and math.isnan(right)) or math.isclose(
                left, right, rel_tol=relative, abs_tol=absolute
            )
    return bool(left == right)


def compare_results(
    expected: models.ResultRows,
    actual: models.ResultRows,
    relative_tolerance: float,
    absolute_tolerance: float,
) -> None:
    """Raises ValueError if result multisets differ beyond numeric tolerance."""
    if len(expected) != len(actual):
        raise ValueError(
            f"Result row count differs: expected {len(expected)}, "
            f"got {len(actual)}"
        )
    pairs = zip(sorted(expected, key=_sort_row), sorted(actual, key=_sort_row))
    for row_index, (left, right) in enumerate(pairs):
        if len(left) != len(right):
            raise ValueError("Result column count differs")
        for column, (a, b) in enumerate(zip(left, right)):
            if not _equal(a, b, relative_tolerance, absolute_tolerance):
                raise ValueError(
                    f"Result mismatch at sorted row {row_index}, "
                    f"column {column}: {a!r} != {b!r}"
                )
