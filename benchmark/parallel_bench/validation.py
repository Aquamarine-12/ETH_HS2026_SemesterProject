"""Compare complete answers outside the measured interval."""

from decimal import Decimal
import math


def numeric(value):
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def sort_cell(value):
    if value is None:
        return (0, "")
    if numeric(value):
        if math.isnan(value):
            return (1, 0)
        return (2, value)
    return (3, type(value).__name__, str(value))


def compare_results(expected, actual, relative_tolerance, absolute_tolerance):
    if len(expected) != len(actual):
        raise ValueError(f"Result row count differs: expected {len(expected)}, got {len(actual)}")
    # TPC-H ORDER BY keys can tie; compare sorted row multisets, preserving duplicates.
    key = lambda row: tuple(sort_cell(value) for value in row)
    for row_index, (left, right) in enumerate(zip(sorted(expected, key=key), sorted(actual, key=key))):
        if len(left) != len(right):
            raise ValueError("Result column count differs")
        for column, (a, b) in enumerate(zip(left, right)):
            equal = a == b
            if numeric(a) and numeric(b):
                # Integer counts/keys must match exactly; tolerate floating aggregate drift.
                if not (isinstance(a, int) and isinstance(b, int)):
                    equal = (math.isnan(a) and math.isnan(b)) or math.isclose(
                        a, b, rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
            if not equal:
                raise ValueError(f"Result mismatch at sorted row {row_index}, column {column}: {a!r} != {b!r}")
