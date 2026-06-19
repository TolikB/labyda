from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_DOWN


def quantize_down(value: float | str | Decimal, step: float | str | Decimal) -> Decimal:
    """Round a value down to the nearest exchange-supported increment."""
    decimal_value = Decimal(str(value))
    decimal_step = Decimal(str(step))
    if decimal_step <= 0:
        raise ValueError("step must be positive")
    return (decimal_value / decimal_step).to_integral_value(rounding=ROUND_DOWN) * decimal_step


def quantize_up(value: float | str | Decimal, step: float | str | Decimal) -> Decimal:
    """Round a positive value up to the nearest exchange-supported increment."""
    decimal_value = Decimal(str(value))
    decimal_step = Decimal(str(step))
    if decimal_value < 0 or decimal_step <= 0:
        raise ValueError("value must be non-negative and step must be positive")
    return (decimal_value / decimal_step).to_integral_value(rounding=ROUND_CEILING) * decimal_step
