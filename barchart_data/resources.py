"""Resource namespaces built on the generic Barchart transport."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, TYPE_CHECKING

from .client import OutputFormat, ResponsePayload

if TYPE_CHECKING:
    from .client import BarchartDataClient


class _Resource:
    def __init__(self, client: "BarchartDataClient") -> None:
        self.client = client

    def _request(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        output: OutputFormat,
    ) -> ResponsePayload:
        return self.client.request(endpoint, params=params, output=output)


class MarketResource(_Resource):
    """Quotes and historical time series."""

    def quote(
        self,
        symbols: str | Iterable[str],
        *,
        fields: str | Iterable[str] | None = None,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        query = {"symbols": _join_values(symbols), **params}
        if fields is not None:
            query["fields"] = _join_values(fields)
        return self._request("getQuote", query, output)

    def history(
        self,
        symbol: str,
        *,
        frequency: str = "daily",
        start_date: str | None = None,
        end_date: str | None = None,
        max_records: int | None = None,
        interval: int | None = None,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        query: dict[str, Any] = {
            "symbol": symbol,
            "type": frequency,
            **params,
        }
        _set_if_not_none(query, "startDate", start_date)
        _set_if_not_none(query, "endDate", end_date)
        _set_if_not_none(query, "maxRecords", max_records)
        _set_if_not_none(query, "interval", interval)
        return self._request("getHistory", query, output)

    def close_price(
        self,
        symbols: str | Iterable[str],
        date: str,
        *,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        query = {"symbols": _join_values(symbols), "date": date, **params}
        return self._request("getClosePrice", query, output)


class FundamentalResource(_Resource):
    """Company profiles, statements, ratios, and related data."""

    def balance_sheets(
        self,
        symbols: str | Iterable[str],
        *,
        frequency: str = "Quarter",
        count: int | None = None,
        raw_data: bool = True,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        return self._statement(
            "getBalanceSheets",
            symbols,
            frequency=frequency,
            count=count,
            raw_data=raw_data,
            output=output,
            params=params,
        )

    def income_statements(
        self,
        symbols: str | Iterable[str],
        *,
        frequency: str = "Quarter",
        count: int | None = None,
        raw_data: bool = True,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        return self._statement(
            "getIncomeStatements",
            symbols,
            frequency=frequency,
            count=count,
            raw_data=raw_data,
            output=output,
            params=params,
        )

    def cash_flow(
        self,
        symbols: str | Iterable[str],
        *,
        frequency: str = "Quarter",
        count: int | None = None,
        raw_data: bool = True,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        return self._statement(
            "getCashFlow",
            symbols,
            frequency=frequency,
            count=count,
            raw_data=raw_data,
            output=output,
            params=params,
        )

    def _statement(
        self,
        endpoint: str,
        symbols: str | Iterable[str],
        *,
        frequency: str,
        count: int | None,
        raw_data: bool,
        output: OutputFormat,
        params: Mapping[str, Any],
    ) -> ResponsePayload:
        query = {
            "symbols": _join_values(symbols),
            "frequency": frequency,
            "rawData": int(raw_data),
            **params,
        }
        _set_if_not_none(query, "count", count)
        return self._request(endpoint, query, output)


class MetadataResource(_Resource):
    """Instrument and futures reference data."""

    def instrument_definition(
        self,
        *,
        symbols: str | Iterable[str] | None = None,
        exchange: str | None = None,
        mic: str | None = None,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        query = dict(params)
        if symbols is not None:
            query["symbols"] = _join_values(symbols)
        _set_if_not_none(query, "exchange", exchange)
        _set_if_not_none(query, "mic", mic)
        return self._request("getInstrumentDefinition", query, output)

    def futures_specifications(
        self,
        *,
        symbols: str | Iterable[str] | None = None,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        query = dict(params)
        if symbols is not None:
            query["symbols"] = _join_values(symbols)
        return self._request("getFuturesSpecifications", query, output)

    def futures_expirations(
        self,
        *,
        symbols: str | Iterable[str] | None = None,
        output: OutputFormat = "df",
        **params: Any,
    ) -> ResponsePayload:
        query = dict(params)
        if symbols is not None:
            query["symbols"] = _join_values(symbols)
        return self._request("getFuturesExpirations", query, output)


def _join_values(values: str | Iterable[str]) -> str:
    if isinstance(values, str):
        normalized = values.strip()
        if not normalized:
            raise ValueError("symbol values must not be empty")
        return normalized
    result = ",".join(str(value).strip() for value in values if str(value).strip())
    if not result:
        raise ValueError("symbol values must not be empty")
    return result


def _set_if_not_none(target: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        target[key] = value
