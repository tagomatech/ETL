"""Exceptions raised by the barchart-data client."""

from __future__ import annotations

from typing import Any


class BarchartDataError(Exception):
    """Base class for package errors."""


class BarchartAuthenticationError(BarchartDataError):
    """Raised when an authenticated API call has no API key."""


class BarchartTransportError(BarchartDataError):
    """Raised when an HTTP request cannot be completed."""


class BarchartDecodeError(BarchartDataError):
    """Raised when an API response cannot be decoded."""


class BarchartPublicPageError(BarchartDataError):
    """Raised when a public Barchart quote page cannot be read."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url


class BarchartAPIError(BarchartDataError):
    """Raised when Barchart returns an API or HTTP error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        endpoint: str | None = None,
        payload: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint
        self.payload = payload
