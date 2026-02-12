"""Database client and source adapters."""

from .client import DuckDBClient
from .adapters import CHANNEL_MAP, normalize_channel

__all__ = ["DuckDBClient", "CHANNEL_MAP", "normalize_channel"]
