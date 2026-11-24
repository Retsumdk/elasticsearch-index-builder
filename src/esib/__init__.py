"""elasticsearch-index-builder: infer a mapping, load it, swap the alias, verify.

The library is deliberately dependency-free: it speaks the Elasticsearch HTTP
API over ``urllib``, so a build machine needs nothing but Python.
"""

from __future__ import annotations

from .bulk import BulkIndexer, BulkOutcome, encode_bulk
from .client import Client, ClientStats
from .config import Config
from .errors import (
    AuthenticationError,
    AuthorizationError,
    BulkError,
    BulkFailure,
    ConfigError,
    ConflictError,
    EmptyLoadError,
    ESError,
    NotFoundError,
    PaginationError,
    PayloadTooLarge,
    RateLimited,
    ServerError,
    SourceError,
    TransportError,
    ValidationError,
)
from .indices import BuildReport, IndexManager
from .mapping import InferredMapping, default_settings, infer_mappings
from .retry import Retrier, RetryPolicy
from .search import Page, count, scan, search_after_scan
from .sources import coerce_value, detect_format, iter_documents, iter_paths
from .testing import FakeCluster, Fault
from .transport import CannedResponse, Request, Response, StubTransport, Transport, UrllibTransport

__version__ = "1.0.0"

__all__ = [
    "AuthenticationError",
    "AuthorizationError",
    "BuildReport",
    "BulkError",
    "BulkFailure",
    "BulkIndexer",
    "BulkOutcome",
    "CannedResponse",
    "Client",
    "ClientStats",
    "Config",
    "ConfigError",
    "ConflictError",
    "ESError",
    "EmptyLoadError",
    "FakeCluster",
    "Fault",
    "IndexManager",
    "InferredMapping",
    "NotFoundError",
    "Page",
    "PaginationError",
    "PayloadTooLarge",
    "RateLimited",
    "Request",
    "Response",
    "Retrier",
    "RetryPolicy",
    "ServerError",
    "SourceError",
    "StubTransport",
    "Transport",
    "TransportError",
    "UrllibTransport",
    "ValidationError",
    "__version__",
    "coerce_value",
    "count",
    "default_settings",
    "detect_format",
    "encode_bulk",
    "infer_mappings",
    "iter_documents",
    "iter_paths",
    "scan",
    "search_after_scan",
]
