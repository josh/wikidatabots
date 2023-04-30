# pyright: strict, reportUnknownMemberType=false, reportUnknownVariableType=false

import logging
import math
import os
import platform
import time
from threading import Lock

import polars as pl
import rdflib
from rdflib import URIRef

from actions import warn
from polars_requests import Session, prepare_request, request
from polars_utils import apply_with_tqdm

_LOCK = Lock()
_WIKIDATA_SPARQL_SESSION = Session(
    read_timeout=90,
    retry_statuses={500},
    retry_count=10,
    retry_allowed_methods=["GET", "POST"],
    retry_raise_on_status=True,
    retry_backoff_factor=1.0,
)


_USER_AGENT_PARTS: list[str] = []

if "WIKIDATA_USERNAME" in os.environ:
    _USER_AGENT_PARTS.append(
        "{username}/1.0 (User:{username})".format(
            username=os.environ["WIKIDATA_USERNAME"]
        )
    )
else:
    warn("WIKIDATA_USERNAME unset")

_USER_AGENT_PARTS.append(f"Python/{platform.python_version()}")
_USER_AGENT_STR = " ".join(_USER_AGENT_PARTS)


class SlowQueryWarning(Warning):
    pass


class SPARQLQueryError(Exception):
    pass


def _sparql_csv(query: str, _stacklevel: int = 0) -> bytes:
    with _LOCK:
        start = time.time()
        http = _WIKIDATA_SPARQL_SESSION.poolmanager()
        r = http.request(
            "POST",
            "https://query.wikidata.org/sparql",
            fields={"query": query},
            headers={"Accept": "text/csv", "User-Agent": _USER_AGENT_STR},
            encode_multipart=False,
        )
        duration = time.time() - start

    if r.status != 200:
        raise SPARQLQueryError(f"Query errored with status {r.status}:\n{query}")

    if duration > 45:
        logging.warn(f"sparql: {duration:,.2f}s")
        warn(query, SlowQueryWarning, stacklevel=2 + _stacklevel)
    elif duration > 5:
        logging.info(f"sparql: {duration:,.2f}s")
    else:
        logging.debug(f"sparql: {duration:,.2f}s")

    return r.data


def sparql_df(
    query: str,
    columns: list[str] | None = None,
    schema: dict[str, pl.PolarsDataType] | None = None,
) -> pl.LazyFrame:
    if columns and not schema:
        schema = {column: pl.Utf8 for column in columns}
    assert schema, "missing schema"

    def sparql_df_inner(df: pl.DataFrame) -> pl.DataFrame:
        return pl.read_csv(_sparql_csv(query, _stacklevel=2), dtypes=schema)

    return pl.LazyFrame().map(sparql_df_inner, schema=schema)


_HYDRA = rdflib.Namespace("http://www.w3.org/ns/hydra/core#")
_WD = rdflib.Namespace("http://www.wikidata.org/")
_RDF = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

_WIKIDATA_LDF_SESSION = Session()


def xxx_fetch_property_statements(pid: str) -> pl.LazyFrame:
    return fetch_property_statements(pid)


def fetch_property_statements(pid: str) -> pl.LazyFrame:
    predicate = f"http://www.wikidata.org/prop/statement/{pid}"

    page_count_expr = (
        prepare_request(
            "https://query.wikidata.org/bigdata/ldf",
            fields={"predicate": predicate},
            headers={"Accept": "application/n-triples"},
        )
        .pipe(request, session=_WIKIDATA_LDF_SESSION, log_group="wikidata_query")
        .struct.field("data")
        .pipe(
            apply_with_tqdm,
            _parse_page_count,
            return_dtype=pl.Int64,
            log_group="parse_rdf_triples",
        )
        .cast(pl.UInt32)
    )

    page_expr = pl.arange(1, page_count_expr + 1, dtype=pl.UInt32)
    assert isinstance(page_expr, pl.Expr)

    return (
        pl.LazyFrame()
        .select(
            prepare_request(
                pl.lit("https://query.wikidata.org/bigdata/ldf"),
                fields={"predicate": predicate, "page": page_expr},
                headers={"Accept": "application/n-triples"},
            )
            .pipe(
                request,
                session=_WIKIDATA_LDF_SESSION,
                log_group="wikidata_query",
            )
            .struct.field("data")
            .pipe(
                apply_with_tqdm,
                _parse_page,
                return_dtype=pl.List(
                    pl.Struct(
                        [pl.Field("subject", pl.Utf8), pl.Field("object", pl.Utf8)]
                    )
                ),
                log_group="parse_rdf_triples",
            )
            .explode()
            .alias("triple")
        )
        .unnest("triple")
    )


def _parse_page_count(data: bytes) -> int:
    graph = rdflib.Graph()
    graph.parse(data=data)

    page_collection_uri = next(
        graph.subjects(
            predicate=_RDF.type,
            object=_HYDRA.PagedCollection,
        )
    )
    assert isinstance(page_collection_uri, URIRef)

    total_items_obj = graph.value(
        subject=page_collection_uri,
        predicate=_HYDRA.totalItems,
    )
    assert isinstance(total_items_obj, rdflib.Literal)
    total_items = total_items_obj.value
    assert isinstance(total_items, int)

    items_per_page_obj = graph.value(
        subject=page_collection_uri,
        predicate=_HYDRA.itemsPerPage,
    )
    assert isinstance(items_per_page_obj, rdflib.Literal)
    items_per_page = items_per_page_obj.value
    assert isinstance(items_per_page, int)

    return math.ceil(total_items / items_per_page)


def _parse_page(data: bytes) -> list[dict[str, str]]:
    graph = rdflib.Graph()
    graph.parse(data=data)
    results: list[dict[str, str]] = []
    for subject, predicate, object in graph:
        if isinstance(predicate, str) and predicate.startswith(_WD):
            results.append({"subject": str(subject), "object": str(object)})
    return results
