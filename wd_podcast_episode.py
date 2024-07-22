import feedparser  # type: ignore
import polars as pl
from polars._typing import PolarsDataType

from polars_utils import print_rdf_statements
from sparql import sparql

_EPISODES_QUERY = """
SELECT ?item ?episode_number ?title ?pubdate ?duration ?appleEpisodeId ?overcastEpisodeId WHERE {
  ?item wdt:P31 wd:Q61855877; 
        p:P179 [ ps:P179 ?PODCAST_ITEM; pq:P1545 ?episode_number ]
  OPTIONAL { ?item wdt:P1476 ?title. }
  OPTIONAL { ?item wdt:P577 ?pubdate. }
  OPTIONAL { ?item wdt:P2047 ?duration. }
  OPTIONAL { ?item wdt:P10304 ?appleEpisodeId. }
  OPTIONAL { ?item wdt:P12872 ?overcastEpisodeId. }
}
"""

_EPISODES_QUERY_SCHEMA: dict[str, PolarsDataType] = {
    "item": pl.Utf8,
    "episode_number": pl.UInt32,
    "title": pl.Utf8,
    "pubdate": pl.Datetime,
    "duration": pl.Utf8,
    "appleEpisodeId": pl.Utf8,
    "overcastEpisodeId": pl.Utf8,
}


def _wd_episodes(podcast_item: str) -> pl.LazyFrame:
    query = _EPISODES_QUERY.replace("?PODCAST_ITEM", podcast_item)
    return sparql(query, schema=_EPISODES_QUERY_SCHEMA).with_columns(
        pubdate=pl.col("pubdate").dt.date()
    )


_FEED_PARSER_SCHEMA: dict[str, PolarsDataType] = {
    "title": pl.Utf8,
    "link": pl.Utf8,
    "id": pl.Utf8,
    "author": pl.Utf8,
    "links": pl.List(
        pl.Struct(
            {"rel": pl.Utf8, "type": pl.Utf8, "href": pl.Utf8, "length": pl.UInt32}
        )
    ),
    "media_content": pl.List(
        pl.Struct(
            {
                "url": pl.Utf8,
                "type": pl.Utf8,
                "medium": pl.Utf8,
                "duration": pl.UInt32,
                "lang": pl.Utf8,
            }
        )
    ),
    "published": pl.Utf8,
    "itunes_duration": pl.Utf8,
    "podcast_episode": pl.UInt32,
    "summary": pl.Utf8,
}


def _fetch_feed(url: str) -> pl.LazyFrame:
    def _fetch_feed_sync(df: pl.DataFrame) -> pl.DataFrame:
        d = feedparser.parse(url)
        return pl.DataFrame(d.entries, schema=_FEED_PARSER_SCHEMA)

    return pl.LazyFrame({"url": [url]}).map_batches(
        _fetch_feed_sync, schema=_FEED_PARSER_SCHEMA
    )


_FILMCAST_RSS_FEED = "https://audioboom.com/channels/4997224.rss"
_FILMCAST_PODCAST_ITEM = "wd:Q106378059"


def _filmcast_wd_items() -> pl.LazyFrame:
    return _wd_episodes(podcast_item=_FILMCAST_PODCAST_ITEM)


def _filmcast_feed() -> pl.LazyFrame:
    return _fetch_feed(url=_FILMCAST_RSS_FEED).select(
        episode_number=pl.col("title").str.extract(r"Ep. (\d+)").cast(pl.UInt32),
        title=pl.col("title"),
        link=pl.col("link"),
        pubdate=(
            pl.col("published")
            .str.to_datetime("%a, %d %b %Y %H:%M:%S %z")
            .dt.convert_time_zone(time_zone="America/Los_Angeles")
            .dt.date()
        ),
        duration=pl.col("itunes_duration").cast(pl.UInt32),
        media_url=pl.col("media_content").list.first().struct.field("url"),
    )


def _filmcast_feed_statements() -> pl.LazyFrame:
    return (
        _filmcast_wd_items()
        .join(_filmcast_feed(), on="episode_number", suffix="_feed")
        .select(_RDF_STATEMENT)
    )


_RDF_STATEMENT = pl.format(
    '<{}> wdt:P577 "{}" . ',
    pl.col("item"),
    pl.col("pubdate_feed"),
).alias("rdf_statement")


def _main() -> None:
    pl.enable_string_cache()

    pl.concat(
        [
            _filmcast_feed_statements(),
        ]
    ).pipe(print_rdf_statements)


if __name__ == "__main__":
    _main()
