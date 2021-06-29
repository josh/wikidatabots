import wikitext
from report_utils import duplicate_values

P6398_URL_FORMATTER = "https://itunes.apple.com/us/movie/id{}"


def main():
    print("== Unique value violations ==")
    for (id, statement, rank) in duplicate_values("P6398"):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(id, P6398_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
