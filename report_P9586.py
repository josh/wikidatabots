from tqdm import tqdm

import appletv
import sparql
import wikitext
from report_utils import duplicate_values, sample_qids
from utils import uniq

P6398_URL_FORMATTER = "https://itunes.apple.com/us/movie/id{}"
P9586_URL_FORMATTER = "https://tv.apple.com/us/movie/{}"


def main():
    qids = sample_qids("P9586", count=200)
    results = sparql.fetch_statements(qids, ["P6398", "P9586"])

    not_found = []
    itunes_statements = []
    itunes_diff = []

    for qid in tqdm(results):
        item = results[qid]

        appletv_movie = None
        for (statement, value) in item.get("P9586", []):
            appletv_movie = appletv.movie(value)
            if not appletv_movie:
                not_found.append((statement, value))

        itunes_ids = set()
        for (statement, value) in item.get("P6398", []):
            itunes_ids.add(int(value))

        if not appletv_movie:
            continue

        if len(itunes_ids) == 0 and appletv_movie["itunes_id"]:
            itunes_statements.append(
                (qid, "P6398", '"{}"'.format(appletv_movie["itunes_id"]))
            )
        elif (
            len(itunes_ids) > 0
            and appletv_movie["itunes_id"]
            and appletv_movie["itunes_id"] not in itunes_ids
        ):
            itunes_diff.append((qid, list(itunes_ids)[0], appletv_movie["itunes_id"]))

    not_found.sort()
    itunes_statements.sort()
    itunes_diff.sort()

    print("== Not found ==")
    for (statement, id) in uniq(not_found):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(id, P9586_URL_FORMATTER)
        )
    print("")

    print(
        wikitext.statements_section("iTunes Store suggestions", uniq(itunes_statements))
    )

    print("== iTunes Store differences ==")
    for (qid, actual_itunes_id, expected_itunes_id) in uniq(itunes_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wikitext.external_id(actual_itunes_id, P6398_URL_FORMATTER)
            + " vs "
            + wikitext.external_id(expected_itunes_id, P6398_URL_FORMATTER)
        )
    print("")

    print("== Unique value violations ==")
    for (id, statement, rank) in duplicate_values("P9586"):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(id, P9586_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
