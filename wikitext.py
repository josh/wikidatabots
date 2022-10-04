def link(title: str, url: str) -> str:
    return f"[{url} {title}]"


def item(qid: str) -> str:
    return "{{Q|" + qid.replace("Q", "") + "}}"


def statement(statement: str) -> str:
    statement = statement.replace("$", "-")
    qid, guid = statement.split("-", 1)

    return (
        item(qid)
        + " "
        + link(
            guid,
            f"http://www.wikidata.org/entity/statement/{statement}",
        )
    )
