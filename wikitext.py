def link(title, url):
    return "[{url} {title}]".format(url=url, title=title)


def item(qid):
    return "{{Q|" + qid.replace("Q", "") + "}}"


def statement(statement):
    statement = statement.replace("$", "-")
    qid, guid = statement.split("-", 1)

    return (
        item(qid)
        + " "
        + link(
            guid,
            "http://www.wikidata.org/entity/statement/{}".format(statement),
        )
    )
