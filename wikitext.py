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


def external_id(id, formatter):
    return link(id, formatter.format(id))


def external_ids(ids, formatter):
    ids = list(ids)
    ids.sort()
    return ", ".join([external_id(id, formatter) for id in ids])
