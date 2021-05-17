def link(title, url):
    return "[{url} {title}]".format(url=url, title=title)


def item(qid):
    return "{{Q|" + qid.replace("Q", "") + "}}"


def statement(statement):
    statement = statement.replace("$", "-")
    qid = statement.split("-", 2)[0]

    return (
        item(qid)
        + " "
        + link(
            "wds:{}".format(statement),
            "http://www.wikidata.org/entity/statement/{}".format(statement),
        )
    )
