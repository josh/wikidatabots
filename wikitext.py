import urllib


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


def quickstatements_url(commands):
    hash = urllib.parse.urlencode({"v1": "||".join(["|".join(c) for c in commands])})
    return "https://quickstatements.toolforge.org/#{}".format(hash)


def statements_section(heading, statements):
    statements = list(statements)
    lines = ["== " + heading + " =="]

    for (entity, property, value) in statements:
        lines.append(
            "* {{Statement|" + entity + "|" + property + "|" + str(value) + "}}"
        )

    if statements:
        lines.append("")
        lines.append(link("Add via QuickStatements", quickstatements_url(statements)))

    lines.append("")
    return "\n".join(lines)
