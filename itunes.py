import requests


def batch_lookup(ids):
    """
    Look up many iTunes tracks by IDs.
    """

    session = requests.Session()

    for ids_batch in batches(ids, size=150):
        ids_str = ",".join(map(str, ids_batch))
        r = session.get("https://itunes.apple.com/lookup", params={"id": ids_str})
        r.raise_for_status()
        json = r.json()
        results = {}

        for result in json["results"]:
            type = result["wrapperType"]
            id = result.get(type + "Id") or result["trackId"]
            results[id] = result

        for id in ids_batch:
            yield (id, results.get(id))


def batches(iterable, size):
    batch = []

    for element in iterable:
        batch.append(element)
        if len(batch) == size:
            yield batch
            batch = []

    if batch:
        yield batch
