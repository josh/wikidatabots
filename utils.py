def uniq(*lists):
    seen = []
    for lst in lists:
        for el in lst:
            if el not in seen:
                yield el
                seen.append(el)
