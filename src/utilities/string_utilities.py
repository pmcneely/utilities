def string_to_camel_case(string):
    """Return CamelCase (*not* dromedaryCase) of a string separated by spaces or underscores"""
    return "".join([s.title() for s in string.replace("_", " ").split()])


def string_to_dromedary_case(string):
    """Return dromedaryCase (*not* CasmelCase) of a string separated by spaces or underscores"""
    string_list = [s.title() for s in string.replace("_", " ").split()]
    string_list[0] = string_list[0].lower()
    return "".join(string_list)
