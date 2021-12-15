from datetime import date, datetime

def string_to_camel_case(string: str):
    """Return CamelCase (*not* dromedaryCase) of a string separated by spaces or underscores"""
    return "".join([s.title() for s in string.replace("_", " ").split()])


def string_to_dromedary_case(string: str):
    """Return dromedaryCase (*not* CasmelCase) of a string separated by spaces or underscores"""
    string_list = [s.title() for s in string.replace("_", " ").split()]
    string_list[0] = string_list[0].lower()
    return "".join(string_list)

class TimeConverter:

    def __init__(self, fmt=None):
        self.fmt = '%Y-%m-%d %H:%M:%S' if fmt is None else fmt

    def epoch_to_human(self, timestamp: int):
        """Return human-formatted time from epoch time"""
        try:
            return datetime.fromtimestamp(timestamp).strftime(self.fmt)
        except:
            raise

    def human_to_epoch(self, date_str: str, fmt: str = None):
        try:
            if fmt is None:
                return datetime.strptime(date_str, self.fmt).timestamp()
            return datetime.strptime(date_str, fmt).timestamp()
        except:
            raise