class Borg:

    __shared_state = {}
    value = -1

    def __init__(self):
        self.__dict__ = self.__shared_state

    def next_value(self):
        self.value += 1
        return self.value
