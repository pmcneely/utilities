
class AtomicBool():

    def __init__(self, initial_bool=False):
        self.value = initial_bool

    def set_value(self, new_value):
        self.value = new_value
