from multiprocessing import Queue

class Scoot_Meta(type):
    class_counter = 0
    def __init__(cls, name, bases, dct):
        type.__init__(name, bases, dct)
        #cls.queue = Queue
        cls._order = Scoot_Meta.class_counter
        Scoot_Meta.class_counter += 1
