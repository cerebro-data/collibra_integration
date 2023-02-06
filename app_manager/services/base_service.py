from abc import ABC, abstractmethod


class BaseService(ABC):
    def __init__(self, input_object):
        self.input_object = input_object

        self.result_object = {}

    @abstractmethod
    def prepare(self):
        pass
    
    @abstractmethod
    def process(self):
        pass

    @abstractmethod
    def persist(self):
        pass

    def execute(self):
        self.prepare()
        self.process()
        self.persist()
        return self.result_object
