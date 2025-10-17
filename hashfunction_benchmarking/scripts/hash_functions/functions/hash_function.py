from abc import abstractmethod

class HashFunction:
    @abstractmethod
    def hash(self, data: str):
        pass