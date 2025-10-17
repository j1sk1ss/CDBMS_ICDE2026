import mmh3
from functions.hash_function import HashFunction

class murmurHash3(HashFunction):
    def __init__(self) -> None:
        super().__init__()

    def hash(self, data: str):
        return mmh3.hash(data)