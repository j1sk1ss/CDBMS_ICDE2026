import hashlib
from functions.hash_function import HashFunction

class Blake2B(HashFunction):
    def __init__(self) -> None:
        super().__init__()

    def hash(self, data: str):
        return hashlib.blake2b(data.encode('utf-8')).hexdigest()