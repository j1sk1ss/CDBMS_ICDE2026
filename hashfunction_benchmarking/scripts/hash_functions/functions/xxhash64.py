import xxhash
from functions.hash_function import HashFunction

class xxHash64(HashFunction):
    def __init__(self) -> None:
        super().__init__()

    def hash(self, data: str):
        return xxhash.xxh64(data.encode('utf-8')).intdigest()