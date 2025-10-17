import hashlib
from functions.hash_function import HashFunction

class MD5(HashFunction):
    def __init__(self) -> None:
        super().__init__()

    def hash(self, data: str):
        return hashlib.md5(data.encode('utf-8')).hexdigest()