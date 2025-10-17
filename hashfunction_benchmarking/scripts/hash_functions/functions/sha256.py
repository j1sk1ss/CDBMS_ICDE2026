import hashlib
from functions.hash_function import HashFunction

class SHA256(HashFunction):
    def __init__(self) -> None:
        super().__init__()

    def hash(self, data: str):
        return hashlib.sha256(data.encode('utf-8')).hexdigest()