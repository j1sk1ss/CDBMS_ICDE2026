from Crypto.Hash import RIPEMD160
from functions.hash_function import HashFunction

class Ripemd160(HashFunction):
    def __init__(self) -> None:
        super().__init__()

    def hash(self, data: str):
        h = RIPEMD160.new()
        h.update(data.encode('utf-8'))
        return h.hexdigest()