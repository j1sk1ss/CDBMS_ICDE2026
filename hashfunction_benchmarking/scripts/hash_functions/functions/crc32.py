import zlib
from functions.hash_function import HashFunction

class CRC32(HashFunction):
    def __init__(self) -> None:
        super().__init__()

    def hash(self, data: str):
        return zlib.crc32(data.encode('utf-8')) & 0xffffffff