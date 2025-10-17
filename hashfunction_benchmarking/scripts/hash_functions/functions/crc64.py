import crcmod
from functions.hash_function import HashFunction

class CRC64(HashFunction):
    def __init__(self) -> None:
        super().__init__()
        self.crc64 = crcmod.mkCrcFun(0x142F0E1EBA9EA3693, initCrc=0, rev=False, xorOut=0)

    def hash(self, data: str):
        return self.crc64(data.encode("utf-8"))