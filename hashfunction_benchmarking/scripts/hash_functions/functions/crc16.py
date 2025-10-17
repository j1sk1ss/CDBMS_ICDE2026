import crcmod
from functions.hash_function import HashFunction

class CRC16(HashFunction):
    def __init__(self) -> None:
        super().__init__()
        self.crc16 = crcmod.mkCrcFun(0x11021, initCrc=0xFFFF, rev=False, xorOut=0x0000)

    def hash(self, data: str):
        return (self.crc16(data.encode('utf-8'))) & 0xffffffff