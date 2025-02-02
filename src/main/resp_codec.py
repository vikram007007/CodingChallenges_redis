import asyncio
from typing import Any

class SimpleString:

    def __init__(self, value: str) -> None:
        self.value = value

class RESPCodec:

    OK_RESPONSE = b"+OK\r\n"
    NULL_RESPONSE = b"$-1\r\n"

    @staticmethod
    async def decode(reader: asyncio.StreamReader) -> Any:

        """
        To deserialize a RESP message
        :param reader:
        :return:
        """

        prefix = await reader.readexactly(1)
        if prefix == b'*': #Array
            line = await reader.readline()
            try:
                count = int(line.decode().strip())
            except ValueError:
                raise ValueError("Invalid array length")
            if count == -1:
                return None
            result = []
            for _ in range(count):
                result.append(await RESPCodec.decode(reader))
            return result

        elif prefix == b'$': # Bulk String
            line = await reader.readline()
            try:
                count = int(line.decode().strip())
            except ValueError:
                raise ValueError("Invalid bulk string length")
            if count == -1:
                return None
            data = await reader.readexactly(count)
            await reader.readexactly(2)
            return data.decode()

        elif prefix == b'+': # Simple String
            line = await reader.readline()
            return line.decode().strip()

        elif prefix == b':': # Integer
            line = await reader.readline()
            return int(line.decode().strip())

        elif prefix == b'-': # Error
            line = await reader.readline()
            return Exception(line.decode().strip())

        else:
            raise ValueError(f"Unknown RESP type : {prefix}")


    @staticmethod
    def encode(value: Any) -> bytes:
        """
        Serialize python message to RESP message
        :param value:
        :return:
        """

        if isinstance(value, SimpleString):
            if value.value == "OK":
                return RESPCodec.OK_RESPONSE
            return f"+{value.value}\r\n".encode()
        elif isinstance(value, str):
            return f"${len(value)}\r\n{value}\r\n".encode()
        elif isinstance(value, int):
            return f":{value}\r\n".encode()
        elif value is None:
            return RESPCodec.NULL_RESPONSE
        elif isinstance(value, list):
            result = bytearray()
            result.extend(f"*{len(value)}\r\n".encode())
            for item in value:
                result.extend(RESPCodec.encode(item))
            return bytes(result)
        elif isinstance(value, Exception):
            return f"-{str(value)}\r\n".encode()
        else:
            s = str(value)
            return f"${len(s)}\r\n{s}\r\n".encode()