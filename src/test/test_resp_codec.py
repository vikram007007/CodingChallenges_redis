import unittest
import asyncio
from src.main.resp_codec import RESPCodec, SimpleString


def _get_reader_with_data(data:bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class TestRESPCodec(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    # Encoding Tests
    def test_simple_string_encode(self):
        result = RESPCodec.encode(SimpleString("OK"))
        self.assertEqual(result, b"+OK\r\n")

    def test_bulk_string_encode(self):
        result = RESPCodec.encode("Hello")
        self.assertEqual(result, b"$5\r\nHello\r\n")

    def test_integer_encode(self):
        result = RESPCodec.encode(123)
        self.assertEqual(result, b":123\r\n")

    def test_null_encode(self):
        result = RESPCodec.encode(None)
        self.assertEqual(result, b"$-1\r\n")

    def test_array_encode(self):
        result = RESPCodec.encode(["Hello", 100, SimpleString("OK")])
        expected = b"*3\r\n$5\r\nHello\r\n:100\r\n+OK\r\n"
        self.assertEqual(result, expected)

    # Decoding Tests
    def test_simple_string_decode(self):
        data = b"+PONG\r\n"
        reader = _get_reader_with_data(data)
        result = self.loop.run_until_complete(RESPCodec.decode(reader))
        self.assertEqual(result, "PONG")

    def test_bulk_string_decode(self):
        data = b"$5\r\nHello\r\n"
        reader = _get_reader_with_data(data)
        result = self.loop.run_until_complete(RESPCodec.decode(reader))
        self.assertEqual(result, "Hello")

    def test_integer_decode(self):
        data = b":456\r\n"
        reader = _get_reader_with_data(data)
        result = self.loop.run_until_complete(RESPCodec.decode(reader))
        self.assertEqual(result, 456)

    def test_array_decode(self):
        data = b"*3\r\n$5\r\nHello\r\n:100\r\n+OK\r\n"
        reader = _get_reader_with_data(data)
        result = self.loop.run_until_complete(RESPCodec.decode(reader))
        self.assertEqual(result, ["Hello", 100, "OK"])

    def test_error_decode(self):
        data = b"-Error message\r\n"
        reader = _get_reader_with_data(data)
        result = self.loop.run_until_complete(RESPCodec.decode(reader))
        self.assertIsInstance(result, Exception)
        self.assertEqual(str(result), "Error message")

if __name__ == '__main__':
    unittest.main()
