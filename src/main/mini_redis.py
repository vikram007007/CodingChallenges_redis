import asyncio
from typing import Any
from resp_codec import RESPCodec, SimpleString

class MiniRedisServer:

    def __init__(self, host: str = "0.0.0.0", port: int = 6379) -> None:
        self.host = host
        self.port = port
        self.store: dict[str, str] = {}

    async def handle_client(self, reader : asyncio.StreamReader, writer : asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        print(f"Accepted connection from {addr}")

        try:
            while True:
                try:
                    command = await RESPCodec.decode(reader)
                except asyncio.IncompleteReadError:
                    break
                except Exception as e:
                    writer.write(RESPCodec.encode(Exception(f"Err {e}")))
                    await writer.drain()
                    continue

                if command is None:
                    break

                if not isinstance(command, list) or len(command) == 0:
                    writer.write(RESPCodec.encode(Exception("ERR invalid command format")))
                    await writer.drain()
                    continue

                cmd = command[0].upper() if isinstance(command[0], str) else ""
                print(f"Received command : {command}")

                if cmd == "PING":
                    response : Any = SimpleString("PONG")

                elif cmd == "CONFIG":
                    if len(command) >= 2 and command[1].upper() == "GET":
                        response = ["maxclients", "10000", "maxmemory", "0"]
                    else:
                        response = Exception("ERR unknown CONFIG subcommand")

                elif cmd == "SET":
                    if len(command) < 3:
                        response = Exception("ERR wrong number of arguments for 'SET'")
                    else:
                        key = command[1]
                        value = command[2]
                        self.store[key] = value
                        response = SimpleString("OK")

                elif cmd == "GET":
                    if len(command) < 2:
                        response = Exception("ERR wrong number of arguments for 'GET'")
                    else:
                        key = command[1]
                        response = self.store[key]

                elif cmd == "DEL":
                    if len(command) < 2:
                        response = Exception("ERR wrong number of arguments for 'DEL'")
                    else:
                        key = command[1]
                        if key in self.store:
                            del self.store[key]
                            response = 1
                        else:
                            response = 0

                elif cmd == "EXIT":
                    response = SimpleString("Bye")
                    writer.write(RESPCodec.encode(response))
                    await writer.drain()
                    break

                else:
                    response = Exception(f"ERR Unknown command '{cmd}'")

                writer.write(RESPCodec.encode(response))
                await writer.drain()

        except Exception as e:
            print(f"Error handling client {addr} : {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            print(f"Connection closed from {addr}")

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        print(f"Mini redis server listening on {addrs}")

        async with server:
            await server.serve_forever()

    @classmethod
    def run(cls, host: str = "0.0.0.0", port: int = 6379) -> None:
        instance = cls(host, port)
        asyncio.run(instance.start())

if __name__ == "__main__":
    MiniRedisServer.run()