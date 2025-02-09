import asyncio
import re
import sys
import os
import json
from typing import Any
from resp_codec import RESPCodec, SimpleString

class MiniRedisServer:

    BASE_DIR = os.path.join(os.path.dirname(__file__), "resources", "cache_loader")
    DATA_FILE = os.path.join(BASE_DIR, "kvstore.json")

    def __init__(self, host: str = "0.0.0.0", port: int = 6379) -> None:
        self.host = host
        self.port = port
        self.store: dict[str, Any] = {}
        self.ensure_directory_exists()
        self.load_data()

    def ensure_directory_exists(self):
        if not os.path.exists(self.BASE_DIR):
            os.makedirs(self.BASE_DIR, exist_ok=True)
            print(f"Directories created : {self.BASE_DIR}")

    def save_data(self):
        try:
            with open(self.DATA_FILE, "w") as f:
                json.dump(self.store, f, indent=4)
            print(f"Data saved successfully to disk at {self.DATA_FILE}")
            return SimpleString("OK")
        except Exception as e:
            print(f"ERR saving data : {e}")
            return Exception("ERR saving data to disk")

    def load_data(self):
        if os.path.exists(self.DATA_FILE):
            try:
                with open(self.DATA_FILE, "r") as f:
                    self.store = json.load(f)
                print(f"Data loaded successfully from disk at {self.DATA_FILE}")
            except Exception as e:
                print(f"ERR loading data from disk : {e}")
                self.store = {}

    def is_valid_integer(self, s) -> bool:
        return bool(re.fullmatch(r"-?\d+", s))

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
                        if self.is_valid_integer(value):
                            self.store[key] = int(value)
                        else:
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
                        deleted_keys = 0
                        for i in range(1, len(command)):
                            key = command[1]
                            if key in self.store:
                                self.store.pop(key)
                                deleted_keys += 1
                        response = deleted_keys

                elif cmd == "EXIT":
                    response = SimpleString("Bye")
                    writer.write(RESPCodec.encode(response))
                    await writer.drain()
                    break

                elif cmd == "EXISTS":
                    if len(command) < 2:
                        response = Exception("ERR wrong number of arguments for 'EXISTS'")
                    else:
                        present_keys = 0
                        for i in range(1, len(command)):
                            key = command[i]
                            if key in self.store:
                                present_keys += 1
                        response = present_keys

                elif cmd == "INCR":
                    if len(command) < 2:
                        response = Exception("ERR wrong number of arguments for 'INCR'")
                    else:
                        key = command[1]
                        success_flag = True
                        if key in self.store:
                            if isinstance(self.store[key], int) and (self.store[key] + 1) != sys.maxsize:
                                self.store[key] += 1
                            else:
                                print(f"ERR value type is not an integer or out of range : {self.store[key]} {type(self.store[key])}")
                                response = Exception("ERR value type is not an integer or out of range")
                                success_flag = False
                        else:
                            self.store[key] = 1

                        if success_flag:
                            response = self.store[key]

                elif cmd == "DECR":
                    if len(command) < 2:
                        response = Exception("ERR wrong number of arguments for 'DECR'")
                    else:
                        key = command[1]
                        success_flag = True
                        if key in self.store:
                            if isinstance(self.store[key], int) and (self.store[key] - 1 != (-sys.maxsize - 1)):
                                self.store[key] -= 1
                            else:
                                print(f"ERR value type is not an integer or out of range : {self.store[key]} {type(self.store[key])}")
                                response = Exception("ERR value type is not an integer or out of range")
                                success_flag = False
                        else:
                            self.store[key] = -1

                        if success_flag:
                            response = self.store[key]

                elif cmd == "LPUSH":
                    if len(command) < 3:
                        response = Exception("ERR wrong number of arguments for 'LPUSH'")
                    else:
                        success_flag = True
                        key = command[1]
                        head_list = []
                        if key in self.store:
                            if isinstance(self.store[key], list):
                                head_list = list(self.store[key])
                            else:
                                response = Exception("WRONGTYPE Operation against a key holding the wrong kind of value")
                                success_flag = False
                        if success_flag:
                            for i in range(2, len(command)):
                                head_list.insert(0, command[i])
                            self.store[key] = head_list
                            response = len(head_list)

                elif cmd == "RPUSH":
                    if len(command) < 3:
                        response = Exception("ERR wrong number of arguments for 'RPUSH'")
                    else:
                        success_flag = True
                        key = command[1]
                        head_list = []
                        if key in self.store:
                            if isinstance(self.store[key], list):
                                head_list = list(self.store[key])
                            else:
                                response = Exception("WRONGTYPE Operation against a key holding the wrong kind of value")
                                success_flag = False
                        if success_flag:
                            for i in range(2, len(command)):
                                head_list.append(command[i])
                            self.store[key] = head_list
                            response = len(head_list)

                elif cmd == "SAVE":
                    response = self.save_data()

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