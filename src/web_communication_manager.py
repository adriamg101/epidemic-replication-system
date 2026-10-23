import asyncio
import json
import re
import threading
from pathlib import Path

import websockets

from utils import Utils

_NAME_TO_SERVER_ID = {
    "A1": "l0s0", "A2": "l0s1", "A3": "l0s2",
    "B1": "l1s0", "B2": "l1s1",
    "C1": "l2s0", "C2": "l2s1",
}

FILES_DIR = Path(__file__).resolve().parent.parent / "files"


class WebCommunicationManager:
    def __init__(self, name: str, port: int, utils: Utils, config_path: str):
        self.name = name
        self.server_id = _NAME_TO_SERVER_ID.get(name, name)
        self.port = port
        self.config_path = config_path
        self.clients = set()
        self.data_items = {}
        self.loop = asyncio.new_event_loop()
        self.utils = utils
        self._server = None
        self._on_read  = None
        self._on_write = None
        FILES_DIR.mkdir(parents=True, exist_ok=True)
        threading.Thread(target=self._run_loop, daemon=True).start()

    def register_handlers(self, on_read, on_write):
        self._on_read  = on_read
        self._on_write = on_write

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _handler(self, websocket):
        self.clients.add(websocket)
        self.utils.comment(f"Web client connected. ({len(self.clients)} active)")
        try:
            for key, item in self.data_items.items():
                await websocket.send(json.dumps({
                    "type": "DATA",
                    "serverId": self.server_id,
                    "key": key,
                    "version": item["version"],
                    "value": item["value"],
                }))

            async for raw in websocket:
                await self._handle_incoming(raw, websocket)

        except websockets.exceptions.ConnectionClosedError:
            pass
        finally:
            self.clients.discard(websocket)
            self.utils.comment(f"Web client disconnected. ({len(self.clients)} active)")

    async def _handle_incoming(self, raw: str, websocket):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        t = msg.get("type")
        if t == "CMD_READ":
            await self._handle_cmd_read(msg, websocket)
        elif t == "CMD_WRITE":
            await self._handle_cmd_write(msg, websocket)

    async def _handle_cmd_read(self, msg: dict, websocket):
        key = msg.get("key")
        if key is None or self._on_read is None:
            return
        self.utils.comment(f"[WEB] READ  key={key}")
        try:
            value = await self.loop.run_in_executor(None, self._on_read, key)
            self.utils.comment(f"[WEB] READ  key={key} → {value}")
            await websocket.send(json.dumps({
                "type": "CMD_RESP", "key": key, "value": value,
            }))
        except KeyError:
            self.utils.comment(f"[WEB] READ  key={key} → (not set)")
            await websocket.send(json.dumps({
                "type": "CMD_UNKNOWN", "key": key,
            }))
        except Exception as e:
            self.utils.comment(f"[WEB] CMD_READ error: {e}")

    async def _handle_cmd_write(self, msg: dict, websocket):
        key   = msg.get("key")
        value = msg.get("value")
        if key is None or value is None or self._on_write is None:
            return
        self.utils.comment(f"[WEB] WRITE key={key} value={value}")
        try:
            await self.loop.run_in_executor(None, self._on_write, key, value)
            self.utils.comment(f"[WEB] WRITE key={key} value={value} → done")
            await websocket.send(json.dumps({
                "type": "CMD_WRITE_ACK", "key": key, "value": value,
            }))
        except Exception as e:
            self.utils.comment(f"[WEB] CMD_WRITE error: {e}")

    def start(self):
        asyncio.run_coroutine_threadsafe(self._start_server(), self.loop)

    async def _start_server(self):
        self._server = await websockets.serve(
            self._handler, "0.0.0.0", self.port,
        )
        self.utils.comment(f"WebSocket server listening on port {self.port}.")

    def stop(self):
        if self._server:
            self._server.close()
            asyncio.run_coroutine_threadsafe(self._server.wait_closed(), self.loop)
        self.loop.call_soon_threadsafe(self.loop.stop)

    def send_data(self, key: int, version: int, value: int):
        if key in self.data_items and version <= self.data_items[key]["version"]:
            return
        self.data_items[key] = {"version": version, "value": value}
        msg = json.dumps({
            "type": "DATA",
            "serverId": self.server_id,
            "key": key,
            "version": version,
            "value": value,
        })
        asyncio.run_coroutine_threadsafe(self._broadcast(msg), self.loop)
        self.utils.comment(f"Sending [Key:{key}, Value:{value}, Version:{version}] to web.")

    async def _broadcast(self, msg: str):
        if self.clients:
            await asyncio.gather(
                *(client.send(msg) for client in self.clients),
                return_exceptions=True,
            )
