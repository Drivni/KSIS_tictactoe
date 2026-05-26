from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass, field
from typing import Literal

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

PlayerSymbol = Literal["X", "O"]
GameStatus = Literal["waiting", "playing", "finished"]


def check_winner(board: list[str]) -> PlayerSymbol | None:
    wins = [
        (0, 1, 2),
        (3, 4, 5),
        (6, 7, 8),
        (0, 3, 6),
        (1, 4, 7),
        (2, 5, 8),
        (0, 4, 8),
        (2, 4, 6),
    ]
    for a, b, c in wins:
        if board[a] != "" and board[a] == board[b] == board[c]:
            return board[a]  # type: ignore[return-value]
    return None


@dataclass
class Player:
    player_id: str
    symbol: PlayerSymbol
    ws: WebSocket


@dataclass
class Room:
    room_id: str
    players: dict[PlayerSymbol, Player] = field(default_factory=dict)
    board: list[str] = field(default_factory=lambda: [""] * 9)
    turn: PlayerSymbol = "X"
    status: GameStatus = "waiting"
    winner: PlayerSymbol | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def snapshot(self) -> dict:
        return {
            "type": "state",
            "room_id": self.room_id,
            "status": self.status,
            "board": self.board,
            "turn": self.turn,
            "winner": self.winner,
            "players": {
                "X": self.players["X"].player_id if "X" in self.players else None,
                "O": self.players["O"].player_id if "O" in self.players else None,
            },
        }


class RoomManager:
    def __init__(self) -> None:
        self._rooms: dict[str, Room] = {}
        self._global_lock = asyncio.Lock()

    async def create_room(self) -> Room:
        async with self._global_lock:
            while True:
                rid = secrets.token_urlsafe(6)
                if rid not in self._rooms:
                    room = Room(room_id=rid)
                    self._rooms[rid] = room
                    return room

    async def get_room(self, room_id: str) -> Room | None:
        async with self._global_lock:
            return self._rooms.get(room_id)

    async def delete_room_if_empty(self, room_id: str) -> None:
        async with self._global_lock:
            room = self._rooms.get(room_id)
            if room is not None and not room.players:
                del self._rooms[room_id]


rooms = RoomManager()

app = FastAPI()

from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html_file_path = os.path.join(BASE_DIR, "static", "index.html")
    with open(html_file_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


async def safe_send(ws: WebSocket, message: dict) -> None:
    try:
        await ws.send_json(message)
    except Exception:
        # Connection likely gone; ignore, cleanup happens elsewhere.
        pass


async def broadcast(room: Room, message: dict) -> None:
    await asyncio.gather(
        *[safe_send(p.ws, message) for p in room.players.values()],
        return_exceptions=True,
    )


def assign_symbol(room: Room) -> PlayerSymbol | None:
    if "X" not in room.players:
        return "X"
    if "O" not in room.players:
        return "O"
    return None


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()

    room: Room | None = None
    symbol: PlayerSymbol | None = None
    player_id: str | None = None

    try:
        first = await ws.receive_json()
        if not isinstance(first, dict) or first.get("type") != "join":
            await ws.send_json({"type": "error", "message": "First message must be join"})
            await ws.close()
            return

        desired_room = first.get("room_id")
        action = first.get("action")
        player_id = str(first.get("player_id") or secrets.token_hex(4))

        if action == "create":
            room = await rooms.create_room()
        elif action == "join" and isinstance(desired_room, str):
            room = await rooms.get_room(desired_room)
            if room is None:
                await ws.send_json({"type": "error", "message": "Room not found"})
                await ws.close()
                return
        else:
            await ws.send_json({"type": "error", "message": "Invalid join payload"})
            await ws.close()
            return

        async with room.lock:
            symbol = assign_symbol(room)
            if symbol is None:
                await ws.send_json({"type": "error", "message": "Room is full"})
                await ws.close()
                return

            room.players[symbol] = Player(player_id=player_id, symbol=symbol, ws=ws)
            if len(room.players) == 2 and room.status == "waiting":
                room.status = "playing"
                room.turn = "X"

        await ws.send_json(
            {
                "type": "joined",
                "room_id": room.room_id,
                "player_id": player_id,
                "symbol": symbol,
            }
        )
        await broadcast(room, room.snapshot())

        while True:
            msg = await ws.receive_json()
            if not isinstance(msg, dict):
                continue

            if msg.get("type") == "move":
                if room is None or symbol is None:
                    continue

                cell = msg.get("cell")
                if not isinstance(cell, int) or cell < 0 or cell > 8:
                    await safe_send(ws, {"type": "error", "message": "Invalid cell"})
                    continue

                async with room.lock:
                    if room.status != "playing":
                        await safe_send(ws, {"type": "error", "message": "Game not in playing state"})
                        continue
                    if room.turn != symbol:
                        await safe_send(ws, {"type": "error", "message": "Not your turn"})
                        continue
                    if room.board[cell] != "":
                        await safe_send(ws, {"type": "error", "message": "Cell already taken"})
                        continue

                    room.board[cell] = symbol
                    w = check_winner(room.board)
                    if w is not None:
                        room.status = "finished"
                        room.winner = w
                    elif all(v != "" for v in room.board):
                        room.status = "finished"
                        room.winner = None
                    else:
                        room.turn = "O" if room.turn == "X" else "X"

                await broadcast(room, room.snapshot())

            elif msg.get("type") == "ping":
                await safe_send(ws, {"type": "pong"})

            else:
                await safe_send(ws, {"type": "error", "message": "Unknown message type"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await ws.send_json({"type": "error", "message": f"Server error: {e}"})
        except Exception:
            pass
    finally:
        if room is not None and symbol is not None:
            async with room.lock:
                # remove only if still same connection
                p = room.players.get(symbol)
                if p is not None and p.ws is ws:
                    del room.players[symbol]
                # reset game if someone leaves mid-game
                if room.status == "playing":
                    room.status = "waiting"
                    room.board = [""] * 9
                    room.turn = "X"
                    room.winner = None
            await broadcast(room, room.snapshot())
            await rooms.delete_room_if_empty(room.room_id)
