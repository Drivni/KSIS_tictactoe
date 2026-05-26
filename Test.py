import asyncio
import json
import random
import time
import uuid
import websockets

WS_URL = "ws://localhost:8000/ws"

TOTAL_PAIRS = 25

successful_connections = 0
failed_connections = 0
completed_games = 0


class Client:
    def __init__(self, ws, symbol):
        self.ws = ws
        self.symbol = symbol
        self.state = None
        self.running = True

    async def listener(self):
        while self.running:
            try:
                msg = json.loads(await self.ws.recv())

                if msg.get("type") == "state":
                    self.state = msg

            except Exception:
                break


async def connect_player(action="create", room_id=None):
    ws = await websockets.connect(WS_URL)

    payload = {
        "type": "join",
        "action": action,
        "player_id": str(uuid.uuid4())[:8]
    }

    if room_id:
        payload["room_id"] = room_id

    await ws.send(json.dumps(payload))

    joined = json.loads(await ws.recv())

    return ws, joined


async def play_game(game_id):
    global successful_connections
    global failed_connections
    global completed_games

    try:
        # Первый игрок
        ws1, p1 = await connect_player("create")
        room_id = p1["room_id"]

        # Второй игрок
        ws2, p2 = await connect_player("join", room_id)

        successful_connections += 2

        c1 = Client(ws1, p1["symbol"])
        c2 = Client(ws2, p2["symbol"])

        # запускаем listeners
        t1 = asyncio.create_task(c1.listener())
        t2 = asyncio.create_task(c2.listener())

        players = {
            c1.symbol: c1,
            c2.symbol: c2
        }

        # ждём initial state
        while c1.state is None or c2.state is None:
            await asyncio.sleep(0.01)

        while True:
            state = c1.state

            if state["status"] == "finished":
                completed_games += 1
                break

            turn = state["turn"]

            current_player = players[turn]

            board = state["board"]

            empty = [i for i, v in enumerate(board) if v == ""]

            if not empty:
                break

            move = random.choice(empty)

            await current_player.ws.send(json.dumps({
                "type": "move",
                "cell": move
            }))

            await asyncio.sleep(0.05)

        c1.running = False
        c2.running = False

        t1.cancel()
        t2.cancel()

        await ws1.close()
        await ws2.close()

        print(f"[GAME {game_id}] finished")

    except Exception as e:
        failed_connections += 1
        print(f"[GAME {game_id}] ERROR: {e}")


async def main():
    start = time.perf_counter()

    tasks = [
        asyncio.create_task(play_game(i + 1))
        for i in range(TOTAL_PAIRS)
    ]

    await asyncio.gather(*tasks)

    end = time.perf_counter()

    print("\n========== RESULT ==========")
    print(f"Pairs: {TOTAL_PAIRS}")
    print(f"Clients: {TOTAL_PAIRS * 2}")
    print(f"Successful connections: {successful_connections}")
    print(f"Failed: {failed_connections}")
    print(f"Completed games: {completed_games}")
    print(f"Time: {end - start:.2f}s")
    print("============================")


if __name__ == "__main__":
    asyncio.run(main())