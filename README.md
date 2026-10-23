# Multilayer Epidemic Replication System

A distributed key-value store in Python with eventual consistency. Seven nodes are split into three layers. Nodes talk over a custom binary TCP protocol, the core layer uses Ricart-Agrawala for distributed mutual exclusion, and a web dashboard shows every node's state live over WebSockets.

## Highlights

- Three-layer replication: writes enter at the core and propagate upward to L1 and L2.
- Ricart-Agrawala mutual exclusion guards every write in the core layer.
- Main-backup replication in L1 and L2, with a different flush policy per layer.
- Custom binary TCP protocol between nodes.
- Per-key versioning, so overwrites are visible across layers.
- Vue 3 dashboard with a built-in command panel, fed by one WebSocket per node.
- Topology defined in a single JSON file, so the number of nodes per layer can change without touching code.

## Architecture

```
┌──────────────────────────────────────────┐
│  Layer 2 (L2)  C1 (Main) → C2 (Backup)   │  receives sync from L1 every 10 s
└──────────────────────↑───────────────────┘
                       │ DATA every 10 s
┌──────────────────────┼───────────────────┐
│  Layer 1 (L1)  B1 (Main) → B2 (Backup)   │  replicates to backups on receipt
└──────────────────────↑───────────────────┘
                       │ DATA after 10 or more writes
┌──────────────────────┼───────────────────┐
│  Layer 0 (Core)  A1 ↔ A2 ↔ A3            │  Ricart-Agrawala mutex
└──────────────────────────────────────────┘
```

| Node | Layer | Role   | TCP port | WS port |
|------|-------|--------|----------|---------|
| A1   | core  | Core   | 50001    | 60001   |
| A2   | core  | Core   | 50002    | 60002   |
| A3   | core  | Core   | 50003    | 60003   |
| B1   | l1    | Main   | 50004    | 60004   |
| B2   | l1    | Backup | 50005    | 60005   |
| C1   | l2    | Main   | 50006    | 60006   |
| C2   | l2    | Backup | 50007    | 60007   |

### Replication flow

- **Core (L0):** a write first enters the critical section through Ricart-Agrawala, then the new value is broadcast to the other core nodes. Once the pending, not yet propagated items reach `ITEMS_TO_UPDATE` (10), the core primary flushes them up to L1.
- **L1:** the main node (B1) takes DATA from the core and copies it to its backups straight away. Every `SEC_TO_UPDATE` (10 s) it flushes pending items up to L2.
- **L2:** the main node (C1) takes DATA from L1 and copies it to its backups straight away.
- **Client writes** sent to L1 or L2 are forwarded down to the core, which handles them like any other write.

## Requirements

- Python 3.10+
- The `websockets` library: `pip install websockets`
- Free ports `50001-50007` (node TCP), `60001-60007` (WebSocket) and `8080` (dashboard HTTP server)

## Quick start

```bash
chmod +x run_all.sh
./run_all.sh
```

The script starts all seven nodes in the background, serves the dashboard and config file at `http://localhost:8080`, and opens `http://localhost:8080/web/` in your browser. Press `Ctrl+C` to stop. It sends SIGINT to every process, waits 3 s for a clean shutdown and frees the ports.

### Manual startup

To read each node's logs separately, run one node per terminal:

```bash
cd src
python3 server.py -n A1 -v    # repeat for A2, A3, B1, B2, C1, C2

# In another terminal, from the project root:
python3 -m http.server 8080
```

The `-v` flag prints to the console. Without it, a node only writes to `logs/<name>_log.txt`. Nodes retry their connections every 0.5 s, so start order does not matter.

## Running transactions

### Web dashboard

With the system running, open `http://localhost:8080/web/`. The command panel at the bottom accepts the same syntax as the test files. Type or paste commands and press **Execute**.

### CLI client

`client.py` replays a transaction file from the terminal:

```bash
cd src
python3 client.py -f ../test/test1.txt -c ../config/nodes.json
```

`-c` defaults to `nodes.json` in the current directory. The bundled tests are:

| File        | What it checks                                              |
|-------------|-------------------------------------------------------------|
| `test1.txt` | Basic writes of keys 1-9 to Layer 0                         |
| `test2.txt` | Persistence: reads earlier keys and adds keys 10-12         |
| `test3.txt` | Versioning: overwrites keys 1, 5 and 9 (version 0 to 1)     |
| `test4.txt` | Combined load: writes and reads across all layers           |

### Transaction syntax

```
b           # connect to a random node in Layer 0
b0          # same, explicit
b1          # connect to a random node in Layer 1
b2          # connect to a random node in Layer 2
r(k)        # read key k
w(k,v)      # write value v to key k
c           # close the session
```

Example:

```
b, w(1,10), r(1), c
b1, r(1), c
b2, r(1), c
```

## Configuration

`config/nodes.json` is the single source of truth for node names, hosts and ports:

```json
{
  "core": [
    { "name": "A1", "host": "127.0.0.1", "port": 50001 },
    { "name": "A2", "host": "127.0.0.1", "port": 50002 },
    { "name": "A3", "host": "127.0.0.1", "port": 50003 }
  ],
  "l1": [
    { "name": "B1", "host": "127.0.0.1", "port": 50004 },
    { "name": "B2", "host": "127.0.0.1", "port": 50005 }
  ],
  "l2": [
    { "name": "C1", "host": "127.0.0.1", "port": 50006 },
    { "name": "C2", "host": "127.0.0.1", "port": 50007 }
  ]
}
```

**Multiple machines.** Change `host` and `port` for each node, for example `"host": "192.168.1.10"` for A1. Every machine needs the same `nodes.json`, its TCP ports open in the firewall, and WebSocket ports (TCP port + 10000) reachable from the browser.

**Number of nodes.** The core layer needs at least one node. L1 and L2 also need at least one, and the first entry in each list is the main node. Any extra entries act as additional backups.

## Web dashboard

The dashboard shows each of the seven nodes grouped by layer, with every key, its value and its version. It also shows a WebSocket connection indicator per node, and it reconnects on its own when a node goes down or restarts.

## Logs

Each node writes to `logs/<name>_log.txt`, truncated on every startup:

```
[DD/MM/YYYY HH:MM:SS.ffffff] version=<v> key=<k> value=<val>
```

To compare all nodes at a glance:

```bash
for node in A1 A2 A3 B1 B2 C1 C2; do
  echo "=== $node ===" && tail -3 logs/${node}_log.txt
done
```

## Project structure

```
├── config/
│   └── nodes.json                    # node configuration
├── logs/                             # generated at runtime
├── src/
│   ├── server.py                     # entry point for each node
│   ├── client.py                     # CLI transaction client
│   ├── config_manager.py             # nodes.json loader and validator
│   ├── core_process_manager.py       # Layer 0: Ricart-Agrawala and upward sync
│   ├── non_core_process_manager.py   # L1 and L2: main-backup replication
│   ├── db_manager.py                 # in-memory database and logging
│   ├── protocol.py                   # binary TCP protocol
│   ├── server_socket_manager.py      # server-side sockets
│   ├── client_socket_manager.py      # client-side sockets
│   ├── web_communication_manager.py  # asyncio WebSocket server
│   ├── ricart_agrawala_clock.py      # distributed mutual exclusion
│   ├── read_write_lock.py            # FIFO reader-writer lock
│   └── utils.py                      # shared logging helpers
├── test/                             # transaction files test1-test4
├── web/
│   ├── index.html                    # Vue 3 dashboard
│   ├── app.js                        # dashboard logic
│   └── styles.css
└── run_all.sh                        # starts the whole system
```
