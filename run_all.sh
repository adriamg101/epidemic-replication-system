#!/bin/bash

pids=()

PORTS=(50001 50002 50003 50004 50005 50006 50007 60001 60002 60003 60004 60005 60006 60007 8080)

cleanup() {
  # Ignorar CTRL+C durante el cleanup para que no se interrumpa
  trap '' SIGINT

  echo "Stopping servers..."

  # Graceful shutdown
  for pid in "${pids[@]}"; do
    kill -SIGINT "$pid" 2>/dev/null
  done

  # Give processes 3s to exit cleanly
  sleep 3

  # Force-kill any that are still running
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "Force-killing PID $pid..."
      kill -SIGKILL "$pid" 2>/dev/null
    fi
  done

  wait 2>/dev/null

  # Release any sockets still held on our ports
  echo "Releasing sockets..."
  for port in "${PORTS[@]}"; do
    fuser -k "${port}/tcp" 2>/dev/null
  done

  echo "Done."
  exit 0
}

trap cleanup SIGINT

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/src"

echo "Starting core layer (A1, A2, A3)..."
python3 server.py -n A1 -v &
pids+=($!)
python3 server.py -n A2 -v &
pids+=($!)
python3 server.py -n A3 -v &
pids+=($!)

echo "Starting layer 1 (B1, B2)..."
python3 server.py -n B1 -v &
pids+=($!)
python3 server.py -n B2 -v &
pids+=($!)

echo "Starting layer 2 (C1, C2)..."
python3 server.py -n C1 -v &
pids+=($!)
python3 server.py -n C2 -v &
pids+=($!)

echo "All servers started."
echo ""
echo "TCP ports (inter-node):  50001-50007"
echo "WebSocket ports (web dashboard, TCP+10000):"
echo "  A1 -> ws://localhost:60001"
echo "  A2 -> ws://localhost:60002"
echo "  A3 -> ws://localhost:60003"
echo "  B1 -> ws://localhost:60004"
echo "  B2 -> ws://localhost:60005"
echo "  C1 -> ws://localhost:60006"
echo "  C2 -> ws://localhost:60007"
echo ""

# Servidor HTTP para los ficheros web y config (necesario para fetch())
python3 -m http.server 8080 --directory "$SCRIPT_DIR" &>/dev/null &
pids+=($!)
echo "Dashboard: http://localhost:8080/web/"
echo ""

sleep 0.5
DASHBOARD="http://localhost:8080/web/"
if command -v xdg-open &>/dev/null; then
  xdg-open "$DASHBOARD"
elif command -v open &>/dev/null; then
  open "$DASHBOARD"
else
  echo "Open manually: $DASHBOARD"
fi

wait
