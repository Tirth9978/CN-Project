#!/usr/bin/env bash
set -e

PORT=18081
ROOT_DIR="../../test_topics"
OUT_DIR="../../test_client_out"
TOPIC="test_animal"

echo "=== Building Part 1 ==="
make clean
make all

echo ""
echo "=== Testing Multi-Threaded Server ==="
./server $PORT "$ROOT_DIR" multi > server.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "--- Running Client for topic: $TOPIC ---"
./client 127.0.0.1 $PORT "$TOPIC" "$OUT_DIR"

echo "Stopping server (PID: $SERVER_PID)..."
kill -INT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=== Server Output Log ==="
cat server.log

echo ""
echo "=== Verifying File Integrity (Diff / Checksum) ==="
diff -r "$ROOT_DIR/$TOPIC" "$OUT_DIR/$TOPIC"
if [ $? -eq 0 ]; then
    echo " SUCCESS: All transferred files match original files bit-for-bit!"
else
    echo " FAILURE: File differences detected!"
    exit 1
fi

echo ""
echo "=== Part 1 Test Completed Successfully ==="
