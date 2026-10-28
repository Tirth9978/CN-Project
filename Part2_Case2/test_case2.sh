#!/usr/bin/env bash
set -e

PORT=18084
ROOT_DIR="../../test_topics"
OUT_DIR="../../test_client_out_case2"
TOPIC="test_animal"
FAILURE_PROB=0.6

echo "=== Building Part 2 Case 2 (Stateful Checkpoint & Resume) ==="
make clean
make all

echo ""
echo "=== Starting Server with Failure Probability $FAILURE_PROB ==="
./server $PORT "$ROOT_DIR" $FAILURE_PROB > server_case2.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "--- Running Client for topic: $TOPIC ---"
./client 127.0.0.1 $PORT "$TOPIC" "$OUT_DIR"

echo "Stopping server (PID: $SERVER_PID)..."
kill -INT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=== Server Output Log ==="
cat server_case2.log

echo ""
echo "=== Verifying File Integrity (Diff) ==="
diff -r "$ROOT_DIR/$TOPIC" "$OUT_DIR/$TOPIC"
if [ $? -eq 0 ]; then
    echo " SUCCESS: All files resumed and transferred bit-for-bit identical!"
else
    echo " FAILURE: File differences detected!"
    exit 1
fi

echo ""
echo "=== Part 2 Case 2 Test Completed Successfully ==="
