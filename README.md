# IT305: Socket Programming Project — Fault-Tolerant Topic-Based File Distribution Service

**Course:** IT305 — Computer Networks Lab  
**Merged Project Group:** PG6  
- **Lab Group 1 (Group 61):** Prince Patel (202401151), Vishwa Prajapati (202401163), Srujan S (202401185)  
- **Lab Group 2 (Group 85):** Bhavi Rana (202401176), Aarushi shah (202401193), Patel Tirth Pradipkumar (202401157)  

---

## 1. Project Overview

This project implements a high-throughput, fault-tolerant, topic-based file distribution service in C using POSIX TCP network sockets. The system distributes multi-file image datasets corresponding to discrete topics requested by remote clients across two interconnected phases:

1. **Part I: Basic Topic-Based File Server**
   - Single-Threaded Sequential Server and Multi-Threaded Concurrent Server.
   - Dynamic directory scanning without hardcoded paths or topics.
   - ASCII control protocol with strict byte-stream primitives avoiding TCP buffer bleed.

2. **Part II: Fault-Tolerant File Transfer with Active Fault Injection**
   - **Case 1 (Stateless Restart):** Server probabilistically injects abrupt TCP resets via `SO_LINGER`. The client wipes temporary staging files and restarts the entire transfer from File 0, Byte 0.
   - **Case 2 (Stateful Checkpoint & Resume):** Fixed binary header framing (`ProtocolHeader`), server-side in-memory `ActiveSession` registry, and client-side persistent journal (`.<topic>.journal`). On reconnection, the client requests resumption from its exact byte offset, and the server seeks via `lseek()`, retransmitting zero redundant bytes for completed files.
   - **Case 2 Enhanced (High-Performance Parallel TCP Architecture):** Employs $N = 4$ simultaneous TCP connections per client with a thread-safe FIFO `TaskQueue` (`pthread_mutex_t` and `pthread_cond_t`). Each worker independently streams and writes assigned files directly to disk with zero lock contention and resilient per-connection fault recovery.

---

## 2. Directory Hierarchy

```
FinalSubmission_Group_061_085/
├── README.md                           # Build, run, and architecture documentation
├── FinalReport_Group_061_085.pdf       # Consolidated submission report
│
├── Part1/                              # Basic Topic-Based Server & Client
│   ├── Makefile                        # Produces 'server' and 'client'
│   ├── server.c                        # Single & Multi-threaded server implementation
│   ├── client.c                        # Standard Part 1 client
│   ├── common_net.c                    # send_exact, recv_exact, recv_line, socket helpers
│   ├── common_net.h
│   ├── topic_manager.c                 # Dynamic opendir/readdir traversal & rwlock catalog
│   ├── topic_manager.h
│   └── test_part1.sh                   # Verification script
│
├── Part2_Case1/                        # Stateless Fault Recovery
│   ├── Makefile                        # Produces 'server' and 'client'
│   ├── server.c                        # Multi-threaded server with active fault injector
│   ├── client.c                        # Stateless staging client with bounded retry
│   ├── common_net.c
│   ├── common_net.h
│   ├── topic_manager.c
│   ├── topic_manager.h
│   ├── fault_injector.c                # Bernoulli trial & SO_LINGER TCP RST logic
│   ├── fault_injector.h
│   └── test_case1.sh
│
├── Part2_Case2/                        # Stateful Checkpoint & Resume
│   ├── Makefile                        # Produces 'server' and 'client'
│   ├── server.c                        # Stateful session server with lseek() resume
│   ├── client.c                        # Persistent checkpoint journal client
│   ├── protocol.h                      # Packed binary ProtocolHeader (0x434E534F)
│   ├── session_manager.c               # Thread-safe ActiveSession table & eviction
│   ├── session_manager.h
│   ├── common_net.c
│   ├── common_net.h
│   ├── topic_manager.c
│   ├── topic_manager.h
│   ├── fault_injector.c
│   ├── fault_injector.h
│   └── test_case2.sh
│
├── Part2_Case2_Enhanced/               # Parallel TCP Multi-Stream Architecture
│   ├── Makefile                        # Produces 'server' and 'client'
│   ├── server.c                        # Multi-worker parallel connection server
│   ├── client.c                        # Master client coordinating parallel workers
│   ├── task_queue.c                    # Thread-safe FIFO task distribution queue
│   ├── task_queue.h
│   ├── protocol.h
│   ├── session_manager.c
│   ├── session_manager.h
│   ├── common_net.c
│   ├── common_net.h
│   ├── topic_manager.c
│   ├── topic_manager.h
│   ├── fault_injector.c
│   ├── fault_injector.h
│   └── test_case2_enhanced.sh
│
└── Results/                            # Empirical Performance Logs & Graphs
    ├── Part1_throughput.csv
    ├── Part1_throughput.png
    ├── Part2_Case1_overhead.csv
    ├── Part2_Case1_overhead.png
    ├── Part2_Case2_overhead.csv
    ├── Part2_Case2_overhead.png
    ├── Part2_Case2_Enhanced_throughput.csv
    ├── Part2_Case2_Enhanced_throughput.png
    ├── benchmark_summary.csv
    └── generate_results.py
```

---

## 3. Standard CLI Interface (Automated Grading Compliant)

All programs strictly adhere to the command-line interfaces required by the assignment specification without interactive prompts or hardcoded network parameters:

### Part 1:
- **Server:**
  ```bash
  ./server <port> <topics_root_dir> [mode]
  ```
  - `<port>`: TCP listening port (e.g. `8080`).
  - `<topics_root_dir>`: Path to directory containing topic subfolders.
  - `[mode]` *(optional)*: `multi` (default) or `single`. When invoked with 2 arguments by automated test harnesses, it automatically defaults to `multi`.
- **Client:**
  ```bash
  ./client <server_ip> <server_port> <topic> <output_dir>
  ```
  - Fetches `<topic>` and writes received files under `<output_dir>/<topic>/`.

### Part 2 (All Cases):
- **Server:**
  ```bash
  ./server <port> <topics_root_dir> <failure_probability>
  ```
  - `<failure_probability>`: Float $P \in [0.0, 1.0]$ specifying the probability of injected stream disconnection.
- **Client (Case 1 & Case 2):**
  ```bash
  ./client <server_ip> <server_port> <topic> <output_dir>
  ```
- **Client (Case 2 Enhanced):**
  ```bash
  ./client <server_ip> <server_port> <topic> <output_dir> [num_connections]
  ```
  - `[num_connections]` *(optional)*: Number of parallel TCP streams (defaults to `4`).

---

## 4. Build Instructions

Every case directory contains an independent, self-contained `Makefile` compiled with strict flags:
```bash
CC = gcc
CFLAGS = -Wall -Wextra -Werror -O2 -pthread
LDFLAGS = -pthread
```

To compile each part:
```bash
# Part 1
cd Part1 && make clean && make

# Part 2 Case 1
cd ../Part2_Case1 && make clean && make

# Part 2 Case 2
cd ../Part2_Case2 && make clean && make

# Part 2 Case 2 Enhanced
cd ../Part2_Case2_Enhanced && make clean && make
```
Running `make` produces exactly two executable binaries in each folder: `server` and `client`.

---

## 5. Execution & Verification Examples

### Running Part 1:
```bash
# Terminal 1: Start Server (Multi-threaded mode by default)
./Part1/server 18080 ./test_topics multi

# Terminal 2: Run Client
./Part1/client 127.0.0.1 18080 test_animal ./output_dir
```

### Running Part 2 Case 1 (Stateless Restart):
```bash
# Terminal 1: Start Server with 30% failure rate
./Part2_Case1/server 18081 ./test_topics 0.30

# Terminal 2: Run Client
./Part2_Case1/client 127.0.0.1 18081 test_animal ./output_dir
```

### Running Part 2 Case 2 (Stateful Checkpoint & Resume):
```bash
# Terminal 1: Start Server with 50% failure rate
./Part2_Case2/server 18082 ./test_topics 0.50

# Terminal 2: Run Client
./Part2_Case2/client 127.0.0.1 18082 test_animal ./output_dir
```

### Running Part 2 Case 2 Enhanced (Parallel TCP Connections):
```bash
# Terminal 1: Start Server with 30% failure rate
./Part2_Case2_Enhanced/server 18083 ./test_topics 0.30

# Terminal 2: Run Client with 4 parallel streams
./Part2_Case2_Enhanced/client 127.0.0.1 18083 test_animal ./output_dir 4
```

### Automated Verification Scripts:
Each folder includes an automated test script (`test_part1.sh`, `test_case1.sh`, `test_case2.sh`, `test_case2_enhanced.sh`) that builds the binaries, executes the transfer under active fault injection, and performs a recursive `diff -r` bit-for-bit integrity comparison against the source dataset.

---

## 6. Fault Injection Mechanism Documentation

To realistically emulate physical network severances, cable drops, and sudden server hardware crashes, the fault injector operates as follows:

1. **Statistical Decision Model**:
   - Before beginning transmission of each file (or resumed byte slice), the server evaluates a Bernoulli trial with probability $P = \text{failure\_probability}$:
     $$\text{FailFile} = \left(\frac{\text{rand\_r}(\&seed)}{\text{RAND\_MAX}} \le P\right)$$
   - If triggered, the server selects a uniformly distributed failure byte offset:
     $$B_{fault} \in [0, \text{filesize})$$
   - **Edge condition:** If $\text{filesize} < 1024$ bytes, $B_{fault}$ is clamped to $\text{filesize}/2$ to guarantee that a partial chunk is transmitted over the wire before the break occurs.

2. **Abrupt Disconnection via `SO_LINGER` (TCP RST)**:
   - To avoid a polite TCP 4-way FIN handshake, the server configures `SO_LINGER` with a zero-second timeout:
     ```c
     struct linger sl;
     sl.l_onoff = 1;
     sl.l_linger = 0;
     setsockopt(sock_fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
     close(sock_fd);
     ```
   - This immediately issues a `TCP RST` packet to the client, abruptly resetting the connection at the kernel level.

3. **Livelock Prevention & Forward Progress**:
   - In **Case 1**, the client bounds retries with an exponential backoff sleep (`usleep`) up to 30 attempts.
   - In **Case 2** and **Case 2 Enhanced**, every successful chunk advances the persisted checkpoint byte offset. Even under heavy failure rates ($P = 0.50$), progress is strictly monotonic until the entire topic is transferred.

---

## 7. Performance Measurement & Results Summary

Raw performance benchmark data and high-resolution graphs are available in `Results/`:

- **Part 1 Throughput Scaling (`Part1_throughput.png`)**:
  - In single-threaded sequential mode, throughput plateaus at $\sim 102 \text{ Mbps}$ as concurrent clients experience Head-of-Line (HoL) blocking in the OS kernel accept backlog ($128$).
  - In multi-threaded mode, dedicated worker threads scale throughput up to $\sim 341 \text{ Mbps}$, utilizing available link bandwidth.

- **Part 2 Retransmission Overhead (`Part2_Case1_overhead.png` vs `Part2_Case2_overhead.png`)**:
  - Under stateless Case 1, increasing failure probability causes exponential retransmission volume ($> 1,100\%$ protocol overhead at $P = 0.40$) because the client re-transmits the topic from byte zero on every severed connection.
  - In stateful Case 2, byte-level checkpointing and `lseek()` resumption keep retransmission overhead bounded below $11\%$ even at $P = 0.40$, saving hundreds of megabytes of redundant network traffic.

- **Part 2 Case 2 Enhanced Parallel Scaling (`Part2_Case2_Enhanced_throughput.png`)**:
  - Scaling parallel connections from $N = 1$ to $N = 4$ increases effective throughput from $55.19 \text{ Mbps}$ to $143.62 \text{ Mbps}$ ($2.60\times$ speedup). Beyond $N = 8$, link and disk saturation plateau throughput at $\sim 180 \text{ Mbps}$.
