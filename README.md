# MDB

**MDB** is a lightweight key-value store written in Rust. It provides persistent storage using **SSTables** (Sorted String Tables) and **Write-Ahead Logging (WAL)**. MDB supports basic operations like setting, getting, and deleting keys, with tombstone support for soft deletes, and background flushing/compaction.

MDB now also supports a **TCP server mode**, allowing clients to connect and issue commands over the network.

## Features

- Key-Value storage
- Persistent storage using SSTables
- Write-Ahead Logging (WAL) for crash recovery
- Tombstone support for deletes
- Background flushing and compaction
- TCP server for remote client access

## Usage

Run the server:

```bash
cargo run
```

Connect with a TCP client (e.g., netcat):
```bash
nc 127.0.0.1 4000
```

Issue Commands
```
SET mykey myvalue
GET mykey
DELETE mykey
GET_KEYS
```

<div align="center">

```text
          +-------------------+
          |     TCP Clients   |
          +-------------------+
                    |
                    v
           +----------------+
           |   TCP Server   |
           +----------------+
                    |
                    v
          +-------------------+
          |       DB Core     |
          |------------------|
          |  BTreeMap Cache   |
          |   handle_set/get  |
          +-------------------+
                    |
       ----------------------------
       |                          |
       v                          v
+---------------+          +----------------+
|  Write-Ahead  |          |  Storage Engine|
|      Log      |          |   (SSTables)  |
+---------------+          +----------------+
       |                          |
       v                          v
Background Flusher -------> Periodic Compaction
       |
       v
Deletes old WAL files
```


