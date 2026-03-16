# Task 3: High Performance Transactional Database (Prototype)

This folder contains a minimal **transactional key/value store** prototype that demonstrates the core design patterns of a **high-performance transactional database**, including:

- **MVCC (Multi-Version Concurrency Control)** snapshots
- **Write-Ahead Logging (WAL)** for durability and recovery
- **Conflict detection** for serializability

---

## ✅ What’s implemented

### ✅ Transactional operations
- `begin()` / `commit()` / `rollback()`
- Snapshot reads (transactions see a consistent snapshot of committed data)
- Serializable conflict detection on commit (write-write conflicts)

### ✅ Durability
- Every commit is appended to a WAL (`wal.log`)
- On startup, the engine replays the WAL to recover committed state

### ✅ Evoked Database Design Concepts
- MVCC version chains per key
- Commit timestamps for snapshot isolation
- Grouping of writes into a log record for efficient sequential persistence

---

## 🧪 Running the tests

This uses **pytest**. Install it via:

```bash
pip install -U pytest
```

Run tests from the `task_3` directory:

```bash
cd task_3
pytest -q
```

---

## 🔧 Key files

- `src/txn_db.py` – core MVCC + WAL engine
- `tests/test_txn_db.py` – unit tests validating isolation, conflicts, and recovery

---

## 💡 How to extend

If you want to experiment further, try adding:

- A **checkpoint mechanism** to truncate the WAL
- A **B+tree-backed index** instead of an in-memory dict
- **Concurrency optimizations** (lock-free structures, partitioned storage, etc.)
