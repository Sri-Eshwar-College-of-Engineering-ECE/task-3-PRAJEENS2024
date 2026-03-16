"""Simple high-performance transactional key/value store (MVCC + WAL).

This module provides a minimal in-memory transactional database engine optimized for
throughput and correctness with:

- MVCC (Multi-Version Concurrency Control) snapshots
- Transaction write-set isolation + conflict validation
- Write-Ahead Log (WAL) for durability and recovery

The goal is to demonstrate design and implementation patterns used by high-performance
transactional databases.

Usage:
    from txn_db import TransactionalKVStore

    store = TransactionalKVStore(wal_path="./wal.log")
    txn = store.begin()
    txn.set("x", "42")
    txn.commit()

    txn2 = store.begin()
    print(txn2.get("x"))  # "42"

"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class _Version:
    value: Any
    created_ts: int
    deleted: bool = False


@dataclass
class _TxWrite:
    key: str
    value: Any
    delete: bool = False


class TransactionError(Exception):
    pass


class TransactionalKVStore:
    """A minimal MVCC key/value store with WAL durability."""

    def __init__(self, wal_path: str = "wal.log"):
        self._lock = threading.RLock()
        self._store: Dict[str, List[_Version]] = {}
        self._next_timestamp = 1
        self._active_txns: Dict[int, "Transaction"] = {}
        self._wal_path = wal_path
        self._ensure_wal_dir()
        self._recover()

    def _ensure_wal_dir(self) -> None:
        wal_dir = os.path.dirname(os.path.abspath(self._wal_path))
        if wal_dir and not os.path.exists(wal_dir):
            os.makedirs(wal_dir, exist_ok=True)

    def _allocate_ts(self) -> int:
        with self._lock:
            ts = self._next_timestamp
            self._next_timestamp += 1
            return ts

    def begin(self) -> "Transaction":
        """Start and return a new transaction."""
        start_ts = self._allocate_ts()
        txn = Transaction(self, start_ts)
        with self._lock:
            self._active_txns[start_ts] = txn
        return txn

    def _commit(self, txn: "Transaction") -> None:
        """Apply transaction write set and record to WAL."""
        if txn._committed or txn._rolled_back:
            raise TransactionError("Transaction already completed")

        commit_ts = self._allocate_ts()
        self._write_wal_record(commit_ts, txn._writes)

        with self._lock:
            # Validate write-write conflicts: ensure no newer committed version exists
            for w in txn._writes.values():
                versions = self._store.get(w.key)
                if versions and versions[-1].created_ts > txn.start_ts:
                    raise TransactionError(
                        f"Write conflict detected for key '{w.key}'"
                    )

            # Apply writes
            for w in txn._writes.values():
                version = _Version(value=w.value, created_ts=commit_ts, deleted=w.delete)
                self._store.setdefault(w.key, []).append(version)

            txn._committed = True
            txn._commit_ts = commit_ts
            del self._active_txns[txn.start_ts]

    def _rollback(self, txn: "Transaction") -> None:
        if txn._committed or txn._rolled_back:
            raise TransactionError("Transaction already completed")
        txn._rolled_back = True
        with self._lock:
            del self._active_txns[txn.start_ts]

    def _read_snapshot(self, key: str, snapshot_ts: int) -> Optional[Any]:
        """Return the value of `key` as of snapshot timestamp (inclusive)."""
        versions = self._store.get(key)
        if not versions:
            return None
        # Versions are appended by increasing created_ts
        for v in reversed(versions):
            if v.created_ts <= snapshot_ts:
                return None if v.deleted else v.value
        return None

    def _append_wal(self, record: Dict[str, Any]) -> None:
        with open(self._wal_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _write_wal_record(self, commit_ts: int, writes: Dict[str, _TxWrite]) -> None:
        record = {
            "commit_ts": commit_ts,
            "writes": [
                {"key": w.key, "value": w.value, "delete": w.delete} for w in writes.values()
            ],
        }
        self._append_wal(record)

    def _recover(self) -> None:
        """Recover store state from WAL file."""
        if not os.path.exists(self._wal_path):
            return

        with open(self._wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                commit_ts = int(record.get("commit_ts", 0))
                if commit_ts <= 0:
                    continue

                self._next_timestamp = max(self._next_timestamp, commit_ts + 1)
                for entry in record.get("writes", []):
                    key = entry["key"]
                    value = entry.get("value")
                    deleted = bool(entry.get("delete", False))
                    version = _Version(value=value, created_ts=commit_ts, deleted=deleted)
                    self._store.setdefault(key, []).append(version)

    def snapshot(self) -> Dict[str, Any]:
        """Return a shallow snapshot of the current committed state."""
        with self._lock:
            return {k: self._read_snapshot(k, self._next_timestamp - 1) for k in self._store.keys()}


class Transaction:
    """A transaction that supports MVCC snapshot reads and isolated writes."""

    def __init__(self, store: TransactionalKVStore, start_ts: int):
        self._store = store
        self.start_ts = start_ts
        self._writes: Dict[str, _TxWrite] = {}
        self._committed = False
        self._rolled_back = False
        self._commit_ts: Optional[int] = None

    def get(self, key: str) -> Optional[Any]:
        """Read a key using the transaction snapshot."""
        if self._committed or self._rolled_back:
            raise TransactionError("Cannot read after transaction completion")

        if key in self._writes:
            w = self._writes[key]
            return None if w.delete else w.value

        return self._store._read_snapshot(key, self.start_ts)

    def set(self, key: str, value: Any) -> None:
        """Write a key/value pair within the transaction."""
        if self._committed or self._rolled_back:
            raise TransactionError("Cannot write after transaction completion")
        self._writes[key] = _TxWrite(key=key, value=value, delete=False)

    def delete(self, key: str) -> None:
        """Delete a key within the transaction."""
        if self._committed or self._rolled_back:
            raise TransactionError("Cannot delete after transaction completion")
        self._writes[key] = _TxWrite(key=key, value=None, delete=True)

    def commit(self) -> None:
        """Commit the transaction."""
        self._store._commit(self)

    def rollback(self) -> None:
        """Abort the transaction."""
        self._store._rollback(self)

    @property
    def committed(self) -> bool:
        return self._committed

    @property
    def rolled_back(self) -> bool:
        return self._rolled_back

    @property
    def commit_ts(self) -> Optional[int]:
        return self._commit_ts
